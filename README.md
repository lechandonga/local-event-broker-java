# Local Event Broker（本地事件投递与消费链路）

一个完全运行在本地、不依赖任何外部服务（无 MQ、无外部数据库）的轻量消息中间件模拟。
数据全部以文件形式落在本地磁盘，服务重启后自动恢复事件、消费进度、重试与死信状态。

基于 Spring Boot 3 / Java 21，对外提供 HTTP 接口。

## 核心模型

- **Topic（主题）**：事件按主题发布；每个主题有独立的锁与日志文件，慢主题不会阻塞其他主题。
- **Event（事件）**：由主题内单调递增的绝对偏移 `offset` 标识；可携带业务幂等键 `eventKey`。
- **Consumer Group（消费组）**：每个组独立维护消费进度。同一条事件会被每个组各消费一次；组内多个消费者竞争消费。
- **Delivery Token（投递令牌）**：每次投递生成一次性随机令牌，ACK/NACK 必须携带；重投后旧令牌立即失效。

## 本地持久化与重启恢复

数据目录由 `local.broker.data-dir` 指定，每个主题/消费组对应三类文件：

| 文件 | 说明 |
|---|---|
| `events-<topic>.log` | JSON Lines 仅追加事件日志，每次发布 `force` 落盘（崩溃不丢已发布事件） |
| `meta-<topic>.json` | 日志窗口起始偏移 `baseOffset`，用于消费确认后的前缀截断 |
| `state-<topic>-<group>.json` | 消费组整体状态：`committedOffset`、`deliveredHighWatermark`、每个未决事件的条目 |

写入语义：

- 事件发布：追加写日志并 `force`，返回后即持久化。
- 组状态变更（投递/ACK/NACK/回收）：临时文件 + fsync + 原子 rename，读到的永远是完整状态。
- 日志截断：当一个主题下**所有消费组**都连续确认过某前缀后才删除该前缀；
  截断采用“先写新文件再原子替换 + meta 记录窗口起点”，崩溃后以日志首条偏移和 meta 互相校正自愈。

重启恢复规则（见 `BrokerCore#init` 与 `recoverLeasesAfterRestart`）：

- 已 `committedOffset` 确认的事件不会重新投递。
- 重启前仍处于“持有中（LEASED）”的事件，由于持有它的进程已经消失，会立即转为可重新投递，旧令牌作废。
- 重试等待（RETRY_WAIT）、死信（DEAD）条目原样恢复；死信重启后仍可查询且不会再投递。
- 事件日志中的幂等键索引重新建立，重复发布依旧被去重。

## 投递与确认语义（至少一次 + 幂等入口）

- **发布幂等**：同一主题内相同 `eventKey` 的重复发布只产生一条事件，返回相同 `offset` 与 `duplicated=true`，消费侧不会重复消费。
- **组内互斥**：事件处于 LEASED 期间，同组其它消费者拉不到；并发拉取由主题锁串行化，保证同一事件同一时刻只被一个消费者持有。
- **显式确认**：
  - `ack` 成功后事件不再投递，并推进连续确认位点；
  - 位点不能跨越未决事件（未确认/重试中/死信都挡住位点，避免进度虚报）；
  - 旧令牌（已重投）或重复 ACK 一律返回 `accepted=false`，业务侧可用它识别“迟到处理结果”，从而避免重复副作用。
- **至少一次投递**：业务处理必须自带幂等（推荐使用 `eventKey` 作为业务幂等键），以正确应对重试/租约重投。

## 重试、退避与死信

事件在组内的三种状态：`LEASED`（持有中）、`RETRY_WAIT`（退避等待重投）、`DEAD`（死信）。

- 业务处理失败调用 `nack`，或租约超时被回收，事件进入 `RETRY_WAIT`。
- 指数退避：`等待 = min(max, base × factor^(attempts-1))`。
- 投递次数达到 `maxAttempts`（含首次）后进入死信，不再自动投递，事件内容、次数、原因、最后错误均保留可查。
- 两类失败原因可区分：
  - `BUSINESS_ERROR`：消费者显式 NACK（业务处理失败）；
  - `LEASE_TIMEOUT`：拉取后超过租约时长一直未确认（消费者宕机/卡死）。
- 整个过程中事件始终保留在日志中，不会丢失；旧令牌立即失效，不会出现两个消费者同时生效处理。

### 租约（长时间未确认）

- 拉取即获得一个有时限的租约（`local.broker.default-lease-duration`）。
- 后台 `LeaseReaper` 按 `local.broker.reaper-interval` 周期扫描；过期未确认的事件被回收并按退避重新投递，反复超时超过上限同样进入死信（原因为 `LEASE_TIMEOUT`）。

## 容量与背压

- 每个主题有容量上限（默认 `local.broker.default-capacity`，可按主题覆盖）。
- 达到容量后发布请求**快速失败**，不阻塞、不静默丢弃：HTTP 返回 `429 Too Many Requests`，错误码 `CAPACITY_EXCEEDED`，调用方应在消费释放窗口后重试。
- 所有相关消费组确认日志前缀后，前缀被截断、容量窗口释放，新事件继续使用单调递增的绝对偏移。
- 主题之间互不影响：一个主题阻塞/积压不会卡住其它主题的发布与消费。

## HTTP 接口

基础路径：`/api/broker`

| 方法与路径 | 作用 |
|---|---|
| `POST /topics/{topic}/events` | 发布事件，body：`{"eventKey":"可选幂等键","payload":"..."}`；容量满返回 429 |
| `POST /topics/{topic}/groups/{group}/pull` | 拉取事件，body：`{"maxMessages":10}`，返回投递列表（含 `deliveryToken`、`expiresAt`、`attempt`） |
| `POST /topics/{topic}/groups/{group}/ack` | 确认成功，body：`{"offset":0,"deliveryToken":"..."}` |
| `POST /topics/{topic}/groups/{group}/nack` | 声明业务失败，body：`{"offset":0,"deliveryToken":"...","error":"..."}` |
| `GET  /topics/{topic}/status` | 主题状态：总事件数、容量及各消费组进度 |
| `GET  /topics/{topic}/groups/{group}/status` | 消费组状态：`committedOffset`、`pending`、`leased`、`retryWaiting`、`dead` |
| `GET  /topics/{topic}/groups/{group}/dead-letters` | 死信列表（含 payload、attempts、reason、lastError、deadAt） |

状态口径（可对账）：`pending`（待处理积压）= 持有中 + 退避等待中 + 尚未投递；
`dead` 单列，不计入 pending；`leased` 与 `retryWaiting` 是 pending 的细分。

### 使用示例

```bash
# 发布
curl -X POST localhost:8080/api/broker/topics/orders/events \
  -H 'Content-Type: application/json' \
  -d '{"eventKey":"order-1001","payload":"{\"amount\":99}"}'

# 消费组 billing 拉取
curl -X POST localhost:8080/api/broker/topics/orders/groups/billing/pull \
  -H 'Content-Type: application/json' -d '{"maxMessages":10}'

# 确认（offset/token 来自上一步）
curl -X POST localhost:8080/api/broker/topics/orders/groups/billing/ack \
  -H 'Content-Type: application/json' -d '{"offset":0,"deliveryToken":"abcd1234"}'

# 查询状态与死信
curl localhost:8080/api/broker/topics/orders/groups/billing/status
curl localhost:8080/api/broker/topics/orders/groups/billing/dead-letters
```

## 关键配置项（前缀 `local.broker`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `data-dir` | `~/.local-event-broker/data` | 数据目录，重启据此恢复 |
| `default-capacity` | `10000` | 每个主题容量窗口，满则发布返回 429 |
| `default-max-attempts` | `5` | 最大投递尝试次数（含首次），超限进死信 |
| `default-retry-backoff-base` | `1s` | 初始退避 |
| `default-retry-backoff-factor` | `2` | 退避倍数 |
| `default-retry-backoff-max` | `5m` | 退避上限 |
| `default-lease-duration` | `30s` | 消费租约时长，超时可被重投 |
| `reaper-interval` | `2s` | 过期租约扫描周期 |
| `topics.<topic>.capacity` | - | 按主题覆盖容量 |
| `groups.<group>@<topic>.max-attempts` | - | 按“组@主题”覆盖重试次数 |
| `groups.<group>@<topic>.lease-duration` | - | 覆盖租约时长 |
| `groups.<group>@<topic>.retry-backoff-base/factor/max` | - | 覆盖退避参数 |

## 运行与测试

```bash
mvn spring-boot:run        # 启动服务（默认 8080 端口）
mvn test                   # 运行全部测试
```

测试共 14 个用例，控制台通过 `TEST-COVERAGE` 日志逐条打印用例名称和通过/失败结果，
Broker 业务日志（发布/投递/确认/重试等待/租约回收/死信）同步输出，可以直接看出每个业务的实际过程。

测试覆盖的场景：

1. 16 线程并发重复发布同一事件，仅落一条、仅消费一次（幂等）。
2. 8 个消费者并发拉取 50 条事件，同一条不会被同时持有（组内互斥）。
3. 重启恢复：已确认不重投、未确认重投、旧令牌失效、进度与幂等键恢复；死信重启后保留且不重投。
4. 业务失败指数退避重试，超过上限进入 `BUSINESS_ERROR` 死信；重试成功则无死信无重复副作用。
5. `BUSINESS_ERROR` 与 `LEASE_TIMEOUT` 两类死信原因可区分查询。
6. 租约超时后事件被回收重投，新消费者接手确认，旧消费者迟到 ACK 被拒绝；反复超时进 `LEASE_TIMEOUT` 死信。
7. 容量背压 429、消费确认释放窗口后可继续发布；慢主题不阻塞其它主题。
8. 状态数字与真实进度全程对账；两个消费组进度相互独立；HTTP 端到端全链路。

## 设计边界（模拟语义说明）

- 单进程内并发安全（主题级锁）；面向“本地模拟消息中间件核心语义”，不是多节点网络中间件。
- 投递为**至少一次**，重复投递依赖业务方用 `eventKey` 做幂等去重。
- 死信不自动重放，需要人工/外部系统通过死信查询接口处理。
