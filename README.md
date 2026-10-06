# local-event-broker-java

一个纯本地的 Spring Boot 事件投递与消费链路，模拟消息中间件的核心语义：按主题发布/消费、
消费组独立进度、发布去重、租约式投递、失败重试与死信、容量背压。所有数据落在本地文件，
不依赖任何外部服务，重启后状态完整恢复。

## 快速开始

```bash
mvn spring-boot:run
mvn test   # 运行全部用例，日志中可看到每个用例的名称、覆盖场景与结果
```

## REST 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/broker/topics/{topic}/events` | 发布事件，body `{"eventId":"...","payload":"..."}`。201=已发布，200=重复去重，429=容量已满 |
| POST | `/api/broker/topics/{topic}/groups/{group}/poll?consumerId=c1&max=10` | 消费组拉取投递，返回带租约 token 的事件列表 |
| POST | `/api/broker/topics/{topic}/groups/{group}/ack` | 确认，body `{"seq":1,"token":"..."}`；token 不匹配返回 409 |
| POST | `/api/broker/topics/{topic}/groups/{group}/nack` | 消极确认（业务失败），触发退避重试或死信 |
| GET | `/api/broker/topics/{topic}/groups/{group}/dead-letters` | 查询死信（含原因与已投递次数） |
| GET | `/api/broker/status` | 全量状态：各主题事件总量、未完结量，各组已确认/在途/积压/死信数 |

## 投递与确认语义

- **发布去重**：同一主题内 `eventId` 唯一。重复发布返回 `DUPLICATE`，不会重复入队、重复消费。
- **组内互斥**：`poll` 以租约（lease）形式把事件租给某个消费者，同一事件同一时刻只有一个
  持有者；其他消费者在租约过期前拉不到它。
- **确认**：`ack` 必须携带当前租约的 `token`。过期租约或他人 token 的确认会被拒绝（409），
  防止过期持有者对已重投的事件产生二次副作用；已确认的事件重复 `ack` 幂等返回成功。
- **未确认重投**：租约超过 `broker.lease-timeout-ms` 未确认，后台 reaper 会回收并重新投递，
  投递次数（`attempt`）累计。

## 重试与死信规则

- 每次 `nack`（业务失败）或租约超时后，事件按 `retryBackoffMs * attempt` 线性退避后重投。
- 单事件最大投递次数为 `broker.max-retries`（含首次），超过后进入死信，不再投递。
- 死信原因三选一，可通过死信接口区分：
  - `BUSINESS_FAILURE`：消费者主动 nack（业务处理失败）导致重试耗尽；
  - `ACK_TIMEOUT`：长时间未确认（租约反复过期）导致重试耗尽；
  - `MAX_RETRIES_EXCEEDED`：拉取时发现投递次数已达上限。
- 重试与死信全程状态同步落盘，事件不丢失；配合 token 校验与发布去重，不会产生重复业务副作用。

## 进度恢复方式

持久化目录由 `broker.data-dir` 指定（默认 `data/broker`）：

- `<dataDir>/<topic>/events.jsonl`：事件追加日志，发布即落盘；
- `<dataDir>/<topic>/groups/<group>.json`：消费组快照（已确认序号、投递次数、死信），
  每次状态变更原子替换写入。

重启时重放事件日志并加载各组快照：已确认的事件不再投递；在途（未确认）事件的租约不持久化，
重启后自动变为可投递，保证"未确认必重投、已确认不重投"。

## 容量与背压

- 单主题容量上限为 `broker.topic-capacity`，口径是"尚未被所有消费组处理完（确认或死信）的事件数"。
- 达到上限后发布返回 `FULL`（HTTP 429），行为明确可预期；确认/死信释放容量后即可继续发布。
- 各主题状态与锁相互独立，慢主题积满不影响其他主题的发布与消费。
- `/api/broker/status` 的数据与内存中真实进度同源（同一把锁下计算），积压量、死信量、
  消费进度与实际处理情况始终一致。

## 关键配置项

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `broker.data-dir` | `data/broker` | 本地持久化目录 |
| `broker.topic-capacity` | `10000` | 单主题容量上限，满后发布返回 FULL |
| `broker.max-retries` | `3` | 单事件最大投递次数（含首次），超过进死信 |
| `broker.retry-backoff-ms` | `1000` | 重试基础退避毫秒数，第 n 次重试等待 `n * backoff` |
| `broker.lease-timeout-ms` | `30000` | 消费租约（可见性超时），超时未确认重新投递 |
| `broker.reaper-interval-ms` | `1000` | 租约过期扫描间隔 |

## 测试覆盖

`src/test/java/com/github/lechandonga/localeventbrokerjava/tests/EventBrokerTest.java`：

- 并发重复发布：16 线程同 `eventId` 发布，仅 1 次入队、仅消费 1 次；
- 组内并发拉取：8 个消费者竞争，同一事件只有一个持有者；
- 重启恢复：已确认不重投、未确认重投、投递次数与进度不丢失；
- 失败重试与死信：退避生效、超限进入死信（`BUSINESS_FAILURE`）且可查询；
- 长时间未确认：租约过期重投、过期 token 确认被拒、超时耗尽进死信（`ACK_TIMEOUT`）；
- 容量背压：满后发布返回 FULL、慢主题不阻塞其他主题、确认后容量释放。
