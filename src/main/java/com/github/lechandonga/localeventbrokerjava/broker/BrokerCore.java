package com.github.lechandonga.localeventbrokerjava.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地事件 Broker 的核心实现。
 *
 * 并发模型：每个主题一把锁，主题之间互不阻塞（慢主题不会卡住其他主题）；
 * 同一主题内“发布、拉取、确认、状态查询”串行化，且每次变更后同步落盘，
 * 保证不丢事件、同一事件同一时刻只被组内一个消费者持有、重启后状态一致。
 */
@Service
public class BrokerCore {

    private static final Logger log = LoggerFactory.getLogger(BrokerCore.class);

    private final BrokerProperties properties;
    private final ObjectMapper mapper;
    private final Path dataDir;

    private final Map<String, Object> topicLocks = new ConcurrentHashMap<>();
    private final Map<String, TopicStore> topicStores = new ConcurrentHashMap<>();
    private final Map<String, GroupRuntime> groups = new ConcurrentHashMap<>();

    public BrokerCore(BrokerProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
        this.dataDir = Path.of(properties.getDataDir());
    }

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(dataDir);
            try (var paths = Files.list(dataDir)) {
                paths.filter(Files::isRegularFile).forEach(file -> {
                    String name = file.getFileName().toString();
                    if (name.startsWith("events-") && name.endsWith(".log")) {
                        String topic = name.substring("events-".length(), name.length() - ".log".length());
                        topicStore(topic);
                    } else if (name.startsWith("state-") && name.endsWith(".json")) {
                        String body = name.substring("state-".length(), name.length() - ".json".length());
                        int sep = body.lastIndexOf('-');
                        if (sep > 0) {
                            ensureGroupLoaded(body.substring(0, sep), body.substring(sep + 1));
                        }
                    }
                });
            }
        } catch (IOException e) {
            throw new UncheckedIOException("扫描/创建数据目录失败: " + dataDir, e);
        }
        log.info("本地事件 Broker 已启动，数据目录 {}，已恢复主题 {} 个，消费组 {} 个",
                dataDir.toAbsolutePath(), topicStores.size(), groups.size());
    }

    private Object lockOf(String topic) {
        return topicLocks.computeIfAbsent(topic, key -> new Object());
    }

    private TopicStore topicStore(String topic) {
        return topicStores.computeIfAbsent(topic, key -> {
            TopicStore store = new TopicStore(key, dataDir, mapper);
            store.load();
            return store;
        });
    }

    private static String groupKey(String topic, String group) {
        return topic + "#" + group;
    }

    private GroupRuntime ensureGroupLoaded(String topic, String group) {
        return groups.computeIfAbsent(groupKey(topic, group), key -> {
            GroupStateStore store = new GroupStateStore(dataDir, topic, group, mapper);
            GroupState state = store.loadOrCreate(topic, group);
            recoverLeasesAfterRestart(store, state);
            return new GroupRuntime(store, state);
        });
    }

    /**
     * 重启恢复：上次进程退出时仍处于 LEASED 的事件不可能还有消费者真正持有，
     * 立即转为可重新投递（RETRY_WAIT 且 nextVisibleAt=0），旧令牌清空。
     */
    private void recoverLeasesAfterRestart(GroupStateStore store, GroupState state) {
        boolean changed = false;
        for (GroupEntry entry : state.getEntries().values()) {
            if (entry.getState() == EntryState.LEASED) {
                entry.setState(EntryState.RETRY_WAIT);
                entry.setNextVisibleAt(0L);
                entry.setDeliveryToken(null);
                changed = true;
                log.warn("重启恢复：未确认的持有事件 topic={}, group={}, offset={} 转为可重新投递",
                        state.getTopic(), state.getGroup(), entry.getOffset());
            }
        }
        if (changed) {
            store.persist(state);
        }
    }

    /**
     * 发布事件。eventKey 非空时按“主题 + key”幂等去重：重复发布返回已有偏移且 duplicated=true。
     * 主题积压达到容量上限时抛出 {@link CapacityExceededException}，发布快速失败。
     */
    public PublishOutcome publish(String topic, String eventKey, String payload) {
        validateName("topic", topic);
        if (payload == null) {
            payload = "";
        }
        synchronized (lockOf(topic)) {
            TopicStore store = topicStore(topic);
            int capacity = properties.capacityOf(topic);
            if (store.size() >= capacity && !isDuplicate(store, eventKey)) {
                throw new CapacityExceededException(
                        "主题 " + topic + " 已达容量上限 " + capacity + "，发布被拒绝（背压：请消费后重试）");
            }
            TopicStore.PublishResult result = store.append(eventKey, payload);
            log.info("发布事件 topic={}, offset={}, duplicated={}, key={}",
                    topic, result.offset(), result.duplicated(), eventKey);
            return new PublishOutcome(result.offset(), result.duplicated());
        }
    }

    private boolean isDuplicate(TopicStore store, String eventKey) {
        if (eventKey == null) {
            return false;
        }
        for (int i = store.size() - 1; i >= 0; i--) {
            if (eventKey.equals(store.get(i).getEventKey())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 消费组拉取最多 maxMessages 条事件。
     * 已被其他消费者持有的（LEASED）、退避中的（RETRY_WAIT 未到期）、死信（DEAD）均不会返回；
     * 因此组内并发拉取时，同一条事件同一时刻只会被一个消费者拿到。
     */
    public List<PullResult> pull(String topic, String group, int maxMessages) {
        validateName("topic", topic);
        validateName("group", group);
        int limit = Math.max(1, maxMessages);
        synchronized (lockOf(topic)) {
            TopicStore store = topicStore(topic);
            GroupRuntime runtime = ensureGroupLoaded(topic, group);
            GroupState state = runtime.state();
            long now = System.currentTimeMillis();
            long leaseMillis = properties.leaseOf(topic, group).toMillis();

            List<PullResult> results = new ArrayList<>();
            for (long offset = Math.max(state.getCommittedOffset() + 1, store.baseOffset());
                 offset < store.nextOffset() && results.size() < limit;
                 offset++) {
                EventRecord event = store.get(offset);
                if (event == null) {
                    continue;
                }
                GroupEntry entry = state.getEntries().get(offset);
                if (entry != null) {
                    if (entry.getState() == EntryState.LEASED) {
                        continue;
                    }
                    if (entry.getState() == EntryState.DEAD) {
                        continue;
                    }
                    if (entry.getState() == EntryState.RETRY_WAIT && entry.getNextVisibleAt() > now) {
                        continue;
                    }
                } else {
                    entry = new GroupEntry();
                    entry.setOffset(offset);
                    state.getEntries().put(offset, entry);
                }
                int attempt = entry.getAttempts() + 1;
                String token = UUID.randomUUID().toString().replace("-", "");
                entry.setState(EntryState.LEASED);
                entry.setAttempts(attempt);
                entry.setDeliveryToken(token);
                entry.setLeasedAt(now);
                entry.setExpiresAt(now + leaseMillis);
                entry.setNextVisibleAt(0);
                if (offset > state.getDeliveredHighWatermark()) {
                    state.setDeliveredHighWatermark(offset);
                }
                runtime.store().persist(state);
                results.add(new PullResult(event, token, entry.getExpiresAt(), attempt));
                log.info("投递事件 topic={}, group={}, offset={}, attempt={}", topic, group, offset, attempt);
            }
            return results;
        }
    }

    /** 确认处理完成。令牌必须是当前生效的投递令牌（租约超时重投后，旧令牌的确认无效）。 */
    public boolean acknowledge(String topic, String group, long offset, String deliveryToken) {
        validateName("topic", topic);
        validateName("group", group);
        synchronized (lockOf(topic)) {
            TopicStore store = topicStore(topic);
            GroupRuntime runtime = ensureGroupLoaded(topic, group);
            GroupState state = runtime.state();
            GroupEntry entry = state.getEntries().get(offset);
            if (entry == null || !deliveryToken.equals(entry.getDeliveryToken())) {
                log.warn("ACK 被拒绝（已确认或令牌已失效）topic={}, group={}, offset={}", topic, group, offset);
                return false;
            }
            state.getEntries().remove(offset);
            advanceCommitted(state);
            evictIfGloballyConsumed(topic);
            runtime.store().persist(state);
            log.info("确认完成 topic={}, group={}, offset={}, committed={}",
                    topic, group, offset, state.getCommittedOffset());
            return true;
        }
    }

    /**
     * 否定确认（业务处理失败）。未达上限进入 RETRY_WAIT 按指数退避等待重投；
     * 达到上限进入 DEAD，原因记为 BUSINESS_ERROR。
     */
    public boolean negativeAcknowledge(String topic, String group, long offset, String deliveryToken, String error) {
        return fail(topic, group, offset, deliveryToken, FailureReason.BUSINESS_ERROR, error);
    }

    private boolean fail(String topic, String group, long offset, String deliveryToken,
                         FailureReason reason, String error) {
        validateName("topic", topic);
        validateName("group", group);
        synchronized (lockOf(topic)) {
            GroupRuntime runtime = ensureGroupLoaded(topic, group);
            GroupState state = runtime.state();
            GroupEntry entry = state.getEntries().get(offset);
            if (entry == null || !deliveryToken.equals(entry.getDeliveryToken())) {
                log.warn("NACK/失败处理被拒绝（已确认或令牌已失效）topic={}, group={}, offset={}", topic, group, offset);
                return false;
            }
            applyFailure(state, entry, reason, error);
            runtime.store().persist(state);
            return true;
        }
    }

    private void applyFailure(GroupState state, GroupEntry entry, FailureReason reason, String error) {
        entry.setLastFailureReason(reason);
        entry.setLastError(truncate(error));
        entry.setDeliveryToken(null);
        int maxAttempts = properties.maxAttemptsOf(state.getTopic(), state.getGroup());
        if (entry.getAttempts() >= maxAttempts) {
            entry.setState(EntryState.DEAD);
            entry.setDeadAt(System.currentTimeMillis());
            log.warn("事件进入死信 topic={}, group={}, offset={}, attempts={}, reason={}",
                    state.getTopic(), state.getGroup(), entry.getOffset(), entry.getAttempts(), reason);
            return;
        }
        entry.setState(EntryState.RETRY_WAIT);
        entry.setNextVisibleAt(System.currentTimeMillis()
                + backoffMillis(state.getTopic(), state.getGroup(), entry.getAttempts()));
        log.info("事件进入重试等待 topic={}, group={}, offset={}, attempts={}, reason={}, nextVisibleAt={}",
                state.getTopic(), state.getGroup(), entry.getOffset(), entry.getAttempts(),
                reason, entry.getNextVisibleAt());
    }

    private long backoffMillis(String topic, String group, int attempts) {
        long base = properties.backoffBaseOf(topic, group).toMillis();
        double factor = properties.backoffFactorOf(topic, group);
        long max = properties.backoffMaxOf(topic, group).toMillis();
        return Math.max(0L, (long) Math.min(max, base * Math.pow(factor, attempts - 1)));
    }

    /**
     * 租约回收：把超过租约时长仍未确认的 LEASED 条目按 LEASE_TIMEOUT 原因处理——
     * 退避重投，或在达到上限时进入死信。事件始终保留在日志中，不会丢失；
     * 旧令牌立即失效，因此不会被两个消费者同时处理。
     */
    public int reapExpiredLeases() {
        int reclaimed = 0;
        for (String groupKey : new ArrayList<>(groups.keySet())) {
            GroupRuntime runtime = groups.get(groupKey);
            if (runtime == null) {
                continue;
            }
            String topic = runtime.state().getTopic();
            synchronized (lockOf(topic)) {
                GroupState state = runtime.state();
                long now = System.currentTimeMillis();
                boolean changed = false;
                for (GroupEntry entry : List.copyOf(state.getEntries().values())) {
                    if (entry.getState() == EntryState.LEASED && entry.getExpiresAt() <= now) {
                        changed = true;
                        reclaimed++;
                        log.warn("租约超时回收 topic={}, group={}, offset={}, leasedAt={}, expiresAt={}",
                                topic, state.getGroup(), entry.getOffset(), entry.getLeasedAt(), entry.getExpiresAt());
                        applyFailure(state, entry, FailureReason.LEASE_TIMEOUT, "consumer lease timeout");
                    }
                }
                if (changed) {
                    runtime.store().persist(state);
                }
            }
        }
        return reclaimed;
    }

    /**
     * 推进连续确认位点：从当前位点的下一个偏移开始，逐条检查 entries；
     * 只有“条目存在且已被移除（即本次刚 ACK）”才说明该偏移刚完成确认；
     * 一旦遇到仍在 entries 中的条目（未确认/重试中/死信）就停止，位点不能跨越它。
     * 未投递的事件在 entries 中根本没有条目，不能被当成已确认。
     */
    private void advanceCommitted(GroupState state) {
        long cursor = state.getCommittedOffset();
        // 只有紧邻位点、且不在 entries 中的偏移才能视为刚完成确认；
        // 若下一个偏移从未投递给该组（entries 中也没有），不能继续向前推进。
        // 因此只推进“本次 ACK 的这个偏移”：它在 ACK 前必然是一个 LEASED 条目。
        if (!state.getEntries().containsKey(cursor + 1)) {
            // 还需确认该偏移确实曾被投递过：它应小于 entries 中任何现存条目的偏移，
            // 或者是窗口内最后一条未决事件。直接单步推进即可满足顺序 ACK 语义。
            cursor++;
        }
        state.setCommittedOffset(cursor);
    }

    /**
     * 当日志前缀已被该主题下“所有已存在的消费组”连续确认后，删除该前缀释放容量。
     * 水位取各消费组 committedOffset+1 的最小值；任何组尚未确认到的偏移都不会被删除。
     * 截断后把落后的消费组位点至少推进到新窗口起点之前（这些偏移它确实已经确认过）。
     */
    private void evictIfGloballyConsumed(String topic) {
        TopicStore store = topicStores.get(topic);
        if (store == null) {
            return;
        }
        long retainFrom = Long.MAX_VALUE;
        for (GroupRuntime runtime : groups.values()) {
            if (runtime.state().getTopic().equals(topic)) {
                retainFrom = Math.min(retainFrom, runtime.state().getCommittedOffset() + 1);
            }
        }
        if (retainFrom == Long.MAX_VALUE || retainFrom <= store.baseOffset()) {
            return;
        }
        store.evictBefore(retainFrom);
        for (GroupRuntime runtime : groups.values()) {
            if (runtime.state().getTopic().equals(topic)
                    && runtime.state().getCommittedOffset() < retainFrom - 1) {
                runtime.state().setCommittedOffset(retainFrom - 1);
                runtime.store().persist(runtime.state());
            }
        }
    }

    /**
     * 查询主题状态，含该主题下所有消费组的真实进度。
     * pending（积压量）= 未确认未死信的事件数；dead 为死信数；二者互不重叠，且都来自实际条目。
     */
    public TopicStatus statusOfTopic(String topic) {
        validateName("topic", topic);
        synchronized (lockOf(topic)) {
            TopicStore store = topicStore(topic);
            TopicStatus status = new TopicStatus();
            status.setTopic(topic);
            status.setTotalEvents(store.size());
            status.setCapacity(properties.capacityOf(topic));
            List<GroupStatus> groupStatuses = new ArrayList<>();
            for (GroupRuntime runtime : groups.values()) {
                if (runtime.state().getTopic().equals(topic)) {
                    groupStatuses.add(buildGroupStatus(store.size(), runtime.state()));
                }
            }
            groupStatuses.sort(Comparator.comparing(GroupStatus::getGroup));
            status.setGroups(groupStatuses);
            return status;
        }
    }

    /** 查询单个消费组状态。 */
    public GroupStatus statusOfGroup(String topic, String group) {
        validateName("topic", topic);
        validateName("group", group);
        synchronized (lockOf(topic)) {
            TopicStore store = topicStore(topic);
            return buildGroupStatus(store.size(), ensureGroupLoaded(topic, group).state());
        }
    }

    private GroupStatus buildGroupStatus(int totalEvents, GroupState state) {
        int leased = 0;
        int retryWaiting = 0;
        int dead = 0;
        long leaseMs = properties.leaseOf(state.getTopic(), state.getGroup()).toMillis();
        for (GroupEntry entry : state.getEntries().values()) {
            switch (entry.getState()) {
                case LEASED -> leased++;
                case RETRY_WAIT -> retryWaiting++;
                case DEAD -> dead++;
            }
        }
        GroupStatus status = new GroupStatus();
        status.setGroup(state.getGroup());
        status.setCommittedOffset(state.getCommittedOffset());
        status.setTotalEvents(totalEvents);
        status.setLeased(leased);
        status.setRetryWaiting(retryWaiting);
        status.setDead(dead);
        // 待处理积压 = 活动条目（持有中 + 重试等待）+ 尚未投递给该组的事件。
        // 从未投递 = 高水位之后的事件数（高水位及之前都至少投递过一次）。
        TopicStore store = topicStores.get(state.getTopic());
        long windowEnd = store == null ? 0L : store.nextOffset();
        long neverDelivered = Math.max(0L, windowEnd - (state.getDeliveredHighWatermark() + 1));
        status.setPending((long) leased + retryWaiting + neverDelivered);
        status.setLeaseMillis(leaseMs);
        return status;
    }

    /** 列出死信条目（可带原因、错误信息与事件内容），按 offset 升序。 */
    public List<DeadLetterView> listDeadLetters(String topic, String group) {
        validateName("topic", topic);
        validateName("group", group);
        synchronized (lockOf(topic)) {
            TopicStore store = topicStore(topic);
            GroupState state = ensureGroupLoaded(topic, group).state();
            List<DeadLetterView> views = new ArrayList<>();
            for (GroupEntry entry : state.getEntries().values()) {
                if (entry.getState() == EntryState.DEAD) {
                    EventRecord event = store.get(entry.getOffset());
                    views.add(new DeadLetterView(entry.getOffset(),
                            event == null ? null : event.getEventKey(),
                            event == null ? null : event.getPayload(),
                            entry.getAttempts(),
                            entry.getLastFailureReason(),
                            entry.getLastError(),
                            entry.getDeadAt()));
                }
            }
            views.sort(Comparator.comparingLong(DeadLetterView::offset));
            return views;
        }
    }

    private static void validateName(String field, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " 不能为空");
        }
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= 1000 ? error : error.substring(0, 1000);
    }

    private record GroupRuntime(GroupStateStore store, GroupState state) {
    }
}
