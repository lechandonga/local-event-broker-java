package com.github.lechandonga.localeventbrokerjava.broker;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 本地事件 Broker 核心：按主题发布/消费，消费组独立进度，租约式投递，
 * 失败重试（带退避）、死信、容量背压，全部状态落盘，重启可恢复。
 */
@Service
public class EventBroker {

    private static final Logger log = LoggerFactory.getLogger(EventBroker.class);

    private final EventStore store;
    private final BrokerProperties props;
    private final Clock clock;
    private final Map<String, TopicRuntime> topics = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "event-broker-reaper");
        t.setDaemon(true);
        return t;
    });

    @Autowired
    public EventBroker(EventStore store, BrokerProperties props) {
        this(store, props, Clock.systemUTC());
    }

    EventBroker(EventStore store, BrokerProperties props, Clock clock) {
        this.store = store;
        this.props = props;
        this.clock = clock;
    }

    @PostConstruct
    public void init() {
        for (String topic : store.listTopics()) {
            topics.put(topic, loadTopic(topic));
        }
        reaper.scheduleWithFixedDelay(this::reapExpiredLeases,
                props.getReaperIntervalMs(), props.getReaperIntervalMs(), TimeUnit.MILLISECONDS);
        log.info("EventBroker 启动完成，已恢复主题: {}", topics.keySet());
    }

    @PreDestroy
    public void shutdown() {
        reaper.shutdownNow();
    }

    private TopicRuntime loadTopic(String topic) {
        TopicRuntime runtime = new TopicRuntime();
        for (EventRecord e : store.loadEvents(topic)) {
            runtime.events.add(e);
            runtime.seenEventIds.add(e.eventId());
            runtime.nextSeq = Math.max(runtime.nextSeq, e.seq() + 1);
        }
        for (String group : store.listGroups(topic)) {
            GroupRuntime g = new GroupRuntime();
            var snap = store.loadGroupState(topic, group);
            g.acked.addAll(snap.acked());
            g.attempts.putAll(snap.attempts());
            g.dead.addAll(snap.dead());
            g.deadSeqs.addAll(snap.dead().stream().map(DeadLetter::seq).toList());
            runtime.groups.put(group, g);
        }
        return runtime;
    }

    /** 发布事件。相同 eventId 幂等去重；容量满返回 FULL（背压）。 */
    public PublishResult publish(String topic, String eventId, String payload) {
        TopicRuntime runtime = topics.computeIfAbsent(topic, t -> new TopicRuntime());
        synchronized (runtime) {
            if (runtime.seenEventIds.contains(eventId)) {
                return new PublishResult(PublishStatus.DUPLICATE, -1);
            }
            if (pendingSize(runtime) >= props.getTopicCapacity()) {
                log.warn("主题 [{}] 容量已满({})，拒绝发布 eventId={}", topic, props.getTopicCapacity(), eventId);
                return new PublishResult(PublishStatus.FULL, -1);
            }
            EventRecord event = new EventRecord(runtime.nextSeq++, eventId, payload, clock.millis());
            store.appendEvent(topic, event);
            runtime.events.add(event);
            runtime.seenEventIds.add(eventId);
            return new PublishResult(PublishStatus.PUBLISHED, event.seq());
        }
    }

    /** 拉取投递：为消费组租出最多 max 条可投递事件，组内同一事件同一时刻只租给一个消费者。 */
    public List<Delivery> poll(String topic, String group, String consumerId, int max) {
        TopicRuntime runtime = topics.computeIfAbsent(topic, t -> new TopicRuntime());
        synchronized (runtime) {
            GroupRuntime g = runtime.groups.computeIfAbsent(group, k -> new GroupRuntime());
            long now = clock.millis();
            List<Delivery> deliveries = new ArrayList<>();
            for (EventRecord e : runtime.events) {
                if (deliveries.size() >= max) {
                    break;
                }
                long seq = e.seq();
                if (g.acked.contains(seq) || g.deadSeqs.contains(seq) || g.inFlight.containsKey(seq)) {
                    continue;
                }
                int attempts = g.attempts.getOrDefault(seq, 0);
                if (attempts >= props.getMaxRetries()) {
                    deadLetter(topic, runtime, g, e, DeadReason.MAX_RETRIES_EXCEEDED, attempts);
                    continue;
                }
                Long availableAt = g.notBefore.get(seq);
                if (availableAt != null && availableAt > now) {
                    continue;
                }
                g.attempts.put(seq, attempts + 1);
                String token = UUID.randomUUID().toString();
                g.inFlight.put(seq, new Lease(token, consumerId, now + props.getLeaseTimeoutMs()));
                g.notBefore.remove(seq);
                deliveries.add(new Delivery(seq, token, e.eventId(), e.payload(), attempts + 1));
            }
            if (!deliveries.isEmpty()) {
                persistGroup(topic, group, g);
            }
            return deliveries;
        }
    }

    /** 确认消费。token 必须匹配当前租约；已确认的事件幂等返回 true。 */
    public boolean ack(String topic, String group, long seq, String token) {
        TopicRuntime runtime = topics.computeIfAbsent(topic, t -> new TopicRuntime());
        synchronized (runtime) {
            GroupRuntime g = runtime.groups.computeIfAbsent(group, k -> new GroupRuntime());
            if (g.acked.contains(seq)) {
                return true;
            }
            Lease lease = g.inFlight.get(seq);
            if (lease == null || !lease.token().equals(token)) {
                return false;
            }
            g.inFlight.remove(seq);
            g.acked.add(seq);
            g.attempts.remove(seq);
            persistGroup(topic, group, g);
            return true;
        }
    }

    /** 消极确认：业务处理失败，按退避重试；重试次数耗尽则进入死信。 */
    public boolean nack(String topic, String group, long seq, String token) {
        TopicRuntime runtime = topics.computeIfAbsent(topic, t -> new TopicRuntime());
        synchronized (runtime) {
            GroupRuntime g = runtime.groups.computeIfAbsent(group, k -> new GroupRuntime());
            Lease lease = g.inFlight.get(seq);
            if (lease == null || !lease.token().equals(token)) {
                return false;
            }
            g.inFlight.remove(seq);
            int attempts = g.attempts.getOrDefault(seq, 0);
            if (attempts >= props.getMaxRetries()) {
                EventRecord e = findEvent(runtime, seq);
                deadLetter(topic, runtime, g, e, DeadReason.BUSINESS_FAILURE, attempts);
            } else {
                long backoff = props.getRetryBackoffMs() * attempts;
                g.notBefore.put(seq, clock.millis() + backoff);
                log.info("主题 [{}] 组 [{}] 事件 seq={} 第 {} 次失败，{}ms 后重试",
                        topic, group, seq, attempts, backoff);
            }
            persistGroup(topic, group, g);
            return true;
        }
    }

    /** 租约过期扫描：超时未确认的事件重新可投递；重试耗尽的进入死信。 */
    void reapExpiredLeases() {
        long now = clock.millis();
        for (var topicEntry : topics.entrySet()) {
            String topic = topicEntry.getKey();
            TopicRuntime runtime = topicEntry.getValue();
            synchronized (runtime) {
                for (var groupEntry : runtime.groups.entrySet()) {
                    GroupRuntime g = groupEntry.getValue();
                    boolean changed = false;
                    var it = g.inFlight.entrySet().iterator();
                    while (it.hasNext()) {
                        var leaseEntry = it.next();
                        if (leaseEntry.getValue().expireAt() > now) {
                            continue;
                        }
                        long seq = leaseEntry.getKey();
                        it.remove();
                        changed = true;
                        int attempts = g.attempts.getOrDefault(seq, 0);
                        if (attempts >= props.getMaxRetries()) {
                            EventRecord e = findEvent(runtime, seq);
                            deadLetter(topic, runtime, g, e, DeadReason.ACK_TIMEOUT, attempts);
                        } else {
                            g.notBefore.put(seq, now + props.getRetryBackoffMs() * attempts);
                            log.info("主题 [{}] 组 [{}] 事件 seq={} 租约超时未确认，将重新投递",
                                    topic, groupEntry.getKey(), seq);
                        }
                    }
                    if (changed) {
                        persistGroup(topic, groupEntry.getKey(), g);
                    }
                }
            }
        }
    }

    /** 查询死信。 */
    public List<DeadLetter> deadLetters(String topic, String group) {
        TopicRuntime runtime = topics.get(topic);
        if (runtime == null) {
            return List.of();
        }
        synchronized (runtime) {
            GroupRuntime g = runtime.groups.get(group);
            return g == null ? List.of() : List.copyOf(g.dead);
        }
    }

    /** 查询状态：与内存中真实进度一致。 */
    public Map<String, TopicStatus> status() {
        Map<String, TopicStatus> result = new LinkedHashMap<>();
        topics.forEach((topic, runtime) -> {
            synchronized (runtime) {
                Map<String, GroupStatus> groups = new LinkedHashMap<>();
                runtime.groups.forEach((name, g) -> groups.put(name, new GroupStatus(
                        g.acked.size(), g.inFlight.size(), backlog(runtime, g), g.dead.size())));
                result.put(topic, new TopicStatus(runtime.events.size(), pendingSize(runtime), groups));
            }
        });
        return result;
    }

    private long backlog(TopicRuntime runtime, GroupRuntime g) {
        return runtime.events.stream()
                .filter(e -> !g.acked.contains(e.seq()) && !g.deadSeqs.contains(e.seq()))
                .count();
    }

    /** 容量口径：尚未被所有消费组处理完（确认或死信）的事件数。 */
    private int pendingSize(TopicRuntime runtime) {
        int pending = 0;
        for (EventRecord e : runtime.events) {
            boolean doneForAll = !runtime.groups.isEmpty() && runtime.groups.values().stream()
                    .allMatch(g -> g.acked.contains(e.seq()) || g.deadSeqs.contains(e.seq()));
            if (!doneForAll) {
                pending++;
            }
        }
        return pending;
    }

    private void deadLetter(String topic, TopicRuntime runtime, GroupRuntime g,
                            EventRecord e, DeadReason reason, int attempts) {
        if (g.deadSeqs.contains(e.seq())) {
            return;
        }
        g.inFlight.remove(e.seq());
        g.notBefore.remove(e.seq());
        g.attempts.remove(e.seq());
        DeadLetter letter = new DeadLetter(e.seq(), e.eventId(), e.payload(), reason, attempts, clock.millis());
        g.dead.add(letter);
        g.deadSeqs.add(e.seq());
        log.warn("主题 [{}] 事件 seq={} eventId={} 进入死信，原因={}，已投递 {} 次",
                topic, e.seq(), e.eventId(), reason, attempts);
    }

    private EventRecord findEvent(TopicRuntime runtime, long seq) {
        return runtime.events.stream().filter(e -> e.seq() == seq).findFirst()
                .orElseThrow(() -> new IllegalStateException("事件不存在 seq=" + seq));
    }

    private void persistGroup(String topic, String group, GroupRuntime g) {
        store.saveGroupState(topic, group, new EventStore.GroupStateSnapshot(
                List.copyOf(g.acked), new HashMap<>(g.attempts), List.copyOf(g.dead)));
    }

    public record TopicStatus(int totalEvents, int pendingEvents, Map<String, GroupStatus> groups) {
    }

    public record GroupStatus(int acked, int inFlight, long backlog, int dead) {
    }

    private record Lease(String token, String consumerId, long expireAt) {
    }

    private static class TopicRuntime {
        final List<EventRecord> events = new ArrayList<>();
        final Set<String> seenEventIds = new HashSet<>();
        final Map<String, GroupRuntime> groups = new LinkedHashMap<>();
        long nextSeq = 0;
    }

    private static class GroupRuntime {
        final Set<Long> acked = new HashSet<>();
        final Set<Long> deadSeqs = new HashSet<>();
        final List<DeadLetter> dead = new ArrayList<>();
        final Map<Long, Integer> attempts = new HashMap<>();
        final Map<Long, Lease> inFlight = new HashMap<>();
        final Map<Long, Long> notBefore = new HashMap<>();
    }
}
