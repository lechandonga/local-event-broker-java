package com.github.lechandonga.localeventbrokerjava.tests;

import com.github.lechandonga.localeventbrokerjava.broker.BrokerProperties;
import com.github.lechandonga.localeventbrokerjava.broker.DeadLetter;
import com.github.lechandonga.localeventbrokerjava.broker.DeadReason;
import com.github.lechandonga.localeventbrokerjava.broker.Delivery;
import com.github.lechandonga.localeventbrokerjava.broker.EventBroker;
import com.github.lechandonga.localeventbrokerjava.broker.EventStore;
import com.github.lechandonga.localeventbrokerjava.broker.PublishStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地事件 Broker 核心语义测试：并发去重、重启恢复、失败重试与死信、租约超时重投、容量背压。
 */
class EventBrokerTest {

    private static final Logger log = LoggerFactory.getLogger(EventBrokerTest.class);

    private static final String TOPIC = "orders";
    private static final String GROUP = "billing";

    @TempDir
    Path dataDir;

    private BrokerProperties props;
    private EventBroker broker;

    @BeforeEach
    void setUp(TestInfo info) {
        log.info("==== 开始用例: {} ====", info.getDisplayName());
        props = new BrokerProperties();
        props.setDataDir(dataDir.toString());
        props.setTopicCapacity(100);
        props.setMaxRetries(3);
        props.setRetryBackoffMs(50);
        props.setLeaseTimeoutMs(200);
        props.setReaperIntervalMs(50);
        broker = newBroker();
    }

    @AfterEach
    void tearDown(TestInfo info) {
        broker.shutdown();
        log.info("==== 通过用例: {} ====", info.getDisplayName());
    }

    private EventBroker newBroker() {
        EventBroker b = new EventBroker(new EventStore(props), props);
        b.init();
        return b;
    }

    @Test
    @DisplayName("并发重复发布：相同 eventId 只入队一次、只被消费一次")
    void concurrentDuplicatePublish() throws Exception {
        int threads = 16;
        AtomicInteger published = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                await(start);
                var r = broker.publish(TOPIC, "evt-1", "payload");
                if (r.status() == PublishStatus.PUBLISHED) {
                    published.incrementAndGet();
                } else if (r.status() == PublishStatus.DUPLICATE) {
                    duplicate.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(1, published.get(), "相同 eventId 只能成功发布一次");
        assertEquals(threads - 1, duplicate.get());

        List<Delivery> deliveries = broker.poll(TOPIC, GROUP, "c1", 10);
        assertEquals(1, deliveries.size(), "重复发布的事件只能被消费一次");
        assertTrue(broker.poll(TOPIC, GROUP, "c2", 10).isEmpty());
        log.info("并发 {} 线程重复发布：成功 {}，去重 {}", threads, published.get(), duplicate.get());
    }

    @Test
    @DisplayName("组内并发拉取：同一事件同一时刻只被一个消费者持有")
    void concurrentPollSingleOwner() throws Exception {
        broker.publish(TOPIC, "evt-1", "payload");
        int threads = 8;
        Set<String> owners = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            String consumer = "consumer-" + i;
            pool.submit(() -> {
                await(start);
                if (!broker.poll(TOPIC, GROUP, consumer, 1).isEmpty()) {
                    owners.add(consumer);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1, owners.size(), "同一事件只能租给一个消费者");
        log.info("并发 {} 个消费者拉取，实际持有者: {}", threads, owners);
    }

    @Test
    @DisplayName("重启恢复：已确认不重投，未确认重新投递，进度不丢失")
    void restartRecovery() {
        broker.publish(TOPIC, "evt-1", "p1");
        broker.publish(TOPIC, "evt-2", "p2");
        broker.publish(TOPIC, "evt-3", "p3");

        List<Delivery> first = broker.poll(TOPIC, GROUP, "c1", 3);
        assertEquals(3, first.size());
        assertTrue(broker.ack(TOPIC, GROUP, first.get(0).seq(), first.get(0).token()), "确认 evt-1");
        // evt-2、evt-3 拉走后未确认，重启后应重新投递

        broker.shutdown();
        broker = newBroker();
        log.info("Broker 已重启，从 {} 恢复状态", dataDir);

        List<Delivery> redelivered = broker.poll(TOPIC, GROUP, "c2", 10);
        assertEquals(2, redelivered.size(), "重启后只有未确认的 2 条应被重新投递");
        assertTrue(redelivered.stream().noneMatch(d -> d.eventId().equals("evt-1")),
                "已确认的 evt-1 不得重复投递");
        assertEquals(2, redelivered.get(0).attempt(), "重投后投递次数应累计");

        var status = broker.status().get(TOPIC).groups().get(GROUP);
        assertEquals(1, status.acked());
        assertEquals(2, status.backlog());
    }

    @Test
    @DisplayName("失败重试与死信：业务失败按退避重试，超过上限进入死信且可查询")
    void retryAndDeadLetter() throws Exception {
        broker.publish(TOPIC, "evt-bad", "poison");

        Delivery d1 = pollOne();
        assertEquals(1, d1.attempt());
        assertTrue(broker.nack(TOPIC, GROUP, d1.seq(), d1.token()), "第 1 次业务失败");
        assertTrue(broker.poll(TOPIC, GROUP, "c1", 1).isEmpty(), "退避期内不得立即重投");

        Thread.sleep(120); // 等待退避结束
        Delivery d2 = pollOne();
        assertEquals(2, d2.attempt(), "退避后应重投，次数累计");
        broker.nack(TOPIC, GROUP, d2.seq(), d2.token());

        Thread.sleep(150);
        Delivery d3 = pollOne();
        assertEquals(3, d3.attempt());
        broker.nack(TOPIC, GROUP, d3.seq(), d3.token()); // 达到 maxRetries=3，进入死信

        List<DeadLetter> dead = broker.deadLetters(TOPIC, GROUP);
        assertEquals(1, dead.size());
        assertEquals(DeadReason.BUSINESS_FAILURE, dead.get(0).reason());
        assertEquals("evt-bad", dead.get(0).eventId());
        assertTrue(broker.poll(TOPIC, GROUP, "c1", 1).isEmpty(), "死信不得再被投递");

        var status = broker.status().get(TOPIC).groups().get(GROUP);
        assertEquals(1, status.dead(), "状态查询的死信量必须与实际一致");
        assertEquals(0, status.backlog());
        log.info("死信记录: {}", dead.get(0));
    }

    @Test
    @DisplayName("长时间未确认：租约过期后重新投递，超时耗尽进入死信(ACK_TIMEOUT)")
    void leaseTimeoutRedelivery() throws Exception {
        broker.publish(TOPIC, "evt-slow", "slow");

        Delivery d1 = pollOne();
        // 不确认，等待租约过期由 reaper 回收
        Thread.sleep(500);
        Delivery d2 = pollOne();
        assertEquals(d1.seq(), d2.seq(), "超时未确认的事件必须重新投递");
        assertEquals(2, d2.attempt());
        assertFalse(broker.ack(TOPIC, GROUP, d1.seq(), d1.token()), "过期租约的 token 不得再确认成功");

        // 持续不确认直到超过重试上限
        for (int i = 0; i < 2; i++) {
            Thread.sleep(500);
            broker.poll(TOPIC, GROUP, "c1", 1);
        }
        Thread.sleep(500);
        List<DeadLetter> dead = broker.deadLetters(TOPIC, GROUP);
        assertEquals(1, dead.size(), "超时重试耗尽后应进入死信");
        assertEquals(DeadReason.ACK_TIMEOUT, dead.get(0).reason());
        log.info("超时死信记录: {}", dead.get(0));
    }

    @Test
    @DisplayName("容量背压：队列满后发布返回 FULL，慢主题不阻塞其他主题")
    void capacityBackpressureAndTopicIsolation() {
        props.setTopicCapacity(3);
        for (int i = 0; i < 3; i++) {
            assertEquals(PublishStatus.PUBLISHED, broker.publish(TOPIC, "e" + i, "p").status());
        }
        assertEquals(PublishStatus.FULL, broker.publish(TOPIC, "e3", "p").status(),
                "容量满后发布必须被明确拒绝");

        // 慢主题满了，其他主题不受影响
        assertEquals(PublishStatus.PUBLISHED, broker.publish("fast-topic", "f1", "p").status());
        Delivery d = broker.poll("fast-topic", "g2", "c1", 1).get(0);
        assertEquals("f1", d.eventId());

        // 确认后容量释放，可继续发布
        Delivery first = broker.poll(TOPIC, GROUP, "c1", 1).get(0);
        assertTrue(broker.ack(TOPIC, GROUP, first.seq(), first.token()));
        assertEquals(PublishStatus.PUBLISHED, broker.publish(TOPIC, "e3", "p").status());

        var status = broker.status();
        assertEquals(4, status.get(TOPIC).totalEvents());
        assertEquals(1, status.get("fast-topic").groups().get("g2").inFlight());
    }

    private Delivery pollOne() {
        List<Delivery> list = broker.poll(TOPIC, GROUP, "c1", 1);
        assertEquals(1, list.size(), "应能拉到一条事件");
        return list.get(0);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
