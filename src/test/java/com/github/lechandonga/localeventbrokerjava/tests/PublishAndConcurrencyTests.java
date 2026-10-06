package com.github.lechandonga.localeventbrokerjava.tests;

import com.github.lechandonga.localeventbrokerjava.broker.CapacityExceededException;
import com.github.lechandonga.localeventbrokerjava.broker.PublishOutcome;
import com.github.lechandonga.localeventbrokerjava.broker.PullResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：并发重复发布幂等、组内并发拉取互斥、容量背压快速失败、慢主题不阻塞其他主题。
 */
@DisplayName("场景一：并发发布去重 / 组内互斥拉取 / 容量背压 / 主题隔离")
class PublishAndConcurrencyTests extends BrokerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(PublishAndConcurrencyTests.class);

    @Test
    @DisplayName("同一事件被多线程重复发布，只落一条，消费方只拿到一次")
    void concurrentDuplicatePublishIsIdempotent() throws Exception {
        try (BrokerCoreRunner ignored = new BrokerCoreRunner(newBroker())) {
            BrokerCoreRunner broker = ignored;
            int threads = 16;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<PublishOutcome>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return broker.get().publish("orders", "order-1001", "{\"amount\":99}");
                }));
            }
            start.countDown();
            Set<Long> offsets = new HashSet<>();
            int duplicated = 0;
            for (Future<PublishOutcome> future : futures) {
                PublishOutcome outcome = future.get(5, TimeUnit.SECONDS);
                offsets.add(outcome.offset());
                if (outcome.duplicated()) {
                    duplicated++;
                }
            }
            pool.shutdown();
            assertEquals(1, offsets.size(), "重复发布必须落在同一个 offset");
            assertEquals(threads - 1, duplicated, "除首次外其余全部命中幂等去重");
            assertEquals(1, broker.get().statusOfTopic("orders").getTotalEvents());

            List<PullResult> first = broker.get().pull("orders", "g1", 10);
            assertEquals(1, first.size());
            assertTrue(broker.get().pull("orders", "g1", 10).isEmpty(), "已持有的事件不能再次拉取");
            broker.get().acknowledge("orders", "g1", first.get(0).event().getOffset(), first.get(0).deliveryToken());
            assertTrue(broker.get().pull("orders", "g1", 10).isEmpty(), "确认后不得重复消费");
            log.info("【断言通过】16 个并发重复发布只产生 1 条事件，消费仅发生 1 次");
        }
    }

    @Test
    @DisplayName("组内多个消费者并发拉取，同一事件同一时刻只被一个消费者持有")
    void concurrentPullWithinGroupIsExclusive() throws Exception {
        try (BrokerCoreRunner ignored = new BrokerCoreRunner(newBroker())) {
            BrokerCoreRunner broker = ignored;
            int eventCount = 50;
            for (int i = 0; i < eventCount; i++) {
                broker.get().publish("orders", "key-" + i, "payload-" + i);
            }
            int consumers = 8;
            ExecutorService pool = Executors.newFixedThreadPool(consumers);
            CountDownLatch start = new CountDownLatch(1);
            Set<Long> heldOffsets = ConcurrentHashMap.newKeySet();
            List<Boolean> conflicts = Collections.synchronizedList(new ArrayList<>());
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < consumers; c++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    List<PullResult> batch = broker.get().pull("orders", "g1", 100);
                    for (PullResult result : batch) {
                        if (!heldOffsets.add(result.event().getOffset())) {
                            conflicts.add(true);
                        }
                    }
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertTrue(conflicts.isEmpty(), "不允许任何两个消费者同时持有同一事件");
            assertEquals(eventCount, heldOffsets.size(), "全部事件恰好被分发一次");
            assertTrue(broker.get().pull("orders", "g1", 100).isEmpty(), "已被持有的事件不能再次拉取");
            log.info("【断言通过】8 个消费者并发拉取 50 条事件，持有集合无冲突，无重复分发");
        }
    }

    @Test
    @DisplayName("主题达到容量上限后发布被明确拒绝，消费确认并截断后可以继续发布")
    void capacityBackpressureIsExplicitAndReleasedAfterConsume() {
        com.github.lechandonga.localeventbrokerjava.broker.BrokerProperties small = properties();
        small.setDefaultCapacity(10);
        try (BrokerCoreRunner ignored = new BrokerCoreRunner(newBroker(small))) {
            BrokerCoreRunner broker = ignored;
            for (int i = 0; i < 10; i++) {
                broker.get().publish("orders", "cap-" + i, "p" + i);
            }
            assertThrows(CapacityExceededException.class,
                    () -> broker.get().publish("orders", "cap-overflow", "overflow"),
                    "容量已满必须快速失败而不是阻塞或悄悄丢弃");

            List<PullResult> all = broker.get().pull("orders", "g1", 100);
            assertEquals(10, all.size());
            for (PullResult result : all) {
                broker.get().acknowledge("orders", "g1", result.event().getOffset(), result.deliveryToken());
            }
            PublishOutcome after = broker.get().publish("orders", "cap-after-consume", "new");
            assertEquals(10, after.offset(), "全部确认、日志截断后窗口释放，新发布使用继续递增的绝对偏移");
            log.info("【断言通过】容量满返回明确背压；消费确认后窗口释放可继续发布");
        }
    }

    @Test
    @DisplayName("慢主题不会阻塞其他主题的发布与消费")
    void slowTopicDoesNotBlockOtherTopics() throws Exception {
        try (BrokerCoreRunner ignored = new BrokerCoreRunner(newBroker())) {
            BrokerCoreRunner broker = ignored;
            ExecutorService pool = Executors.newSingleThreadExecutor();
            // slow 主题的消费者拉走一条但迟迟不确认
            broker.get().publish("slow", "s1", "slow-payload");
            broker.get().pull("slow", "gs", 1);

            Future<Boolean> fastFuture = pool.submit(() -> {
                for (int i = 0; i < 20; i++) {
                    broker.get().publish("fast", "f" + i, "fast-payload");
                    List<PullResult> pulled = broker.get().pull("fast", "gf", 1);
                    if (pulled.size() != 1) {
                        return false;
                    }
                    broker.get().acknowledge("fast", "gf",
                            pulled.get(0).event().getOffset(), pulled.get(0).deliveryToken());
                }
                return true;
            });
            assertTrue(fastFuture.get(5, TimeUnit.SECONDS), "fast 主题全程不被 slow 主题阻塞");
            pool.shutdown();
            assertEquals(1, broker.get().statusOfGroup("slow", "gs").getLeased(), "slow 的事件仍被持有");
            assertEquals(0, broker.get().statusOfGroup("fast", "gf").getPending(), "fast 已全部确认");
            log.info("【断言通过】slow 主题挂起期间，fast 主题 20 轮发布/消费/确认全部即时完成");
        }
    }
}
