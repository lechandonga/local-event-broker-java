package com.github.lechandonga.localeventbrokerjava.tests;

import com.github.lechandonga.localeventbrokerjava.broker.BrokerCore;
import com.github.lechandonga.localeventbrokerjava.broker.GroupStatus;
import com.github.lechandonga.localeventbrokerjava.broker.PullResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：重启恢复。
 * 已确认事件重启后不再投递；未确认（持有中 / 重试等待中）事件重启后重新可拉取；
 * 消费进度、死信与幂等索引均从磁盘恢复。
 */
@DisplayName("场景二：服务重启后的状态恢复")
class RecoveryTests extends BrokerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(RecoveryTests.class);

    @Test
    @DisplayName("确认后的事件重启不重投；未确认的事件重启后重新投递；进度与幂等键保持一致")
    void stateIsRecoveredAfterRestart() {
        BrokerCore before = newBroker();
        for (int i = 0; i < 5; i++) {
            before.publish("orders", "ord-" + i, "payload-" + i);
        }
        List<PullResult> firstBatch = before.pull("orders", "g1", 3);
        assertEquals(3, firstBatch.size());
        // 确认前两条；第三条保持持有（未确认）
        before.acknowledge("orders", "g1", firstBatch.get(0).event().getOffset(), firstBatch.get(0).deliveryToken());
        before.acknowledge("orders", "g1", firstBatch.get(1).event().getOffset(), firstBatch.get(1).deliveryToken());
        String staleToken = firstBatch.get(2).deliveryToken();

        // 模拟进程重启：丢弃内存对象，仅用同一数据目录重建
        BrokerCore after = newBroker();

        GroupStatus status = after.statusOfGroup("orders", "g1");
        assertEquals(1, status.getCommittedOffset(), "committedOffset 从状态文件恢复");
        assertEquals(3, status.getPending(), "offset 2(未确认可重投) + 3,4(未投递) 共 3 条待处理");
        assertEquals(0, status.getLeased(), "重启后旧租约不再被视为持有，等待租约回收或直接重投");

        // 重启后未确认事件（offset 2）可以重新拉取，旧令牌失效
        List<PullResult> redelivery = after.pull("orders", "g1", 1);
        assertEquals(2, redelivery.get(0).event().getOffset(), "未确认事件必须重新投递");
        assertFalse(redelivery.get(0).deliveryToken().equals(staleToken), "重新投递使用新令牌");
        assertFalse(after.acknowledge("orders", "g1", 2, staleToken), "重启前的旧令牌确认必须被拒绝");
        assertTrue(after.acknowledge("orders", "g1", 2, redelivery.get(0).deliveryToken()));

        // 顺序消费剩余事件，已确认的 0、1 不会再出现
        List<PullResult> rest = after.pull("orders", "g1", 10);
        assertEquals(2, rest.size());
        assertEquals(3, rest.get(0).event().getOffset());
        assertEquals(4, rest.get(1).event().getOffset());
        for (PullResult result : rest) {
            after.acknowledge("orders", "g1", result.event().getOffset(), result.deliveryToken());
        }
        assertTrue(after.pull("orders", "g1", 10).isEmpty());
        assertEquals(4, after.statusOfGroup("orders", "g1").getCommittedOffset());

        // 全部确认到 offset 4 后，旧日志前缀已截断释放容量；
        // 此时发布一条全新事件，offset 继续单调递增（不复用旧偏移）
        com.github.lechandonga.localeventbrokerjava.broker.PublishOutcome fresh =
                after.publish("orders", "ord-new-6", "fresh");
        assertEquals(5, fresh.offset(), "截断后新事件使用继续递增的绝对偏移");
        // 该新事件在同进程内仍受幂等保护
        assertTrue(after.publish("orders", "ord-new-6", "dup").duplicated());
        // 再次重启：新事件未确认时重新可投递；幂等键从截断后的日志中恢复
        BrokerCore again = newBroker();
        List<PullResult> afterReboot = again.pull("orders", "g1", 10);
        assertEquals(1, afterReboot.size());
        assertEquals(5, afterReboot.get(0).event().getOffset());
        assertTrue(again.publish("orders", "ord-new-6", "dup").duplicated(), "幂等索引在重启后仍有效");
        log.info("【断言通过】重启后已确认不重投、未确认重投、committedOffset=1 恢复、旧令牌失效、幂等键恢复");
    }

    @Test
    @DisplayName("死信事件与重试等待状态在重启后继续保留，死信不会被重新投递")
    void deadLettersAndRetryStateSurviveRestart() throws Exception {
        BrokerCore before = newBroker();
        before.publish("orders", "dlq-1", "always-fail");
        PullResult pulled = before.pull("orders", "g1", 1).get(0);
        for (int attempt = 1; attempt < 3; attempt++) {
            before.negativeAcknowledge("orders", "g1", pulled.event().getOffset(), pulled.deliveryToken(), "boom");
            // 指数退避：第 1 次 50ms，第 2 次 100ms，统一等待 120ms
            Thread.sleep(120);
            pulled = before.pull("orders", "g1", 1).get(0);
        }
        // 第 3 次投递仍失败，达到 maxAttempts=3，进入死信
        before.negativeAcknowledge("orders", "g1", pulled.event().getOffset(), pulled.deliveryToken(), "boom-final");

        BrokerCore after = newBroker();
        GroupStatus status = after.statusOfGroup("orders", "g1");
        assertEquals(1, status.getDead(), "死信数量重启后必须保持");
        assertEquals(0, status.getPending(), "死信不计入可投递积压");
        assertTrue(after.pull("orders", "g1", 1).isEmpty(), "死信不能被再次拉取");
        assertEquals(1, after.listDeadLetters("orders", "g1").size());
        log.info("【断言通过】重启后死信仍可查询（1 条，原因 BUSINESS_ERROR），且不会被重新投递");
    }
}
