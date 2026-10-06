package com.github.lechandonga.localeventbrokerjava.tests;

import com.github.lechandonga.localeventbrokerjava.broker.BrokerCore;
import com.github.lechandonga.localeventbrokerjava.broker.BrokerProperties;
import com.github.lechandonga.localeventbrokerjava.broker.DeadLetterView;
import com.github.lechandonga.localeventbrokerjava.broker.FailureReason;
import com.github.lechandonga.localeventbrokerjava.broker.PullResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：消费者拉取后长时间不确认（宕机/卡死），
 * 租约到期事件被回收，按退避重新投递给其他消费者；反复超时超过上限则进入 LEASE_TIMEOUT 死信。
 */
@DisplayName("场景四：长时间未确认后的重新投递")
class LeaseTimeoutTests extends BrokerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(LeaseTimeoutTests.class);

    @Test
    @DisplayName("租约超时后事件重新可拉取，新消费者可以接手确认，旧消费者的迟到确认无效")
    void expiredLeaseIsRedeliveredAndOldAckRejected() {
        BrokerProperties props = properties();
        props.setDefaultLeaseDuration(Duration.ofMillis(100));
        props.setDefaultRetryBackoffBase(Duration.ofMillis(20));
        BrokerCore broker = newBroker(props);

        broker.publish("jobs", "job-1", "work");
        PullResult first = broker.pull("jobs", "g1", 1).get(0);
        assertEquals(1, broker.statusOfGroup("jobs", "g1").getLeased());
        assertTrue(broker.pull("jobs", "g1", 1).isEmpty(), "租约内其他消费者拿不到该事件");

        sleep(110);
        int reclaimed = broker.reapExpiredLeases();
        assertEquals(1, reclaimed, "到期租约应被回收");
        assertEquals(0, broker.statusOfGroup("jobs", "g1").getLeased());

        // 旧消费者“复活”后迟到的确认必须被拒绝
        assertFalse(broker.acknowledge("jobs", "g1", first.event().getOffset(), first.deliveryToken()),
                "旧令牌已随租约回收失效");

        sleep(30);
        PullResult second = broker.pull("jobs", "g1", 1).get(0);
        assertEquals(0, second.event().getOffset());
        assertEquals(2, second.attempt(), "租约超时重新投递，投递次数累加");
        assertTrue(broker.acknowledge("jobs", "g1", 0, second.deliveryToken()), "接手的消费者可以确认");
        assertEquals(0, broker.statusOfGroup("jobs", "g1").getPending());
        assertTrue(broker.pull("jobs", "g1", 1).isEmpty());
        log.info("【断言通过】租约到期回收 -> 旧 ACK 拒绝 -> 退避后第 2 次投递 -> 新消费者确认成功");
    }

    @Test
    @DisplayName("反复长时间未确认超过投递上限，进入 LEASE_TIMEOUT 死信且事件可查询不丢失")
    void repeatedLeaseTimeoutEndsInDeadLetter() {
        BrokerProperties props = properties();
        props.setDefaultLeaseDuration(Duration.ofMillis(80));
        props.setDefaultRetryBackoffBase(Duration.ofMillis(10));
        props.setDefaultMaxAttempts(3);
        BrokerCore broker = newBroker(props);

        broker.publish("jobs", "job-stuck", "work");
        for (int round = 1; round <= 3; round++) {
            List<PullResult> pulled = broker.pull("jobs", "g1", 1);
            assertEquals(1, pulled.size(), "第 " + round + " 轮拿到事件");
            assertEquals(round, pulled.get(0).attempt());
            sleep(90);
            broker.reapExpiredLeases();
            sleep(20); // 等待退避窗口过去
        }
        // 三轮都未确认：第三次回收时达到上限，事件进入死信，之后不再投递
        assertTrue(broker.pull("jobs", "g1", 1).isEmpty(), "达到上限后不再投递");
        List<DeadLetterView> dead = broker.listDeadLetters("jobs", "g1");
        assertEquals(1, dead.size());
        assertEquals(FailureReason.LEASE_TIMEOUT, dead.get(0).reason());
        assertEquals(3, dead.get(0).attempts());
        assertEquals("job-stuck", dead.get(0).eventKey(), "事件内容保留在死信中，没有丢失");
        assertEquals("work", dead.get(0).payload());
        log.info("【断言通过】3 次租约超时后进入 LEASE_TIMEOUT 死信，事件 payload 完整可查询");
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
