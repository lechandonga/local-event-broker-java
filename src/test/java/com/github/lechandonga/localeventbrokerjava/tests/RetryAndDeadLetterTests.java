package com.github.lechandonga.localeventbrokerjava.tests;

import com.github.lechandonga.localeventbrokerjava.broker.BrokerProperties;
import com.github.lechandonga.localeventbrokerjava.broker.DeadLetterView;
import com.github.lechandonga.localeventbrokerjava.broker.FailureReason;
import com.github.lechandonga.localeventbrokerjava.broker.GroupStatus;
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
 * 业务覆盖：业务失败的指数退避重试、重试上限进入死信、死信原因可区分、
 * 重试成功后正常确认不留死信、业务失败与租约超时两类原因区分。
 */
@DisplayName("场景三：失败重试、退避与死信")
class RetryAndDeadLetterTests extends BrokerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTests.class);

    @Test
    @DisplayName("业务处理失败按指数退避重试，达到上限进入死信且原因标记为 BUSINESS_ERROR")
    void businessFailureRetriesWithBackoffThenDeadLetters() {
        BrokerProperties props = properties();
        props.setDefaultMaxAttempts(4);
        props.setDefaultRetryBackoffBase(Duration.ofMillis(60));
        props.setDefaultRetryBackoffFactor(2.0);
        var broker = newBroker(props);

        broker.publish("payments", "pay-1", "money");
        long firstBackoffMs;
        PullResult pulled = broker.pull("payments", "g1", 1).get(0);
        assertEquals(1, pulled.attempt());

        // 第 1 次失败：退避 60ms，退避期内不可见
        assertTrue(broker.negativeAcknowledge("payments", "g1", 0, pulled.deliveryToken(), "E1"));
        assertTrue(broker.pull("payments", "g1", 1).isEmpty(), "退避窗口内不能立即重新拉取");
        sleep(70);
        pulled = broker.pull("payments", "g1", 1).get(0);
        assertEquals(2, pulled.attempt());

        // 第 2 次失败：退避 120ms
        broker.negativeAcknowledge("payments", "g1", 0, pulled.deliveryToken(), "E2");
        sleep(70);
        assertTrue(broker.pull("payments", "g1", 1).isEmpty(), "第二次退避(120ms)窗口更长，70ms 时仍不可见");
        sleep(70);
        pulled = broker.pull("payments", "g1", 1).get(0);
        assertEquals(3, pulled.attempt());

        // 第 3 次失败：退避 240ms 后第 4 次投递
        broker.negativeAcknowledge("payments", "g1", 0, pulled.deliveryToken(), "E3");
        sleep(260);
        pulled = broker.pull("payments", "g1", 1).get(0);
        assertEquals(4, pulled.attempt());

        // 第 4 次失败：达到上限，进入死信
        broker.negativeAcknowledge("payments", "g1", 0, pulled.deliveryToken(), "E4-final");
        GroupStatus status = broker.statusOfGroup("payments", "g1");
        assertEquals(1, status.getDead());
        assertEquals(0, status.getPending());
        assertTrue(broker.pull("payments", "g1", 1).isEmpty(), "死信事件不再被投递");

        List<DeadLetterView> dead = broker.listDeadLetters("payments", "g1");
        assertEquals(1, dead.size());
        assertEquals(0, dead.get(0).offset());
        assertEquals(FailureReason.BUSINESS_ERROR, dead.get(0).reason(), "死信必须保留失败原因");
        assertEquals(4, dead.get(0).attempts());
        assertEquals("E4-final", dead.get(0).lastError());
        log.info("【断言通过】退避 60ms/120ms/240ms 逐级生效，4 次尝试后进入死信，原因=BUSINESS_ERROR");
    }

    @Test
    @DisplayName("重试期间业务恢复，下一次投递成功并确认，不产生死信也不重复业务事件")
    void retryCanRecoverWithoutDeadLetter() {
        var broker = newBroker();
        broker.publish("payments", "pay-2", "money");
        PullResult pulled = broker.pull("payments", "g1", 1).get(0);
        broker.negativeAcknowledge("payments", "g1", 0, pulled.deliveryToken(), "temporary");
        sleep(60);
        PullResult redelivered = broker.pull("payments", "g1", 1).get(0);
        assertEquals(2, redelivered.attempt(), "同一业务事件的第 2 次投递");
        assertEquals("money", redelivered.event().getPayload());
        assertTrue(broker.acknowledge("payments", "g1", 0, redelivered.deliveryToken()));
        assertEquals(0, broker.statusOfGroup("payments", "g1").getDead());
        assertEquals(0, broker.statusOfGroup("payments", "g1").getPending());
        assertTrue(broker.listDeadLetters("payments", "g1").isEmpty());
        // 首次失败时的旧令牌确认必须被拒绝，防止过期处理结果造成重复副作用
        assertFalse(broker.acknowledge("payments", "g1", 0, pulled.deliveryToken()));
        log.info("【断言通过】第 2 次投递成功确认，无死信、无残留积压，旧令牌的迟到 ACK 被拒绝");
    }

    @Test
    @DisplayName("业务失败死信(BUSINESS_ERROR)与长时间未确认死信(LEASE_TIMEOUT)可以区分查询")
    void failureReasonsAreDistinguishable() {
        BrokerProperties props = properties();
        props.setDefaultMaxAttempts(2);
        props.setDefaultRetryBackoffBase(Duration.ofMillis(20));
        props.setDefaultLeaseDuration(Duration.ofMillis(60));
        var broker = newBroker(props);

        // A：业务两次失败 -> BUSINESS_ERROR 死信
        broker.publish("mix", "a", "A");
        broker.publish("mix", "b", "B");
        PullResult a = broker.pull("mix", "g1", 10).stream().filter(r -> r.event().getOffset() == 0).findFirst().orElseThrow();
        broker.negativeAcknowledge("mix", "g1", 0, a.deliveryToken(), "biz-1");
        sleep(30);
        PullResult a2 = broker.pull("mix", "g1", 10).stream().filter(r -> r.event().getOffset() == 0).findFirst().orElseThrow();
        broker.negativeAcknowledge("mix", "g1", 0, a2.deliveryToken(), "biz-2");

        // B：两次租约超时 -> LEASE_TIMEOUT 死信（拉走后不确认，等待回收）
        broker.pull("mix", "g1", 10);
        broker.reapExpiredLeases(); // offset1 租约未到期，无影响（60ms）
        sleep(70);
        broker.reapExpiredLeases(); // 第 1 次超时：重试等待
        sleep(30);
        broker.pull("mix", "g1", 10); // 第 2 次持有 B
        sleep(70);
        broker.reapExpiredLeases(); // 第 2 次超时：进入死信

        List<DeadLetterView> dead = broker.listDeadLetters("mix", "g1");
        assertEquals(2, dead.size());
        assertEquals(FailureReason.BUSINESS_ERROR, dead.get(0).reason());
        assertEquals(FailureReason.LEASE_TIMEOUT, dead.get(1).reason());
        GroupStatus status = broker.statusOfGroup("mix", "g1");
        assertEquals(2, status.getDead());
        assertEquals(0, status.getPending());
        log.info("【断言通过】死信列表中 offset=0 为 BUSINESS_ERROR，offset=1 为 LEASE_TIMEOUT，两类原因可区分");
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
