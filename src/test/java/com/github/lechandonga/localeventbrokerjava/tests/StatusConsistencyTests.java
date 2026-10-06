package com.github.lechandonga.localeventbrokerjava.tests;

import com.github.lechandonga.localeventbrokerjava.broker.BrokerCore;
import com.github.lechandonga.localeventbrokerjava.broker.BrokerProperties;
import com.github.lechandonga.localeventbrokerjava.broker.GroupStatus;
import com.github.lechandonga.localeventbrokerjava.broker.PullResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 业务覆盖：状态查询与真实处理进度对账。
 * pending = 未确认非死信事件；leased/retryWaiting 是 pending 的细分；dead 单列；
 * 各消费组进度相互独立。
 */
@DisplayName("场景五：积压/死信/进度状态对账与消费组独立性")
class StatusConsistencyTests extends BrokerTestSupport {

    private static final Logger log = LoggerFactory.getLogger(StatusConsistencyTests.class);

    @Test
    @DisplayName("状态数字始终与真实进度一致：确认推进位点、NACK 进入重试、上限进入死信，且两组互不影响")
    void statusNumbersMatchReality() {
        BrokerProperties props = properties();
        props.setDefaultRetryBackoffBase(Duration.ofMillis(20));
        BrokerCore broker = newBroker(props);

        for (int i = 0; i < 6; i++) {
            broker.publish("topic-x", "k" + i, "p" + i);
        }
        GroupStatus g1 = broker.statusOfGroup("topic-x", "g1");
        GroupStatus g2 = broker.statusOfGroup("topic-x", "g2");
        assertEquals(6, g1.getPending());
        assertEquals(6, g2.getPending(), "不同消费组各自维护独立进度");

        // g1 消费并确认前 3 条
        List<PullResult> g1Pulled = broker.pull("topic-x", "g1", 3);
        for (PullResult result : g1Pulled) {
            broker.acknowledge("topic-x", "g1", result.event().getOffset(), result.deliveryToken());
        }
        g1 = broker.statusOfGroup("topic-x", "g1");
        assertEquals(2, g1.getCommittedOffset());
        assertEquals(3, g1.getPending());

        // g1 第 4 条持有不确认：leased=1，pending 仍为 3
        PullResult held = broker.pull("topic-x", "g1", 1).get(0);
        g1 = broker.statusOfGroup("topic-x", "g1");
        assertEquals(1, g1.getLeased());
        assertEquals(3, g1.getPending());

        // g1 第 5 条拿出来并 NACK：retryWaiting=1；第 6 条尚未投递；pending=3（leased+retry+未投递）
        PullResult failing = broker.pull("topic-x", "g1", 1).get(0);
        broker.negativeAcknowledge("topic-x", "g1", failing.event().getOffset(), failing.deliveryToken(), "err");
        g1 = broker.statusOfGroup("topic-x", "g1");
        // 注意下面需要等待退避窗口，统一放到拉取之前
        assertEquals(1, g1.getLeased());
        assertEquals(1, g1.getRetryWaiting());
        assertEquals(3, g1.getPending());
        assertEquals(0, g1.getDead());

        // 反复处理第 5 条直到死信（maxAttempts=3，退避 20ms/40ms）
        sleep(40);
        PullResult r2 = broker.pull("topic-x", "g1", 1).get(0);
        assertEquals(4, r2.event().getOffset());
        broker.negativeAcknowledge("topic-x", "g1", 4, r2.deliveryToken(), "err2");
        sleep(60);
        // 只拉重试到期的 offset=4（maxMessages=1，offset=3 仍 LEASED 会被跳过，
        // 因此这里先临时确认 offset=3 会改变状态；改为多次小批量拉取并精确取 offset=4，
        // 同时记住可能一起被拉走的 offset=5）
        java.util.List<PullResult> batch = broker.pull("topic-x", "g1", 10);
        PullResult r3 = batch.stream().filter(r -> r.event().getOffset() == 4).findFirst().orElseThrow();
        PullResult preheld5 = batch.stream().filter(r -> r.event().getOffset() == 5).findFirst().orElse(null);
        broker.negativeAcknowledge("topic-x", "g1", 4, r3.deliveryToken(), "err3");
        g1 = broker.statusOfGroup("topic-x", "g1");
        assertEquals(1, g1.getDead(), "offset=4 已进死信");
        assertEquals(2, g1.getPending(), "持有中的 offset=3 + 未投递的 offset=5");

        // g2 完全不受 g1 影响
        g2 = broker.statusOfGroup("topic-x", "g2");
        assertEquals(6, g2.getPending());
        assertEquals(0, g2.getDead());

        // g1 确认 offset=3，位点只能停在 3（offset=4 是死信，不能跨越）
        broker.acknowledge("topic-x", "g1", 3, held.deliveryToken());
        g1 = broker.statusOfGroup("topic-x", "g1");
        assertEquals(3, g1.getCommittedOffset(), "死信挡住连续位点，进度不能虚报");
        assertEquals(1, g1.getPending(), "仅剩 offset=5 待投递");
        // 消费掉 offset=5 并确认；pending=0，死信仍为 1（offset=5 可能已在上面批次中被持有）
        PullResult last = preheld5 != null
                ? preheld5
                : broker.pull("topic-x", "g1", 1).get(0);
        assertEquals(5, last.event().getOffset());
        broker.acknowledge("topic-x", "g1", 5, last.deliveryToken());
        g1 = broker.statusOfGroup("topic-x", "g1");
        assertEquals(0, g1.getPending());
        assertEquals(1, g1.getDead());
        log.info("【断言通过】pending/leased/retryWaiting/dead/committedOffset 全程对账一致，死信不虚报进度，g1/g2 独立");
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
