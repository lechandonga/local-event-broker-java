package com.github.lechandonga.localeventbrokerjava.broker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 周期回收超过租约时长仍未确认的投递，使“长时间未确认”的事件能被重新投递。
 */
@Component
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final BrokerCore brokerCore;

    public LeaseReaper(BrokerCore brokerCore) {
        this.brokerCore = brokerCore;
    }

    @Scheduled(fixedDelayString = "${local.broker.reaper-interval:2s}")
    public void reap() {
        int reclaimed = brokerCore.reapExpiredLeases();
        if (reclaimed > 0) {
            log.warn("租约回收完成，本次重新投递/死信事件数: {}", reclaimed);
        }
    }
}
