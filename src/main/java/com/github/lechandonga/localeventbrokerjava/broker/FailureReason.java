package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 区分两类失败原因：
 * 业务处理失败（消费者显式 NACK）与长时间未确认（租约过期被回收）。
 * 进入死信时该原因会被保留，因此死信中可以区分“业务失败超过重试上限”
 * 与“长时间未确认超过重试上限”。
 */
public enum FailureReason {
    BUSINESS_ERROR,
    LEASE_TIMEOUT
}
