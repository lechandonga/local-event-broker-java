package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 死信视图。reason 区分两种来源：
 * BUSINESS_ERROR（业务反复失败超过上限）与 LEASE_TIMEOUT（长时间未确认超过上限）。
 */
public record DeadLetterView(long offset,
                             String eventKey,
                             String payload,
                             int attempts,
                             FailureReason reason,
                             String lastError,
                             long deadAt) {
}
