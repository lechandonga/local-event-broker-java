package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 死信原因。
 */
public enum DeadReason {
    /** 业务处理失败（消费者主动 nack）且重试次数耗尽。 */
    BUSINESS_FAILURE,
    /** 长时间未确认（租约过期）且重试次数耗尽。 */
    ACK_TIMEOUT,
    /** 超过最大重试上限。 */
    MAX_RETRIES_EXCEEDED
}
