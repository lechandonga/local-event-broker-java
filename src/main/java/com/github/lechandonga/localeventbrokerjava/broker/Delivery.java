package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 一次投递（租约）。ack/nack 时必须携带 token，防止过期租约的持有者对事件产生二次副作用。
 */
public record Delivery(long seq, String token, String eventId, String payload, int attempt) {
}
