package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 一次成功投递：携带事件内容与本次投递的令牌。
 */
public record PullResult(EventRecord event, String deliveryToken, long expiresAt, int attempt) {
}
