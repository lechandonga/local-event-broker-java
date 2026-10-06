package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 发布结果。seq 仅在 PUBLISHED 时有意义。
 */
public record PublishResult(PublishStatus status, long seq) {
}
