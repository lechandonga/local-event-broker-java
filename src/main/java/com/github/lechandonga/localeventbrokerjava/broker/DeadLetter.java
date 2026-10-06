package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 一条死信记录。
 */
public record DeadLetter(long seq, String eventId, String payload, DeadReason reason,
                         int attempts, long deadAt) {
}
