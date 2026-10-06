package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 主题内持久化的一条事件。
 *
 * @param seq       主题内单调递增序号
 * @param eventId   业务侧事件 ID，用于发布去重
 * @param payload   事件内容
 * @param timestamp 发布时间（epoch millis）
 */
public record EventRecord(long seq, String eventId, String payload, long timestamp) {
}
