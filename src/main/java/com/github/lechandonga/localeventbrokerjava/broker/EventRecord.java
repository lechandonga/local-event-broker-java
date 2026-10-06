package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 主题日志中的一条事件。offset 为主题内单调递增的偏移量（从 0 开始）。
 */
public class EventRecord {

    private long offset;
    /** 业务幂等键：同一主题内重复发布相同 key 只保留第一条，不会被重复消费。 */
    private String eventKey;
    private String payload;
    private long publishedAt;

    public EventRecord() {
    }

    public EventRecord(long offset, String eventKey, String payload, long publishedAt) {
        this.offset = offset;
        this.eventKey = eventKey;
        this.payload = payload;
        this.publishedAt = publishedAt;
    }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public String getEventKey() { return eventKey; }
    public void setEventKey(String eventKey) { this.eventKey = eventKey; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public long getPublishedAt() { return publishedAt; }
    public void setPublishedAt(long publishedAt) { this.publishedAt = publishedAt; }
}
