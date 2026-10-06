package com.github.lechandonga.localeventbrokerjava.web;

public class PublishRequest {
    /** 业务幂等键，可选；同一主题内相同 key 的重复发布只会产生一条事件。 */
    private String eventKey;
    private String payload;

    public String getEventKey() { return eventKey; }
    public void setEventKey(String eventKey) { this.eventKey = eventKey; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}
