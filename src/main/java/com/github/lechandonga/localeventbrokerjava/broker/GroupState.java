package com.github.lechandonga.localeventbrokerjava.broker;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一个消费组在一个主题上的完整消费状态，整体序列化为一个 JSON 文件并原子改写。
 */
public class GroupState {

    private String topic;
    private String group;
    /** 已连续确认的最大偏移量；小于等于该值的事件保证不会再被投递。 */
    private long committedOffset = -1;
    /** 已投递给该组的事件中最大的偏移量；该偏移之前（含）都至少投递过一次。初始 -1。 */
    private long deliveredHighWatermark = -1;
    /** offset -> 条目；仅保存 committedOffset 之后、尚未终结的事件，压缩控制文件体积。 */
    private Map<Long, GroupEntry> entries = new LinkedHashMap<>();

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }
    public long getCommittedOffset() { return committedOffset; }
    public void setCommittedOffset(long committedOffset) { this.committedOffset = committedOffset; }
    public long getDeliveredHighWatermark() { return deliveredHighWatermark; }
    public void setDeliveredHighWatermark(long deliveredHighWatermark) { this.deliveredHighWatermark = deliveredHighWatermark; }
    public Map<Long, GroupEntry> getEntries() { return entries; }
    public void setEntries(Map<Long, GroupEntry> entries) { this.entries = entries; }
}
