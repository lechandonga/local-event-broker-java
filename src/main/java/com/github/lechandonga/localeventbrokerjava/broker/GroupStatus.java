package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 消费组实时状态。所有数字均直接由事件日志与消费组状态条目计算得出：
 * pending = 尚未确认的非死信事件；dead = 死信事件；leased = 当前被消费者持有的事件；
 * retryWaiting = 退避等待重投的事件。pending 中包含 leased 与 retryWaiting，三者口径互斥可对账。
 */
public class GroupStatus {
    private String group;
    private long committedOffset;
    private long totalEvents;
    private long pending;
    private int leased;
    private int retryWaiting;
    private int dead;
    private long leaseMillis;

    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }
    public long getCommittedOffset() { return committedOffset; }
    public void setCommittedOffset(long committedOffset) { this.committedOffset = committedOffset; }
    public long getTotalEvents() { return totalEvents; }
    public void setTotalEvents(long totalEvents) { this.totalEvents = totalEvents; }
    public long getPending() { return pending; }
    public void setPending(long pending) { this.pending = pending; }
    public int getLeased() { return leased; }
    public void setLeased(int leased) { this.leased = leased; }
    public int getRetryWaiting() { return retryWaiting; }
    public void setRetryWaiting(int retryWaiting) { this.retryWaiting = retryWaiting; }
    public int getDead() { return dead; }
    public void setDead(int dead) { this.dead = dead; }
    public long getLeaseMillis() { return leaseMillis; }
    public void setLeaseMillis(long leaseMillis) { this.leaseMillis = leaseMillis; }
}
