package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 单条事件在单个消费组内的状态条目，会随消费组状态文件一起持久化。
 */
public class GroupEntry {

    private long offset;
    private EntryState state;
    /** 已投递次数（首次投递即为 1）。 */
    private int attempts;
    /** 当前持有的投递令牌；每次重新投递都会更换，旧令牌的 ACK/NACK 会被拒绝。 */
    private String deliveryToken;
    private long leasedAt;
    private long expiresAt;
    /** RETRY_WAIT 状态下最早可再次投递的时间（epoch millis）。 */
    private long nextVisibleAt;
    private FailureReason lastFailureReason;
    private String lastError;
    private long deadAt;

    public GroupEntry() {
    }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public EntryState getState() { return state; }
    public void setState(EntryState state) { this.state = state; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public String getDeliveryToken() { return deliveryToken; }
    public void setDeliveryToken(String deliveryToken) { this.deliveryToken = deliveryToken; }
    public long getLeasedAt() { return leasedAt; }
    public void setLeasedAt(long leasedAt) { this.leasedAt = leasedAt; }
    public long getExpiresAt() { return expiresAt; }
    public void setExpiresAt(long expiresAt) { this.expiresAt = expiresAt; }
    public long getNextVisibleAt() { return nextVisibleAt; }
    public void setNextVisibleAt(long nextVisibleAt) { this.nextVisibleAt = nextVisibleAt; }
    public FailureReason getLastFailureReason() { return lastFailureReason; }
    public void setLastFailureReason(FailureReason lastFailureReason) { this.lastFailureReason = lastFailureReason; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public long getDeadAt() { return deadAt; }
    public void setDeadAt(long deadAt) { this.deadAt = deadAt; }
}
