package com.github.lechandonga.localeventbrokerjava.broker;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地事件 Broker 的关键配置项。
 */
@ConfigurationProperties(prefix = "broker")
public class BrokerProperties {

    /** 本地持久化目录，重启后从该目录恢复状态。 */
    private String dataDir = "data/broker";

    /** 单主题容量上限（未被所有消费组处理完的事件数），达到上限后发布返回 FULL。 */
    private int topicCapacity = 10_000;

    /** 单事件最大投递次数（含首次），超过后进入死信。 */
    private int maxRetries = 3;

    /** 重试基础退避毫秒数，第 n 次重试等待 backoffMs * n。 */
    private long retryBackoffMs = 1_000;

    /** 消费租约（可见性超时）毫秒数，超时未确认的事件会被重新投递。 */
    private long leaseTimeoutMs = 30_000;

    /** 租约过期扫描间隔毫秒数。 */
    private long reaperIntervalMs = 1_000;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getTopicCapacity() {
        return topicCapacity;
    }

    public void setTopicCapacity(int topicCapacity) {
        this.topicCapacity = topicCapacity;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(long retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    public long getLeaseTimeoutMs() {
        return leaseTimeoutMs;
    }

    public void setLeaseTimeoutMs(long leaseTimeoutMs) {
        this.leaseTimeoutMs = leaseTimeoutMs;
    }

    public long getReaperIntervalMs() {
        return reaperIntervalMs;
    }

    public void setReaperIntervalMs(long reaperIntervalMs) {
        this.reaperIntervalMs = reaperIntervalMs;
    }
}
