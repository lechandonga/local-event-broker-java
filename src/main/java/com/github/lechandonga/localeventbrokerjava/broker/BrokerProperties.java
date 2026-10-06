package com.github.lechandonga.localeventbrokerjava.broker;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * 本地事件 Broker 的可配置项，前缀 local.broker。
 */
@ConfigurationProperties(prefix = "local.broker")
public class BrokerProperties {

    /** 数据目录：事件日志与消费组状态全部落在这里，服务重启后从该目录恢复。 */
    private String dataDir = "./data/local-event-broker";

    /** 每个主题允许积压（保存）的最大事件数，达到后新的发布请求被明确拒绝（快速失败）。 */
    private int defaultCapacity = 10000;

    /** 单条事件最大投递尝试次数（含首次），超过后进入死信。 */
    private int defaultMaxAttempts = 5;

    /** 失败重试的初始退避时长。 */
    private Duration defaultRetryBackoffBase = Duration.ofSeconds(1);

    /** 退避倍数：第 n 次失败后等待 base * factor^(n-1)。 */
    private double defaultRetryBackoffFactor = 2.0;

    /** 退避上限，防止指数退避无限增长。 */
    private Duration defaultRetryBackoffMax = Duration.ofMinutes(5);

    /** 消费租约时长：拉取后超过该时长仍未确认，事件会被重新投递。 */
    private Duration defaultLeaseDuration = Duration.ofSeconds(30);

    /** 过期租约扫描周期。 */
    private Duration reaperInterval = Duration.ofSeconds(2);

    /** 每个主题单独覆盖容量配置，key 为主题名。 */
    private Map<String, TopicOverride> topics = new HashMap<>();

    /** 每个消费组单独覆盖重试与租约配置，key 为 group@topic。 */
    private Map<String, GroupOverride> groups = new HashMap<>();

    public int capacityOf(String topic) {
        TopicOverride override = topics.get(topic);
        return override != null && override.getCapacity() > 0 ? override.getCapacity() : defaultCapacity;
    }

    public int maxAttemptsOf(String topic, String group) {
        GroupOverride override = groups.get(group + "@" + topic);
        return override != null && override.getMaxAttempts() > 0 ? override.getMaxAttempts() : defaultMaxAttempts;
    }

    public Duration leaseOf(String topic, String group) {
        GroupOverride override = groups.get(group + "@" + topic);
        return override != null && override.getLeaseDuration() != null ? override.getLeaseDuration() : defaultLeaseDuration;
    }

    public Duration backoffBaseOf(String topic, String group) {
        GroupOverride override = groups.get(group + "@" + topic);
        return override != null && override.getRetryBackoffBase() != null
                ? override.getRetryBackoffBase() : defaultRetryBackoffBase;
    }

    public double backoffFactorOf(String topic, String group) {
        GroupOverride override = groups.get(group + "@" + topic);
        return override != null && override.getRetryBackoffFactor() > 0
                ? override.getRetryBackoffFactor() : defaultRetryBackoffFactor;
    }

    public Duration backoffMaxOf(String topic, String group) {
        GroupOverride override = groups.get(group + "@" + topic);
        return override != null && override.getRetryBackoffMax() != null
                ? override.getRetryBackoffMax() : defaultRetryBackoffMax;
    }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getDefaultCapacity() { return defaultCapacity; }
    public void setDefaultCapacity(int defaultCapacity) { this.defaultCapacity = defaultCapacity; }
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }
    public Duration getDefaultRetryBackoffBase() { return defaultRetryBackoffBase; }
    public void setDefaultRetryBackoffBase(Duration defaultRetryBackoffBase) { this.defaultRetryBackoffBase = defaultRetryBackoffBase; }
    public double getDefaultRetryBackoffFactor() { return defaultRetryBackoffFactor; }
    public void setDefaultRetryBackoffFactor(double defaultRetryBackoffFactor) { this.defaultRetryBackoffFactor = defaultRetryBackoffFactor; }
    public Duration getDefaultRetryBackoffMax() { return defaultRetryBackoffMax; }
    public void setDefaultRetryBackoffMax(Duration defaultRetryBackoffMax) { this.defaultRetryBackoffMax = defaultRetryBackoffMax; }
    public Duration getDefaultLeaseDuration() { return defaultLeaseDuration; }
    public void setDefaultLeaseDuration(Duration defaultLeaseDuration) { this.defaultLeaseDuration = defaultLeaseDuration; }
    public Duration getReaperInterval() { return reaperInterval; }
    public void setReaperInterval(Duration reaperInterval) { this.reaperInterval = reaperInterval; }
    public Map<String, TopicOverride> getTopics() { return topics; }
    public void setTopics(Map<String, TopicOverride> topics) { this.topics = topics; }
    public Map<String, GroupOverride> getGroups() { return groups; }
    public void setGroups(Map<String, GroupOverride> groups) { this.groups = groups; }

    public static class TopicOverride {
        private int capacity;
        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }
    }

    public static class GroupOverride {
        private Integer maxAttempts;
        private Duration leaseDuration;
        private Duration retryBackoffBase;
        private Double retryBackoffFactor;
        private Duration retryBackoffMax;
        public Integer getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(Integer maxAttempts) { this.maxAttempts = maxAttempts; }
        public Duration getLeaseDuration() { return leaseDuration; }
        public void setLeaseDuration(Duration leaseDuration) { this.leaseDuration = leaseDuration; }
        public Duration getRetryBackoffBase() { return retryBackoffBase; }
        public void setRetryBackoffBase(Duration retryBackoffBase) { this.retryBackoffBase = retryBackoffBase; }
        public Double getRetryBackoffFactor() { return retryBackoffFactor; }
        public void setRetryBackoffFactor(Double retryBackoffFactor) { this.retryBackoffFactor = retryBackoffFactor; }
        public Duration getRetryBackoffMax() { return retryBackoffMax; }
        public void setRetryBackoffMax(Duration retryBackoffMax) { this.retryBackoffMax = retryBackoffMax; }
    }
}
