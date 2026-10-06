package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 发布结果状态。
 */
public enum PublishStatus {
    /** 已接受并持久化。 */
    PUBLISHED,
    /** 相同 eventId 已存在，幂等去重，未重复入队。 */
    DUPLICATE,
    /** 主题容量已满，发布被拒绝（背压）。 */
    FULL
}
