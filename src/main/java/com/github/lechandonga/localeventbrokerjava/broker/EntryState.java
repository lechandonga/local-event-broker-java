package com.github.lechandonga.localeventbrokerjava.broker;

/**
 * 事件在某个消费组中的处理状态。
 * <ul>
 *   <li>LEASED：已投递给某个消费者并持有租约，同一时刻其它消费者拿不到；</li>
 *   <li>RETRY_WAIT：业务失败（NACK）或租约超时，正在按退避时间等待重新投递；</li>
 *   <li>DEAD：重试次数达到上限，进入死信，不再自动投递。</li>
 * </ul>
 */
public enum EntryState {
    LEASED,
    RETRY_WAIT,
    DEAD
}
