package com.github.lechandonga.localeventbrokerjava.tests;

/** 测试辅助：用 try-with-resources 表达 Broker 生命周期（本实现为纯内存对象，关闭仅作语义占位）。 */
class BrokerCoreRunner implements AutoCloseable {

    private final com.github.lechandonga.localeventbrokerjava.broker.BrokerCore core;

    BrokerCoreRunner(com.github.lechandonga.localeventbrokerjava.broker.BrokerCore core) {
        this.core = core;
    }

    com.github.lechandonga.localeventbrokerjava.broker.BrokerCore get() {
        return core;
    }

    @Override
    public void close() {
        // 数据每次变更都已同步落盘，无需额外关闭动作
    }
}
