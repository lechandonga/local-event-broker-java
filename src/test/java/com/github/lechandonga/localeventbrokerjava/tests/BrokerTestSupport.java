package com.github.lechandonga.localeventbrokerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.localeventbrokerjava.broker.BrokerCore;
import com.github.lechandonga.localeventbrokerjava.broker.BrokerProperties;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

/**
 * 直接构造 BrokerCore 的测试基类：每个用例拿到独立临时数据目录，
 * 测试中可以手动模拟“关闭再重建”来验证磁盘恢复。
 */
public abstract class BrokerTestSupport {

    @TempDir
    Path dataDir;

    protected final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    protected BrokerCore newBroker() {
        return newBroker(properties());
    }

    protected BrokerProperties properties() {
        BrokerProperties props = new BrokerProperties();
        props.setDataDir(dataDir.toString());
        props.setDefaultRetryBackoffBase(java.time.Duration.ofMillis(50));
        props.setDefaultRetryBackoffFactor(2.0);
        props.setDefaultRetryBackoffMax(java.time.Duration.ofSeconds(10));
        props.setDefaultLeaseDuration(java.time.Duration.ofSeconds(2));
        props.setDefaultMaxAttempts(3);
        props.setDefaultCapacity(1000);
        return props;
    }

    protected BrokerCore newBroker(BrokerProperties props) {
        BrokerCore core = new BrokerCore(props, mapper);
        core.init();
        return core;
    }
}
