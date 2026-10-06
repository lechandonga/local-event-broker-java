package com.github.lechandonga.localeventbrokerjava.tests;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 统一打印每个测试用例的名称与结果，配合业务日志（发布/投递/确认/重试/回收/死信）
 * 让“跑单测的日志能看出覆盖了哪些业务、每个用例名称和结果”。
 */
public class TestLoggingListener implements TestExecutionListener {

    private static final Logger log = LoggerFactory.getLogger("TEST-COVERAGE");

    @Override
    public void executionStarted(TestIdentifier testIdentifier) {
        if (testIdentifier.isTest()) {
            log.info(">>> 开始用例 >>> {}", displayName(testIdentifier));
        }
    }

    @Override
    public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult result) {
        if (testIdentifier.isTest()) {
            switch (result.getStatus()) {
                case SUCCESSFUL -> log.info("<<< 用例通过 <<< {}", displayName(testIdentifier));
                case FAILED -> log.warn("<<< 用例失败 <<< {} : {}", displayName(testIdentifier),
                        result.getThrowable().map(Throwable::getMessage).orElse(""));
                case ABORTED -> log.warn("<<< 用例中止 <<< {}", displayName(testIdentifier));
            }
        }
    }

    private String displayName(TestIdentifier id) {
        return id.getDisplayName();
    }
}
