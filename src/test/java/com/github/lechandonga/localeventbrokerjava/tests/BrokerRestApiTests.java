package com.github.lechandonga.localeventbrokerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.junit.jupiter.api.BeforeEach;

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 业务覆盖：HTTP 入口端到端——发布（含幂等与 429 背压）、拉取、确认、重试/死信、状态与死信查询。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@DisplayName("场景六：REST 入口端到端（发布/拉取/确认/重试/死信/状态/背压）")
class BrokerRestApiTests {

    private static final Logger log = LoggerFactory.getLogger(BrokerRestApiTests.class);
    private static final Path DATA_DIR;

    static {
        try {
            DATA_DIR = java.nio.file.Files.createTempDirectory("rest-broker-data-");
        } catch (java.io.IOException e) {
            throw new ExceptionInInitializerError(e);
        }
        DATA_DIR.toFile().deleteOnExit();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("local.broker.data-dir", () -> DATA_DIR.toAbsolutePath().toString());
        registry.add("local.broker.default-capacity", () -> "5");
        registry.add("local.broker.default-max-attempts", () -> "2");
        registry.add("local.broker.default-retry-backoff-base", () -> "20ms");
        registry.add("local.broker.default-lease-duration", () -> "60m");
    }

    @Autowired
    private WebApplicationContext context;
    private MockMvc mockMvc;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("完整 HTTP 链路：发布去重 -> 拉取 -> NACK 重试 -> 再失败进死信 -> 状态/死信查询一致 -> 容量背压 429")
    void fullHttpLifecycle() throws Exception {
        // 发布
        mockMvc.perform(post("/api/broker/topics/api-topic/events")
                        .contentType("application/json")
                        .content("{\"eventKey\":\"evt-1\",\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.offset").value(0))
                .andExpect(jsonPath("$.duplicated").value(false));
        // 重复发布
        mockMvc.perform(post("/api/broker/topics/api-topic/events")
                        .contentType("application/json")
                        .content("{\"eventKey\":\"evt-1\",\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicated").value(true))
                .andExpect(jsonPath("$.offset").value(0));

        // 拉取
        String pullBody = mockMvc.perform(post("/api/broker/topics/api-topic/groups/grp/pull")
                        .contentType("application/json")
                        .content("{\"maxMessages\":1}"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        JsonNode deliveries = mapper.readTree(pullBody);
        String token = deliveries.get(0).get("deliveryToken").asText();

        // 第一次失败
        mockMvc.perform(post("/api/broker/topics/api-topic/groups/grp/nack")
                        .contentType("application/json")
                        .content("{\"offset\":0,\"deliveryToken\":\"" + token + "\",\"error\":\"http-fail-1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true));
        Thread.sleep(40);
        // 第二次投递再失败（maxAttempts=2）-> 死信
        String pullBody2 = mockMvc.perform(post("/api/broker/topics/api-topic/groups/grp/pull")
                        .contentType("application/json").content("{\"maxMessages\":1}"))
                .andReturn().getResponse().getContentAsString();
        String token2 = mapper.readTree(pullBody2).get(0).get("deliveryToken").asText();
        mockMvc.perform(post("/api/broker/topics/api-topic/groups/grp/nack")
                        .contentType("application/json")
                        .content("{\"offset\":0,\"deliveryToken\":\"" + token2 + "\",\"error\":\"http-fail-2\"}"))
                .andExpect(jsonPath("$.accepted").value(true));

        // 状态与死信查询
        mockMvc.perform(get("/api/broker/topics/api-topic/groups/grp/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.dead").value(1))
                .andExpect(jsonPath("$.pending").value(0));
        mockMvc.perform(get("/api/broker/topics/api-topic/groups/grp/dead-letters"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].offset").value(0))
                .andExpect(jsonPath("$[0].reason").value("BUSINESS_ERROR"))
                .andExpect(jsonPath("$[0].lastError").value("http-fail-2"));

        // 容量背压：窗口中仍有 1 条（死信占用，不能被确认），再发 4 条后达到容量 5，第 6 条发布返回 429
        // 窗口当前 1 条（死信），再补 4 条达到容量 5；第 5 条（第 6 次发布）返回 429
        for (int i = 2; i <= 5; i++) {
            mockMvc.perform(post("/api/broker/topics/api-topic/events")
                            .contentType("application/json")
                            .content("{\"eventKey\":\"evt-" + i + "\",\"payload\":\"p" + i + "\"}"))
                    .andExpect(status().isOk());
        }
        mockMvc.perform(post("/api/broker/topics/api-topic/events")
                        .contentType("application/json")
                        .content("{\"eventKey\":\"evt-over\",\"payload\":\"x\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.error").value("CAPACITY_EXCEEDED"));

        // 过期令牌确认被拒绝
        mockMvc.perform(post("/api/broker/topics/api-topic/groups/grp/ack")
                        .contentType("application/json")
                        .content("{\"offset\":0,\"deliveryToken\":\"" + token + "\"}"))
                .andExpect(jsonPath("$.accepted").value(false));

        // 主题状态
        mockMvc.perform(get("/api/broker/topics/api-topic/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.capacity").value(5))
                .andExpect(jsonPath("$.totalEvents").value(5))
                .andExpect(jsonPath("$.groups[0].dead").value(1));
        log.info("【断言通过】HTTP 全链路：幂等发布、NACK 重试、BUSINESS_ERROR 死信、429 背压、旧令牌拒绝、状态对账");
    }
}
