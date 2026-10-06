package com.github.lechandonga.localeventbrokerjava.web;

import com.github.lechandonga.localeventbrokerjava.broker.BrokerCore;
import com.github.lechandonga.localeventbrokerjava.broker.DeadLetterView;
import com.github.lechandonga.localeventbrokerjava.broker.EventRecord;
import com.github.lechandonga.localeventbrokerjava.broker.GroupStatus;
import com.github.lechandonga.localeventbrokerjava.broker.PullResult;
import com.github.lechandonga.localeventbrokerjava.broker.PublishOutcome;
import com.github.lechandonga.localeventbrokerjava.broker.TopicStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/broker")
public class BrokerController {

    private final BrokerCore brokerCore;

    public BrokerController(BrokerCore brokerCore) {
        this.brokerCore = brokerCore;
    }

    /** 发布事件：POST /api/broker/topics/{topic}/events */
    @PostMapping("/topics/{topic}/events")
    public PublishResponse publish(@PathVariable String topic, @RequestBody PublishRequest request) {
        PublishOutcome outcome = brokerCore.publish(topic, request.getEventKey(), request.getPayload());
        return new PublishResponse(topic, outcome.offset(), outcome.duplicated());
    }

    /** 消费组拉取：POST /api/broker/topics/{topic}/groups/{group}/pull */
    @PostMapping("/topics/{topic}/groups/{group}/pull")
    public List<DeliveryResponse> pull(@PathVariable String topic,
                                       @PathVariable String group,
                                       @RequestBody(required = false) PullRequest request) {
        int max = request == null ? 1 : request.getMaxMessages();
        return brokerCore.pull(topic, group, max).stream().map(BrokerController::toResponse).toList();
    }

    /** 确认处理成功：POST /api/broker/topics/{topic}/groups/{group}/ack */
    @PostMapping("/topics/{topic}/groups/{group}/ack")
    public AckResponse ack(@PathVariable String topic,
                           @PathVariable String group,
                           @RequestBody AckRequest request) {
        boolean accepted = brokerCore.acknowledge(topic, group, request.getOffset(), request.getDeliveryToken());
        return new AckResponse(accepted, accepted ? "acknowledged" : "rejected: already confirmed or stale token");
    }

    /** 声明业务处理失败（触发重试/死信）：POST /api/broker/topics/{topic}/groups/{group}/nack */
    @PostMapping("/topics/{topic}/groups/{group}/nack")
    public AckResponse nack(@PathVariable String topic,
                            @PathVariable String group,
                            @RequestBody AckRequest request) {
        boolean accepted = brokerCore.negativeAcknowledge(
                topic, group, request.getOffset(), request.getDeliveryToken(), request.getError());
        return new AckResponse(accepted, accepted ? "negative-acknowledged" : "rejected: already confirmed or stale token");
    }

    /** 主题状态（总量、容量、各消费组进度）：GET /api/broker/topics/{topic}/status */
    @GetMapping("/topics/{topic}/status")
    public TopicStatus topicStatus(@PathVariable String topic) {
        return brokerCore.statusOfTopic(topic);
    }

    /** 消费组状态：GET /api/broker/topics/{topic}/groups/{group}/status */
    @GetMapping("/topics/{topic}/groups/{group}/status")
    public GroupStatus groupStatus(@PathVariable String topic, @PathVariable String group) {
        return brokerCore.statusOfGroup(topic, group);
    }

    /** 死信查询：GET /api/broker/topics/{topic}/groups/{group}/dead-letters */
    @GetMapping("/topics/{topic}/groups/{group}/dead-letters")
    public List<DeadLetterView> deadLetters(@PathVariable String topic, @PathVariable String group) {
        return brokerCore.listDeadLetters(topic, group);
    }

    private static DeliveryResponse toResponse(PullResult result) {
        EventRecord event = result.event();
        return new DeliveryResponse(event.getOffset(), event.getEventKey(), event.getPayload(),
                result.deliveryToken(), result.expiresAt(), result.attempt());
    }
}
