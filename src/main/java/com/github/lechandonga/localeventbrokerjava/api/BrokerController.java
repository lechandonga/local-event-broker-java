package com.github.lechandonga.localeventbrokerjava.api;

import com.github.lechandonga.localeventbrokerjava.broker.DeadLetter;
import com.github.lechandonga.localeventbrokerjava.broker.Delivery;
import com.github.lechandonga.localeventbrokerjava.broker.EventBroker;
import com.github.lechandonga.localeventbrokerjava.broker.PublishResult;
import com.github.lechandonga.localeventbrokerjava.broker.PublishStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 对外 REST 入口：发布、拉取、确认/消极确认、死信与状态查询。
 */
@RestController
@RequestMapping("/api/broker")
public class BrokerController {

    private final EventBroker broker;

    public BrokerController(EventBroker broker) {
        this.broker = broker;
    }

    public record PublishRequest(String eventId, String payload) {
    }

    public record PublishResponse(String status, long seq) {
    }

    @PostMapping("/topics/{topic}/events")
    public ResponseEntity<PublishResponse> publish(@PathVariable String topic,
                                                   @RequestBody PublishRequest request) {
        PublishResult result = broker.publish(topic, request.eventId(), request.payload());
        HttpStatus http = switch (result.status()) {
            case PUBLISHED -> HttpStatus.CREATED;
            case DUPLICATE -> HttpStatus.OK;
            case FULL -> HttpStatus.TOO_MANY_REQUESTS;
        };
        return ResponseEntity.status(http).body(new PublishResponse(result.status().name(), result.seq()));
    }

    @PostMapping("/topics/{topic}/groups/{group}/poll")
    public List<Delivery> poll(@PathVariable String topic, @PathVariable String group,
                               @RequestParam String consumerId,
                               @RequestParam(defaultValue = "1") int max) {
        return broker.poll(topic, group, consumerId, max);
    }

    public record AckRequest(long seq, String token) {
    }

    @PostMapping("/topics/{topic}/groups/{group}/ack")
    public ResponseEntity<Map<String, Object>> ack(@PathVariable String topic, @PathVariable String group,
                                                   @RequestBody AckRequest request) {
        boolean ok = broker.ack(topic, group, request.seq(), request.token());
        return ok ? ResponseEntity.ok(Map.of("acked", true))
                : ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("acked", false));
    }

    @PostMapping("/topics/{topic}/groups/{group}/nack")
    public ResponseEntity<Map<String, Object>> nack(@PathVariable String topic, @PathVariable String group,
                                                    @RequestBody AckRequest request) {
        boolean ok = broker.nack(topic, group, request.seq(), request.token());
        return ok ? ResponseEntity.ok(Map.of("nacked", true))
                : ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("nacked", false));
    }

    @GetMapping("/topics/{topic}/groups/{group}/dead-letters")
    public List<DeadLetter> deadLetters(@PathVariable String topic, @PathVariable String group) {
        return broker.deadLetters(topic, group);
    }

    @GetMapping("/status")
    public Map<String, EventBroker.TopicStatus> status() {
        return broker.status();
    }
}
