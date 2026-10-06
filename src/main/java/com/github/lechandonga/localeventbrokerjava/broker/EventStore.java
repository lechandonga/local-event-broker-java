package com.github.lechandonga.localeventbrokerjava.broker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 本地文件持久化：
 * <ul>
 *   <li>事件日志：{@code <dataDir>/<topic>/events.jsonl}，追加写，重启后重放。</li>
 *   <li>消费组状态：{@code <dataDir>/<topic>/groups/<group>.json}，整体快照原子替换。</li>
 * </ul>
 */
@Component
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final Path dataDir;

    public EventStore(BrokerProperties properties) {
        this.dataDir = Path.of(properties.getDataDir());
    }

    public synchronized void appendEvent(String topic, EventRecord event) {
        try {
            Path topicDir = topicDir(topic);
            Files.createDirectories(topicDir);
            String line = mapper.writeValueAsString(event) + "\n";
            Files.writeString(topicDir.resolve("events.jsonl"), line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化事件失败 topic=" + topic, e);
        }
    }

    public List<EventRecord> loadEvents(String topic) {
        Path file = topicDir(topic).resolve("events.jsonl");
        List<EventRecord> events = new ArrayList<>();
        if (!Files.exists(file)) {
            return events;
        }
        try (var lines = Files.lines(file, StandardCharsets.UTF_8)) {
            lines.filter(l -> !l.isBlank()).forEach(l -> {
                try {
                    events.add(mapper.readValue(l, EventRecord.class));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("加载事件失败 topic=" + topic, e);
        }
        return events;
    }

    public synchronized void saveGroupState(String topic, String group, GroupStateSnapshot state) {
        try {
            Path groupDir = topicDir(topic).resolve("groups");
            Files.createDirectories(groupDir);
            Path target = groupDir.resolve(group + ".json");
            Path tmp = groupDir.resolve(group + ".json.tmp");
            Files.writeString(tmp, mapper.writeValueAsString(state), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("持久化消费组状态失败 topic=" + topic + " group=" + group, e);
        }
    }

    public GroupStateSnapshot loadGroupState(String topic, String group) {
        Path file = topicDir(topic).resolve("groups").resolve(group + ".json");
        if (!Files.exists(file)) {
            return GroupStateSnapshot.empty();
        }
        try {
            return mapper.readValue(Files.readString(file, StandardCharsets.UTF_8), GroupStateSnapshot.class);
        } catch (IOException e) {
            throw new UncheckedIOException("加载消费组状态失败 topic=" + topic + " group=" + group, e);
        }
    }

    public List<String> listTopics() {
        if (!Files.isDirectory(dataDir)) {
            return List.of();
        }
        try (var stream = Files.list(dataDir)) {
            return stream.filter(Files::isDirectory).map(p -> p.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<String> listGroups(String topic) {
        Path groupDir = topicDir(topic).resolve("groups");
        if (!Files.isDirectory(groupDir)) {
            return List.of();
        }
        try (var stream = Files.list(groupDir)) {
            return stream.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".json"))
                    .map(n -> n.substring(0, n.length() - ".json".length()))
                    .sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path topicDir(String topic) {
        return dataDir.resolve(sanitize(topic));
    }

    private String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /**
     * 消费组持久化快照：已确认序号、各序号已投递次数、死信。
     */
    public record GroupStateSnapshot(List<Long> acked, java.util.Map<Long, Integer> attempts,
                                     List<DeadLetter> dead) {
        static GroupStateSnapshot empty() {
            return new GroupStateSnapshot(new ArrayList<>(), new java.util.HashMap<>(), new ArrayList<>());
        }
    }
}
