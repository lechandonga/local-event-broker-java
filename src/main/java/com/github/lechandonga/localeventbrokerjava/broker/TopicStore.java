package com.github.lechandonga.localeventbrokerjava.broker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 单个主题的持久化存储：
 * events-<topic>.log 为仅追加的 JSON Lines 文件，每次发布 force 落盘；
 * meta-<topic>.json 记录当前日志窗口的起始偏移 baseOffset。
 *
 * 当日志中所有事件都已被该主题的全部消费组连续确认后，前缀会被截断删除，
 * 因此“容量”表示主题允许积压的事件窗口大小，消费确认后窗口释放、可以继续发布。
 * 截断采用“先写新文件再原子替换”，配合 meta 重放，崩溃恢复时：
 * meta 领先则跳过日志中已过期的前缀，日志领先则以日志首条偏移修正 meta，两种情况都能自愈。
 */
public class TopicStore {

    private final String topic;
    private final Path logFile;
    private final Path metaFile;
    private final ObjectMapper mapper;

    private final List<EventRecord> events = new ArrayList<>();
    private final Set<String> dedupKeys = new HashSet<>();
    private long baseOffset = 0;

    public TopicStore(String topic, Path dataDir, ObjectMapper mapper) {
        this.topic = topic;
        this.logFile = dataDir.resolve("events-" + topic + ".log");
        this.metaFile = dataDir.resolve("meta-" + topic + ".json");
        this.mapper = mapper;
    }

    public synchronized void load() {
        events.clear();
        dedupKeys.clear();
        baseOffset = readBaseOffset();
        if (!Files.exists(logFile)) {
            return;
        }
        List<EventRecord> loaded = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(logFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    loaded.add(mapper.readValue(line, EventRecord.class));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("恢复主题 " + topic + " 的事件日志失败", e);
        }
        if (loaded.isEmpty()) {
            return;
        }
        long firstOffset = loaded.get(0).getOffset();
        if (firstOffset > baseOffset) {
            // 日志已截断但 meta 落后：以日志为准修正 meta。
            baseOffset = firstOffset;
            writeMeta();
        }
        for (EventRecord record : loaded) {
            if (record.getOffset() < baseOffset) {
                // meta 领先但日志尚未完成截断：跳过过期前缀，稍后重写压缩。
                continue;
            }
            if (record.getOffset() != baseOffset + events.size()) {
                throw new IllegalStateException("主题 " + topic + " 的事件日志偏移不连续，无法恢复: "
                        + record.getOffset());
            }
            events.add(record);
            if (record.getEventKey() != null) {
                dedupKeys.add(record.getEventKey());
            }
        }
        if (loaded.size() != events.size()) {
            rewriteLog();
        }
    }

    private long readBaseOffset() {
        if (!Files.exists(metaFile)) {
            return 0;
        }
        try {
            Meta meta = mapper.readValue(Files.readAllBytes(metaFile), Meta.class);
            return meta.baseOffset == null ? 0 : Math.max(0, meta.baseOffset);
        } catch (IOException e) {
            throw new UncheckedIOException("读取主题 " + topic + " 的 meta 失败", e);
        }
    }

    private void writeMeta() {
        Path tmp = metaFile.resolveSibling(metaFile.getFileName() + ".tmp");
        try {
            byte[] bytes = mapper.writeValueAsBytes(new Meta(baseOffset));
            Files.write(tmp, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
            Files.move(tmp, metaFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("写入主题 " + topic + " 的 meta 失败", e);
        }
    }

    /**
     * 追加事件。duplicated=true 表示命中幂等键、没有产生新事件。
     */
    public synchronized PublishResult append(String eventKey, String payload) {
        if (eventKey != null && dedupKeys.contains(eventKey)) {
            return new PublishResult(findByKey(eventKey).getOffset(), true);
        }
        EventRecord record = new EventRecord(nextOffset(), eventKey, payload, System.currentTimeMillis());
        byte[] line;
        try {
            line = (mapper.writeValueAsString(record) + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (FileChannel ch = FileChannel.open(logFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ch.write(java.nio.ByteBuffer.wrap(line));
            ch.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("写入主题 " + topic + " 的事件日志失败", e);
        }
        events.add(record);
        if (eventKey != null) {
            dedupKeys.add(eventKey);
        }
        return new PublishResult(record.getOffset(), false);
    }

    /**
     * 删除所有 offset 小于 retainFromOffset 的事件前缀（它们已被全部消费组连续确认）。
     * 先更新 meta 再原子替换日志，任一步崩溃都可在 {@link #load()} 中自愈。
     */
    public synchronized void evictBefore(long retainFromOffset) {
        if (retainFromOffset <= baseOffset || events.isEmpty()) {
            return;
        }
        baseOffset = retainFromOffset;
        writeMeta();
        rewriteLog();
    }

    private void rewriteLog() {
        Path tmp = logFile.resolveSibling(logFile.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (EventRecord record : events) {
                if (record.getOffset() < baseOffset) {
                    continue;
                }
                byte[] line = (mapper.writeValueAsString(record) + System.lineSeparator())
                        .getBytes(StandardCharsets.UTF_8);
                ch.write(java.nio.ByteBuffer.wrap(line));
            }
            ch.force(false);
        } catch (IOException e) {
            throw new UncheckedIOException("压缩主题 " + topic + " 的事件日志失败", e);
        }
        try {
            Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("替换主题 " + topic + " 的事件日志失败", e);
        }
        dedupKeys.clear();
        events.removeIf(record -> {
            if (record.getOffset() < baseOffset) {
                return true;
            }
            if (record.getEventKey() != null) {
                dedupKeys.add(record.getEventKey());
            }
            return false;
        });
    }

    private EventRecord findByKey(String eventKey) {
        for (EventRecord event : events) {
            if (eventKey.equals(event.getEventKey())) {
                return event;
            }
        }
        throw new IllegalStateException("幂等索引与事件日志不一致: " + eventKey);
    }

    public synchronized EventRecord get(long offset) {
        int index = (int) (offset - baseOffset);
        if (index < 0 || index >= events.size()) {
            return null;
        }
        return events.get(index);
    }

    /** 当前日志窗口中的事件数（即主题当前积压窗口大小）。 */
    public synchronized int size() {
        return events.size();
    }

    public synchronized long baseOffset() {
        return baseOffset;
    }

    /** 下一条事件将使用的绝对偏移。 */
    public synchronized long nextOffset() {
        return baseOffset + events.size();
    }

    public String getTopic() {
        return topic;
    }

    public record PublishResult(long offset, boolean duplicated) {
    }

    private static final class Meta {
        @JsonProperty("baseOffset")
        private Long baseOffset;

        Meta() {
        }

        Meta(long baseOffset) {
            this.baseOffset = baseOffset;
        }
    }
}
