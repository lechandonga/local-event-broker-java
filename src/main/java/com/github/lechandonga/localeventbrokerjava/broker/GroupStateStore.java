package com.github.lechandonga.localeventbrokerjava.broker;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 消费组状态文件读写：整体 JSON 持久化，写入采用临时文件 + 原子 move，
 * 保证崩溃时要么读到旧状态、要么读到新状态，不会读到半截文件。
 */
public class GroupStateStore {

    private final Path stateFile;
    private final ObjectMapper mapper;

    public GroupStateStore(Path dataDir, String topic, String group, ObjectMapper mapper) {
        this.stateFile = dataDir.resolve("state-" + topic + "-" + group + ".json");
        this.mapper = mapper;
    }

    public GroupState loadOrCreate(String topic, String group) {
        if (!Files.exists(stateFile)) {
            GroupState state = new GroupState();
            state.setTopic(topic);
            state.setGroup(group);
            return state;
        }
        try {
            byte[] bytes = Files.readAllBytes(stateFile);
            if (bytes.length == 0) {
                GroupState state = new GroupState();
                state.setTopic(topic);
                state.setGroup(group);
                return state;
            }
            return mapper.readValue(bytes, GroupState.class);
        } catch (IOException e) {
            throw new UncheckedIOException("恢复消费组状态失败: " + stateFile, e);
        }
    }

    public synchronized void persist(GroupState state) {
        Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
        try {
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
            try (var out = Files.newOutputStream(tmp)) {
                out.write(bytes);
                out.flush();
                if (out instanceof java.io.FileOutputStream fos) {
                    fos.getFD().sync();
                }
            }
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // 忽略临时文件清理失败
            }
            throw new UncheckedIOException("持久化消费组状态失败: " + stateFile, e);
        }
    }

    public Path path() {
        return stateFile;
    }
}
