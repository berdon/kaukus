package io.hnsn.kaukus.configuration;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import io.hnsn.kaukus.node.state.NodeState;
import io.hnsn.kaukus.persistence.LSMTree;

public class LSMTreeSystemStore implements SystemStore {
    private static final String KEY_IDENTIFIER = "KEY_IDENTIFIER";
    private static final String KEY_STATE = "KEY_STATE";
    private static final String KEY_LAST_STARTED_AT = "KEY_LAST_STARTED_AT";
    private static final String KEY_LAST_VERSION = "KEY_LAST_VERSION";

    private final LSMTree lsmTree;

    public LSMTreeSystemStore(LSMTree lsmTree) {
        this.lsmTree = lsmTree;
    }

    @Override
    public synchronized void setIdentifier(String identifier) throws IOException {
        lsmTree.put(KEY_IDENTIFIER, identifier);
    }

    @Override
    public synchronized String getIdentifier() {
        return lsmTree.get(KEY_IDENTIFIER);
    }

    @Override
    public synchronized void setState(NodeState state) throws IOException {
        lsmTree.put(KEY_STATE, state.toString());
    }

    @Override
    public synchronized NodeState getState() {
        // TODO: Improve get/default perf
        if (!lsmTree.containsKey(KEY_STATE)) return null;
        return Enum.valueOf(NodeState.class, lsmTree.get(KEY_STATE));
    }

    @Override
    public synchronized void setLastStartedAt(LocalDateTime time) throws IOException {
        lsmTree.put(KEY_LAST_STARTED_AT, time.atZone(ZoneOffset.UTC).toString());
    }

    @Override
    public synchronized LocalDateTime getLastStartedAt() {
        // TODO: Improve get/default perf
        if (!lsmTree.containsKey(KEY_LAST_STARTED_AT)) return null;
        return ZonedDateTime.parse(lsmTree.get(KEY_LAST_STARTED_AT)).toLocalDateTime();
    }

    @Override
    public void setLastVersion(String version) throws IOException {
        lsmTree.put(KEY_LAST_VERSION, version);
    }

    @Override
    public String getLastVersion() {
        return lsmTree.get(KEY_LAST_VERSION);
    }

    @Override
    public synchronized void close() throws IOException {
        lsmTree.close();
    }
}
