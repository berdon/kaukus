package io.hnsn.kaukus.node.configuration;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import com.esotericsoftware.kryo.Kryo;

import io.hnsn.kaukus.persistence.LSMTree;

public class LSMTreeSystemStore implements SystemStore {
    private final Kryo kryo;
    private final LSMTree lsmTree;

    public LSMTreeSystemStore(LSMTree lsmTree, Kryo kryo) {
        this.kryo = kryo;
        this.lsmTree = lsmTree;
    }

    @Override
    public void setLastStartedAt(LocalDateTime time) throws IOException {
        lsmTree.put(KEY_LAST_STARTED_AT, time.atZone(ZoneOffset.UTC).toString());
    }

    @Override
    public LocalDateTime getLastStartedAt() {
        if (!lsmTree.containsKey(KEY_LAST_STARTED_AT)) return null;
        return ZonedDateTime.parse(lsmTree.get(KEY_LAST_STARTED_AT)).toLocalDateTime();
    }

    @Override
    public void close() throws IOException {
        lsmTree.close();
    }

    private static final String KEY_LAST_STARTED_AT = "KEY_LAST_STARTED_AT";
}
