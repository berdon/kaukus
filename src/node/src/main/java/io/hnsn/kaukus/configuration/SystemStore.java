package io.hnsn.kaukus.configuration;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;

import io.hnsn.kaukus.node.state.NodeState;

public interface SystemStore extends Closeable {
    void setIdentifier(String identifier) throws IOException;
    String getIdentifier();

    void setState(NodeState state) throws IOException;
    NodeState getState();

    void setLastStartedAt(LocalDateTime time) throws IOException;
    LocalDateTime getLastStartedAt();

    void setLastVersion(String version) throws IOException;
    String getLastVersion();
}
