package io.hnsn.kaukus.node.agents;

import java.io.Closeable;

public interface Agent extends Closeable {
    void start() throws AgentException;
}
