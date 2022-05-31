package io.hnsn.kaukus.node.agents.server;

import io.hnsn.kaukus.node.agents.connection.Connection;

public interface OnClientDisconnectedListener {
    void onDisconnected(Connection connection);
}
