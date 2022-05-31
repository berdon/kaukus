package io.hnsn.kaukus.node.agents.server;

import io.hnsn.kaukus.node.agents.connection.Connection;

public interface OnClientConnectedListener {
    void onConnected(Connection connection);
}
