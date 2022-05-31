package io.hnsn.kaukus.node.agents.client;

import io.hnsn.kaukus.node.agents.connection.Connection;

public interface OnConnectionEstablishedListener {
    void onConnected(Connection connection);
}
