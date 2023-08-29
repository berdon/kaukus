package io.hnsn.kaukus.node.agents.client;

import io.hnsn.kaukus.node.agents.Agent;
import io.hnsn.kaukus.node.agents.connection.Connection;

public interface ClientAgent extends Agent {
    Connection connectToNode(String targetNodeIdentifier, String address, int port);

    void registerOnConnectionEstablishedListener(OnConnectionEstablishedListener listener);
    void unregisterOnConnectionEstablishedListener(OnConnectionEstablishedListener listener);

    void registerOnConnectionClosedListener(OnConnectionClosedListener listener);
    void unregisterOnConnectionClosedListener(OnConnectionClosedListener listener);
}
