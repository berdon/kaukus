package io.hnsn.kaukus.node.agents.server;

import java.net.InetAddress;

import io.hnsn.kaukus.node.agents.Agent;

public interface ServerAgent extends Agent {
    InetAddress getBoundAddress();
    int getBoundPort();

    void registerOnClientConnectedListener(OnClientConnectedListener listener);
    void unregisterOnClientConnectedListener(OnClientConnectedListener listener);

    void registerOnClientDisconnectedListener(OnClientDisconnectedListener listener);
    void unregisterOnClientDisconnectedListener(OnClientDisconnectedListener listener);
}
