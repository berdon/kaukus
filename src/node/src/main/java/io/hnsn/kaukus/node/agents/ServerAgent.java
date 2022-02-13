package io.hnsn.kaukus.node.agents;

import java.net.InetAddress;

public interface ServerAgent extends Agent {
    InetAddress getBoundAddress();
    int getBoundPort();

    void registerOnClientConnectedListener(OnClientConnectedListener listener);
    void unregisterOnClientConnectedListener(OnClientConnectedListener listener);
}
