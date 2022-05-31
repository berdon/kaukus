package io.hnsn.kaukus.node.agents.broadcast;

import java.net.InetAddress;

import io.hnsn.kaukus.node.agents.Agent;

public interface BroadcastAgent extends Agent {
    InetAddress getBoundAddress();
    int getBoundPort();

    void registerOnNodeDiscoveredListener(OnNodeDiscoveredListener listener);
    void unregisterOnNodeDiscoveredListener(OnNodeDiscoveredListener listener);
}