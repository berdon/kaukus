package io.hnsn.kaukus.node.agents;

import java.net.InetAddress;

public interface DiscoveryAgent extends Agent {
    InetAddress getBoundAddress();
    int getBoundPort();

    void registerOnBroadcastReceivedListener(OnBroadcastReceivedListener listener);
    void unregisterOnBroadcastReceivedListener(OnBroadcastReceivedListener listener);
}