package io.hnsn.kaukus.node.agents.broadcast;

import io.hnsn.kaukus.encoding.HelloBroadcast;

public interface OnNodeDiscoveredListener {
    void onReceived(HelloBroadcast packet);
}
