package io.hnsn.kaukus.node.agents.broadcast;

import io.hnsn.kaukus.encoding.RequestLeaderBroadcast;

public interface OnLeaderRequestedListener {
    void onLeaderRequested(RequestLeaderBroadcast broadcast);
}
