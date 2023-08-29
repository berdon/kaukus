package io.hnsn.kaukus.node.agents.broadcast;

import io.hnsn.kaukus.encoding.LeaderElectedBroadcast;

public interface OnLeaderElectedListener {
    void onLeaderElected(LeaderElectedBroadcast broadcast);
}
