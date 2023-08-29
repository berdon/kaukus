package io.hnsn.kaukus.node.agents.broadcast;

import io.hnsn.kaukus.encoding.RequestLeaderVoteBroadcast;

public interface OnLeaderVoteRequestedListener {
    void onLeaderVoteRequested(RequestLeaderVoteBroadcast broadcast);
}
