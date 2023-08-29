package io.hnsn.kaukus.node.agents.broadcast;

import io.hnsn.kaukus.encoding.LeaderVoteBroadcast;

public interface OnLeaderVoteListener {
    void onLeaderVote(LeaderVoteBroadcast broadcast);
}
