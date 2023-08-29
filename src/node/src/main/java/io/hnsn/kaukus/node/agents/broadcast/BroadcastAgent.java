package io.hnsn.kaukus.node.agents.broadcast;

import io.hnsn.kaukus.types.Namespace;
import java.net.InetAddress;

import io.hnsn.kaukus.node.agents.Agent;

public interface BroadcastAgent extends Agent {
    InetAddress getBoundAddress();
    int getBoundPort();

    void broadcastRequestLeader();
    void broadcastRequestLeaderVote(int epoch);
    void broadcastLeaderVote(int vote, int epoch);
    void broadcastLeaderElected(int epoch);

    void registerOnNodeDiscoveredListener(OnNodeDiscoveredListener listener);
    void unregisterOnNodeDiscoveredListener(OnNodeDiscoveredListener listener);

    void registerOnLeaderRequested(OnLeaderRequestedListener listener);
    void unregisterOnLeaderRequested(OnLeaderRequestedListener listener);

    void registerOnLeaderVoteRequested(OnLeaderVoteRequestedListener listener);
    void unregisterOnLeaderVoteRequested(OnLeaderVoteRequestedListener listener);

    void registerOnLeaderVote(OnLeaderVoteListener listener);
    void unregisterOnLeaderVote(OnLeaderVoteListener listener);

    void registerOnLeaderElected(OnLeaderElectedListener listener);
    void unregisterOnLeaderElected(OnLeaderElectedListener listener);
}