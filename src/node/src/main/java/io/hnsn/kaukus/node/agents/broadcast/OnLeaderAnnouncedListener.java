package io.hnsn.kaukus.node.agents.broadcast;

import io.hnsn.kaukus.encoding.LeaderAnnouncementMessage;

public interface OnLeaderAnnouncedListener {
    void onLeaderAnnounced(LeaderAnnouncementMessage message);
}
