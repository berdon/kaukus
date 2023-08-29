package io.hnsn.kaukus.node.agents.quorum;

import io.hnsn.kaukus.node.agents.connection.Connection;

public interface OnLeaderChangedListener {
    void onChanged(String oldNodeIdentifier, String nodeIdentifier);
}
