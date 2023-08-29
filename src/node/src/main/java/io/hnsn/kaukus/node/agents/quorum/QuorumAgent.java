package io.hnsn.kaukus.node.agents.quorum;

import io.hnsn.kaukus.node.agents.Agent;
import io.hnsn.kaukus.node.agents.connection.Connection;
import io.hnsn.kaukus.types.Namespace;

public interface QuorumAgent extends Agent {
  String getLeaderNodeIdentifier();
  boolean isLeader();
  void requestStorageSet(Namespace namespace, String key, String value);
  void requestStorageDelete(Namespace namespace, String key);
  void registerOnLeaderChangedListener(OnLeaderChangedListener listener);
  void unregisterOnLeaderChangedListener(OnLeaderChangedListener listener);
}
