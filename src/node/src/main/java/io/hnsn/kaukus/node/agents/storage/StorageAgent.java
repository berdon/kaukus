package io.hnsn.kaukus.node.agents.storage;

import io.hnsn.kaukus.node.agents.Agent;
import io.hnsn.kaukus.types.Namespace;

public interface StorageAgent extends Agent {
  String get(Namespace namespace, String key);
  void set(Namespace namespace, String key, String value);

  void delete(Namespace namespace, String key);
}
