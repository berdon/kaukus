package io.hnsn.kaukus.node.agents.storage;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.node.agents.AgentException;
import io.hnsn.kaukus.persistence.LSMTree;
import io.hnsn.kaukus.types.Namespace;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class StorageAgentImpl implements StorageAgent {
  // TODO: Cleanup
  private final NodeConfiguration nodeConfiguration;
  private final Map<Namespace, LSMTree> storageMap = new HashMap<>();

  @Override
  public void start() throws AgentException {
    final var dataPath = nodeConfiguration.getDataStorePath();
    if (!Files.exists(dataPath)) {
      try {
        Files.createDirectories(dataPath);
      } catch (IOException e) {
        throw new AgentException("Failed to create data directory.");
      }
    }
  }

  @Override
  public String get(Namespace namespace, String key) {
    final var storage = storageMap.computeIfAbsent(namespace, (n) -> getOrCreate(n, false));
    if (storage == null) {
      return null;
    }
    return storage.get(key);
  }

  @Override
  public void set(Namespace namespace, String key, String value) {
    storageMap.computeIfAbsent(namespace, this::getOrCreate).put(key, value);
  }

  @Override
  public void delete(Namespace namespace, String key) {
    storageMap.computeIfAbsent(namespace, this::getOrCreate).remove(key);
  }

  @Nonnull
  private LSMTree getOrCreate(Namespace namespace) {
    return Objects.requireNonNull(getOrCreate(namespace, true));
  }

  private LSMTree getOrCreate(Namespace namespace, boolean shouldCreate) {
    // TODO: LRU Cache?
    final var namespacePath = nodeConfiguration.getDataStorePath().resolve(namespace.toString());
    final var exists = Files.exists(namespacePath);
    if (exists || shouldCreate) {
      return LSMTree.openOrCreate(namespacePath);
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    for (var storage : storageMap.values()) {
      storage.close();
    }
  }
}
