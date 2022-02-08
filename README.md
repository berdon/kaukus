# Kaukus

Toy, leaderless, distributed key-value store.

## Persistence

Key-value pairs are stored using an LSM-Tree; WAL file for crash recovery.

### LSM-Tree

Pretty naive implementation that uses a two-ish layer approach:

- In-memory hash map of key/values
  - WAL log for all actions not persisted to disk
- Cascading, append-only SSTables

In-memory for hot access (though reads aren't transferred over yet; only writes).

#### Writes

1. WAL is appended with a new key/value
2. In-memory hash map is updated with key/value

#### Periodic Flushing (TODO)

1. Current in-memory hash map is written out to disk as a successive SSTable
2. WAL is cleared