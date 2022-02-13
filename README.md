# Kaukus

Toy, leaderless, distributed key-value store.

## Persistence

Key-value pairs are stored using an LSM-Tree; WAL file for crash recovery.

## Tentative Design

Real loose, tenative plan:

### Discovery / Initial Connection

1. ServerAgent binds and listens to a socket
   1. Default starting port with incrementing or arbitrary selection on conflict
   2. Doesn't really need to be known provided discovery can work (via IP multicast)
2. DiscoveryAgent listens on a multicast group
3. ClientAgent broadcasts and says "Hi!"
4. DiscoveryAgents receive "Hi!" message and
   1. Could just issue a connection to the new client?
   2. Could respond with a broadcast of there own (probably not this)

### Epoch / Pre-Epoch Maintenance

- Clients/Server periodic heartbeats
- Clients periodically send out their client "phonebooks"
  - Phonebooks basically consist of known clients/info, latency times, other metadata
  - Other metadata might mean suspected dead clients, suspected faulty/questionable clients
- Given heartbeats clients can propose a leader vote

### Leadership Election
1. When no leader / dead leader
2. Node volunteers as leader if they know of > 2 other nodes (requires at least 3 total nodes)
   1. Proposal includes an epoch number (their last known value + 1), a clout number (random number)
   2. Other nodes respond yes if they know of > 2 other nodes (including the proposer)
   3. Conflicting vote proposals
      1. Proposer with the greatest clout wins (Agreement)
      2. Conflicting random number (Agreement)
         1. Nodes conscede to the proposer with the lexicographically greater node ID
         2. Nodes who share their random number reset their random generator, seeded with the current time
            - Might fail on same system - could seed based on time + port?
3. Nodes vote
   1. Nodes who know of a leader with a higher epoch respond with no and the current epoch
   2. Nodes who have responded yes to a previous proposal with a higher epoch or clout number respond with a no and that epoch/clout, and the proposal counts they've responded to
   3. Proposer nodes who receive a proposal with a higher epoch/clout abort their proposal
   4. Nodes otherwise respond yes
4. Proposers wait (possibly forever) to receive a majority of yes or nos
   1. Proposers who receive majority of yes acknowledgements sends out a message saying they're the leader
   2. Nodes who receive the leader message update their epoch/leader mapping

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