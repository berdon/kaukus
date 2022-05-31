package io.hnsn.kaukus.node.agents.connection;

import io.hnsn.kaukus.node.agents.Agent;

public interface ConnectionAgent extends Agent {
    void addConnection(Connection connection);
    boolean hasConnection(String nodeIdentifier);
    void closeConnection(String nodeIdentifier);
    Connection getConnection(String nodeIdentifier);
}
