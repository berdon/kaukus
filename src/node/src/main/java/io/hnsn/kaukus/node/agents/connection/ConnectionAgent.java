package io.hnsn.kaukus.node.agents.connection;

import io.hnsn.kaukus.node.agents.Agent;
import java.util.Set;

public interface ConnectionAgent extends Agent {
    void addConnection(Connection connection);
    boolean hasConnection(String nodeIdentifier);
    void closeConnection(String nodeIdentifier);
    Connection getConnection(String nodeIdentifier);
    Set<String> getConnectedNodeIdentifiers();

    void registerOnConnectionEstablishedListener(OnConnectionEstablishedListener listener);
    void unregisterOnConnectionEstablishedListener(OnConnectionEstablishedListener listener);

    void registerOnConnectionClosedListener(OnConnectionClosedListener listener);
    void unregisterOnConnectionClosedListener(OnConnectionClosedListener listener);
}
