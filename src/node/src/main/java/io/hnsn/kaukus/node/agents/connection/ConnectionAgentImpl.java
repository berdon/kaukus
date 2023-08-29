package io.hnsn.kaukus.node.agents.connection;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.agents.AgentException;
import io.hnsn.kaukus.node.state.ListenerSource;

public class ConnectionAgentImpl implements ConnectionAgent {
    private final Logger logger;
    private final ListenerSource listeners = new ListenerSource();
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();

    public ConnectionAgentImpl(LoggerProvider loggerProvider) {
        logger = loggerProvider.get("ConnectionAgent");
    }

    @Override
    public void addConnection(Connection connection) {
        final var existed = connections.containsKey(connection.getNodeIdentifier());
        connections.put(connection.getNodeIdentifier(), connection);
        connection.registerOnClosedListener(this::onConnectionClosed);
        if (!existed) {
            onConnectionEstablished(connection);
        }
    }

    @Override
    public boolean hasConnection(String nodeIdentifier) {
        return connections.containsKey(nodeIdentifier);
    }

    @Override
    public void closeConnection(String nodeIdentifier) {
        connections.computeIfPresent(nodeIdentifier, (key, connection) -> {
            try { connection.close(); }
            catch (IOException e) { logger.warn("Exception occurred while closing connection", e); }
            return null;
        });
    }

    @Override
    public Connection getConnection(String nodeIdentifier) {
        return connections.get(nodeIdentifier);
    }

    @Override
    public Set<String> getConnectedNodeIdentifiers() {
        return connections.keySet();
    }

    @Override
    public void start() throws AgentException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void registerOnConnectionEstablishedListener(OnConnectionEstablishedListener listener) {
        listeners.get(OnConnectionEstablishedListener.class).add(listener);
    }

    @Override
    public void unregisterOnConnectionEstablishedListener(OnConnectionEstablishedListener listener) {
        listeners.get(OnConnectionEstablishedListener.class).remove(listener);
    }

    @Override
    public void registerOnConnectionClosedListener(OnConnectionClosedListener listener) {
        listeners.get(OnConnectionClosedListener.class).add(listener);
    }

    @Override
    public void unregisterOnConnectionClosedListener(OnConnectionClosedListener listener) {
        listeners.get(OnConnectionClosedListener.class).remove(listener);
    }

    private void onConnectionEstablished(Connection connection) {
        for (var listener : listeners.get(OnConnectionEstablishedListener.class)) {
            listener.onConnected(connection);
        }
    }

    private void onConnectionClosed(Connection connection) {
        for (var listener : listeners.get(OnConnectionClosedListener.class)) {
            listener.onClosed(connection);
        }
    }

    @Override
    public void close() throws IOException {
        for (var nodeId : connections.keySet()) {
            closeConnection(nodeId);
        }
    }
}
