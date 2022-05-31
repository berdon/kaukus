package io.hnsn.kaukus.node.agents.connection;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.agents.AgentException;

public class ConnectionAgentImpl implements ConnectionAgent {
    private final Logger logger;
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();

    public ConnectionAgentImpl(LoggerProvider loggerProvider) {
        logger = loggerProvider.get("ConnectionAgent");
    }

    @Override
    public void addConnection(Connection connection) {
        connections.put(connection.getNodeIdentifier(), connection);
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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void start() throws AgentException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void close() throws IOException {
        for (var nodeId : connections.keySet()) {
            closeConnection(nodeId);
        }
    }
}
