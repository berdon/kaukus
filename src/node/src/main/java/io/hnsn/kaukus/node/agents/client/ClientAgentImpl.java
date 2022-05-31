package io.hnsn.kaukus.node.agents.client;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.guiceModules.NodeModule.SharedExecutor;
import io.hnsn.kaukus.node.agents.AgentException;
import io.hnsn.kaukus.node.agents.connection.ClientConnection;
import io.hnsn.kaukus.node.agents.server.ServerAgent;
import io.hnsn.kaukus.node.state.ListenerSource;

public class ClientAgentImpl implements ClientAgent {
    private final NodeConfiguration configuration;
    private final ServerAgent serverAgent;
    private final ExecutorService executorService;
    private final ListenerSource listeners = new ListenerSource();
    private final Logger log;
    private final Map<String, Socket> sockets = new ConcurrentHashMap<>();

    public ClientAgentImpl(
        NodeConfiguration configuration,
        ServerAgent serverAgent,
        LoggerProvider loggerProvider,
        @SharedExecutor ExecutorService executorService
    ) {
        this.configuration = configuration;
        this.serverAgent = serverAgent;
        this.executorService = executorService;
        this.log = loggerProvider.get("ClientAgent");
    }

    @Override
    public void start() throws AgentException { }

    @Override
    public void connectToNode(String targetNodeIdentifier, String address, int port) {
        executorService.submit(() -> {
            try {
                var socket = new Socket(address, port);
                sockets.put(targetNodeIdentifier, socket);
                var connection = new ClientConnection(targetNodeIdentifier, (con) -> {
                    sockets.remove(targetNodeIdentifier);
                    try { socket.close(); }
                    catch (IOException e) { log.warn("Exception occurred while closing socket", e); }
                
                    executorService.submit(() -> {
                        listeners.get(OnConnectionClosedListener.class).forEach(listener -> listener.onClosed(con));
                    });
                }, socket);

                executorService.submit(() -> {
                    listeners.get(OnConnectionEstablishedListener.class).forEach(listener -> listener.onConnected(connection));
                });
            }
            catch (IOException e) {
                log.warn("Failed to establish connection", e);
            }
        });
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
    
    @Override
    public void close() throws IOException {
        // TODO
    }
}
