package io.hnsn.kaukus.node;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.google.inject.Inject;

import org.slf4j.Logger;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.configuration.SystemStore;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.guiceModules.NodeModule.SharedExecutor;
import io.hnsn.kaukus.node.agents.broadcast.BroadcastAgent;
import io.hnsn.kaukus.node.agents.client.ClientAgent;
import io.hnsn.kaukus.node.agents.connection.ConnectionAgent;
import io.hnsn.kaukus.node.agents.server.ServerAgent;
import io.hnsn.kaukus.node.state.NodeStateMachine;

public class Node implements Closeable {
    private final SystemStore systemStore;
    private final NodeConfiguration configuration;
    private final NodeStateMachine stateMachine;
    private final OnUnrecoverableErrorListener errorListener;
    private ConnectionAgent connectionAgent;
    private final ClientAgent clientAgent;
    private final ServerAgent serverAgent;
    private final BroadcastAgent discoveryAgent;
    private final Logger log;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;

    private final Socket socket;

    private String nodeIdentifier;

    @Inject
    public Node(
        SystemStore systemStore,
        NodeConfiguration configuration,
        NodeStateMachine nodeStateMachine,
        OnUnrecoverableErrorListener errorListener,
        ConnectionAgent connectionAgent,
        ServerAgent serverAgent,
        ClientAgent clientAgent,
        BroadcastAgent broadcastAgent,
        LoggerProvider loggerProvider,
        @SharedExecutor ExecutorService executorService,
        @SharedExecutor ScheduledExecutorService scheduledExecutorService)
    {
        this.systemStore = systemStore;
        this.configuration = configuration;
        this.stateMachine = nodeStateMachine;
        this.errorListener = errorListener;
        this.connectionAgent = connectionAgent;
        this.clientAgent = clientAgent;
        this.executorService = executorService;
        this.scheduledExecutorService = scheduledExecutorService;
        this.socket = new Socket();
        this.serverAgent = serverAgent;
        this.discoveryAgent = broadcastAgent;
        this.log = loggerProvider.get("Node");

        stateMachine.registerIdentifierListener(nodeIdentifier -> this.nodeIdentifier = nodeIdentifier);
        stateMachine.registerUnrecoverableErrorListener((message, throwable) -> {
            errorListener.onError(message, throwable);
        });

        serverAgent.registerOnClientConnectedListener((connection) -> {
            log.info("Server connection established with {}", connection.getNodeIdentifier());
            connectionAgent.addConnection(connection);
        });

        serverAgent.registerOnClientDisconnectedListener((connection) -> {
            log.info("Server connection {} disconnected", connection.getNodeIdentifier());
            connectionAgent.closeConnection(connection.getNodeIdentifier());
        });

        clientAgent.registerOnConnectionEstablishedListener((connection) -> {
            log.info("Client connection established to {}", connection.getNodeIdentifier());
            connectionAgent.addConnection(connection);
        });

        clientAgent.registerOnConnectionClosedListener((connection) -> {
            log.info("Client connection {} disconnected", connection.getNodeIdentifier());
            connectionAgent.closeConnection(connection.getNodeIdentifier());
        });

        broadcastAgent.registerOnNodeDiscoveredListener((helloBroadcast) -> {
            log.info("HELLO: {}:{} ({})", helloBroadcast.getSourceAddress(), helloBroadcast.getSourcePort(), helloBroadcast.getVersion());

            clientAgent.connectToNode(
                helloBroadcast.getNodeIdentifier(),
                helloBroadcast.getSourceAddress(),
                helloBroadcast.getSourcePort());
        });
    }

    public void start() {
        stateMachine.start();
    }

    @Override
    public void close() throws IOException {
        stateMachine.stop();
        executorService.shutdown();
        scheduledExecutorService.shutdown();
    }
}