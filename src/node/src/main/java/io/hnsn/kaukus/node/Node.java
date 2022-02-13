package io.hnsn.kaukus.node;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

import com.google.inject.Inject;

import org.slf4j.Logger;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.configuration.SystemStore;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.agents.DiscoveryAgent;
import io.hnsn.kaukus.node.agents.ServerAgent;
import io.hnsn.kaukus.node.state.NodeState;
import io.hnsn.kaukus.node.state.NodeStateMachine;

public class Node implements Closeable {
    private final SystemStore systemStore;
    private final NodeConfiguration configuration;
    private final NodeStateMachine stateMachine;
    private final OnUnrecoverableErrorListener errorListener;
    private final ServerAgent serverAgent;
    private final DiscoveryAgent discoveryAgent;
    private final Logger log;

    private final Socket socket;

    private String nodeIdentifier;

    @Inject
    public Node(
        SystemStore systemStore,
        NodeConfiguration configuration,
        NodeStateMachine nodeStateMachine,
        OnUnrecoverableErrorListener errorListener,
        ServerAgent serverAgent,
        DiscoveryAgent discoveryAgent,
        LoggerProvider loggerProvider)
    {
        this.systemStore = systemStore;
        this.configuration = configuration;
        this.stateMachine = nodeStateMachine;
        this.errorListener = errorListener;
        this.socket = new Socket();
        this.serverAgent = serverAgent;
        this.discoveryAgent = discoveryAgent;
        this.log = loggerProvider.get("Node");

        stateMachine.registerIdentifierListener(nodeIdentifier -> this.nodeIdentifier = nodeIdentifier);
        stateMachine.registerUnrecoverableErrorListener((message, throwable) -> {
            errorListener.onError(message, throwable);
        });
    }

    public void start() {
        stateMachine.start();
    }

    @Override
    public void close() throws IOException {
        stateMachine.stop();
    }
}
