package io.hnsn.kaukus.node;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.google.inject.Inject;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.configuration.SystemStore;
import io.hnsn.kaukus.node.state.NodeStateMachine;

public class Node implements Closeable {
    private final SystemStore systemStore;
    private final NodeConfiguration nodeConfiguration;
    private final ExecutorService executorService;
    private final NodeStateMachine stateMachine;
    private final OnUnrecoverableErrorListener errorListener;

    private String nodeIdentifier;

    @Inject
    public Node(
        SystemStore systemStore,
        NodeConfiguration nodeConfiguration,
        ExecutorService executorService,
        NodeStateMachine nodeStateMachine,
        OnUnrecoverableErrorListener errorListener)
    {
        this.systemStore = systemStore;
        this.nodeConfiguration = nodeConfiguration;
        this.executorService = executorService;
        this.stateMachine = nodeStateMachine;
        this.errorListener = errorListener;

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
        executorService.shutdown();
    }
}
