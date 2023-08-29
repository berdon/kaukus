package io.hnsn.kaukus.node;

import io.hnsn.kaukus.encoding.LeaderAnnouncementMessage;
import io.hnsn.kaukus.node.agents.quorum.QuorumAgent;
import io.hnsn.kaukus.node.agents.storage.StorageAgent;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.google.inject.Inject;

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.configuration.SystemStore;
import io.hnsn.kaukus.directory.NodeDirectory;
import io.hnsn.kaukus.directory.NodeIdentity;
import io.hnsn.kaukus.directory.NodeStatus;
import io.hnsn.kaukus.encoding.HelloMessage;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.guiceModules.NodeModule.SharedExecutor;
import io.hnsn.kaukus.node.agents.broadcast.BroadcastAgent;
import io.hnsn.kaukus.node.agents.client.ClientAgent;
import io.hnsn.kaukus.node.agents.connection.ConnectionAgent;
import io.hnsn.kaukus.node.agents.connection.OnMessageReceivedListener;
import io.hnsn.kaukus.node.agents.connection.ServerConnection;
import io.hnsn.kaukus.node.agents.server.ServerAgent;
import io.hnsn.kaukus.node.state.NodeStateMachine;

public class Node implements Closeable {
    private final NodeStateMachine stateMachine;
    @Getter
    private final StorageAgent storageAgent;
    private final Logger log;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Set<CompletableFuture<?>> tasks = new HashSet<>();
    private final AtomicBoolean isTerminating = new AtomicBoolean(false);

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
        StorageAgent storageAgent,
        NodeDirectory nodeDirectory,
        LoggerProvider loggerProvider,
        @SharedExecutor ExecutorService executorService,
        @SharedExecutor ScheduledExecutorService scheduledExecutorService)
    {
        this.stateMachine = nodeStateMachine;
        this.executorService = executorService;
        this.scheduledExecutorService = scheduledExecutorService;
        this.storageAgent = storageAgent;
        this.log = loggerProvider.get("Node");

        stateMachine.registerIdentifierListener(nodeIdentifier -> this.nodeIdentifier = nodeIdentifier);
        stateMachine.registerUnrecoverableErrorListener(errorListener);

        serverAgent.registerOnClientConnectedListener((connection) -> {
            // We don't add the connection immediately - we need to wait for a response of who they are
            connection.registerOnMessageReceivedListener((listener, payload) -> {
                if (payload.getSchema().getName().compareTo(HelloMessage.SCHEMA$.getName()) == 0) {
                    final var helloMessage = (HelloMessage) SpecificData.getForSchema(HelloMessage.SCHEMA$).deepCopy(payload.getSchema(), payload);

                    // Received a synack
                    ((ServerConnection) connection).setNodeIdentifier(helloMessage.getNodeIdentifier());
                    ((ServerConnection) connection).setAddress(helloMessage.getSourceAddress());
                    ((ServerConnection) connection).setPort(helloMessage.getSourcePort());
                    connection.unregisterOnMessageReceivedListener(listener);
                    connectionAgent.addConnection(connection);
                }
            });
        });
        serverAgent.registerOnClientDisconnectedListener((connection) -> connectionAgent.closeConnection(connection.getNodeIdentifier()));

        clientAgent.registerOnConnectionEstablishedListener((connection) -> {
            // We've established a connection with a server, we know their details but must send them
            // ours
            connectionAgent.addConnection(connection);

            // Send a hello message
            connection.sendMessage(
                new HelloMessage(
                    nodeIdentifier,
                    serverAgent.getBoundAddress().toString(),
                    serverAgent.getBoundPort(),
                    configuration.getVersion()));
        });
        clientAgent.registerOnConnectionClosedListener((connection) -> connectionAgent.closeConnection(connection.getNodeIdentifier()));

        broadcastAgent.registerOnNodeDiscoveredListener((helloBroadcast) -> {
            log.info("Received HELLO: {}:{} ({})", helloBroadcast.getSourceAddress(), helloBroadcast.getSourcePort(), helloBroadcast.getVersion());

            // Don't connect if we're already connected
            if (!connectionAgent.hasConnection(helloBroadcast.getNodeIdentifier())) {
                executorService.execute(() -> {
                    clientAgent.connectToNode(
                        helloBroadcast.getNodeIdentifier(),
                        helloBroadcast.getSourceAddress(),
                        helloBroadcast.getSourcePort());
                });
            }
        });

        connectionAgent.registerOnConnectionEstablishedListener((connection) -> {
            log.info("Connection {} established ({}:{})", connection.getNodeIdentifier(), connection.getAddress(), connection.getPort());
            nodeDirectory.compute(connection.getNodeIdentifier(), (key, identity) -> new NodeIdentity(connection, NodeStatus.Alive)).update(connection);
        });

        connectionAgent.registerOnConnectionClosedListener((connection) -> {
            log.info("Connection {} closed ({}:{})", connection.getNodeIdentifier(), connection.getAddress(), connection.getPort());
            nodeDirectory.compute(connection.getNodeIdentifier(), (key, identity) -> new NodeIdentity(connection, NodeStatus.Alive)).setStatus(NodeStatus.Disconnected);

            // Attempt to reconnect
            // TODO: Move to ClientAgent
            final var reconnectTask = CompletableFuture.supplyAsync(() -> {
                var sleepAmount = 2000;
                for (var i = 0; i < 5; i++) {
                    if (executorService.isShutdown()) break;
                    try { Thread.sleep(sleepAmount); } catch (InterruptedException ignored) { }
                    // Don't connect if we're already connected
                    if (!connectionAgent.hasConnection(connection.getNodeIdentifier())) {
                        log.info("Attempting to re-establish a connection with {} ({}/{}).", connection.getNodeIdentifier(), i, 5);
                        var newConnection = clientAgent.connectToNode(
                            connection.getNodeIdentifier(),
                            connection.getAddress(),
                            connection.getPort());
                        if (newConnection != null) return newConnection;
                    }
                    sleepAmount *= 2;
                }

                log.info("Failed to re-establish a connection with {}.", connection.getNodeIdentifier());
                return null;
            });
            tasks.add(reconnectTask);
            reconnectTask.thenApply(r -> tasks.remove(reconnectTask));
            reconnectTask.whenComplete((c, t) -> {
                // Handle the possibility that we connected after trying to close
                if (isTerminating.get() && c != null) {
                    try { c.close(); } catch (IOException ignored) { }
                }
            });
        });
    }

    public void start() {
        stateMachine.start();
    }

    @Override
    public void close() throws IOException {
        isTerminating.set(true);
        tasks.forEach(t -> t.cancel(true));
        stateMachine.stop();
        executorService.shutdown();
        scheduledExecutorService.shutdown();
    }
}