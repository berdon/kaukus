package io.hnsn.kaukus.guiceModules;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.hnsn.kaukus.configuration.LSMTreeSystemStore;
import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.configuration.SystemStore;
import io.hnsn.kaukus.directory.NodeDirectory;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.OnUnrecoverableErrorListener;
import io.hnsn.kaukus.node.agents.broadcast.BroadcastAgent;
import io.hnsn.kaukus.node.agents.broadcast.BroadcastAgentImpl;
import io.hnsn.kaukus.node.agents.client.ClientAgent;
import io.hnsn.kaukus.node.agents.client.ClientAgentImpl;
import io.hnsn.kaukus.node.agents.connection.ConnectionAgent;
import io.hnsn.kaukus.node.agents.connection.ConnectionAgentImpl;
import io.hnsn.kaukus.node.agents.quorum.LeaderQuorumAgent;
import io.hnsn.kaukus.node.agents.quorum.QuorumAgent;
import io.hnsn.kaukus.node.agents.server.ServerAgent;
import io.hnsn.kaukus.node.agents.server.ServerAgentImpl;
import io.hnsn.kaukus.node.agents.storage.StorageAgent;
import io.hnsn.kaukus.node.agents.storage.StorageAgentImpl;
import io.hnsn.kaukus.node.agents.webserver.JettyWebServerAgent;
import io.hnsn.kaukus.node.agents.webserver.WebServerAgent;
import io.hnsn.kaukus.node.state.NodeStateMachine;
import io.hnsn.kaukus.node.state.NodeStateMachineImpl;
import io.hnsn.kaukus.persistence.LSMTree;
import java.lang.annotation.Retention;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Qualifier;
import javax.net.ServerSocketFactory;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NodeModule extends AbstractModule {
    @Qualifier
    @Retention(RUNTIME)
    public @interface SharedExecutor {}

    @Qualifier
    @Retention(RUNTIME)
    public @interface NodeIdentifier {}

    private final LSMTree systemLSMTree;
    private final OnUnrecoverableErrorListener errorHandler;

    @Provides @Singleton
    OnUnrecoverableErrorListener provideOnUnrecoverableErrorHandler() { return errorHandler; }

    @Provides @Singleton
    SystemStore provideSystemStore() {
        return new LSMTreeSystemStore(systemLSMTree);
    }

    @SharedExecutor
    @Provides @Singleton
    ExecutorService provideExecutorService() {
        return Executors.newCachedThreadPool();
    }

    @SharedExecutor
    @Provides @Singleton
    ScheduledExecutorService provideScheduledExecutorService() {
        return Executors.newScheduledThreadPool(10);
    }

    @Provides @Singleton
    NodeStateMachine provideNodeStateMachine(NodeStateMachineImpl impl) { return impl; }

    @Provides @Singleton
    ConnectionAgent provideConnectionAgent(LoggerProvider loggerProvider) {
        return new ConnectionAgentImpl(loggerProvider);
    }

    @Provides @Singleton
    ServerAgent provideServerAgent(
        NodeConfiguration configuration,
        LoggerProvider loggerProvider,
        @SharedExecutor ExecutorService executorService,
        @NodeIdentifier Provider<String> nodeIdentifier) {
        return new ServerAgentImpl(
            configuration.getSystemAddress(),
            0,
            configuration.getSystemPort(),
            ServerSocketFactory.getDefault(),
            loggerProvider,
            executorService,
            configuration,
            nodeIdentifier);
    }

    @Provides @Singleton
    BroadcastAgent provideDiscoveryAgent(
        NodeConfiguration configuration,
        ServerAgent serverAgent,
        LoggerProvider loggerProvider,
        @SharedExecutor ExecutorService executorService,
        @SharedExecutor ScheduledExecutorService scheduledExecutorService,
        @NodeIdentifier Provider<String> nodeIdentifier
    ) {
        return new BroadcastAgentImpl(
            configuration.getBroadcastAddress(),
            configuration.getBroadcastPort(),
            loggerProvider,
            serverAgent,
            executorService,
            scheduledExecutorService,
            configuration,
            nodeIdentifier);
    }

    @Provides @Singleton
    ClientAgent provideClientAgent(NodeConfiguration configuration, ServerAgent serverAgent, LoggerProvider loggerProvider, @SharedExecutor ScheduledExecutorService executorService) {
        return new ClientAgentImpl(configuration, serverAgent, loggerProvider, executorService);
    }

    @Provides @Singleton
    StorageAgent provideStorageAgent(NodeConfiguration configuration) {
        return new StorageAgentImpl(configuration);
    }

    @Provides @Singleton
    QuorumAgent provideQuorumAgent(NodeConfiguration configuration, ConnectionAgent connectionAgent, BroadcastAgent broadcastAgent, StorageAgent storageAgent, LoggerProvider loggerProvider, @SharedExecutor ScheduledExecutorService executorService, @NodeIdentifier Provider<String> nodeIdentifier) {
        return new LeaderQuorumAgent(connectionAgent, broadcastAgent, executorService, loggerProvider.get(
            LeaderQuorumAgent.class.getSimpleName()), nodeIdentifier, storageAgent);
    }

    @Provides @Singleton
    WebServerAgent provideWebServerAgent(NodeConfiguration configuration, QuorumAgent quorumAgent, StorageAgent storageAgent) {
        return new JettyWebServerAgent(configuration, quorumAgent, storageAgent);
    }

    @Provides @NodeIdentifier
    String provideNodeIdentifier(SystemStore systemStore) {
        return systemStore.getIdentifier();
    }

    @Provides @Singleton
    NodeDirectory provideDirectory() {
        return new NodeDirectory();
    }
}
