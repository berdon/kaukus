package io.hnsn.kaukus.guiceModules;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Qualifier;
import javax.net.ServerSocketFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import io.hnsn.kaukus.configuration.LSMTreeSystemStore;
import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.configuration.SystemStore;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.OnUnrecoverableErrorListener;
import io.hnsn.kaukus.node.agents.ClientAgent;
import io.hnsn.kaukus.node.agents.ClientAgentImpl;
import io.hnsn.kaukus.node.agents.DiscoveryAgent;
import io.hnsn.kaukus.node.agents.DiscoveryAgentImpl;
import io.hnsn.kaukus.node.agents.ServerAgent;
import io.hnsn.kaukus.node.agents.ServerAgentImpl;
import io.hnsn.kaukus.node.state.NodeStateMachine;
import io.hnsn.kaukus.node.state.NodeStateMachineImpl;
import io.hnsn.kaukus.persistence.LSMTree;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NodeModule extends AbstractModule {
    @Qualifier
    @Retention(RUNTIME)
    public @interface SharedExecutor {}

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
    ServerAgent provideServerAgent(NodeConfiguration configuration, LoggerProvider loggerProvider, @SharedExecutor ExecutorService executorService) {
        return new ServerAgentImpl(
            configuration.getSystemAddress(),
            0,
            configuration.getSystemPort(),
            ServerSocketFactory.getDefault(),
            loggerProvider,
            executorService);
    }

    @Provides @Singleton
    DiscoveryAgent provideDiscoveryAgent(NodeConfiguration configuration, ServerAgent serverAgent, LoggerProvider loggerProvider, @SharedExecutor ExecutorService executorService) {
        return new DiscoveryAgentImpl(
            configuration.getBroadcastAddress(),
            configuration.getBroadcastPort(),
            loggerProvider,
            serverAgent,
            executorService);
    }

    @Provides @Singleton
    ClientAgent provideClientAgent(NodeConfiguration configuration, ServerAgent serverAgent, LoggerProvider loggerProvider, @SharedExecutor ScheduledExecutorService executorService) {
        return new ClientAgentImpl(configuration, serverAgent, loggerProvider, executorService);
    }
}
