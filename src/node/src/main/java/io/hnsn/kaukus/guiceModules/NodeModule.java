package io.hnsn.kaukus.guiceModules;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import io.hnsn.kaukus.configuration.LSMTreeSystemStore;
import io.hnsn.kaukus.configuration.SystemStore;
import io.hnsn.kaukus.node.OnUnrecoverableErrorListener;
import io.hnsn.kaukus.node.state.NodeStateMachine;
import io.hnsn.kaukus.node.state.NodeStateMachineImpl;
import io.hnsn.kaukus.persistence.LSMTree;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class NodeModule extends AbstractModule {
    private final LSMTree systemLSMTree;
    private final OnUnrecoverableErrorListener errorHandler;

    @Provides @Singleton
    OnUnrecoverableErrorListener provideOnUnrecoverableErrorHandler() { return errorHandler; }

    @Provides @Singleton
    SystemStore provideSystemStore() {
        return new LSMTreeSystemStore(systemLSMTree);
    }

    @Provides @Singleton
    ExecutorService provideExecutorService() {
        return Executors.newSingleThreadExecutor();
    }

    @Provides @Singleton
    NodeStateMachine provideNodeStateMachine(NodeStateMachineImpl impl) { return impl; }
}
