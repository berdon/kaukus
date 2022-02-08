package io.hnsn.kaukus.node.state;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;

import com.github.oxo42.stateless4j.StateMachine;
import com.github.oxo42.stateless4j.StateMachineConfig;
import com.github.oxo42.stateless4j.delegates.Trace;
import com.github.oxo42.stateless4j.transitions.Transition;
import com.github.oxo42.stateless4j.triggers.TriggerWithParameters2;

import org.slf4j.Logger;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.configuration.SystemStore;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.OnUnrecoverableErrorListener;
import io.hnsn.kaukus.std.NullCoallesce;
import io.hnsn.kaukus.std.StringUtils;

public class NodeStateMachineImpl implements NodeStateMachine {
    private final ExecutorService executorService;
    private final SystemStore systemStore;
    private final NodeConfiguration configuration;
    private final StateMachine<NodeState, NodeStateTrigger> stateMachine;
    private final Logger log;
    private final AtomicBoolean hasLoaded = new AtomicBoolean(false);

    private final ListenerSource listeners = new ListenerSource();

    private String nodeIdentifier;

    @Inject
    public NodeStateMachineImpl(ExecutorService executorService, SystemStore systemStore, NodeConfiguration configuration, LoggerProvider loggerProvider) {
        this.executorService = executorService;
        this.systemStore = systemStore;
        this.configuration = configuration;
        this.log = loggerProvider.get("NodeState");

        var stateMachineConfig = new StateMachineConfig<NodeState, NodeStateTrigger>();
        stateMachineConfig.setTriggerParameters(NodeStateTrigger.UnrecoverableError, String.class, Throwable.class);
        stateMachineConfig.configure(NodeState.Unloaded)
            .permit(NodeStateTrigger.UnrecoverableError, NodeState.UnrecoverableError)
            .permit(NodeStateTrigger.Stopped, NodeState.Stopped)
            .permitDynamic(NodeStateTrigger.Load, this::initializeState);
        
        stateMachineConfig.configure(NodeState.Stopping)
            .permit(NodeStateTrigger.UnrecoverableError, NodeState.UnrecoverableError)
            .permit(NodeStateTrigger.Stopped, NodeState.Stopped)
            .onEntry(this::onStopping);

        stateMachineConfig.configure(NodeState.Stopped)
        .permit(NodeStateTrigger.UnrecoverableError, NodeState.UnrecoverableError)
            .permit(NodeStateTrigger.Start, NodeState.Starting)
            .onEntry(this::onStopped);
        
        stateMachineConfig.configure(NodeState.Starting)
            .permit(NodeStateTrigger.UnrecoverableError, NodeState.UnrecoverableError)
            .permit(NodeStateTrigger.Ready, NodeState.Running)
            .onEntry(this::onStarting);
        
        stateMachineConfig.configure(NodeState.UnrecoverableError)
            .onEntry(this::onUnrecoverableError);
        
        stateMachineConfig.configure(NodeState.Running)
            .permit(NodeStateTrigger.UnrecoverableError, NodeState.UnrecoverableError)
            .permit(NodeStateTrigger.Stop, NodeState.Stopping)
            .onEntry(this::onRunning);

        stateMachine = new StateMachine<>(NodeState.Unloaded, stateMachineConfig);
        stateMachine.setTrace(new Trace<>() {
            @Override
            public void trigger(NodeStateTrigger trigger) { }

            @Override
            public void transition(NodeStateTrigger trigger, NodeState source, NodeState destination) {
                log.trace("Node [{}] transitioning from {} to {}", nodeIdentifier, source, destination);
                listeners.get(OnStateChangedListener.class).forEach(listener -> listener.onChanged(source, destination));
            } 
        });
    }

    private NodeState initializeState() {
        var lastStartedAt = systemStore.getLastStartedAt();

        nodeIdentifier = systemStore.getIdentifier();
        if (nodeIdentifier == null) {
            if (lastStartedAt != null) {
                log.error("Kaukus node doesn't have an assigned identifier but has ran before");
                return NodeState.UnrecoverableError;
            }

            nodeIdentifier = configuration.getIdentifier();
            if (StringUtils.isNullOrEmpty(nodeIdentifier)) {
                nodeIdentifier = UUID.randomUUID().toString();
            }
            log.info("Kaukus node starting for the first time and no identifier has been assigned; using {}", nodeIdentifier);

            try {
                systemStore.setIdentifier(nodeIdentifier);
            } catch (IOException e) {
                log.error("Failed to persist the generated node id");
                return NodeState.UnrecoverableError;
            }
        }

        var lastState = NullCoallesce.of(systemStore.getState(), NodeState.Stopped);
        if (lastState != null && lastState != NodeState.Stopped) {
            if (lastStartedAt != null) {
                log.error("Kaukus node [{}] not in a Stopped state but has ran before", nodeIdentifier);
                return NodeState.UnrecoverableError;
            }

            log.warn("Detected corrupted final state; Kaukus node [{}] did not properly shut down", nodeIdentifier);
            return NodeState.UnrecoverableError;
        }

        return lastState;
    }

    private void onStopped() {
        if (!hasLoaded.get()) return;

        persistState();

        try {
            systemStore.close();
        } catch (IOException e) {
            error("Failed to persist to the system store", e);
            return;
        }

        log.info("Kaukus node [{}] is shut down; final state = {}", nodeIdentifier, stateMachine.getState());

        synchronized(this) {
            notifyAll();
        }
    }

    @Override
    public void error(String message, Throwable throwable) {
        executorService.submit(() -> { stateMachine.fire(new TriggerWithParameters2<>(NodeStateTrigger.UnrecoverableError, String.class, Throwable.class), message, throwable); });
    }

    private void onUnrecoverableError(Transition<NodeState, NodeStateTrigger> transition, Object[] args) {
        String message;
        Throwable throwable;

        if (hasLoaded.get()) {
            if (args.length != 0) throw new RuntimeException("Eek? TODO");
            if (!(args[0] instanceof String)) throw new RuntimeException("Eek? TODO");
            if (!(args[1] instanceof Throwable)) throw new RuntimeException("Eek? TODO");

            message = (String) args[0];
            throwable = (Throwable) args[1];

            log.error(message, throwable);
        }
        else {
            message = "Failed to load initial state";
            throwable = null;
        }

        persistState(true);

        try {
            systemStore.close();
        } catch (IOException e) {
            log.error("Failed to persist to the system store while handling terminal failure", e);
        }

        log.info("Kaukus node [{}] is shut down; final state = {}", nodeIdentifier, stateMachine.getState());

        synchronized(this) {
            notifyAll();
        }

        listeners.get(OnUnrecoverableErrorListener.class).forEach(listener -> listener.onError(message, throwable));
    }

    @Override
    public void start() {
        executorService.submit(() -> {
            stateMachine.fire(NodeStateTrigger.Load);
            hasLoaded.set(true);
            stateMachine.fire(NodeStateTrigger.Start);
        });
    }

    private void onStarting() {
        persistState();
        var lastStartedAt = systemStore.getLastStartedAt();
        if (lastStartedAt == null) {
            log.info("Kaukus node [{}] starting for the first time", nodeIdentifier);
        }
        else {
            log.info("Kaukus node [{}] last started at {}", nodeIdentifier, lastStartedAt);
        }

        executorService.submit(() -> { stateMachine.fire(NodeStateTrigger.Ready); });
    }

    private void onRunning() {
        persistState();
        try {
            systemStore.setLastStartedAt(LocalDateTime.now());
        } catch (IOException e) {
            error("Failed to persist to the system store", e);
        }

        listeners.get(OnIdentifierRegisteredListener.class).forEach(listener -> listener.onIdentifierRegistered(nodeIdentifier));
    }

    @Override
    public synchronized void stop() {
        executorService.submit(() -> { stateMachine.fire(NodeStateTrigger.Stop); });
        if (!stateMachine.isInState(NodeState.Stopped) && !stateMachine.isInState(NodeState.UnrecoverableError)) {
            try {
                wait();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }
        }
    }

    private void onStopping() {
        persistState();
        log.info("Shutting down Kaukus node [{}]", nodeIdentifier);
        executorService.submit(() -> { stateMachine.fire(NodeStateTrigger.Stopped); });
    }

    private void persistState() { persistState(false); }
    private void persistState(boolean inErrorState) {
        try {
            systemStore.setState(stateMachine.getState());
        } catch (IOException e) {
            if (!inErrorState)
                executorService.submit(() -> { stateMachine.fire(new TriggerWithParameters2<>(NodeStateTrigger.UnrecoverableError, String.class, Throwable.class), "Failed to persist to the system store", e); });
            else {
                log.error("Failed to persist to the system store", e);
            }
        }
    }

    @Override
    public void registerUnrecoverableErrorListener(OnUnrecoverableErrorListener listener) {
        listeners.get(OnUnrecoverableErrorListener.class).add(listener);
    }

    @Override
    public void unregisterUnrecoverableErrorListener(OnUnrecoverableErrorListener listener) {
        listeners.get(OnUnrecoverableErrorListener.class).remove(listener);
    }

    @Override
    public void registerIdentifierListener(OnIdentifierRegisteredListener listener) {
        listeners.get(OnIdentifierRegisteredListener.class).add(listener);
    }

    @Override
    public void unregisterIdentifierListener(OnIdentifierRegisteredListener listener) {
        listeners.get(OnIdentifierRegisteredListener.class).remove(listener);
    }
}
