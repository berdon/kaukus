package io.hnsn.kaukus.node.state;

import io.hnsn.kaukus.node.OnUnrecoverableErrorListener;

public interface NodeStateMachine {
    void error(String message, Throwable throwable);
    void start();
    void stop();

    void registerStateChangedListener(OnStateChangedListener listener);
    void registerUnrecoverableErrorListener(OnUnrecoverableErrorListener listener);
    void registerIdentifierListener(OnIdentifierRegisteredListener listener);

    void unregisterUnrecoverableErrorListener(OnUnrecoverableErrorListener listener);
    void unregisterStateChangedListener(OnStateChangedListener listener);
    void unregisterIdentifierListener(OnIdentifierRegisteredListener listener);
}