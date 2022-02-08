package io.hnsn.kaukus.node.state;

import io.hnsn.kaukus.node.OnUnrecoverableErrorListener;

public interface NodeStateMachine {
    void error(String message, Throwable throwable);
    void start();
    void stop();

    void registerUnrecoverableErrorListener(OnUnrecoverableErrorListener listener);
    void unregisterUnrecoverableErrorListener(OnUnrecoverableErrorListener listener);

    void registerIdentifierListener(OnIdentifierRegisteredListener listener);
    void unregisterIdentifierListener(OnIdentifierRegisteredListener listener);
}