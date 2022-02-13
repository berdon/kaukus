package io.hnsn.kaukus.node.state;

import io.hnsn.kaukus.node.OnUnrecoverableErrorListener;
import io.hnsn.kaukus.node.agents.OnBroadcastReceivedListener;
import io.hnsn.kaukus.node.agents.OnClientConnectedListener;

public interface NodeStateMachine {
    void error(String message, Throwable throwable);
    void start();
    void stop();

    void registerOnBroadcastReceivedListener(OnBroadcastReceivedListener listener);
    void registerOnClientConnectedListener(OnClientConnectedListener listener);
    void registerStateChangedListener(OnStateChangedListener listener);
    void registerUnrecoverableErrorListener(OnUnrecoverableErrorListener listener);
    void registerIdentifierListener(OnIdentifierRegisteredListener listener);

    void unregisterOnBroadcastReceivedListener(OnBroadcastReceivedListener listener);
    void unregisterOnClientConnectedListener(OnClientConnectedListener listener);
    void unregisterUnrecoverableErrorListener(OnUnrecoverableErrorListener listener);
    void unregisterStateChangedListener(OnStateChangedListener listener);
    void unregisterIdentifierListener(OnIdentifierRegisteredListener listener);
}