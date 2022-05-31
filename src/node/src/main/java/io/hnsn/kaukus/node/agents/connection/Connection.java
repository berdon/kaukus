package io.hnsn.kaukus.node.agents.connection;

import java.io.Closeable;

public interface Connection extends Closeable {
    public String getNodeIdentifier();
    public <T> void sendMessage(T Message, Class<T> messageClass);
}
