package io.hnsn.kaukus.node.agents.connection;

public abstract class BaseConnection implements Connection {
    @Override
    public <T> void sendMessage(T Message, Class<T> messageClass) {
        
    }
}
