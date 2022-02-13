package io.hnsn.kaukus.node.agents;

public class AgentException extends Exception {

    public AgentException(String message) {
        super(message);
    }

    public AgentException(String message, Throwable cause) {
        super(message, cause);
    }

}
