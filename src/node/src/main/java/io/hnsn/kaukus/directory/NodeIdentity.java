package io.hnsn.kaukus.directory;

import io.hnsn.kaukus.node.agents.connection.Connection;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Setter;

@Data
public class NodeIdentity {
    private final String identifier;
    private String address;
    private int port;
    private NodeStatus status;

    public NodeIdentity(Connection connection, NodeStatus status) {
        this.identifier = connection.getNodeIdentifier();
        this.address = connection.getAddress();
        this.port = connection.getPort();
        this.status = status;
    }

    public void update(Connection connection) {
        this.address = connection.getAddress();
        this.port = connection.getPort();
    }
}
