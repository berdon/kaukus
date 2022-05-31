package io.hnsn.kaukus.node.agents.connection;

import java.io.IOException;
import java.net.Socket;

public class ServerConnection extends BaseConnection {
    private final String nodeIdentifier;
    private final ConnectionCloseable closeable;
    private final Socket socket;

    public ServerConnection(String nodeIdentifier, ConnectionCloseable closeable, Socket socket) {
        this.nodeIdentifier = nodeIdentifier;
        this.closeable = closeable;
        this.socket = socket;
    }

    @Override
    public void close() throws IOException {
        closeable.close(this);
    }

    @Override
    public String getNodeIdentifier() {
        return nodeIdentifier;
    }
}
