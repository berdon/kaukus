package io.hnsn.kaukus.node.agents.connection;

import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;

import io.hnsn.kaukus.guice.LoggerProvider;
import lombok.Getter;

public class ClientConnection extends SocketConnection {
    @Getter
    private final String nodeIdentifier;
    @Getter
    private final String address;
    @Getter
    private final int port;
    private final Logger log;

    public ClientConnection(LoggerProvider loggerProvider, String nodeIdentifier, String address, int port, ConnectionCloseable closeable, Socket socket) throws IOException {
        super(loggerProvider, closeable, socket);
        this.nodeIdentifier = nodeIdentifier;
        this.address = address;
        this.port = port;
        this.log = loggerProvider.get("ClientConnection");
    }
}
