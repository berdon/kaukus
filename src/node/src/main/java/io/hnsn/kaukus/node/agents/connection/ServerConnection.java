package io.hnsn.kaukus.node.agents.connection;

import java.net.Socket;

import org.slf4j.Logger;

import io.hnsn.kaukus.guice.LoggerProvider;
import lombok.Getter;
import lombok.Setter;

public class ServerConnection extends SocketConnection {
    @Getter @Setter
    private String nodeIdentifier;
    @Getter @Setter
    private String address;
    @Getter @Setter
    private int port;
    private final Logger log;

    public ServerConnection(LoggerProvider loggerProvider, String nodeIdentifier, ConnectionCloseable closeable, Socket socket) {
        super(loggerProvider, closeable, socket);
        this.nodeIdentifier = nodeIdentifier;
        this.log = loggerProvider.get("ServerConnection");
    }
}
