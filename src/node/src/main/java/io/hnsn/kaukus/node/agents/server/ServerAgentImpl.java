package io.hnsn.kaukus.node.agents.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.net.ServerSocketFactory;

import org.slf4j.Logger;

import com.google.inject.Provider;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.encoding.HelloMessage;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.agents.AgentException;
import io.hnsn.kaukus.node.agents.connection.ServerConnection;
import io.hnsn.kaukus.node.state.ListenerSource;
import io.hnsn.kaukus.guiceModules.NodeModule.NodeIdentifier;

public class ServerAgentImpl implements ServerAgent {
    private final String address;
    private final int port;
    private final ServerSocketFactory serverSocketFactory;
    private final Logger log;
    private final LoggerProvider loggerProvider;
    private final ExecutorService executorService;
    private final ListenerSource listeners = new ListenerSource();
    private final Map<String, Socket> sockets = new ConcurrentHashMap<>();
    private final Provider<String> nodeIdentifier;
	private final NodeConfiguration nodeConfiguration;
    
    private ServerSocket systemSocket;
    private Thread serverThread;

    public ServerAgentImpl(
        String address,
        int backlog,
        int port,
        ServerSocketFactory serverSocketFactory,
        LoggerProvider loggerProvider,
        ExecutorService executorService,
        NodeConfiguration nodeConfiguration,
        @NodeIdentifier Provider<String> nodeIdentifier
    ) {
        this.address = address;
        this.port = port;
        this.serverSocketFactory = serverSocketFactory;
        this.executorService = executorService;
		this.nodeConfiguration = nodeConfiguration;
        this.log = loggerProvider.get("ServerAgent");
        this.loggerProvider = loggerProvider;
        this.nodeIdentifier = nodeIdentifier;
    }

    @Override
    public void start() throws AgentException {
        try {
            var systemAddress = InetAddress.getByName(address);
            this.systemSocket = serverSocketFactory.createServerSocket(port, 0, systemAddress);

            log.info("Starting server thread");

            // Start the server thread
            serverThread = new Thread(() -> {
                log.info("Listening for incoming connections");
                while(!serverThread.isInterrupted()) {
                    try {
                        var socket = systemSocket.accept();
                        socket.setKeepAlive(true);
                        // TODO: Only add socket after initial handshake getting node ID
                        var socketId = String.format("%s:%d", socket.getLocalAddress(), socket.getPort());
                        sockets.put(socketId, socket);
                        var connection = new ServerConnection(loggerProvider, socketId, (con) -> {
                            final var s = sockets.remove(socketId);
                            try { s.close(); }
                            catch (IOException e) { log.warn("Exception occurred while closing socket", e); }
                        
                            log.info("Server connection {} disconnected", con.getNodeIdentifier());
                            executorService.submit(() -> {
                                listeners.get(OnClientDisconnectedListener.class).forEach(listener -> listener.onDisconnected(con));
                            });
                        }, socket);

                        log.info("Server connection established with {}", connection.getNodeIdentifier());
                        executorService.submit(() -> {
                            listeners.get(OnClientConnectedListener.class).forEach(listener -> listener.onConnected(connection));
                        });
                    } catch (IOException e) {
                        if (!serverThread.isInterrupted())
                            log.warn("Failed to create client socket for server communication", e);
                    }
                }
                var i = 5;
            });
            serverThread.start();
        } catch (IOException e) {
            throw new AgentException("Failed to create a server socket", e);
        }
    }

    @Override
    public void close() throws IOException {
        do {
            log.debug("Terminating server thread.");
            serverThread.interrupt();
            sockets.values().forEach(s -> {
                log.debug("Terminating socket connection [{}].", s.toString());
                try { s.close(); } catch (IOException ignored) { }
            });
            log.debug("Terminating system socket.");
            try { systemSocket.close(); } catch (IOException ignored) { }
            log.debug("Waiting for server thread to terminate.");
            try { serverThread.join(1000); } catch (InterruptedException ignored) { }
        } while(serverThread.isAlive());
    }

    @Override
    public InetAddress getBoundAddress() {
        return systemSocket.getInetAddress();
    }

    @Override
    public int getBoundPort() {
        return systemSocket.getLocalPort();
    }

    @Override
    public void registerOnClientConnectedListener(OnClientConnectedListener listener) {
        listeners.get(OnClientConnectedListener.class).add(listener);
    }

    @Override
    public void unregisterOnClientConnectedListener(OnClientConnectedListener listener) {
        listeners.get(OnClientConnectedListener.class).remove(listener);
    }

    @Override
    public void registerOnClientDisconnectedListener(OnClientDisconnectedListener listener) {
        listeners.get(OnClientDisconnectedListener.class).add(listener);
    }

    @Override
    public void unregisterOnClientDisconnectedListener(OnClientDisconnectedListener listener) {
        listeners.get(OnClientDisconnectedListener.class).remove(listener);
    }
}
