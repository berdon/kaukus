package io.hnsn.kaukus.node.agents;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;

import javax.net.ServerSocketFactory;

import org.slf4j.Logger;

import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.state.ListenerSource;

public class ServerAgentImpl implements ServerAgent {
    private final String address;
    private final int port;
    private final ServerSocketFactory serverSocketFactory;
    private final Logger log;
    private final ExecutorService executorService;
    private final ListenerSource listeners = new ListenerSource();
    
    private ServerSocket systemSocket;
    private Thread serverThread;

    public ServerAgentImpl(String address, int backlog, int port, ServerSocketFactory serverSocketFactory, LoggerProvider loggerProvider, ExecutorService executorService) {
        this.address = address;
        this.port = port;
        this.serverSocketFactory = serverSocketFactory;
        this.executorService = executorService;
        this.log = loggerProvider.get("ServerAgent");
    }

    @Override
    public void start() throws AgentException {
        try {
            var systemAddress = InetAddress.getByName(address);
            this.systemSocket = serverSocketFactory.createServerSocket(port, 0, systemAddress);

            // Start the server thread
            serverThread = new Thread(() -> {
                while(!serverThread.isInterrupted()) {
                    try {
                        var socket = systemSocket.accept();
                        executorService.submit(() -> {
                            listeners.get(OnClientConnectedListener.class).forEach(listener -> listener.onConnected(socket));
                        });
                    } catch (IOException e) {
                        if (!serverThread.isInterrupted())
                            log.warn("Failed to create client socket for server communication", e);
                    }
                }
            });
            serverThread.start();
        } catch (IOException e) {
            throw new AgentException("Failed to create a server socket", e);
        }
    }

    @Override
    public void close() throws IOException {
        do {
            serverThread.interrupt();
            try { systemSocket.close(); } catch (IOException e) { }
            try { serverThread.join(1000); } catch (InterruptedException e) { }
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
}
