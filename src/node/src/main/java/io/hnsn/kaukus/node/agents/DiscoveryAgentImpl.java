package io.hnsn.kaukus.node.agents;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.ExecutorService;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;

import io.hnsn.kaukus.encoding.Hello;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.state.ListenerSource;

public class DiscoveryAgentImpl implements DiscoveryAgent {
    private final String broadcastAddress;
    private final int broadcastPort;
    private final Logger log;
    private final ExecutorService executorService;
    private final ListenerSource listeners = new ListenerSource();
    private final ServerAgent serverAgent;
    
    private InetAddress boundAddress;
    private MulticastSocket multicastSocket;
    private Thread serverThread;

    public DiscoveryAgentImpl(String broadcastAddress, int broadcastPort, LoggerProvider loggerProvider, ServerAgent serverAgent, ExecutorService executorService) {
        this.broadcastAddress = broadcastAddress;
        this.broadcastPort = broadcastPort;
        this.serverAgent = serverAgent;
        this.executorService = executorService;
        this.log = loggerProvider.get("ServerAgent");
    }

    @Override
    public void start() throws AgentException {
        try {
            boundAddress = InetAddress.getByName(broadcastAddress);
            multicastSocket = new MulticastSocket(broadcastPort);
            multicastSocket.joinGroup(boundAddress);

            // Start the broadcast listening thread
            serverThread = new Thread(() -> {
                while(!serverThread.isInterrupted()) {
                    byte[] buffer = new byte[508];
                    var packet = new DatagramPacket(buffer, buffer.length);
                    try {
                        multicastSocket.receive(packet);
                        executorService.submit(() -> onBroadcastReceived(packet));
                    } catch (IOException e) {
                        if (!serverThread.isInterrupted())
                            log.warn("Failed to receive system broadcast message");
                    }
                }
            });
            serverThread.start();
        } catch (IOException e) {
            throw new AgentException("Failed to create a server socket", e);
        }
    }

    private void onBroadcastReceived(DatagramPacket packet) {
        var datumReader = new GenericDatumReader<GenericRecord>();
        // new DataReader
        try (var dataReader = new DataFileStream<GenericRecord>(new ByteArrayInputStream(packet.getData()), datumReader)) {
            var payload = dataReader.next();
            if (payload.getSchema().getName().compareTo(Hello.SCHEMA$.getName()) == 0) {
                Hello hello = (Hello) SpecificData.getForSchema(Hello.SCHEMA$).deepCopy(payload.getSchema(), payload);
                if (serverAgent.getBoundAddress().getHostAddress().compareTo(hello.getSourceAddress()) == 0 && serverAgent.getBoundPort() == hello.getSourcePort()) return;
                log.info("Received hello broadcast from {}:{}", hello.getSourceAddress(), hello.getSourcePort());
            }
        } catch (Exception e) {
            log.warn("Exception occurred", e);
        }

        listeners.get(OnBroadcastReceivedListener.class).forEach(listener -> listener.onReceived(packet));
    }

    @Override
    public void close() throws IOException {
        do {
            serverThread.interrupt();
            multicastSocket.close();
            try { serverThread.join(1000); } catch (InterruptedException e) { }
        } while(serverThread.isAlive());
    }

    @Override
    public InetAddress getBoundAddress() {
        return boundAddress;
    }

    @Override
    public int getBoundPort() {
        return broadcastPort;
    }

    @Override
    public void registerOnBroadcastReceivedListener(OnBroadcastReceivedListener listener) {
        listeners.get(OnBroadcastReceivedListener.class).add(listener);
    }

    @Override
    public void unregisterOnBroadcastReceivedListener(OnBroadcastReceivedListener listener) {
        listeners.get(OnBroadcastReceivedListener.class).remove(listener);
    }
}
