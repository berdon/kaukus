package io.hnsn.kaukus.node.agents.broadcast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.encoding.HelloBroadcast;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.guiceModules.NodeModule.NodeIdentifier;
import io.hnsn.kaukus.node.agents.AgentException;
import io.hnsn.kaukus.node.agents.server.ServerAgent;
import io.hnsn.kaukus.node.state.ListenerSource;

public class BroadcastAgentImpl implements BroadcastAgent {
    private final String broadcastAddress;
    private final int broadcastPort;
    private final Logger log;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ListenerSource listeners = new ListenerSource();
    private final ServerAgent serverAgent;
    private final NodeConfiguration configuration;
    private final String nodeIdentifier;
    
    private InetAddress boundAddress;
    private MulticastSocket multicastSocket;
    private Thread serverThread;

    private ScheduledFuture<?> broadcastTask;

    public BroadcastAgentImpl(
        String broadcastAddress,
        int broadcastPort,
        LoggerProvider loggerProvider,
        ServerAgent serverAgent,
        ExecutorService executorService,
        ScheduledExecutorService scheduledExecutorService,
        NodeConfiguration configuration,
        @NodeIdentifier String nodeIdentifier
    ) {
        this.broadcastAddress = broadcastAddress;
        this.broadcastPort = broadcastPort;
        this.serverAgent = serverAgent;
        this.executorService = executorService;
        this.scheduledExecutorService = scheduledExecutorService;
        this.configuration = configuration;
        this.nodeIdentifier = nodeIdentifier;
        this.log = loggerProvider.get("BroadcastAgent");
    }

    @Override
    public void start() throws AgentException {
        startBroadcastListener();
        broadcastHello();
    }

    private void startBroadcastListener() throws AgentException {
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

    private void broadcastHello() {
        broadcastTask = this.scheduledExecutorService.schedule(() -> {
            // Send out a broadcast
            var helloPacket = new HelloBroadcast(
                nodeIdentifier,
                serverAgent.getBoundAddress().getHostAddress(),
                serverAgent.getBoundPort(),
                configuration.getVersion());
            var datumWriter = new SpecificDatumWriter<>(HelloBroadcast.class);
            var buffer = new ByteArrayOutputStream();
            try {
                try (var dataFileWriter = new DataFileWriter<>(datumWriter)) {
                    dataFileWriter.create(HelloBroadcast.getClassSchema(), buffer);
                    dataFileWriter.append(helloPacket);
                    dataFileWriter.close();
                }

                var socket = new DatagramSocket();
                
                var group = InetAddress.getByName(configuration.getBroadcastAddress());
                var packet = new DatagramPacket(buffer.toByteArray(), buffer.toByteArray().length, group, configuration.getBroadcastPort());

                socket.send(packet);
                socket.close();
            } catch (Exception e) {
                log.warn("Failed to send out hello broadcast");
            }
        }, 1, TimeUnit.SECONDS);
    }

    private void onBroadcastReceived(DatagramPacket packet) {
        var datumReader = new GenericDatumReader<GenericRecord>();
        // new DataReader
        try (var dataReader = new DataFileStream<GenericRecord>(new ByteArrayInputStream(packet.getData()), datumReader)) {
            var payload = dataReader.next();
            if (payload.getSchema().getName().compareTo(HelloBroadcast.SCHEMA$.getName()) == 0) {
                HelloBroadcast helloBroadcast = (HelloBroadcast) SpecificData.getForSchema(HelloBroadcast.SCHEMA$).deepCopy(payload.getSchema(), payload);
                if (serverAgent.getBoundAddress().getHostAddress().compareTo(helloBroadcast.getSourceAddress()) == 0 && serverAgent.getBoundPort() == helloBroadcast.getSourcePort()) return;
                log.info("Received hello broadcast from {}:{} ({})", helloBroadcast.getSourceAddress(), helloBroadcast.getSourcePort(), helloBroadcast.getVersion());

                listeners.get(OnNodeDiscoveredListener.class).forEach(listener -> listener.onReceived(helloBroadcast));
            }
        } catch (Exception e) {
            log.warn("Exception occurred", e);
        }
    }

    @Override
    public void close() throws IOException {
        do {
            broadcastTask.cancel(true);
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
    public void registerOnNodeDiscoveredListener(OnNodeDiscoveredListener listener) {
        listeners.get(OnNodeDiscoveredListener.class).add(listener);
    }

    @Override
    public void unregisterOnNodeDiscoveredListener(OnNodeDiscoveredListener listener) {
        listeners.get(OnNodeDiscoveredListener.class).remove(listener);
    }
}
