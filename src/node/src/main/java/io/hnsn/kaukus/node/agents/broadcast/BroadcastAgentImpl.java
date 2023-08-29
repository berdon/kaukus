package io.hnsn.kaukus.node.agents.broadcast;

import com.google.inject.Provider;
import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.encoding.HelloBroadcast;
import io.hnsn.kaukus.encoding.LeaderElectedBroadcast;
import io.hnsn.kaukus.encoding.LeaderVoteBroadcast;
import io.hnsn.kaukus.encoding.RequestLeaderBroadcast;
import io.hnsn.kaukus.encoding.RequestLeaderVoteBroadcast;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.guiceModules.NodeModule.NodeIdentifier;
import io.hnsn.kaukus.node.agents.AgentException;
import io.hnsn.kaukus.node.agents.server.ServerAgent;
import io.hnsn.kaukus.node.state.ListenerSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;

public class BroadcastAgentImpl implements BroadcastAgent {
    private final String broadcastAddress;
    private final int broadcastPort;
    private final Logger log;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ListenerSource listeners = new ListenerSource();
    private final ServerAgent serverAgent;
    private final NodeConfiguration configuration;
    private final Provider<String> nodeIdentifier;
    private InetAddress broadcastGroup;
    
    private InetAddress boundAddress;
    private MulticastSocket multicastSocket;
    private Thread serverThread;

    private Set<Future<?>> tasks = new HashSet<>();

    public BroadcastAgentImpl(
        String broadcastAddress,
        int broadcastPort,
        LoggerProvider loggerProvider,
        ServerAgent serverAgent,
        ExecutorService executorService,
        ScheduledExecutorService scheduledExecutorService,
        NodeConfiguration configuration,
        @NodeIdentifier Provider<String> nodeIdentifier
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
        try {
            broadcastGroup = InetAddress.getByName(configuration.getBroadcastAddress());
        } catch (UnknownHostException e) {
            throw new AgentException("Unable to determined broadcast group hostname.", e);
        }

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
                byte[] buffer = new byte[508];
                while(!serverThread.isInterrupted()) {
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

    private <TBroadcast extends SpecificRecord> CompletableFuture<?> broadcastMessage(TBroadcast broadcast) {
        final var task = CompletableFuture.runAsync(() -> {
            log.info("Broadcasting {}.", broadcast.getClass().getSimpleName());
            @SuppressWarnings("unchecked")
            final var datumWriter = new SpecificDatumWriter<TBroadcast>((Class<TBroadcast>) broadcast.getClass());
            final var buffer = new ByteArrayOutputStream();
            try {
                try (var dataFileWriter = new DataFileWriter<TBroadcast>(datumWriter)) {
                    dataFileWriter.create(
                        (Schema) broadcast.getClass().getDeclaredMethod("getClassSchema").invoke(null),
                        buffer);
                    dataFileWriter.append(broadcast);
                }

                final var socket = new DatagramSocket();
                final var group = InetAddress.getByName(configuration.getBroadcastAddress());
                final var packet = new DatagramPacket(buffer.toByteArray(), buffer.toByteArray().length, group, configuration.getBroadcastPort());

                socket.send(packet);
                socket.close();
            } catch (Exception e) {
                log.warn("Failed to send out hello broadcast");
            }
        });
        tasks.add(task);
        task.thenApply((r) -> tasks.remove(task));
        return task;
    }

    private void broadcastHello() {
        broadcastMessage(new HelloBroadcast(
            nodeIdentifier.get(),
            serverAgent.getBoundAddress().getHostAddress(),
            serverAgent.getBoundPort(),
            configuration.getVersion()));
    }

    @Override
    public void broadcastRequestLeader() {
        broadcastMessage(new RequestLeaderBroadcast(nodeIdentifier.get(),
            serverAgent.getBoundAddress().getHostAddress(),
            serverAgent.getBoundPort()));
    }

    @Override
    public void broadcastRequestLeaderVote(int epoch) {
        broadcastMessage(new RequestLeaderVoteBroadcast(epoch,
            nodeIdentifier.get(),
            serverAgent.getBoundAddress().getHostAddress(),
            serverAgent.getBoundPort()));
    }

    @Override
    public void broadcastLeaderVote(int vote, int epoch) {
        broadcastMessage(new LeaderVoteBroadcast(vote,
            epoch,
            nodeIdentifier.get(),
            serverAgent.getBoundAddress().getHostAddress(),
            serverAgent.getBoundPort()));
    }

    @Override
    public void broadcastLeaderElected(int epoch) {
        broadcastMessage(new LeaderElectedBroadcast(epoch,
            nodeIdentifier.get(),
            serverAgent.getBoundAddress().getHostAddress(),
            serverAgent.getBoundPort()));
    }

    private void onBroadcastReceived(DatagramPacket packet) {
        var datumReader = new GenericDatumReader<GenericRecord>();
        // new DataReader
        try (var dataReader = new DataFileStream<>(new ByteArrayInputStream(packet.getData()), datumReader)) {
            var payload = dataReader.next();
            if (payload.getSchema().getName().compareTo(HelloBroadcast.SCHEMA$.getName()) == 0) {
                final var broadcast = (HelloBroadcast) SpecificData.getForSchema(HelloBroadcast.SCHEMA$).deepCopy(payload.getSchema(), payload);
                if (serverAgent.getBoundAddress().getHostAddress().compareTo(broadcast.getSourceAddress()) == 0 && serverAgent.getBoundPort() == broadcast.getSourcePort()) return;
                log.info("Received hello broadcast from {}:{} ({})", broadcast.getSourceAddress(), broadcast.getSourcePort(), broadcast.getVersion());

                listeners.get(OnNodeDiscoveredListener.class).forEach(listener -> listener.onReceived(broadcast));
            }
            else if (payload.getSchema().getName().compareTo(RequestLeaderBroadcast.SCHEMA$.getName()) == 0) {
                final var broadcast = (RequestLeaderBroadcast) SpecificData.getForSchema(RequestLeaderBroadcast.SCHEMA$).deepCopy(payload.getSchema(), payload);
                if (serverAgent.getBoundAddress().getHostAddress().compareTo(broadcast.getSourceAddress()) == 0 && serverAgent.getBoundPort() == broadcast.getSourcePort()) return;
                log.info("Received request leader broadcast from {}:{}", broadcast.getSourceAddress(), broadcast.getSourcePort());

                listeners.get(OnLeaderRequestedListener.class).forEach(listener -> listener.onLeaderRequested(broadcast));
            }
            else if (payload.getSchema().getName().compareTo(RequestLeaderVoteBroadcast.SCHEMA$.getName()) == 0) {
                final var broadcast = (RequestLeaderVoteBroadcast) SpecificData.getForSchema(RequestLeaderVoteBroadcast.SCHEMA$).deepCopy(payload.getSchema(), payload);
                if (serverAgent.getBoundAddress().getHostAddress().compareTo(broadcast.getSourceAddress()) == 0 && serverAgent.getBoundPort() == broadcast.getSourcePort()) return;
                log.info("Received request leader vote broadcast from {}:{}", broadcast.getSourceAddress(), broadcast.getSourcePort());

                listeners.get(OnLeaderVoteRequestedListener.class).forEach(listener -> listener.onLeaderVoteRequested(broadcast));
            }
            else if (payload.getSchema().getName().compareTo(LeaderVoteBroadcast.SCHEMA$.getName()) == 0) {
                final var broadcast = (LeaderVoteBroadcast) SpecificData.getForSchema(LeaderVoteBroadcast.SCHEMA$).deepCopy(payload.getSchema(), payload);
                if (serverAgent.getBoundAddress().getHostAddress().compareTo(broadcast.getSourceAddress()) == 0 && serverAgent.getBoundPort() == broadcast.getSourcePort()) return;
                log.info("Received leader vote broadcast from {}:{}", broadcast.getSourceAddress(), broadcast.getSourcePort());

                listeners.get(OnLeaderVoteListener.class).forEach(listener -> listener.onLeaderVote(broadcast));
            }
            else if (payload.getSchema().getName().compareTo(LeaderElectedBroadcast.SCHEMA$.getName()) == 0) {
                final var broadcast = (LeaderElectedBroadcast) SpecificData.getForSchema(LeaderElectedBroadcast.SCHEMA$).deepCopy(payload.getSchema(), payload);
                if (serverAgent.getBoundAddress().getHostAddress().compareTo(broadcast.getSourceAddress()) == 0 && serverAgent.getBoundPort() == broadcast.getSourcePort()) return;
                log.info("Received leader elected broadcast from {}:{}", broadcast.getSourceAddress(), broadcast.getSourcePort());

                listeners.get(OnLeaderElectedListener.class).forEach(listener -> listener.onLeaderElected(broadcast));
            }
        } catch (Exception e) {
            log.warn("Exception occurred", e);
        }
    }

    @Override
    public void close() throws IOException {
        do {
            tasks.forEach(t -> t.cancel(true));
            serverThread.interrupt();
            multicastSocket.close();
            try { serverThread.join(1000); } catch (InterruptedException ignored) { }
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

    @Override
    public void registerOnLeaderRequested(OnLeaderRequestedListener listener) {
        listeners.get(OnLeaderRequestedListener.class).add(listener);
    }

    @Override
    public void unregisterOnLeaderRequested(OnLeaderRequestedListener listener) {
        listeners.get(OnLeaderRequestedListener.class).remove(listener);
    }

    @Override
    public void registerOnLeaderVoteRequested(OnLeaderVoteRequestedListener listener) {
        listeners.get(OnLeaderVoteRequestedListener.class).add(listener);
    }

    @Override
    public void unregisterOnLeaderVoteRequested(OnLeaderVoteRequestedListener listener) {
        listeners.get(OnLeaderVoteRequestedListener.class).remove(listener);
    }

    @Override
    public void registerOnLeaderVote(OnLeaderVoteListener listener) {
        listeners.get(OnLeaderVoteListener.class).add(listener);
    }

    @Override
    public void unregisterOnLeaderVote(OnLeaderVoteListener listener) {
        listeners.get(OnLeaderVoteListener.class).remove(listener);
    }

    @Override
    public void registerOnLeaderElected(OnLeaderElectedListener listener) {
        listeners.get(OnLeaderElectedListener.class).add(listener);
    }

    @Override
    public void unregisterOnLeaderElected(OnLeaderElectedListener listener) {
        listeners.get(OnLeaderElectedListener.class).remove(listener);
    }
}
