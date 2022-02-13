package io.hnsn.kaukus.node.agents;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.encoding.Hello;
import io.hnsn.kaukus.guice.LoggerProvider;

public class ClientAgentImpl implements ClientAgent {
    private final NodeConfiguration configuration;
    private final ServerAgent serverAgent;
    private final ScheduledExecutorService executorService;
    private final Logger log;

    private ScheduledFuture<?> broadcastTask;

    public ClientAgentImpl(NodeConfiguration configuration, ServerAgent serverAgent, LoggerProvider loggerProvider, ScheduledExecutorService executorService) {
        this.configuration = configuration;
        this.serverAgent = serverAgent;
        this.executorService = executorService;
        this.log = loggerProvider.get("ClientAgent");
    }

    @Override
    public void start() throws AgentException {
        broadcastTask = this.executorService.schedule(() -> {
                // Send out a broadcast
            var helloPacket = new Hello(serverAgent.getBoundAddress().getHostAddress(), serverAgent.getBoundPort(), "0.0.1");
            var datumWriter = new SpecificDatumWriter<>(Hello.class);
            var buffer = new ByteArrayOutputStream();
            try {
                try (var dataFileWriter = new DataFileWriter<>(datumWriter)) {
                    dataFileWriter.create(Hello.getClassSchema(), buffer);
                    dataFileWriter.append(helloPacket);
                    dataFileWriter.close();
                }

                var socket = new DatagramSocket();
                
                var group = InetAddress.getByName("230.0.0.0");
                var packet = new DatagramPacket(buffer.toByteArray(), buffer.toByteArray().length, group, configuration.getBroadcastPort());

                socket.send(packet);
                socket.close();
            } catch (Exception e) {
                log.warn("Failed to send out hello broadcast");
            }
        }, 1, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws IOException {
        broadcastTask.cancel(true);
        executorService.shutdown();
    }
}
