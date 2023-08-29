package io.hnsn.kaukus.node.agents.connection;

import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;

import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.node.state.ListenerSource;

public abstract class BaseConnection implements Connection {
    private final ListenerSource listeners = new ListenerSource();
    private final Logger log;

    public BaseConnection(LoggerProvider loggerProvider) {
        this.log = loggerProvider.get("BaseConnection");
    }

    protected void onMessageReceived(GenericRecord payload) {
        for (var listener : listeners.get(OnMessageReceivedListener.class)) {
            listener.onMessage(listener, payload);
        }
    }

    @Override
    public synchronized <TMessage extends SpecificRecord> void sendMessage(TMessage message) {
        @SuppressWarnings("unchecked")
        var datumWriter = new SpecificDatumWriter<TMessage>((Class<TMessage>) message.getClass());
        var buffer = new ByteArrayOutputStream();
        try {
            try (var dataFileWriter = new DataFileWriter<TMessage>(datumWriter)) {
                dataFileWriter.create(
                    (Schema) message.getClass().getDeclaredMethod("getClassSchema").invoke(null),
                    buffer);
                dataFileWriter.append(message);
            }

            sendMessage(buffer.toByteArray());
        } catch (Exception e) {
            log.warn("Failed to send out hello broadcast");
        }
    }

    @Override
    public void registerOnMessageReceivedListener(OnMessageReceivedListener listener) {
        listeners.get(OnMessageReceivedListener.class).add(listener);
    }

    @Override
    public void unregisterOnMessageReceivedListener(OnMessageReceivedListener listener) {
        listeners.get(OnMessageReceivedListener.class).remove(listener);
    }

    @Override
    public void registerOnClosedListener(OnConnectionClosedListener listener) {
        listeners.get(OnConnectionClosedListener.class).add(listener);
    }

    @Override
    public void unregisterOnClosedListener(OnConnectionClosedListener listener) {
        listeners.get(OnConnectionClosedListener.class).remove(listener);
    }

    protected void onClosed() {
        for (var closedListeners : listeners.get(OnConnectionClosedListener.class)) {
            closedListeners.onClosed(this);
        }

        listeners.clear();
    }
}
