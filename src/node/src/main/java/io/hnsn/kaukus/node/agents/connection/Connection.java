package io.hnsn.kaukus.node.agents.connection;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public interface Connection extends Closeable {
    public String getNodeIdentifier();
    public String getAddress();
    public int getPort();
    public <TMessage extends SpecificRecord>  void sendMessage(TMessage Message);
    public void sendMessage(byte[] buffer) throws IOException;

    void registerOnMessageReceivedListener(OnMessageReceivedListener listener);
    void unregisterOnMessageReceivedListener(OnMessageReceivedListener listener);

    void registerOnClosedListener(OnConnectionClosedListener listener);
    void unregisterOnClosedListener(OnConnectionClosedListener listener);
}
