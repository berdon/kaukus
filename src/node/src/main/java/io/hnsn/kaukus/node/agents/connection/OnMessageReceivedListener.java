package io.hnsn.kaukus.node.agents.connection;

import org.apache.avro.generic.GenericRecord;

public interface OnMessageReceivedListener {
    void onMessage(OnMessageReceivedListener listener, GenericRecord payload);
}
