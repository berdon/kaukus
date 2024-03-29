package io.hnsn.kaukus.persistence;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;
import java.util.Base64.Encoder;

import io.hnsn.kaukus.persistence.serialization.SerializerFactory;
import io.hnsn.kaukus.persistence.serialization.StreamSerializer;

public class SSTableWriter implements Closeable, Flushable {
    private final OutputStream out;
    private final Encoder encoder = Base64.getEncoder();
    private final StreamSerializer serializer;

    public SSTableWriter(OutputStream out, SerializerFactory serializerFactory) {
        this.out = out;
        this.serializer = serializerFactory.createStreamSerializer(out);
    }

    public void write(String key, String value) throws IOException {
        out.write(encoder.encode(key.getBytes()));
        out.write(':');
        serializer.write(value);
        out.write('\n');
    }

    public void writeTombstone(String key) throws IOException {
        out.write(encoder.encode(key.getBytes()));
        out.write('\n');
    }

    public void flush() throws IOException {
        out.flush();
    }

    public void close() throws IOException {
        out.close();
    }
}
