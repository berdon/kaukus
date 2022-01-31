package io.hnsn.kaukus.persistence;

import java.io.Closeable;
import java.io.IOException;

public interface StreamSerializer extends Closeable {
    void write(String value) throws IOException;
}
