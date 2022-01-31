package io.hnsn.kaukus.persistence;

import java.io.IOException;

public interface Deserializer {
    String read() throws IOException;
}
