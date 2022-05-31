package io.hnsn.kaukus.persistence.serialization;

import java.io.IOException;

public interface Deserializer {
    String read() throws IOException;
}
