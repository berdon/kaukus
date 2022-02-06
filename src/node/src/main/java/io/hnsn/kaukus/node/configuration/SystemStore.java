package io.hnsn.kaukus.node.configuration;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;

public interface SystemStore extends Closeable {
    void setLastStartedAt(LocalDateTime time) throws IOException;
    LocalDateTime getLastStartedAt();
}
