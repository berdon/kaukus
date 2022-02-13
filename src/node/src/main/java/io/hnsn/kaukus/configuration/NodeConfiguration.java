package io.hnsn.kaukus.configuration;

import java.nio.file.Path;

public interface NodeConfiguration {
    public String getIdentifier();
    public Path getSystemStorePath();
    public int getSystemPort();
    public String getSystemAddress();
    public String getBroadcastAddress();
    public int getBroadcastPort();
}
