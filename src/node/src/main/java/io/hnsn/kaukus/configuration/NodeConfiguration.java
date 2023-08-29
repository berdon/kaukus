package io.hnsn.kaukus.configuration;

import java.nio.file.Path;

public interface NodeConfiguration {
    String getVersion();
    String getIdentifier();
    Path getSystemStorePath();
    Path getDataStorePath();
    int getSystemPort();
    String getSystemAddress();
    String getBroadcastAddress();
    int getBroadcastPort();
    String getWebServerHostname();
    int getWebServerPort();
    int getElectionTimeoutInSeconds();
}
