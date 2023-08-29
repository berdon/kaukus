package io.hnsn.kaukus.configuration;

import java.nio.file.Path;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.hnsn.kaukus.parameters.NodeParameters;
import io.hnsn.kaukus.std.NullCoallesce;
import java.util.Objects;

public class SimpleNodeConfiguration implements NodeConfiguration {
    private static final String KEY_VERSION = "node.version";
    private static final String KEY_IDENTITIFIER = "node.identifier";
    private static final String KEY_SYSTEM_STORE = "node.system.store";
    private static final String KEY_DATA_STORE = "node.data.store";
    private static final String KEY_SYSTEM_PORT = "node.system.port";
    private static final String KEY_SYSTEM_ADDRESS = "node.system.address";
    private static final String KEY_BROADCAST_ADDRESS = "node.broadcast.address";
    private static final String KEY_BROADCAST_PORT = "node.broadcast.port";
    private static final String KEY_WEBSERVER_HOSTNAME = "node.webserver.hostname";
    private static final String KEY_WEBSERVER_PORT = "node.webserver.port";
    private static final String KEY_ELECTION_TIMEOUT = "node.election.timeout";
    private static final String DEFAULT_SYSTEM_STORE = "/etc/kaukus/system";
    private static final String DEFAULT_DATA_STORE = "/etc/kaukus/data";
    private static final int DEFAULT_SYSTEM_PORT = 21000;
    private static final int DEFAULT_BROADCAST_PORT = 21012;
    private static final String DEFAULT_SYSTEM_ADDRESS = "localhost";
    private static final String DEFAULT_BROADCAST_ADDRESS = "230.0.0.0";
    private static final String DEFAULT_WEBSERVER_HOSTNAME = "localhost";
    private static final int DEFAULT_WEBSERVER_PORT = 8080;
    private static final int DEFAULT_ELECTION_TIMEOUT_IN_SECONDS = 3;

    private final Config config;
    private final NodeParameters parameters;

    public SimpleNodeConfiguration(Config config, NodeParameters parameters) {
        config.checkValid(ConfigFactory.defaultReference(), "node");
        this.config = config;
        this.parameters = parameters;
    }

    @Override
    public String getVersion() {
        return (String) config.getAnyRef(KEY_VERSION);
    }

    @Override
    public String getIdentifier() {
        return NullCoallesce.of(parameters.getIdentifier(), getOrNull(String.class, KEY_IDENTITIFIER));
    }

    @Override
    public Path getSystemStorePath() {
        return Path.of(Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getSystemStorePath(), getOrNull(String.class, KEY_SYSTEM_STORE)),
            DEFAULT_SYSTEM_STORE));
    }

    @Override
    public Path getDataStorePath() {
        return Path.of(Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getDataStorePath(), getOrNull(String.class, KEY_DATA_STORE)),
            DEFAULT_DATA_STORE));
    }

    @Override
    public int getSystemPort() {
        return Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getSystemPort(), getOrNull(Integer.class, KEY_SYSTEM_PORT)),
            DEFAULT_SYSTEM_PORT);
    }

    @Override
    public String getSystemAddress() {
        return Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getSystemAddress(), getOrNull(String.class, KEY_SYSTEM_ADDRESS)),
            DEFAULT_SYSTEM_ADDRESS);
    }

    @Override
    public String getBroadcastAddress() {
        return Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getBroadcastAddress(), getOrNull(String.class, KEY_BROADCAST_ADDRESS)),
            DEFAULT_BROADCAST_ADDRESS);
    }

    @Override
    public int getBroadcastPort() {
        return Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getBroadcastPort(), getOrNull(Integer.class, KEY_BROADCAST_PORT)),
            DEFAULT_BROADCAST_PORT);
    }

    @Override
    public String getWebServerHostname() {
        return Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getWebserverHostname(), getOrNull(String.class, KEY_WEBSERVER_HOSTNAME)),
            DEFAULT_WEBSERVER_HOSTNAME);
    }

    @Override
    public int getWebServerPort() {
        return Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getWebserverPort(), getOrNull(Integer.class, KEY_WEBSERVER_PORT)),
            DEFAULT_WEBSERVER_PORT);
    }

    @Override
    public int getElectionTimeoutInSeconds() {
        return Objects.requireNonNullElse(
            NullCoallesce.of(parameters.getElectionTimeout(), getOrNull(Integer.class, KEY_ELECTION_TIMEOUT)),
            DEFAULT_ELECTION_TIMEOUT_IN_SECONDS
        );
    }

    @SuppressWarnings("unchecked")
    private <TValue> TValue getOrNull(Class<TValue> cls, String path) {
        return config.hasPath(path) ? (TValue) config.getAnyRef(path) : null;
    }
}
