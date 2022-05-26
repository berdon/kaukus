package io.hnsn.kaukus.configuration;

import java.nio.file.Path;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.hnsn.kaukus.parameters.NodeParameters;
import io.hnsn.kaukus.std.NullCoallesce;

public class SimpleNodeConfiguration implements NodeConfiguration {
    private static final String KEY_VERSION = "node.version";
    private static final String KEY_IDENTITIFIER = "node.identifier";
    private static final String KEY_SYSTEM_STORE = "node.system.store";
    private static final String KEY_SYSTEM_PORT = "node.system.port";
    private static final String KEY_SYSTEM_ADDRESS = "node.system.address";
    private static final String KEY_BROADCAST_ADDRESS = "node.broadcast.address";
    private static final String KEY_BROADCAST_PORT = "node.broadcast.port";
    private static final String DEFAULT_SYSTEM_STORE = "/etc/kaukus/system";
    private static final int DEFAULT_SYSTEM_PORT = 21000;
    private static final int DEFAULT_BROADCAST_PORT = 21012;
    private static final String DEFAULT_SYSTEM_ADDRESS = "localhost";
    private static final String DEFAULT_BROADCAST_ADDRESS = "230.0.0.0";

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
        return Path.of(NullCoallesce.of(parameters.getSystemStorePath(), getOrNull(String.class, KEY_SYSTEM_STORE), DEFAULT_SYSTEM_STORE));
    }

    @Override
    public int getSystemPort() {
        return NullCoallesce.of(parameters.getSystemPort(), getOrNull(Integer.class, KEY_SYSTEM_PORT), DEFAULT_SYSTEM_PORT);
    }

    @Override
    public String getSystemAddress() {
        return NullCoallesce.of(parameters.getSystemAddress(), getOrNull(String.class, KEY_SYSTEM_ADDRESS), DEFAULT_SYSTEM_ADDRESS);
    }

    @Override
    public String getBroadcastAddress() {
        return NullCoallesce.of(parameters.getBroadcastAddress(), getOrNull(String.class, KEY_BROADCAST_ADDRESS), DEFAULT_BROADCAST_ADDRESS);
    }

    @Override
    public int getBroadcastPort() {
        return NullCoallesce.of(parameters.getBroadcastPort(), getOrNull(Integer.class, KEY_BROADCAST_PORT), DEFAULT_BROADCAST_PORT);
    }

    @SuppressWarnings("unchecked")
    private <TValue> TValue getOrNull(Class<TValue> cls, String path) {
        return config.hasPath(path) ? (TValue) config.getAnyRef(path) : null;
    }
}
