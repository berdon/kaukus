package io.hnsn.kaukus.configuration;

import java.nio.file.Path;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.hnsn.kaukus.parameters.NodeParameters;
import io.hnsn.kaukus.std.NullCoallesce;

public class SimpleNodeConfiguration implements NodeConfiguration {
    private final Config config;
    private final NodeParameters parameters;

    public SimpleNodeConfiguration(Config config, NodeParameters parameters) {
        config.checkValid(ConfigFactory.defaultReference(), "node");
        this.config = config;
        this.parameters = parameters;
    }

    @Override
    public String getIdentifier() {
        return NullCoallesce.of(parameters.getIdentifier(), config.getString(KEY_IDENTITIFIER));
    }

    @Override
    public Path getSystemStorePath() {
        var path = config.getString(KEY_SYSTEM_STORE);
        if (path == null || path.isBlank()) return Path.of(DEFAULT_SYSTEM_STORE);
        return Path.of(path);
    }

    private static final String KEY_IDENTITIFIER = "node.identifier";
    private static final String KEY_SYSTEM_STORE = "node.system.store";
    private static final String DEFAULT_SYSTEM_STORE = "/etc/kaukus/system";
}
