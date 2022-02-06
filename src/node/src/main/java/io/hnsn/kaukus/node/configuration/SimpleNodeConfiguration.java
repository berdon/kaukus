package io.hnsn.kaukus.node.configuration;

import java.nio.file.Path;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class SimpleNodeConfiguration implements NodeConfiguration {
    private final Config _config;

    public SimpleNodeConfiguration(Config config) {
        config.checkValid(ConfigFactory.defaultReference(), "node");
        _config = config;
    }

    @Override
    public String getIdentifier() {
        return _config.getString(KEY_IDENTITIFIER);
    }

    @Override
    public Path getSystemStorePath() {
        var path = _config.getString(KEY_SYSTEM_STORE);
        if (path == null || path.isBlank()) return Path.of(DEFAULT_SYSTEM_STORE);
        return Path.of(path);
    }

    private static final String KEY_IDENTITIFIER = "node.identifier";
    private static final String KEY_SYSTEM_STORE = "node.system.store";
    private static final String DEFAULT_SYSTEM_STORE = "/etc/kaukus/system";
}
