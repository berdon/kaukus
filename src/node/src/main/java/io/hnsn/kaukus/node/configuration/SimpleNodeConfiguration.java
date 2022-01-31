package io.hnsn.kaukus.node.configuration;

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

    private static final String KEY_IDENTITIFIER = "node.identifier";
}
