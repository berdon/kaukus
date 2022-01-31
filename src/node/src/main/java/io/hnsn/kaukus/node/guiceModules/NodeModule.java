package io.hnsn.kaukus.node.guiceModules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.typesafe.config.ConfigFactory;

import io.hnsn.kaukus.node.configuration.NodeConfiguration;
import io.hnsn.kaukus.node.configuration.SimpleNodeConfiguration;

public class NodeModule extends AbstractModule {
    @Provides
    static NodeConfiguration provideNodeConfiguration() {
        return new SimpleNodeConfiguration(ConfigFactory.load());
    }
}
