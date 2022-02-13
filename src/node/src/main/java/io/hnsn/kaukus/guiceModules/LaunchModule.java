package io.hnsn.kaukus.guiceModules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;

import io.hnsn.kaukus.configuration.NodeConfiguration;
import io.hnsn.kaukus.configuration.SimpleNodeConfiguration;
import io.hnsn.kaukus.guice.LoggerProvider;
import io.hnsn.kaukus.parameters.NodeParameters;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LaunchModule extends AbstractModule {
    private final NodeParameters parameters;

    @Provides
    NodeConfiguration provideNodeConfiguration() {
        return new SimpleNodeConfiguration(ConfigFactory.load(ConfigParseOptions.defaults()), parameters);
    }

    @Singleton @Provides
    LoggerProvider provideLoggerProvider() {
        return new LoggerProvider();
    }
}
