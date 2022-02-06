package io.hnsn.kaukus.node.guiceModules;

import java.time.LocalDateTime;

import com.esotericsoftware.kryo.Kryo;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import io.hnsn.kaukus.node.configuration.LSMTreeSystemStore;
import io.hnsn.kaukus.node.configuration.SystemStore;
import io.hnsn.kaukus.persistence.LSMTree;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AppModule extends AbstractModule {
    private LSMTree systemStore;

    @Provides
    @Singleton
    SystemStore provideSystemStore(Kryo kryo) {
        return new LSMTreeSystemStore(systemStore, kryo);
    }

    @Provides
    @Singleton
    Kryo providesKryo() {
        var kryo = new Kryo();
        kryo.register(LocalDateTime.class);
        return kryo;
    }
}
