package io.hnsn.kaukus.persistence;

import lombok.Builder.Default;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class SSTableConfiguration {
    @Default
    private final float mappingRatio = 0.1f;
    @Default
    final SerializerFactory serializerFactory = new Base64SerializerFactory();
}