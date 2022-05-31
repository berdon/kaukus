package io.hnsn.kaukus.persistence.serialization;

public interface Serializer {
    byte[] serialize(String value);
}
