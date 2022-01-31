package io.hnsn.kaukus.persistence;

public interface Serializer {
    byte[] serialize(String value);
}
