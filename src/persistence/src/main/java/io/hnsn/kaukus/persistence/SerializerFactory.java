package io.hnsn.kaukus.persistence;

import java.io.InputStream;
import java.io.OutputStream;

public interface SerializerFactory {
    Serializer createSerializer();
    StreamSerializer createStreamSerializer(OutputStream outputStream);

    Deserializer createDeserializer(InputStream inputStream);
}
