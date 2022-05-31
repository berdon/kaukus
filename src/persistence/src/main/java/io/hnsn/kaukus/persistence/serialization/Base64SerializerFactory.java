package io.hnsn.kaukus.persistence.serialization;

import java.io.InputStream;
import java.io.OutputStream;

public class Base64SerializerFactory implements SerializerFactory {
    @Override
    public StreamSerializer createStreamSerializer(OutputStream outputStream) {
        return new Base64StreamSerializer(outputStream);
    }

    @Override
    public Deserializer createDeserializer(InputStream inputStream) {
        return new Base64Deserializer(inputStream);
    }

    @Override
    public Serializer createSerializer() {
        // TODO Auto-generated method stub
        return null;
    }
}
