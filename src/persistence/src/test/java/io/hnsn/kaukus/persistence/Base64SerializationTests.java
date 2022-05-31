package io.hnsn.kaukus.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;

import org.junit.jupiter.api.Test;

import io.hnsn.kaukus.persistence.serialization.Base64Deserializer;
import io.hnsn.kaukus.persistence.serialization.Base64StreamSerializer;

public class Base64SerializationTests {
    @Test
    public void SerializingStringResultsInDeserializableByteArray() throws IOException {
        var outputStream = new ByteArrayOutputStream();
        try (var serializer = new Base64StreamSerializer(outputStream)) {
            serializer.write("some-text");

            var byteArray = outputStream.toByteArray();
            assertEquals("some-text", new String(Base64.getDecoder().decode(byteArray)));
        }
    }

    @Test
    public void CanDeserializingString() throws IOException {
        var bytes = Base64.getEncoder().encode("some-text".getBytes());
        var deserializer = new Base64Deserializer(new ByteArrayInputStream(bytes));
        assertEquals("some-text", deserializer.read());     
    }
}
