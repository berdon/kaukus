package io.hnsn.kaukus.base64;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Base64;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

public class Base64EncodingOutputStreamTests {
    @Test
    public void canEncodeMultiplePayloads() throws IOException {
        var output = new ByteArrayOutputStream();
        var serializingStream = new Base64EncodingOutputStream(output, true);
        serializingStream.write("some text".getBytes());
        serializingStream.flushWithPadding();
        output.write('\n');
        serializingStream.write("some more text".getBytes());
        serializingStream.flushWithPadding();
        serializingStream.close();

        var stringRepresentation = new String(output.toByteArray());
        var lines = stringRepresentation.lines().collect(Collectors.toList()).toArray(new String[0]);
        var decoder = Base64.getDecoder();
        assertEquals("some text", new String(decoder.decode(lines[0].getBytes())));
        assertEquals("some more text", new String(decoder.decode(lines[1].getBytes())));
    }
}