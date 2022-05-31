package io.hnsn.kaukus.persistence.serialization;

import java.io.IOException;
import java.io.OutputStream;

import io.hnsn.kaukus.base64.Base64EncodingOutputStream;

public class Base64StreamSerializer implements StreamSerializer {
    private Base64EncodingOutputStream outputStream;

    public Base64StreamSerializer(OutputStream outputStream) {
        this.outputStream = new Base64EncodingOutputStream(outputStream, true);
    }

    @Override
    public void write(String value) throws IOException {
        // TODO: Encoding
        outputStream.write(value.getBytes());
        outputStream.flushWithPadding();
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}
