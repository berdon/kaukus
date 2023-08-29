package io.hnsn.kaukus.streams;

import java.io.IOException;
import java.io.InputStream;

public class BoundlessInputStream extends InputStream {
    private final InputStream inputStream;

    public BoundlessInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public int read(byte[] buffer) throws IOException {
        var read = 0;
        try {
            while (read != buffer.length) {
                var count = inputStream.read(buffer, read, buffer.length - read);
                read += (Math.max(count, 0));
            }
        }
        catch (IOException ignored) { }

        return read;
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
