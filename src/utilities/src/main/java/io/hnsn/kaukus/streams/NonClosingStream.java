package io.hnsn.kaukus.streams;

import java.io.IOException;
import java.io.OutputStream;

public class NonClosingStream extends OutputStream {
    private final OutputStream outputStream;

    public NonClosingStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void close() throws IOException { }
}
