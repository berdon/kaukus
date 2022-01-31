package io.hnsn.kaukus.base64;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;

import io.hnsn.kaukus.streams.NonClosingStream;

public class Base64EncodingOutputStream extends OutputStream {
    private final OutputStream outputStream;
    private final boolean closeUnderlyingStream;
    private OutputStream serializingStream;

    public Base64EncodingOutputStream(OutputStream outputStream) {
        this(outputStream, false);
    }

    public Base64EncodingOutputStream(OutputStream outputStream, boolean closeUnderlyingStream) {
        this.outputStream = outputStream;
        this.closeUnderlyingStream = closeUnderlyingStream;
        this.serializingStream = createSerializingStream(outputStream);
    }

    @Override
    public void write(int b) throws IOException {
        this.serializingStream.write(b);
    }

    public void flushWithPadding() throws IOException {
        serializingStream.flush();
        serializingStream.close();
        serializingStream = createSerializingStream(outputStream);
    }

    @Override
    public void close() throws IOException {
        serializingStream.close();
        if (closeUnderlyingStream) outputStream.close();
    }

    private static OutputStream createSerializingStream(OutputStream outputStream) {
        return  Base64.getEncoder().wrap(new NonClosingStream(outputStream));
    }
}
