package io.hnsn.kaukus.streams;

import java.io.IOException;
import java.io.InputStream;

public class RangedInputStream extends InputStream {
    private final InputStream inputStream;
    private final boolean allowClose;
    private final int length;
    private int bytesRead = 0;

    public RangedInputStream(InputStream inputStream, int length) {
        this(inputStream, length, false);
    }

    public RangedInputStream(InputStream inputStream, int length, boolean allowClose) {
        this.inputStream = inputStream;
        this.length = length;
        this.allowClose = allowClose;
    }

    @Override
    public int read() throws IOException {
        if (bytesRead >= length) return -1;
        var value = inputStream.read();
        if (value == -1) return -1;
        bytesRead++;
        return value;
    }
    
    @Override
    public void close() throws IOException {
        if (allowClose)
            inputStream.close();
    }
}
