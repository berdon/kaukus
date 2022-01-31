package io.hnsn.kaukus.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

public class Base64Deserializer implements Deserializer {
    private final InputStream inputStream;

    public Base64Deserializer(InputStream inputStream) {
        this.inputStream = Base64.getDecoder().wrap(inputStream);
    }

    @Override
    public String read() throws IOException {
        return new String(this.inputStream.readAllBytes());
    }    
}
