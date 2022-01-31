package io.hnsn.kaukus.persistence;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import lombok.AllArgsConstructor;
import lombok.Data;

public class SSTable {
    private final Path filePath;
    private final SSTableConfiguration configuration;

    private IndexTuple[] index;
    private Object indexLock = new Object();

    public SSTable(Path filePath, SSTableConfiguration configuration) {
        this.filePath = filePath;
        this.configuration = configuration;
    }

    public SSTableResult tryGetValue(String key) throws FileNotFoundException, IOException {
        return tryGetValueOrContains(key, false);
    }

    public boolean containsKey(String key) throws FileNotFoundException, IOException {
        var result = tryGetValueOrContains(key, true);
        return result != null && !result.isTombstone;
    }

    /*package*/ SSTableResult tryGetValueOrContains(String key, boolean existsOnly) throws FileNotFoundException, IOException {
        var index = getIndex();
        var left = 0;
        var right = index.length;

        // Binary search the index to find the item/range
        while (left < right) {
            var middle = (left + right) / 2;
            var compare = index[middle].getKey().compareTo(key);
            if (compare == 0) {
                if (existsOnly == true) {
                    // Checking for existence only; skipping file open if we can
                    return index[middle].isTombstone ? SSTableResult.TOMBSTONE : SSTableResult.EMPTY;
                }

                // Grab the bytes
                var value = getByteValueAtOffset(index[middle].getIndex());

                // Double check it isn't a tombstone
                if (value == null) return SSTableResult.TOMBSTONE;

                // TODO: Probably clean up to reduce object creation?
                return new SSTableResult(configuration.serializerFactory.createDeserializer(new ByteArrayInputStream(value)).read());
            }
            else if (compare > 0) right = middle;
            else if (compare < 0) left = middle + 1;
        }

        // Walk the file starting left to the next index
        left = left - 1;
        if (left < 0 || left >= index.length) return null;
        var tuple = index[left];
        var offset = tuple.getIndex();
        long end = -1;

        end = left + 1 < index.length ? index[left + 1].getIndex() : Files.size(filePath);
        try (var randomAccessFile = new RandomAccessFile(filePath.toString(), "r")) {
            randomAccessFile.seek(offset);
            while (offset < end) {
                var line = randomAccessFile.readLine();
                offset += line.length() + 1;
                var tokens = line.split(":");
                var lineKey = new String(Base64.getDecoder().decode(tokens[0]));
                
                if (lineKey.compareTo(key) == 0) {
                    if (existsOnly == true) {
                        // Existence only; skip deserializing the value
                        return tokens.length == 1 ? SSTableResult.TOMBSTONE : SSTableResult.EMPTY;
                    }

                    // Read and deserialize
                    var lineValue = configuration.serializerFactory.createDeserializer(new ByteArrayInputStream(tokens[1].getBytes())).read();
                    return new SSTableResult(lineValue);
                }
            }
        }

        return null;
    }

    private byte[] getByteValueAtOffset(long offset) {
        try (var randomAccessFile = new RandomAccessFile(filePath.toString(), "r")) {
            randomAccessFile.seek(offset);

            // Read and split the line
            var line = randomAccessFile.readLine();
            offset += line.length();
            var tokens = line.split(":");

            // Tombstone marker
            if (tokens.length == 1) return null;

            // Return the bytes
            return tokens[1].getBytes();
        } catch (IOException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    private IndexTuple[] getIndex() {
        if (index == null) {
            synchronized (indexLock) {
                if (index == null) {
                    try {
                        index = new ByteRatioLineMapper().mapLines(configuration.getMappingRatio(), filePath);
                    } catch (IOException e) {
                        // TODO
                        throw new RuntimeException();
                    }
                }
            }
        }

        return index;
    }

    @AllArgsConstructor
    @Data
    /*package*/ static class IndexTuple {
        private final String key;
        private final long index;
        private final boolean isTombstone;
    }
}
