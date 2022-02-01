package io.hnsn.kaukus.persistence;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

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

    /**
     * Returns whether the SSTable contains the key or not. Note: Containing the key doesn't
     * mean the SSTable "contains" the key in the sense of a Map interface. Containment is
     * dependant upon whether the value is tombstoned (deleted) or not. If the value
     * is a tombstone then this SSTable has marked the item as having been deleted.
     * @param key The key to query
     * @return A ContainsKey object that specifies whether the key is tracked and whether
     *         or not the key exists (isHasKey(), isTombstone())
     * @throws FileNotFoundException
     * @throws IOException
     */
    public ContainsKey containsKey(String key) throws FileNotFoundException, IOException {
        var result = tryGetValueOrContains(key, true);
        if (result == null) return ContainsKey.FALSE;
        return new ContainsKey(true, result.isTombstone);
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

    public static void compact(Path olderPath, Path newPath, Path outputPath) throws FileNotFoundException, IOException {
        try (var outputStream = new FileOutputStream(outputPath.toString())) {
            final var decoder = Base64.getDecoder();
            var olderEntry = new Entry();
            var newerEntry = new Entry();
            var olderIter = Files.lines(olderPath).map(line -> olderEntry.set(decoder, line)).iterator();
            var newIter = Files.lines(newPath).map(line -> newerEntry.set(decoder, line)).iterator();
            Entry olderLine, newerLine;
            olderLine = olderIter.hasNext() ? olderIter.next() : null;
            newerLine = newIter.hasNext() ? newIter.next() : null;
            while(olderLine != null || newerLine != null) {
                var comparison = olderLine != null && newerLine != null ? olderLine.key.compareTo(newerLine.key) : 0;
                if (olderLine != null && newerLine != null && comparison == 0) {
                    outputStream.write(newerLine.line.getBytes());
                    outputStream.write('\n');
                    olderLine = olderIter.hasNext() ? olderIter.next() : null;
                    newerLine = newIter.hasNext() ? newIter.next() : null;
                }
                else if (olderLine != null && (newerLine == null || comparison < 0)) {
                    outputStream.write(olderLine.line.getBytes());
                    outputStream.write('\n');
                    olderLine = olderIter.hasNext() ? olderIter.next() : null;
                }
                else if (newerLine != null && (olderLine == null || comparison > 0)) {
                    outputStream.write(newerLine.line.getBytes());
                    outputStream.write('\n');
                    newerLine = newIter.hasNext() ? newIter.next() : null;
                }
            }
        }
    }

    /*package*/ static Stream<Entry> readAllLines(Path filePath) throws IOException {
        final var decoder = Base64.getDecoder();
        return Files.lines(filePath).map(line -> new Entry(decoder, line));
    }

    @AllArgsConstructor
    @Getter
    public static class ContainsKey {
        private static final ContainsKey FALSE = new ContainsKey(false, false);
        private boolean hasKey;
        private boolean isTombstone;
    }

    /*package*/ static class Entry {
        public String key;
        public String line;
        public boolean isTombstone = false;
        public String value;

        public Entry() { }

        public Entry(Decoder decoder, String line) {
            set(decoder, line);
        }

        public Entry set(Decoder decoder, String line) {
            var tokens = line.split(":");
            if (tokens.length == 1) isTombstone = true;
            else value = tokens[1];
            this.key = new String(decoder.decode(tokens[0].getBytes()));
            this.line = line;
            return this;
        }

        public String deserialize(SerializerFactory factory) throws IOException {
            if (isTombstone) return null;
            return factory.createDeserializer(new ByteArrayInputStream(value.getBytes())).read();
        }
    }
}
