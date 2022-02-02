package io.hnsn.kaukus.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedList;
import java.util.Base64.Decoder;

import io.hnsn.kaukus.persistence.SSTable.IndexTuple;

/*package*/ class ByteRatioLineMapper implements LineMapper {
    private final Decoder decoder = Base64.getDecoder();

    @Override
    public IndexTuple[] mapLines(float mappingRatio, Path filePath) throws IOException {
        var length = Files.size(filePath);
        final var lengthStep = (int) Math.floor(length * mappingRatio);

        var index = new LinkedList<IndexTuple>();
        
        var lastIndex = Long.MIN_VALUE;
        var bytesRead = 0;

        // Use a flywheel to minimize object creation ¯\_(ツ)_/¯
        var lineTokens = new LineTokens();

        var lineIterator = Files.lines(filePath).map(line -> {
            var isTombstone = !line.contains(":");
            var key = isTombstone ? line : line.split(":")[0];
            return lineTokens.set((long) line.length() + 1, new String(decoder.decode(key)), isTombstone);
        }).iterator();

        while (lineIterator.hasNext()) {
            var tokens = lineIterator.next();
            if (bytesRead > lastIndex + lengthStep) {
                index.add(new IndexTuple(tokens.key, bytesRead, tokens.isTombstone));
                lastIndex = bytesRead;
            }
            bytesRead += tokens.length;
        }

        return index.toArray(new IndexTuple[index.size()]);
    }

    private static class LineTokens {
        public long length;
        public String key;
        public boolean isTombstone;

        public LineTokens() { }

        public LineTokens set(long length, String key, boolean isTombstone) {
            this.length = length;
            this.key = key;
            this.isTombstone = isTombstone;
            return this;
        }
    }
}