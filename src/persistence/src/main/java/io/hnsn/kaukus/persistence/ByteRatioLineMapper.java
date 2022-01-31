package io.hnsn.kaukus.persistence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedList;

import io.hnsn.kaukus.persistence.SSTable.IndexTuple;

/*package*/ class ByteRatioLineMapper implements LineMapper {

    @Override
    public IndexTuple[] mapLines(float mappingRatio, Path filePath) throws IOException {
        var length = Files.size(filePath);
        final var lengthStep = (int) Math.floor(length * mappingRatio);

        var index = new LinkedList<IndexTuple>();
        
        // var index = new IndexTuple[(int) Math.ceil(1 / mappingRatio)];
        var lastIndex = Long.MIN_VALUE;
        var bytesRead = 0;
        var lineIterator = Files.lines(filePath).map(line -> {
            var tokens = line.split(":");
            return new Object[] { (long) line.length(), new String(Base64.getDecoder().decode(tokens[0])), tokens.length == 1 };
        }).iterator();

        while (lineIterator.hasNext()) {
            var tokens = lineIterator.next();
            var lineLength = (long) tokens[0] + 1;
            var key = (String) tokens[1];
            var isTombstone = (boolean) tokens[2];
            if (bytesRead > lastIndex + lengthStep) {
                index.add(new IndexTuple(key, bytesRead, isTombstone));
                lastIndex = bytesRead;
            }
            bytesRead += lineLength;
        }

        return index.toArray(new IndexTuple[index.size()]);
    }
}