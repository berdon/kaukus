package io.hnsn.kaukus.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Base64;

import org.junit.jupiter.api.Test;

public class ByteRatioLineMapperTests {
    @Test
    public void CanBuildIndex() throws URISyntaxException, IOException {
        var filePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI());
        var index = new ByteRatioLineMapper().mapLines(0.33f, filePath);
        
        assertNotNull(index);

        try (var randomAccessFile = new RandomAccessFile(filePath.toString(), "r")) {
            for(var tuple : index) {
                randomAccessFile.seek(tuple.getIndex());
                var tokens = randomAccessFile.readLine().split(":");
                assertEquals(tuple.getKey(), new String(Base64.getDecoder().decode(tokens[0])));
            }
        }
    }

    @Test
    public void CanReadTombstone() throws URISyntaxException, IOException {
        var filePath = Paths.get(getClass().getClassLoader().getResource("SSTableTombstoneTest.0").toURI());
        var index = new ByteRatioLineMapper().mapLines(1, filePath);
        
        assertNotNull(index);
        assertTrue(index[0].isTombstone());
    }
}
