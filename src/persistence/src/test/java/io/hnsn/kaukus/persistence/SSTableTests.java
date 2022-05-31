package io.hnsn.kaukus.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.hnsn.kaukus.persistence.SSTable.Entry;
import io.hnsn.kaukus.persistence.serialization.Base64SerializerFactory;

public class SSTableTests {
    @Test
    public void CanFindKeys() throws URISyntaxException, FileNotFoundException, IOException {
        var filePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI());
        var sstable = new SSTable(filePath, new SSTableConfiguration.SSTableConfigurationBuilder().build());
        for (var i = 0; i < 10; i++) {
            assertTrue(sstable.containsKey(MessageFormat.format("some-key-{0}", i)).isHasKey());
        }
        assertFalse(sstable.containsKey("not-valid-key").isHasKey());
    }

    @Test
    public void CanGetValues() throws URISyntaxException, FileNotFoundException, IOException {
        var filePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI());
        var sstable = new SSTable(filePath, new SSTableConfiguration.SSTableConfigurationBuilder().build());
        for (var i = 0; i < 10; i++) {
            assertEquals(MessageFormat.format("some-value-{0}", i), sstable.tryGetValue(MessageFormat.format("some-key-{0}", i)).getValue());
        }
        assertNull(sstable.tryGetValue("invalid-key"));
    }

    @Test
    public void canReadEmptyString() throws URISyntaxException, FileNotFoundException, IOException {
        var filePath = Paths.get(getClass().getClassLoader().getResource("SSTableEmptyStringTest.0").toURI());
        var sstable = new SSTable(filePath, new SSTableConfiguration.SSTableConfigurationBuilder().build());
        assertTrue(sstable.containsKey("some-empty-value").isHasKey());
        assertEquals("", sstable.tryGetValue("some-empty-value").getValue());
    }

    @Test
    public void canReadTombstone() throws URISyntaxException, FileNotFoundException, IOException {
        var filePath = Paths.get(getClass().getClassLoader().getResource("SSTableTombstoneTest.0").toURI());
        var sstable = new SSTable(filePath, new SSTableConfiguration.SSTableConfigurationBuilder().build());
        assertTrue(sstable.containsKey("some-deleted-value").isHasKey());
        assertNull(sstable.tryGetValue("some-deleted-value").getValue());
    }

    @Test
    public void canCompactInterleavedLines() throws URISyntaxException, IOException {
        var olderFile = Paths.get(getClass().getClassLoader().getResource("SSTableCompactTest.0").toURI());
        var newerFile = Paths.get(getClass().getClassLoader().getResource("SSTableCompactTest.1").toURI());
        var tempFile = File.createTempFile("test", null);
        var outputPath = Path.of(tempFile.getPath());

        SSTable.compact(olderFile, newerFile, outputPath);

        var serializerFactory = new Base64SerializerFactory();
        var lines = SSTable.readAllLines(outputPath).collect(Collectors.toList()).toArray(new Entry[0]);

        assertEquals("some-deleted-value", lines[0].key);
        assertEquals("phoenix", lines[0].deserialize(serializerFactory));

        for (var i = 0; i < 10; i++) {
            assertEquals(MessageFormat.format("some-key-{0}", i), lines[i + 1].key);
            assertFalse(lines[10].isTombstone);
            assertEquals(MessageFormat.format("some-value-{0}", i), lines[i + 1].deserialize(serializerFactory));
        }

        assertEquals("some-older-untouched-value", lines[11].key);
        assertEquals("legacy value", lines[11].deserialize(serializerFactory));

        assertEquals("some-value-to-delete", lines[12].key);
        assertTrue(lines[12].isTombstone);
        assertEquals(null, lines[12].deserialize(serializerFactory));

        assertEquals("some-value-to-overwrite", lines[13].key);
        assertEquals("newer value", lines[13].deserialize(serializerFactory));
    }
}
