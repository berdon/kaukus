package io.hnsn.kaukus.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.text.MessageFormat;

import org.junit.jupiter.api.Test;

public class SSTableTests {
    @Test
    public void CanFindKeys() throws URISyntaxException, FileNotFoundException, IOException {
        var filePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI());
        var sstable = new SSTable(filePath, new SSTableConfiguration.SSTableConfigurationBuilder().build());
        for (var i = 0; i < 10; i++) {
            assertTrue(sstable.containsKey(MessageFormat.format("some-key-{0}", i)));
        }
        assertFalse(sstable.containsKey("not-valid-key"));
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
}
