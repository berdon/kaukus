package io.hnsn.kaukus.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

public class LSMTreeTests {
    @Test
    public void lsmLoadsSegments() throws URISyntaxException, IOException {
        var segmentFilePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI()).toString();
        var filePath = Paths.get(segmentFilePath.substring(0, segmentFilePath.length() - 2));
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            for (var i = 0; i < 10; i++) {
                var result = lsmTree.get(MessageFormat.format("some-key-{0}", i));
                assertEquals(MessageFormat.format("some-value-{0}", i), result);
            }
        }
    }

    @Test
    public void canStoreInMemory() throws URISyntaxException, IOException {
        var segmentFilePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI()).toString();
        var filePath = Paths.get(segmentFilePath.substring(0, segmentFilePath.length() - 2));
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            lsmTree.put("some-other-key", "a value");
            assertEquals("a value", lsmTree.get("some-other-key"));
            lsmTree.put("some-other-key2", "a value2");
            assertEquals("a value2", lsmTree.get("some-other-key2"));
        }
    }

    @Test
    public void tombstonesDoNotExist() throws URISyntaxException, IOException {
        var segmentFilePath = Paths.get(getClass().getClassLoader().getResource("SSTableTombstoneTest.0").toURI()).toString();
        var filePath = Paths.get(segmentFilePath.substring(0, segmentFilePath.length() - 2));
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            assertFalse(lsmTree.containsKey("some-deleted-value"));
            assertNull(lsmTree.get("some-deleted-value"));
        }
    }

    @Test
    public void canFlushInMemoryToNextSegment() throws IOException {
        var tempFile = File.createTempFile("test", null);
        var filePath = Path.of(tempFile.getPath());
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            for (var i = 0; i < 10; i++) {
                lsmTree.put(MessageFormat.format("some-key-{0}", i), MessageFormat.format("some-value-{0}", i));
            }

            // Ensure base64/padding serializes correctly
            lsmTree.put("k", "v");
            lsmTree.put("ke", "v");
            lsmTree.put("key", "v");
            lsmTree.put("key1", "va");
            lsmTree.put("key2", "val");
            lsmTree.put("key3", "valu");
            lsmTree.put("key4", "value");


            lsmTree.flush();

            var sstable = new SSTable(Path.of(filePath.toString() + ".0"), SSTableConfiguration.builder().build());
            for (var i = 0; i < 10; i++) {
                assertTrue(sstable.containsKey(MessageFormat.format("some-key-{0}", i)).isHasKey());
                var result = sstable.tryGetValue(MessageFormat.format("some-key-{0}", i));
                assertEquals(MessageFormat.format("some-value-{0}", i), result.getValue());
            }

            assertTrue(sstable.containsKey("k").isHasKey());
            assertEquals("v", sstable.tryGetValue("k").getValue());

            assertTrue(sstable.containsKey("ke").isHasKey());
            assertEquals("v", sstable.tryGetValue("ke").getValue());

            assertTrue(sstable.containsKey("key").isHasKey());
            assertEquals("v", sstable.tryGetValue("key").getValue());

            assertTrue(sstable.containsKey("key1").isHasKey());
            assertEquals("va", sstable.tryGetValue("key1").getValue());

            assertTrue(sstable.containsKey("key2").isHasKey());
            assertEquals("val", sstable.tryGetValue("key2").getValue());

            assertTrue(sstable.containsKey("key3").isHasKey());
            assertEquals("valu", sstable.tryGetValue("key3").getValue());

            assertTrue(sstable.containsKey("key4").isHasKey());
            assertEquals("value", sstable.tryGetValue("key4").getValue());

            // Assert the wal is gone
            assertFalse(Files.exists(filePath));
        }
    }

    @Test
    public void walLogsWritten() throws URISyntaxException, IOException {
        var segmentFilePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI()).toString();
        var filePath = Paths.get(segmentFilePath.substring(0, segmentFilePath.length() - 2));
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            lsmTree.put("some-other-key", "a value");
            lsmTree.put("some-other-key2", "a value 2");
            lsmTree.put("some-other-key3", "a value 3");
            lsmTree.remove("some-other-key2");
            lsmTree.put("some-other-key3", "a new value 3");

            var decoder = Base64.getDecoder();
            var lines = Files.lines(filePath).map(line -> {
                var tokens = line.split(":");
                var key = new String(decoder.decode(tokens[0]));
                var isTombstone = tokens.length == 1;
                try {
                    var value = isTombstone
                        ? SSTableResult.TOMBSTONE
                        : new SSTableResult(new Base64Deserializer(new ByteArrayInputStream(tokens[1].getBytes())).read());
                    return new Object[] { key, isTombstone, value };
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList()).toArray(new Object[0][]);

            assertEquals("some-other-key", lines[0][0]);
            assertFalse((boolean) lines[0][1]);
            assertEquals("a value", ((SSTableResult) lines[0][2]).getValue());

            assertEquals("some-other-key2", lines[1][0]);
            assertFalse((boolean) lines[1][1]);
            assertEquals("a value 2", ((SSTableResult) lines[1][2]).getValue());

            assertEquals("some-other-key3", lines[2][0]);
            assertFalse((boolean) lines[2][1]);
            assertEquals("a value 3", ((SSTableResult) lines[2][2]).getValue());

            assertEquals("some-other-key2", lines[3][0]);
            assertTrue((boolean) lines[3][1]);
            assertTrue(((SSTableResult) lines[3][2]).isTombstone());

            assertEquals("some-other-key3", lines[4][0]);
            assertFalse((boolean) lines[4][1]);
            assertEquals("a new value 3", ((SSTableResult) lines[4][2]).getValue());
        }
    }

    @Test
    public void canReadValuesFromRecoveredWal() throws URISyntaxException, IOException {
        var filePath = Paths.get(getClass().getClassLoader().getResource("SSTableWalFileTest").toURI());
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            /*
            lsmTree.set("some-other-key", "a value");
            lsmTree.set("some-other-key2", "a value 2");
            lsmTree.set("some-other-key3", "a value 3");
            lsmTree.remove("some-other-key2");
            lsmTree.set("some-other-key3", "a new value 3");
            */
            assertEquals("a value", lsmTree.get("some-other-key"));
            assertFalse(lsmTree.containsKey("some-other-key2"));
            assertNull(lsmTree.get("some-other-key2"));
            assertEquals("a new value 3", lsmTree.get("some-other-key3"));
        }
    }

    @Test
    public void canCompact() throws URISyntaxException, FileNotFoundException, IOException {
        var segmentResource0 = Paths.get(getClass().getClassLoader().getResource("SSTableCompactTest.0").toURI());
        var segmentResource1 = Paths.get(getClass().getClassLoader().getResource("SSTableCompactTest.1").toURI());
        var tempDirectory = Files.createTempDirectory(null);
        Files.copy(segmentResource0, tempDirectory.resolve("SSTableCompactTest.0"));
        Files.copy(segmentResource1, tempDirectory.resolve("SSTableCompactTest.1"));
        var lsmTree = LSMTree.openOrCreate(tempDirectory.resolve("SSTableCompactTest"));

        for (var i = 0; i < 10; i++) {
            var key = MessageFormat.format("some-key-{0}", i);
            var value = MessageFormat.format("some-value-{0}", i);
            assertTrue(lsmTree.containsKey(key));
            assertEquals(value, lsmTree.get(key));
        }

        assertTrue(lsmTree.containsKey("some-value-to-overwrite"));
        assertEquals("newer value", lsmTree.get("some-value-to-overwrite"));

        assertFalse(lsmTree.containsKey("some-value-to-delete"));
        assertNull(lsmTree.get("some-value-to-delete"));

        assertTrue(lsmTree.containsKey("some-deleted-value"));
        assertEquals("phoenix", lsmTree.get("some-deleted-value"));

        assertTrue(lsmTree.containsKey("some-older-untouched-value"));
        assertEquals("legacy value", lsmTree.get("some-older-untouched-value"));
        
        lsmTree.compact();

        for (var i = 0; i < 10; i++) {
            var key = MessageFormat.format("some-key-{0}", i);
            var value = MessageFormat.format("some-value-{0}", i);
            assertTrue(lsmTree.containsKey(key));
            assertEquals(value, lsmTree.get(key));
        }

        assertTrue(lsmTree.containsKey("some-value-to-overwrite"));
        assertEquals("newer value", lsmTree.get("some-value-to-overwrite"));

        assertFalse(lsmTree.containsKey("some-value-to-delete"));
        assertNull(lsmTree.get("some-value-to-delete"));

        assertTrue(lsmTree.containsKey("some-deleted-value"));
        assertEquals("phoenix", lsmTree.get("some-deleted-value"));

        assertTrue(lsmTree.containsKey("some-older-untouched-value"));
        assertEquals("legacy value", lsmTree.get("some-older-untouched-value"));
    }

    @Test
    public void putNullValueThrows() throws IOException {
        var filePath = Path.of(File.createTempFile("test", null).getPath());
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            assertThrows(InvalidParameterException.class, () -> {
                lsmTree.put("some-null-value", null);
            });
        }
    }

    @Test
    public void canStoreEmptyStringValue() throws IOException {
        var filePath = Path.of(File.createTempFile("test", null).getPath());
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            lsmTree.put("some-empty-value", "");
        }
    }

    @Test
    public void putNullKeyThrows() throws IOException {
        var filePath = Path.of(File.createTempFile("test", null).getPath());
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            assertThrows(InvalidParameterException.class, () -> {
                lsmTree.put(null, "some-null-value");
            });
        }
    }

    @Test
    public void emptyKeyThrows() throws IOException {
        var filePath = Path.of(File.createTempFile("test", null).getPath());
        try (var lsmTree = LSMTree.openOrCreate(filePath)) {
            assertThrows(InvalidParameterException.class, () -> {
                lsmTree.put("", "some-empty-value");
            });
        }
    }
}
