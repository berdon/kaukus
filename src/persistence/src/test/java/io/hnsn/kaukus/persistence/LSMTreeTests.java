package io.hnsn.kaukus.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

public class LSMTreeTests {
    @Test
    public void lsmLoadsSegments() throws URISyntaxException, IOException {
        var segmentFilePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI()).toString();
        var filePath = Paths.get(segmentFilePath.substring(0, segmentFilePath.length() - 2));
        try (var lsmTree = new LSMTree(filePath)) {
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
        try (var lsmTree = new LSMTree(filePath)) {
            lsmTree.set("some-other-key", "a value");
            assertEquals("a value", lsmTree.get("some-other-key"));
            lsmTree.set("some-other-key2", "a value2");
            assertEquals("a value2", lsmTree.get("some-other-key2"));
        }
    }

    @Test
    public void tombstonesDoNotExist() throws URISyntaxException, IOException {
        var segmentFilePath = Paths.get(getClass().getClassLoader().getResource("SSTableTombstoneTest.0").toURI()).toString();
        var filePath = Paths.get(segmentFilePath.substring(0, segmentFilePath.length() - 2));
        try (var lsmTree = new LSMTree(filePath)) {
            assertFalse(lsmTree.containsKey("some-deleted-value"));
            assertNull(lsmTree.get("some-deleted-value"));
        }
    }

    @Test
    public void canFlushInMemoryToNextSegment() throws IOException {
        var tempFile = File.createTempFile("test", null);
        var filePath = Path.of(tempFile.getPath());
        try (var lsmTree = new LSMTree(filePath)) {
            for (var i = 0; i < 10; i++) {
                lsmTree.set(MessageFormat.format("some-key-{0}", i), MessageFormat.format("some-value-{0}", i));
            }

            // Ensure base64/padding serializes correctly
            lsmTree.set("k", "v");
            lsmTree.set("ke", "v");
            lsmTree.set("key", "v");
            lsmTree.set("key1", "va");
            lsmTree.set("key2", "val");
            lsmTree.set("key3", "valu");
            lsmTree.set("key4", "value");


            lsmTree.flush();

            var sstable = new SSTable(Path.of(filePath.toString() + ".0"), SSTableConfiguration.builder().build());
            for (var i = 0; i < 10; i++) {
                assertTrue(sstable.containsKey(MessageFormat.format("some-key-{0}", i)));
                var result = sstable.tryGetValue(MessageFormat.format("some-key-{0}", i));
                assertEquals(MessageFormat.format("some-value-{0}", i), result.getValue());
            }

            assertTrue(sstable.containsKey("k"));
            assertEquals("v", sstable.tryGetValue("k").getValue());

            assertTrue(sstable.containsKey("ke"));
            assertEquals("v", sstable.tryGetValue("ke").getValue());

            assertTrue(sstable.containsKey("key"));
            assertEquals("v", sstable.tryGetValue("key").getValue());

            assertTrue(sstable.containsKey("key1"));
            assertEquals("va", sstable.tryGetValue("key1").getValue());

            assertTrue(sstable.containsKey("key2"));
            assertEquals("val", sstable.tryGetValue("key2").getValue());

            assertTrue(sstable.containsKey("key3"));
            assertEquals("valu", sstable.tryGetValue("key3").getValue());

            assertTrue(sstable.containsKey("key4"));
            assertEquals("value", sstable.tryGetValue("key4").getValue());

            // Assert the wal is gone
            assertFalse(Files.exists(filePath));
        }
    }

    @Test
    public void walLogsWritten() throws URISyntaxException, IOException {
        var segmentFilePath = Paths.get(getClass().getClassLoader().getResource("SSTableTest.0").toURI()).toString();
        var filePath = Paths.get(segmentFilePath.substring(0, segmentFilePath.length() - 2));
        try (var lsmTree = new LSMTree(filePath)) {
            lsmTree.set("some-other-key", "a value");
            lsmTree.set("some-other-key2", "a value 2");
            lsmTree.set("some-other-key3", "a value 3");
            lsmTree.remove("some-other-key2");
            lsmTree.set("some-other-key3", "a new value 3");

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
        try (var lsmTree = new LSMTree(filePath)) {
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
}
