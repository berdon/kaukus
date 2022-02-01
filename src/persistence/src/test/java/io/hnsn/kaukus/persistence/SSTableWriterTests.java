package io.hnsn.kaukus.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.text.MessageFormat;

import org.junit.jupiter.api.Test;

public class SSTableWriterTests {
    @Test
    public void canWriteSSTable() throws IOException {
        var tempFile = File.createTempFile("test", null);
        var filePath = Path.of(tempFile.getPath());
        var outputStream = new FileOutputStream(tempFile);
        try (var sstableWriter = new SSTableWriter(outputStream, new Base64SerializerFactory())) {
            for (var i = 0; i < 10; i++) {
                sstableWriter.write(MessageFormat.format("some-key-{0}", i), MessageFormat.format("some-value-{0}", i));
            }
            sstableWriter.flush();
        }
        
        var sstable = new SSTable(filePath, SSTableConfiguration.builder().build());
        for (var i = 0; i < 10; i++) {
            assertTrue(sstable.containsKey(MessageFormat.format("some-key-{0}", i)).isHasKey());
            var result = sstable.tryGetValue(MessageFormat.format("some-key-{0}", i));
            assertEquals(MessageFormat.format("some-value-{0}", i), result.getValue());
        }
    }

    @Test
    public void canWriteTombstoneSSTable() throws IOException {
        var tempFile = File.createTempFile("test", null);
        var filePath = Path.of(tempFile.getPath());
        var outputStream = new FileOutputStream(tempFile);
        try (var sstableWriter = new SSTableWriter(outputStream, new Base64SerializerFactory())) {
            sstableWriter.writeTombstone("some-deleted-value");
            sstableWriter.flush();
        }
        
        var sstable = new SSTable(filePath, SSTableConfiguration.builder().build());
        assertTrue(sstable.containsKey("some-deleted-value").isHasKey());
        assertTrue(sstable.containsKey("some-deleted-value").isTombstone());
    }

    // @Test
    // public void canWrite() throws IOException {
    //     var tempFile = File.createTempFile("test", null);
    //     var filePath = Path.of(tempFile.getPath());
    //     var outputStream = new FileOutputStream(tempFile);
    //     try (var sstableWriter = new SSTableWriter(outputStream, new Base64SerializerFactory())) {
    //         for (var i = 0; i < 10; i++) {
    //             sstableWriter.write(MessageFormat.format("some-key-{0}", i), MessageFormat.format("some-value-{0}", i));
    //         }
    //         sstableWriter.write("some-value-to-overwrite", "newer value");
    //         sstableWriter.writeTombstone("some-value-to-delete");
    //         sstableWriter.write("some-deleted-value", "phoenix");
    //         sstableWriter.write("some-older-untouched-value", "legacy value");
    //         sstableWriter.flush();
    //     }
    // }
}
