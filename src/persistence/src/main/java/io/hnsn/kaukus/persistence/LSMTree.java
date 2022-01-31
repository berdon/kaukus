package io.hnsn.kaukus.persistence;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;
import lombok.Setter;

public class LSMTree implements Closeable {
    private final Path filePath;
    private final Path fileName;
    private final Path walFile;
    private final Map<String, LSMTreeValue> memoryMap = new ConcurrentHashMap<>();
    private final SSTableConfiguration configuration = SSTableConfiguration.builder().build();

    // Lazy
    private SortedMap<String, SSTable> segments = null;
    private OutputStream walOutputStream = null;
    private StreamSerializer walSerializer = null;
    private final Object segmentLock = new Object();
    private final Object walLock = new Object();
    private final Encoder encoder = Base64.getEncoder();

    // Test Hooks
    
    public LSMTree(Path filePath) {
        this.filePath = filePath.getParent();
        fileName = filePath.getName(filePath.getNameCount() - 1);
        walFile = filePath;

        // TODO: Handle crash recovery
        if (Files.exists(walFile)) {
            rebuildIndex();
        }
    }

    private void rebuildIndex() {
        try {
            final var decoder = Base64.getDecoder();
            Files.lines(walFile).forEach(line -> {
                var tokens = line.split(":");
                var isTombstone = tokens.length == 1;
                var key = new String(decoder.decode(tokens[0]));
                try {
                    var value = isTombstone
                        ? LSMTreeValue.TOMBSTONE
                        : new LSMTreeValue(configuration.serializerFactory.createDeserializer(new ByteArrayInputStream(tokens[1].getBytes())).read());
                    memoryMap.put(key, value);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException(e);
        }
    }

    public boolean containsKey(String key) {
        if (memoryMap.containsKey(key)) {
            var lsmTreeValue = memoryMap.get(key);
            return lsmTreeValue != null && !lsmTreeValue.isTombstone;
        }

        var segments = getSegments();
        for (var segment : segments.entrySet()) {
            try {
                return segment.getValue().containsKey(key);
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }
        }

        return false;
    }

    public String get(String key) {
        if (memoryMap.containsKey(key)) {
            var lsmTreeValue = memoryMap.get(key);
            // Tombstoned
            if (lsmTreeValue == LSMTreeValue.TOMBSTONE) return null;
            return lsmTreeValue.getValue();
        }

        var segments = getSegments();
        for (var segment : segments.entrySet()) {
            try {
                var result = segment.getValue().tryGetValue(key);
                if (result != null) return result.getValue();
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }
        }

        return null;
    }

    public <TValue> void set(String key, String value) {
        if (value == null) throw new NullPointerException("Value cannot be null");
        walWrite(key, value);
        memoryMap.put(key, new LSMTreeValue(value));
    }

    public void remove(String key) {
        walDelete(key);
        memoryMap.put(key, LSMTreeValue.TOMBSTONE);
    }

    public void flush() {
        // Write the sorted map to a new segment file
        synchronized (segmentLock) {
            var segments = getSegments();

            try {
                // TODO: Come up with a better way to avoid needless? locking
                // Ensure the wal is locked as it stops all writing
                synchronized (walLock) {
                    var entries = new ArrayList<>(memoryMap.entrySet());

                    entries.sort((Map.Entry<String, LSMTreeValue> a, Map.Entry<String, LSMTreeValue> b) -> {
                        return a.getKey().compareTo(b.getKey());
                    });

                    var nextSegmentFileName = filePath.resolve(fileName + "." + segments.size()).toString();
                    try (var out = new FileOutputStream(nextSegmentFileName); var sstableWriter = new SSTableWriter(out, configuration.serializerFactory)) {
                        for (var pair : entries) {
                            var key = pair.getKey();
                            var lsmTreeValue = pair.getValue();

                            // Write out the entry
                            if (lsmTreeValue != null) sstableWriter.write(key, lsmTreeValue.value);
                            else sstableWriter.writeTombstone(key);
                        }
                    }

                    // Delete the wall
                    Files.delete(walFile);
                    memoryMap.clear();
                    segments.put(nextSegmentFileName, new SSTable(Path.of(nextSegmentFileName), configuration));
                }
            } catch (IOException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }
    }

    private void walWrite(String key, String value) {
        synchronized (walLock) {
            try {
                if (walOutputStream == null) {
                    walOutputStream = new FileOutputStream(walFile.toString());
                    walSerializer = configuration.serializerFactory.createStreamSerializer(walOutputStream);
                }

                walOutputStream.write(encoder.encode(key.getBytes()));
                walOutputStream.write(':');
                walSerializer.write(value);
                walOutputStream.write('\n');
                walOutputStream.flush();
            } catch (IOException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }
    }

    private void walDelete(String key) {
        synchronized (walLock) {
            try {
                if (walOutputStream == null) {
                    walOutputStream = new FileOutputStream(walFile.toString());
                    walSerializer = configuration.serializerFactory.createStreamSerializer(walOutputStream);
                }

                walOutputStream.write(encoder.encode(key.getBytes()));
                walOutputStream.write(':');
                walOutputStream.write('\n');
                walOutputStream.flush();
            } catch (IOException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }
    }

    private SortedMap<String, SSTable> getSegments() {
        if (segments == null) {
            synchronized (segmentLock) {
                if (segments == null) {
                    segments = new TreeMap<>(Comparator.reverseOrder());
                    var pathMatcher = FileSystems.getDefault().getPathMatcher(MessageFormat.format("regex:{0}/{1}\\.[0-9]*$", filePath, fileName));
                    try (var files = Files.newDirectoryStream(filePath, pathMatcher::matches)) {
                        for (var file : files) {
                            segments.put(file.toString(), new SSTable(file, configuration));
                        }
                    } catch (IOException e) {
                        // TODO
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        return segments;
    }

    @Override
    public void close() throws IOException {
        if (walOutputStream != null) walOutputStream.close();
        if (walSerializer != null) walSerializer.close();
    }

    @Getter
    @Setter
    private static class LSMTreeValue {
        public static final LSMTreeValue TOMBSTONE = new LSMTreeValue(true);

        private final String value;
        private final boolean isTombstone;

        public LSMTreeValue(String value) {
            this.value = value;
            this.isTombstone = false;
        }

        private LSMTreeValue(boolean isTombstone) {
            this.value = null;
            this.isTombstone = true;
        }
    }
}
