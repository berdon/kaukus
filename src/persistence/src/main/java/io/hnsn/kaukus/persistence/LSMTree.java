package io.hnsn.kaukus.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StringBufferInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.InvalidParameterException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.RuntimeErrorException;

import io.hnsn.kaukus.persistence.serialization.StreamSerializer;

/**
 * Log Structured Merge Tree (LSM-Tree)
 * Keeps an internal cache (map for now) of key/value pairs. Writes are added to a
 * WAL-file that is flushed to disk immediately and, as needed, flushed out to
 * String Sorted Tables (SSTables). Reads check the in-memory cache first and then
 * walk backwards through the available SSTables.
 */
public class LSMTree implements Closeable {
    /**
     * Full path of the directory containing the SSTables and WAL File.
     */
    private final Path filePath;
    /**
     * The filename of the LSMTree which is used for generating SSTable file
     * names.
     */
    private final Path fileName;
    /**
     * Full path to be used for the WAL file; this is actually:
     * `filePath + fileName`
     */
    private final Path walFile;
    /**
     * In-memory cache of key/value pairs.
     */
    private final Map<String, LSMTreeValue> memoryMap = new ConcurrentHashMap<>();
    /**
     * Configuration for the internal SSTables.
     */
    private final SSTableConfiguration configuration = SSTableConfiguration.builder().build();

    // Lazy
    private SortedMap<String, SSTable> segments = null;
    private OutputStream walOutputStream = null;
    private StreamSerializer walSerializer = null;
    private final Object segmentLock = new Object();
    private final Object walLock = new Object();
    private final Encoder encoder = Base64.getEncoder();

    /**
     * Opens of creates a new LSMTree. This is a blocking call as it handles
     * loading any unwritten data from the WAL file.
     * @param filePath
     * @return
     */
    public static LSMTree openOrCreate(Path filePath) {
        var lsmTree = new LSMTree(filePath);

        if (!Files.exists(lsmTree.filePath)) {
            try {
                Files.createDirectories(lsmTree.filePath);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }
        }

        // Delete any orphaned merge results "file.1-0"
        try {
            lsmTree.purgeOrphanedSegments();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException(e);
        }

        if (Files.exists(lsmTree.walFile)) {
            lsmTree.rebuildIndex();
        }

        return lsmTree;
    }
    
    /**
     * Private constructor; use {@code}openOrCreate(){@code}
     * @param filePath
     */
    private LSMTree(Path filePath) {
        this.filePath = filePath.getParent();
        fileName = filePath.getName(filePath.getNameCount() - 1);
        walFile = filePath;
    }

    /**
     * Reads each line of an unprocessed WAL file and updates the in-memory
     * cache.
     */
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

    /**
     * Returns true if the key is contained within the LSMTree.
     * @param key The key to search for
     * @return True if the key is contained within the LSMTree; false otherwise
     */
    public boolean containsKey(String key) {
        if (memoryMap.containsKey(key)) {
            var lsmTreeValue = memoryMap.get(key);
            return lsmTreeValue != null && !lsmTreeValue.isTombstone();
        }

        var segments = getSegments();
        for (var segment : segments.entrySet()) {
            try {
                var containsKey = segment.getValue().containsKey(key);
                if (containsKey.isHasKey()) return !containsKey.isTombstone();
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

    public <T> T get(String key, Class<T> cls) throws ClassNotFoundException {
        var serializedString = get(key);
        if (serializedString == null) return null;
        // TODO: Encoding
        try (var inputStream = new ByteArrayInputStream(serializedString.getBytes(StandardCharsets.ISO_8859_1))) {
            var adsf = 5;
            try (var outputStream = new ObjectInputStream(inputStream)) {
                var output = (T) outputStream.readObject();
                return output;
            }
            catch (Exception e) {
                adsf = 1;
            }
        }
        catch (IOException exception) {
            // Ignoring thrown close exception
        }

        // Shouldn't be possible...
        throw new RuntimeException("Unexpected unserialized? response");
    }

    /**
     * Returns the value associated with the key or null if it is not set in
     * the LSMTree.
     * @param key The key to search for
     * @return The value associated or null
     */
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

    public <T> void put(String key, T value) {
        if (key == null) throw new InvalidParameterException("Key cannot be null");
        if (key.isEmpty()) throw new InvalidParameterException("Key cannot be empty");
        if (value == null) throw new InvalidParameterException("Value cannot be null");

        try (var outputStream = new ByteArrayOutputStream()) {
            try (var objectStream = new ObjectOutputStream(outputStream)) {
                objectStream.writeObject(value);
                objectStream.flush();
                objectStream.close();
            }

            var string = new String(outputStream.toByteArray(), StandardCharsets.ISO_8859_1);
            put(key, string);
        } catch (IOException exception) {
            // Ignore
        }
    }

    /**
     * Associates a value with a key. {@code}value{@code} cannot be null.
     * @param key The key to associate the value with
     * @param value The value to be associated
     */
    public void put(String key, String value) {
        if (key == null) throw new InvalidParameterException("Key cannot be null");
        if (key.isEmpty()) throw new InvalidParameterException("Key cannot be empty");
        if (value == null) throw new InvalidParameterException("Value cannot be null");

        walWrite(key, value);
        memoryMap.put(key, new LSMTreeValue(value));
    }

    /**
     * Removes a key/value pair from the LSMTree.
     * @param key
     */
    public void remove(String key) {
        walDelete(key);
        memoryMap.put(key, LSMTreeValue.TOMBSTONE);
    }

    /**
     * Flushes the LSMTree to disk; this writes all entries in the in-memory
     * cache out to a new SSTable.
     */
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

                    var nextSegmentIndex = segments.size() == 0 ? 0 : Integer.parseInt(segments.lastKey().substring(segments.lastKey().lastIndexOf('.') + 1)) + 1;
                    var nextSegmentFileName = filePath.resolve(fileName + "." + nextSegmentIndex).toString();
                    try (var out = new FileOutputStream(nextSegmentFileName); var sstableWriter = new SSTableWriter(out, configuration.serializerFactory)) {
                        // TODO: Only write out entries that have changed from their last SSTable entry
                        for (var pair : entries) {
                            var key = pair.getKey();
                            var lsmTreeValue = pair.getValue();

                            // Write out the entry
                            if (lsmTreeValue != null && lsmTreeValue.getValue() != null) sstableWriter.write(key, lsmTreeValue.getValue());
                            else sstableWriter.writeTombstone(key);
                        }
                    }

                    // Delete the wall
                    Files.deleteIfExists(walFile);
                    memoryMap.clear();
                    segments.put(nextSegmentFileName, new SSTable(Path.of(nextSegmentFileName), configuration));
                }
            } catch (IOException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Blocking call that compacts all SSTables into a single SSTable.
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void compact() throws FileNotFoundException, IOException {
        var segments = getSegments();
        while (segments.size() > 1) {
            var keys = segments.keySet().toArray(new String[0]);
            var olderFile = keys[keys.length - 1];
            var newerFile = keys[keys.length - 2];
            incrementalCompact(Path.of(olderFile), Path.of(newerFile));
        }
    }

    private void incrementalCompact(Path olderFile, Path newerFile) throws FileNotFoundException, IOException {
        var outputFile = Path.of(newerFile.toString() + "-0");

        // Compact new,old to new-0
        // Segments, if reloaded at this point, will still only grab new/old
        SSTable.compact(olderFile, newerFile, outputFile);

        // Need to lock the segment when we perform the overwrite as the replaced index needs to
        // be regenerated
        synchronized (segmentLock) {
            // Overwrite new with new-0; potentially leaving old behind which is fine as it's values
            // are safely merged into new-0
            Files.move(outputFile, newerFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            segments.put(newerFile.toString(), new SSTable(newerFile, configuration));
            segments.remove(olderFile.toString());
        }

        // Finally, delete old
        Files.delete(olderFile);
    }

    private void purgeOrphanedSegments() throws IOException {
        var pathMatcher = FileSystems.getDefault().getPathMatcher(MessageFormat.format("regex:{0}/{1}\\.[0-9]*\\-0$", filePath, fileName));
        try (var files = Files.newDirectoryStream(filePath, pathMatcher::matches)) {
            for (var file : files) {
                try { Files.deleteIfExists(file); } catch (IOException e) { }
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
        flush();
    }
}
