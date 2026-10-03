package io.gfs.chunkserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Stores each chunk as one file: {dataDir}/{handle}.chunk
 *
 * Concurrency model: one ReentrantReadWriteLock PER CHUNK. Writers on different
 * chunks never contend; writers on the same chunk are serialized; readers share.
 * write() refuses to run unless the caller holds the chunk's write lock - the
 * primary needs to hold that lock across "apply locally + replicate" so every
 * replica sees mutations to a chunk in the same order.
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);

    /** Handles become file names: restrict the alphabet to prevent path traversal. */
    private static final Pattern HANDLE_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final String SUFFIX = ".chunk";

    private final Path dataDir;
    private final boolean fsync;
    private final long maxChunkSize;
    private final long maxPayload;
    private final long lockTimeoutMs;

    private final ConcurrentMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> sizes = new ConcurrentHashMap<>();

    public ChunkStore(ChunkserverProperties props) throws IOException {
        this.dataDir = Path.of(props.dataDir()).toAbsolutePath().normalize();
        this.fsync = props.fsyncOnWrite();
        this.maxChunkSize = props.maxChunkSizeBytes();
        this.maxPayload = props.maxWriteBytes();
        this.lockTimeoutMs = props.lockTimeoutMs();

        Files.createDirectories(dataDir);
        try (Stream<Path> files = Files.list(dataDir)) {
            files.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(SUFFIX))
                    .forEach(p -> {
                        String name = p.getFileName().toString();
                        String handle = name.substring(0, name.length() - SUFFIX.length());
                        if (HANDLE_PATTERN.matcher(handle).matches()) {
                            try {
                                sizes.put(handle, Files.size(p));
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    });
        }
        log.info("ChunkStore at {} loaded {} existing chunks", dataDir, sizes.size());
    }

    // ------------------------------------------------------------------ locking

    @FunctionalInterface
    public interface AutoLock extends AutoCloseable {
        @Override
        void close();
    }

    public static class ChunkNotFoundException extends RuntimeException {
        public ChunkNotFoundException(String handle) {
            super("Chunk '" + handle + "' not found on this server");
        }
    }

    public static class ChunkBusyException extends RuntimeException {
        public ChunkBusyException(String message) {
            super(message);
        }
    }

    public AutoLock writeLock(String handle) {
        return acquire(lockFor(handle).writeLock(), handle, "write");
    }

    public AutoLock readLock(String handle) {
        return acquire(lockFor(handle).readLock(), handle, "read");
    }

    private ReentrantReadWriteLock lockFor(String handle) {
        // NOTE: lock objects are never evicted; fine at this scale (one tiny object per chunk).
        return locks.computeIfAbsent(handle, h -> new ReentrantReadWriteLock());
    }

    private AutoLock acquire(Lock lock, String handle, String mode) {
        try {
            if (!lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new ChunkBusyException("Timed out acquiring " + mode + " lock on chunk '" + handle + "'");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ChunkBusyException("Interrupted waiting for " + mode + " lock on chunk '" + handle + "'");
        }
        return lock::unlock;
    }

    // ------------------------------------------------------------------ data access

    public static void validateHandle(String handle) {
        if (handle == null || !HANDLE_PATTERN.matcher(handle).matches()) {
            throw new IllegalArgumentException("Invalid chunk handle (allowed: A-Z a-z 0-9 _ -, max 64 chars)");
        }
    }

    /** Caller MUST hold writeLock(handle). */
    public void write(String handle, long offset, byte[] data) throws IOException {
        validateHandle(handle);
        if (offset < 0 || offset + data.length > maxChunkSize) {
            throw new IllegalArgumentException("Write range [" + offset + ", " + (offset + data.length)
                    + ") exceeds max chunk size " + maxChunkSize);
        }
        if (!lockFor(handle).isWriteLockedByCurrentThread()) {
            throw new IllegalStateException("write() requires holding the chunk write lock");
        }
        try (FileChannel ch = FileChannel.open(path(handle),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            long pos = offset;
            while (buf.hasRemaining()) {
                pos += ch.write(buf, pos);
            }
            if (fsync) {
                ch.force(true);
            }
            sizes.put(handle, ch.size());
        }
    }

    public record ReadResult(byte[] data, long chunkSize) {
    }

    /** Caller should hold readLock(handle) (or writeLock). */
    public ReadResult read(String handle, long offset, long length) throws IOException {
        validateHandle(handle);
        if (!sizes.containsKey(handle)) {
            throw new ChunkNotFoundException(handle);
        }
        try (FileChannel ch = FileChannel.open(path(handle), StandardOpenOption.READ)) {
            long size = ch.size();
            if (offset < 0 || offset > size) {
                throw new IllegalArgumentException("Offset " + offset + " outside chunk of size " + size);
            }
            long available = size - offset;
            long toRead = length <= 0 ? available : Math.min(length, available);
            if (toRead > maxPayload) {
                throw new IllegalArgumentException("Requested " + toRead + " bytes exceeds max payload "
                        + maxPayload + "; read in smaller pieces using offset/length");
            }
            ByteBuffer buf = ByteBuffer.allocate((int) toRead);
            long pos = offset;
            while (buf.hasRemaining()) {
                int n = ch.read(buf, pos);
                if (n < 0) {
                    break;
                }
                pos += n;
            }
            byte[] out = new byte[buf.position()];
            buf.flip();
            buf.get(out);
            return new ReadResult(out, size);
        }
    }

    /** Point-in-time copy of handle -> size, used for heartbeat chunk reports. */
    public Map<String, Long> snapshot() {
        return Map.copyOf(sizes);
    }

    private Path path(String handle) {
        return dataDir.resolve(handle + SUFFIX);
    }
}