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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Stores each chunk as {dataDir}/{handle}.chunk plus a {handle}.ver sidecar holding the
 * chunk's write version.
 *
 * Concurrency: one ReentrantReadWriteLock PER CHUNK. write() and installSnapshot() refuse
 * to run unless the caller holds the chunk's write lock.
 *
 * Versioning: a write with version v is applied only if the local version is v-1 (the normal
 * case) or v (an idempotent retry). Anything else means this replica missed writes and the
 * write is refused, so a stale replica can never silently look up to date.
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);

    /** Handles become file names: restrict the alphabet to prevent path traversal. */
    private static final Pattern HANDLE_PATTERN = Pattern.compile("[A-Za-z0-9_-]{1,64}");
    private static final String SUFFIX = ".chunk";
    private static final String VERSION_SUFFIX = ".ver";
    private static final String TMP_SUFFIX = ".chunk.tmp";

    private final Path dataDir;
    private final boolean fsync;
    private final long maxChunkSize;
    private final long maxPayload;
    private final long lockTimeoutMs;

    private final ConcurrentMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> sizes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> versions = new ConcurrentHashMap<>();

    public ChunkStore(ChunkserverProperties props) throws IOException {
        this.dataDir = Path.of(props.dataDir()).toAbsolutePath().normalize();
        this.fsync = props.fsyncOnWrite();
        this.maxChunkSize = props.maxChunkSizeBytes();
        this.maxPayload = props.maxWriteBytes();
        this.lockTimeoutMs = props.lockTimeoutMs();

        Files.createDirectories(dataDir);
        List<Path> files;
        try (Stream<Path> s = Files.list(dataDir)) {
            files = s.filter(Files::isRegularFile).toList();
        }
        for (Path p : files) {
            String name = p.getFileName().toString();
            if (name.endsWith(TMP_SUFFIX)) {
                Files.deleteIfExists(p); // leftover from an interrupted repair
            } else if (name.endsWith(SUFFIX)) {
                String handle = name.substring(0, name.length() - SUFFIX.length());
                if (HANDLE_PATTERN.matcher(handle).matches()) {
                    try {
                        sizes.put(handle, Files.size(p));
                        versions.put(handle, loadVersion(handle));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            }
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

    public static class VersionMismatchException extends RuntimeException {
        public VersionMismatchException(String handle, long local, long incoming) {
            super("chunk '" + handle + "' is at version " + local + " but write carries version " + incoming
                    + " (this replica is behind and needs repair)");
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
    public void write(String handle, long offset, byte[] data, long version) throws IOException {
        validateHandle(handle);
        if (offset < 0 || offset + data.length > maxChunkSize) {
            throw new IllegalArgumentException("Write range [" + offset + ", " + (offset + data.length)
                    + ") exceeds max chunk size " + maxChunkSize);
        }
        if (!lockFor(handle).isWriteLockedByCurrentThread()) {
            throw new IllegalStateException("write() requires holding the chunk write lock");
        }
        long current = versions.getOrDefault(handle, 0L);
        if (current != version - 1 && current != version) {
            throw new VersionMismatchException(handle, current, version);
        }
        long newSize;
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
            newSize = ch.size();
        }
        // Version advances only after the data is durable: a crash in between leaves the replica
        // looking BEHIND (safe: it gets repaired), never ahead.
        if (version > current) {
            writeVersionFile(handle, version);
            versions.put(handle, version);
        }
        sizes.put(handle, newSize);
    }

    /** Caller MUST hold writeLock(handle). Atomically replaces the chunk with the downloaded temp file. */
    public void installSnapshot(String handle, long version) throws IOException {
        validateHandle(handle);
        if (!lockFor(handle).isWriteLockedByCurrentThread()) {
            throw new IllegalStateException("installSnapshot() requires holding the chunk write lock");
        }
        Files.move(tempPath(handle), path(handle),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        writeVersionFile(handle, version);
        versions.put(handle, version);
        sizes.put(handle, Files.size(path(handle)));
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

    public record Info(long size, long version) {
    }

    /** Point-in-time copy of handle -> (size, version), used for heartbeat chunk reports. */
    public Map<String, Info> snapshot() {
        Map<String, Info> m = new HashMap<>();
        sizes.forEach((h, s) -> m.put(h, new Info(s, versions.getOrDefault(h, 0L))));
        return m;
    }

    /** @return null if the chunk is not stored here. */
    public Info info(String handle) {
        Long s = sizes.get(handle);
        return s == null ? null : new Info(s, versions.getOrDefault(handle, 0L));
    }

    public Path tempPath(String handle) {
        validateHandle(handle);
        return dataDir.resolve(handle + TMP_SUFFIX);
    }

    private Path path(String handle) {
        return dataDir.resolve(handle + SUFFIX);
    }

    private Path versionPath(String handle) {
        return dataDir.resolve(handle + VERSION_SUFFIX);
    }

    private void writeVersionFile(String handle, long version) throws IOException {
        Files.writeString(versionPath(handle), Long.toString(version));
    }

    private long loadVersion(String handle) {
        Path p = versionPath(handle);
        try {
            return Files.exists(p) ? Long.parseLong(Files.readString(p).trim()) : 0L;
        } catch (IOException | NumberFormatException e) {
            log.warn("Unreadable version file for chunk {} ({}); treating as version 0", handle, e.toString());
            return 0L;
        }
    }
}