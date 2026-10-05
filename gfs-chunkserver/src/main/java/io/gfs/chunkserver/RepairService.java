package io.gfs.chunkserver;

import io.gfs.proto.ChunkServiceGrpc;
import io.gfs.proto.CopyOrder;
import io.gfs.proto.ServerAddress;
import io.gfs.proto.StreamChunkRequest;
import io.gfs.proto.StreamChunkResponse;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Executes copy orders from the master (re-replication and catch-up of stale replicas).
 *
 * Pull model: stream a consistent snapshot from a fresh replica into a temp file, then take
 * the chunk's write lock and atomically swap it in. Ordinary writes to this chunk are never
 * routed here while we are stale/new (the master only sends writes to fresh replicas), so the
 * swap cannot lose acknowledged writes.
 */
@Component
public class RepairService {

    private static final Logger log = LoggerFactory.getLogger(RepairService.class);

    private final ChunkserverProperties props;
    private final ChunkStore store;
    private final ChannelRegistry channels;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "repair"));
    private final Set<String> active = ConcurrentHashMap.newKeySet();

    public RepairService(ChunkserverProperties props, ChunkStore store, ChannelRegistry channels) {
        this.props = props;
        this.store = store;
        this.channels = channels;
    }

    /** Queue an order; duplicates for a chunk already queued/running are ignored. */
    public void submit(CopyOrder order, Runnable onDone) {
        String handle = order.getChunkHandle();
        if (!active.add(handle)) {
            return;
        }
        executor.execute(() -> {
            try {
                copy(order);
            } catch (Exception e) {
                log.warn("REPAIR of chunk {} from {} failed: {}", handle, order.getSource().getServerId(), e.toString());
            } finally {
                active.remove(handle);
                onDone.run(); // push a heartbeat so the master sees the new replica quickly
            }
        });
    }

    private void copy(CopyOrder order) throws IOException {
        String handle = order.getChunkHandle();
        ChunkStore.validateHandle(handle);
        ServerAddress src = order.getSource();
        if (src.getServerId().equals(props.id())) {
            return;
        }
        log.info("REPAIR start: chunk={} from={} requiredVersion={}", handle, src.getServerId(), order.getRequiredVersion());

        ChunkServiceGrpc.ChunkServiceBlockingStub stub =
                ChunkServiceGrpc.newBlockingStub(channels.get(src.getHost(), src.getPort()))
                        .withDeadlineAfter(props.copyTimeoutMs(), TimeUnit.MILLISECONDS);

        Path tmp = store.tempPath(handle);
        long version = -1;
        long expected = -1;
        long written = 0;
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            Iterator<StreamChunkResponse> it =
                    stub.streamChunk(StreamChunkRequest.newBuilder().setChunkHandle(handle).build());
            while (it.hasNext()) {
                StreamChunkResponse piece = it.next();
                if (version < 0) {
                    version = piece.getVersion();
                    expected = piece.getChunkSize();
                }
                ByteBuffer buf = piece.getData().asReadOnlyByteBuffer();
                while (buf.hasRemaining()) {
                    written += ch.write(buf);
                }
            }
            ch.force(true);
        } catch (StatusRuntimeException e) {
            Files.deleteIfExists(tmp);
            throw new IOException("stream from " + src.getServerId() + " failed: " + e.getStatus(), e);
        }

        if (version < 0 || written != expected) {
            Files.deleteIfExists(tmp);
            throw new IOException("incomplete copy: got " + written + " of " + expected + " bytes");
        }
        try (ChunkStore.AutoLock ignored = store.writeLock(handle)) {
            store.installSnapshot(handle, version);
        }
        log.info("REPAIRED chunk={} version={} bytes={} from={}", handle, version, written, src.getServerId());
    }

    @PreDestroy
    void stop() {
        executor.shutdownNow();
    }
}