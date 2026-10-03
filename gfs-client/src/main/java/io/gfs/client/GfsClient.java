package io.gfs.client;

import com.google.protobuf.ByteString;
import io.gfs.proto.ChunkServiceGrpc;
import io.gfs.proto.GetClusterStatusRequest;
import io.gfs.proto.GetClusterStatusResponse;
import io.gfs.proto.GetPrimaryRequest;
import io.gfs.proto.GetPrimaryResponse;
import io.gfs.proto.LocateChunkRequest;
import io.gfs.proto.MasterServiceGrpc;
import io.gfs.proto.ReadChunkRequest;
import io.gfs.proto.ReadChunkResponse;
import io.gfs.proto.ServerAddress;
import io.gfs.proto.WriteChunkRequest;
import io.gfs.proto.WriteChunkResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Client library. Writes: ask the master who the primary is, send the write there,
 * and on ANY failure re-ask the master (the primary may have changed) after a backoff.
 * Retrying is safe because writes are offset-based and therefore idempotent.
 */
@Service
public class GfsClient {

    private static final Logger log = LoggerFactory.getLogger(GfsClient.class);

    public static class GfsException extends RuntimeException {
        public GfsException(String message) {
            super(message);
        }

        public GfsException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @GrpcClient("master")
    private MasterServiceGrpc.MasterServiceBlockingStub master;

    private final ClientProperties props;
    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public GfsClient(ClientProperties props) {
        this.props = props;
    }

    // ------------------------------------------------------------------ write

    public WriteChunkResponse write(String handle, long offset, byte[] data) {
        int max = props.writeMaxAttempts();
        String lastError = "none";

        for (int attempt = 1; attempt <= max; attempt++) {
            ServerAddress primary = null;
            try {
                GetPrimaryResponse p = master.withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                        .getPrimary(GetPrimaryRequest.getDefaultInstance());
                if (!p.getHasPrimary()) {
                    lastError = "master has no primary";
                    log.warn("write attempt {}/{}: master has no primary right now; waiting", attempt, max);
                    sleep(props.retryBackoffMs());
                    continue;
                }
                primary = p.getPrimary();
                WriteChunkResponse resp = stub(primary.getHost(), primary.getPort())
                        .withDeadlineAfter(props.writeTimeoutMs(), TimeUnit.MILLISECONDS)
                        .writeChunk(WriteChunkRequest.newBuilder()
                                .setChunkHandle(handle)
                                .setOffset(offset)
                                .setData(ByteString.copyFrom(data))
                                .build());
                if (attempt > 1) {
                    log.info("write of '{}' succeeded on attempt {} via primary {}", handle, attempt, primary.getServerId());
                }
                return resp;
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) {
                    throw new GfsException("invalid write: " + e.getStatus().getDescription(), e);
                }
                lastError = describe(e);
                log.warn("write attempt {}/{} via {} failed: {} -> re-discovering primary",
                        attempt, max, primary == null ? "master" : primary.getServerId(), lastError);
                sleep(props.retryBackoffMs());
            }
        }
        throw new GfsException("write of chunk '" + handle + "' failed after " + max
                + " attempts; last error: " + lastError);
    }

    /** Bypasses discovery: send a write straight to one chunkserver (used to prove lease enforcement). */
    public WriteChunkResponse writeDirect(String host, int port, String handle, long offset, byte[] data) {
        return stub(host, port)
                .withDeadlineAfter(props.writeTimeoutMs(), TimeUnit.MILLISECONDS)
                .writeChunk(WriteChunkRequest.newBuilder()
                        .setChunkHandle(handle)
                        .setOffset(offset)
                        .setData(ByteString.copyFrom(data))
                        .build());
    }

    // ------------------------------------------------------------------ read

    public ReadChunkResponse read(String handle, long offset, long length) {
        int max = props.readMaxAttempts();
        String lastError = "none";

        for (int attempt = 1; attempt <= max; attempt++) {
            List<ServerAddress> replicas;
            try {
                replicas = master.withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                        .locateChunk(LocateChunkRequest.newBuilder().setChunkHandle(handle).build())
                        .getServersList();
            } catch (StatusRuntimeException e) {
                lastError = "master unavailable: " + describe(e);
                log.warn("read attempt {}/{}: {}", attempt, max, lastError);
                sleep(props.retryBackoffMs());
                continue;
            }
            if (replicas.isEmpty()) {
                lastError = "master knows no live replica of chunk '" + handle + "'";
                log.warn("read attempt {}/{}: {}", attempt, max, lastError);
                sleep(props.retryBackoffMs());
                continue;
            }
            for (ServerAddress replica : replicas) {
                try {
                    return stub(replica.getHost(), replica.getPort())
                            .withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                            .readChunk(ReadChunkRequest.newBuilder()
                                    .setChunkHandle(handle).setOffset(offset).setLength(length).build());
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) {
                        throw new GfsException("invalid read: " + e.getStatus().getDescription(), e);
                    }
                    lastError = replica.getServerId() + ": " + describe(e);
                    log.warn("read from {} failed ({}); trying next replica", replica.getServerId(), describe(e));
                }
            }
            sleep(props.retryBackoffMs());
        }
        throw new GfsException("read of chunk '" + handle + "' failed after " + max
                + " attempts; last error: " + lastError);
    }

    // ------------------------------------------------------------------ status

    public GetClusterStatusResponse status() {
        try {
            return master.withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .getClusterStatus(GetClusterStatusRequest.getDefaultInstance());
        } catch (StatusRuntimeException e) {
            throw new GfsException("cannot reach master: " + describe(e), e);
        }
    }

    // ------------------------------------------------------------------ helpers

    private ChunkServiceGrpc.ChunkServiceBlockingStub stub(String host, int port) {
        ManagedChannel ch = channels.computeIfAbsent(host + ":" + port,
                k -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
        return ChunkServiceGrpc.newBlockingStub(ch);
    }

    public static String describe(StatusRuntimeException e) {
        String desc = e.getStatus().getDescription();
        return e.getStatus().getCode() + (desc == null ? "" : " - " + desc);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GfsException("interrupted");
        }
    }

    @PreDestroy
    void close() {
        channels.values().forEach(ManagedChannel::shutdown);
    }
}