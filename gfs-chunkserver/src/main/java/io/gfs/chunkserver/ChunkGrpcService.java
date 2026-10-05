package io.gfs.chunkserver;

import com.google.protobuf.ByteString;
import io.gfs.proto.CheckPrimaryRequest;
import io.gfs.proto.CheckPrimaryResponse;
import io.gfs.proto.ChunkServiceGrpc;
import io.gfs.proto.CommitWriteRequest;
import io.gfs.proto.MasterServiceGrpc;
import io.gfs.proto.ReadChunkRequest;
import io.gfs.proto.ReadChunkResponse;
import io.gfs.proto.ReplicateChunkRequest;
import io.gfs.proto.ReplicateChunkResponse;
import io.gfs.proto.ServerAddress;
import io.gfs.proto.StreamChunkRequest;
import io.gfs.proto.StreamChunkResponse;
import io.gfs.proto.WriteChunkRequest;
import io.gfs.proto.WriteChunkResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.client.inject.GrpcClient;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@GrpcService
public class ChunkGrpcService extends ChunkServiceGrpc.ChunkServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(ChunkGrpcService.class);

    @GrpcClient("master")
    private MasterServiceGrpc.MasterServiceBlockingStub master;

    private final ChunkserverProperties props;
    private final ChunkStore store;
    private final LeaseFence fence;
    private final ChannelRegistry channels;
    private final HeartbeatService heartbeat;

    public ChunkGrpcService(ChunkserverProperties props, ChunkStore store, LeaseFence fence,
                            ChannelRegistry channels, HeartbeatService heartbeat) {
        this.props = props;
        this.store = store;
        this.fence = fence;
        this.channels = channels;
        this.heartbeat = heartbeat;
    }

    // ================================================================= WriteChunk (primary only)

    @Override
    public void writeChunk(WriteChunkRequest req, StreamObserver<WriteChunkResponse> out) {
        respond(out, () -> {
            WriteChunkResponse resp = doWrite(req);
            heartbeat.triggerNow();
            return resp;
        });
    }

    private WriteChunkResponse doWrite(WriteChunkRequest req) {
        String handle = req.getChunkHandle();
        validatePayload(handle, req.getOffset(), req.getData().size());
        byte[] bytes = req.getData().toByteArray();

        // The per-chunk lock is held across lease check, local apply, replication and commit, so
        // every replica sees mutations to a chunk in the same order and versions are contiguous.
        try (ChunkStore.AutoLock ignored = store.writeLock(handle)) {

            // 1. LIVE lease check against the master. Fail closed: if we cannot prove we are the
            //    primary, we do not write. No server id is ever hardcoded.
            CheckPrimaryResponse lease = checkLease(handle);
            if (!lease.getIsPrimary()) {
                log.warn("Rejecting write to '{}': {} does not hold the write lease", handle, props.id());
                throw new RpcFailure(Status.Code.FAILED_PRECONDITION,
                        "NOT_PRIMARY: " + props.id() + " does not hold the write lease");
            }
            if (!fence.admit(lease.getLeaseId())) {
                throw new RpcFailure(Status.Code.FAILED_PRECONDITION,
                        "STALE_LEASE: lease " + lease.getLeaseId() + " has been superseded");
            }
            if (lease.getReplicasCount() == 0) {
                throw new RpcFailure(Status.Code.UNAVAILABLE, "master returned no live replicas for '" + handle + "'");
            }
            long version = lease.getVersion();

            // 2. Apply to every fresh replica: our own copy first (if we are a member), then the others.
            //    If we are not a member of this chunk's replica set we act as coordinator only.
            List<String> acked = new ArrayList<>();
            for (ServerAddress r : lease.getReplicasList()) {
                if (r.getServerId().equals(props.id())) {
                    store.write(handle, req.getOffset(), bytes, version);
                    acked.add(r.getServerId());
                }
            }
            for (ServerAddress r : lease.getReplicasList()) {
                if (!r.getServerId().equals(props.id())) {
                    replicate(r, req, lease.getLeaseId(), version);
                    acked.add(r.getServerId());
                }
            }

            // 3. Tell the master exactly which replicas now hold this version.
            commit(handle, lease.getLeaseId(), version, acked);

            ChunkStore.Info info = store.info(handle);
            log.info("WRITE chunk={} offset={} bytes={} version={} lease={} replicas={}",
                    handle, req.getOffset(), bytes.length, version, lease.getLeaseId(), acked);
            return WriteChunkResponse.newBuilder()
                    .setLeaseId(lease.getLeaseId())
                    .setReplicasAcked(acked.size())
                    .setNewChunkSize(info == null ? 0 : info.size())
                    .setVersion(version)
                    .build();
        } catch (ChunkStore.ChunkBusyException e) {
            throw new RpcFailure(Status.Code.UNAVAILABLE, e.getMessage());
        } catch (ChunkStore.VersionMismatchException e) {
            log.warn("Write to '{}' refused locally: {}", handle, e.getMessage());
            throw new RpcFailure(Status.Code.FAILED_PRECONDITION, "VERSION_MISMATCH: " + e.getMessage());
        } catch (IOException e) {
            log.error("I/O error writing chunk {}", handle, e);
            throw new RpcFailure(Status.Code.INTERNAL, "I/O error: " + e.getMessage());
        }
    }

    private CheckPrimaryResponse checkLease(String handle) {
        try {
            return master.withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .checkPrimary(CheckPrimaryRequest.newBuilder()
                            .setServerId(props.id())
                            .setChunkHandle(handle)
                            .build());
        } catch (StatusRuntimeException e) {
            log.warn("Rejecting write to '{}': cannot verify lease with master ({})", handle, e.getStatus());
            throw new RpcFailure(Status.Code.UNAVAILABLE, "Cannot verify primary lease with master: "
                    + e.getStatus().getCode() + descriptionSuffix(e));
        }
    }

    private void commit(String handle, long leaseId, long version, List<String> acked) {
        try {
            master.withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .commitWrite(CommitWriteRequest.newBuilder()
                            .setServerId(props.id())
                            .setChunkHandle(handle)
                            .setLeaseId(leaseId)
                            .setVersion(version)
                            .addAllAckedServerIds(acked)
                            .build());
        } catch (StatusRuntimeException e) {
            log.warn("Commit of '{}' v{} failed: {}", handle, version, e.getStatus());
            throw new RpcFailure(Status.Code.UNAVAILABLE, "COMMIT_FAILED: " + e.getStatus().getCode()
                    + descriptionSuffix(e));
        }
    }

    private void replicate(ServerAddress target, WriteChunkRequest req, long leaseId, long version) {
        ChunkServiceGrpc.ChunkServiceBlockingStub stub =
                ChunkServiceGrpc.newBlockingStub(channels.get(target.getHost(), target.getPort()));
        try {
            stub.withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .replicateChunk(ReplicateChunkRequest.newBuilder()
                            .setChunkHandle(req.getChunkHandle())
                            .setOffset(req.getOffset())
                            .setData(req.getData())
                            .setLeaseId(leaseId)
                            .setPrimaryId(props.id())
                            .setVersion(version)
                            .build());
        } catch (StatusRuntimeException e) {
            log.warn("Replication of '{}' to {} failed: {}", req.getChunkHandle(), target.getServerId(), e.getStatus());
            throw new RpcFailure(Status.Code.UNAVAILABLE,
                    "REPLICATION_FAILED: " + target.getServerId() + " (" + e.getStatus().getCode()
                            + descriptionSuffix(e) + ")");
        }
    }

    // ================================================================= ReplicateChunk (secondary)

    @Override
    public void replicateChunk(ReplicateChunkRequest req, StreamObserver<ReplicateChunkResponse> out) {
        respond(out, () -> {
            String handle = req.getChunkHandle();
            validatePayload(handle, req.getOffset(), req.getData().size());
            try (ChunkStore.AutoLock ignored = store.writeLock(handle)) {
                // Checked INSIDE the lock so a newer lease cannot slip in between check and apply.
                if (!fence.admit(req.getLeaseId())) {
                    log.warn("Rejected replication from '{}': stale lease {}", req.getPrimaryId(), req.getLeaseId());
                    throw new RpcFailure(Status.Code.FAILED_PRECONDITION,
                            "STALE_LEASE: lease " + req.getLeaseId() + " has been superseded");
                }
                store.write(handle, req.getOffset(), req.getData().toByteArray(), req.getVersion());
                log.info("REPLICATE chunk={} offset={} bytes={} version={} from={} lease={}",
                        handle, req.getOffset(), req.getData().size(), req.getVersion(),
                        req.getPrimaryId(), req.getLeaseId());
            } catch (ChunkStore.ChunkBusyException e) {
                throw new RpcFailure(Status.Code.UNAVAILABLE, e.getMessage());
            } catch (ChunkStore.VersionMismatchException e) {
                log.warn("Rejected replication of '{}': {}", handle, e.getMessage());
                throw new RpcFailure(Status.Code.FAILED_PRECONDITION, "VERSION_MISMATCH: " + e.getMessage());
            } catch (IOException e) {
                log.error("I/O error replicating chunk {}", handle, e);
                throw new RpcFailure(Status.Code.INTERNAL, "I/O error: " + e.getMessage());
            }
            heartbeat.triggerNow();
            return ReplicateChunkResponse.getDefaultInstance();
        });
    }

    // ================================================================= ReadChunk (any replica)

    @Override
    public void readChunk(ReadChunkRequest req, StreamObserver<ReadChunkResponse> out) {
        respond(out, () -> {
            String handle = req.getChunkHandle();
            try {
                ChunkStore.validateHandle(handle);
            } catch (IllegalArgumentException e) {
                throw new RpcFailure(Status.Code.INVALID_ARGUMENT, e.getMessage());
            }
            try (ChunkStore.AutoLock ignored = store.readLock(handle)) {
                ChunkStore.ReadResult r = store.read(handle, req.getOffset(), req.getLength());
                log.info("READ chunk={} offset={} bytes={}", handle, req.getOffset(), r.data().length);
                return ReadChunkResponse.newBuilder()
                        .setData(ByteString.copyFrom(r.data()))
                        .setChunkSize(r.chunkSize())
                        .setServedBy(props.id())
                        .build();
            } catch (ChunkStore.ChunkNotFoundException e) {
                throw new RpcFailure(Status.Code.NOT_FOUND, e.getMessage());
            } catch (ChunkStore.ChunkBusyException e) {
                throw new RpcFailure(Status.Code.UNAVAILABLE, e.getMessage());
            } catch (IllegalArgumentException e) {
                throw new RpcFailure(Status.Code.INVALID_ARGUMENT, e.getMessage());
            } catch (IOException e) {
                log.error("I/O error reading chunk {}", handle, e);
                throw new RpcFailure(Status.Code.INTERNAL, "I/O error: " + e.getMessage());
            }
        });
    }

    // ================================================================= StreamChunk (repair source)

    /** Streams a consistent snapshot: the chunk read lock is held for the whole stream, so writers wait. */
    @Override
    public void streamChunk(StreamChunkRequest req, StreamObserver<StreamChunkResponse> out) {
        String handle = req.getChunkHandle();
        try {
            ChunkStore.validateHandle(handle);
            int piece = (int) Math.min(props.copyPieceBytes(), props.maxWriteBytes());
            try (ChunkStore.AutoLock ignored = store.readLock(handle)) {
                ChunkStore.Info info = store.info(handle);
                if (info == null) {
                    throw new ChunkStore.ChunkNotFoundException(handle);
                }
                long off = 0;
                do {
                    long len = Math.min(piece, info.size() - off);
                    byte[] data = len > 0 ? store.read(handle, off, len).data() : new byte[0];
                    out.onNext(StreamChunkResponse.newBuilder()
                            .setData(ByteString.copyFrom(data))
                            .setChunkSize(info.size())
                            .setVersion(info.version())
                            .build());
                    off += len;
                } while (off < info.size());
                log.info("STREAM chunk={} version={} bytes={} (repair source)", handle, info.version(), info.size());
            }
            out.onCompleted();
        } catch (IllegalArgumentException e) {
            out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        } catch (ChunkStore.ChunkNotFoundException e) {
            out.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
        } catch (ChunkStore.ChunkBusyException e) {
            out.onError(Status.UNAVAILABLE.withDescription(e.getMessage()).asRuntimeException());
        } catch (IOException e) {
            log.error("I/O error streaming chunk {}", handle, e);
            out.onError(Status.INTERNAL.withDescription("I/O error: " + e.getMessage()).asRuntimeException());
        } catch (RuntimeException e) {
            log.error("Unexpected error streaming chunk {}", handle, e);
            out.onError(Status.INTERNAL.withDescription(e.getClass().getSimpleName()).asRuntimeException());
        }
    }

    // ================================================================= helpers

    private void validatePayload(String handle, long offset, int length) {
        try {
            ChunkStore.validateHandle(handle);
        } catch (IllegalArgumentException e) {
            throw new RpcFailure(Status.Code.INVALID_ARGUMENT, e.getMessage());
        }
        if (offset < 0) {
            throw new RpcFailure(Status.Code.INVALID_ARGUMENT, "offset must be >= 0");
        }
        if (length > props.maxWriteBytes()) {
            throw new RpcFailure(Status.Code.INVALID_ARGUMENT,
                    "payload of " + length + " bytes exceeds max " + props.maxWriteBytes());
        }
        if (offset + length > props.maxChunkSizeBytes()) {
            throw new RpcFailure(Status.Code.INVALID_ARGUMENT,
                    "write would exceed max chunk size " + props.maxChunkSizeBytes());
        }
    }

    private static String descriptionSuffix(StatusRuntimeException e) {
        String d = e.getStatus().getDescription();
        return d == null ? "" : " - " + d;
    }

    private <T> void respond(StreamObserver<T> out, Supplier<T> action) {
        try {
            T result = action.get();
            out.onNext(result);
            out.onCompleted();
        } catch (RpcFailure f) {
            out.onError(f.toStatus());
        } catch (RuntimeException e) {
            log.error("Unexpected error handling RPC", e);
            out.onError(Status.INTERNAL
                    .withDescription(e.getClass().getSimpleName() + ": " + e.getMessage())
                    .asRuntimeException());
        }
    }

    private static final class RpcFailure extends RuntimeException {
        private final Status.Code code;

        RpcFailure(Status.Code code, String message) {
            super(message);
            this.code = code;
        }

        StatusRuntimeException toStatus() {
            return code.toStatus().withDescription(getMessage()).asRuntimeException();
        }
    }
}