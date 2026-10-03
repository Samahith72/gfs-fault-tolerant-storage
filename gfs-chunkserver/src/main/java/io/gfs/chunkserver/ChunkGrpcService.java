package io.gfs.chunkserver;

import com.google.protobuf.ByteString;
import io.gfs.proto.CheckPrimaryRequest;
import io.gfs.proto.CheckPrimaryResponse;
import io.gfs.proto.ChunkServiceGrpc;
import io.gfs.proto.MasterServiceGrpc;
import io.gfs.proto.ReadChunkRequest;
import io.gfs.proto.ReadChunkResponse;
import io.gfs.proto.ReplicateChunkRequest;
import io.gfs.proto.ReplicateChunkResponse;
import io.gfs.proto.ServerAddress;
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

        // 1. LIVE lease check against the master. Fail closed: if we cannot prove
        //    we are the primary, we do not write. No server id is ever hardcoded.
        CheckPrimaryResponse lease;
        try {
            lease = master.withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .checkPrimary(CheckPrimaryRequest.newBuilder().setServerId(props.id()).build());
        } catch (StatusRuntimeException e) {
            log.warn("Rejecting write to '{}': cannot verify lease with master ({})", handle, e.getStatus());
            throw new RpcFailure(Status.Code.UNAVAILABLE,
                    "Cannot verify primary lease with master: " + e.getStatus().getCode());
        }
        if (!lease.getIsPrimary()) {
            log.warn("Rejecting write to '{}': {} does not hold the write lease", handle, props.id());
            throw new RpcFailure(Status.Code.FAILED_PRECONDITION,
                    "NOT_PRIMARY: " + props.id() + " does not hold the write lease");
        }

        // 2. Per-chunk write lock held across local apply + replication, so all replicas
        //    apply writes to this chunk in the same order.
        try (ChunkStore.AutoLock ignored = store.writeLock(handle)) {
            if (!fence.admit(lease.getLeaseId())) {
                throw new RpcFailure(Status.Code.FAILED_PRECONDITION,
                        "STALE_LEASE: lease " + lease.getLeaseId() + " has been superseded");
            }
            store.write(handle, req.getOffset(), req.getData().toByteArray());

            int acks = 1; // the primary itself
            for (ServerAddress secondary : lease.getSecondariesList()) {
                replicate(secondary, req, lease.getLeaseId());
                acks++;
            }
            log.info("WRITE chunk={} offset={} bytes={} lease={} replicasAcked={}",
                    handle, req.getOffset(), req.getData().size(), lease.getLeaseId(), acks);
            return WriteChunkResponse.newBuilder()
                    .setLeaseId(lease.getLeaseId())
                    .setReplicasAcked(acks)
                    .setNewChunkSize(store.snapshot().getOrDefault(handle, 0L))
                    .build();
        } catch (ChunkStore.ChunkBusyException e) {
            throw new RpcFailure(Status.Code.UNAVAILABLE, e.getMessage());
        } catch (IOException e) {
            log.error("I/O error writing chunk {}", handle, e);
            throw new RpcFailure(Status.Code.INTERNAL, "I/O error: " + e.getMessage());
        }
    }

    private void replicate(ServerAddress target, WriteChunkRequest req, long leaseId) {
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
                            .build());
        } catch (StatusRuntimeException e) {
            log.warn("Replication of '{}' to {} failed: {}", req.getChunkHandle(), target.getServerId(), e.getStatus());
            throw new RpcFailure(Status.Code.UNAVAILABLE,
                    "REPLICATION_FAILED: " + target.getServerId() + " (" + e.getStatus().getCode() + ")");
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
                store.write(handle, req.getOffset(), req.getData().toByteArray());
                log.info("REPLICATE chunk={} offset={} bytes={} from={} lease={}",
                        handle, req.getOffset(), req.getData().size(), req.getPrimaryId(), req.getLeaseId());
            } catch (ChunkStore.ChunkBusyException e) {
                throw new RpcFailure(Status.Code.UNAVAILABLE, e.getMessage());
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