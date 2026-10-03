package io.gfs.chunkserver;

import io.gfs.proto.ChunkInfo;
import io.gfs.proto.HeartbeatRequest;
import io.gfs.proto.HeartbeatResponse;
import io.gfs.proto.MasterServiceGrpc;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PreDestroy;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Sends heartbeats (liveness + full chunk report) to the master. A single-thread
 * executor serializes the periodic beats AND the out-of-band beats triggered after
 * writes, so the master always sees reports in order.
 *
 * The primary flag in the response is informational only (logging) - write
 * authorization is always re-checked live via CheckPrimary.
 */
@Component
public class HeartbeatService {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);

    @GrpcClient("master")
    private MasterServiceGrpc.MasterServiceBlockingStub master;

    private final ChunkserverProperties props;
    private final ChunkStore store;
    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "heartbeat"));

    // touched only from the heartbeat thread
    private boolean wasPrimary = false;
    private long lastLeaseId = 0;

    public HeartbeatService(ChunkserverProperties props, ChunkStore store) {
        this.props = props;
        this.store = store;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        log.info("Chunkserver '{}' started; heartbeating to master every {} ms",
                props.id(), props.heartbeatIntervalMs());
        executor.scheduleWithFixedDelay(this::beat, 0, props.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    /** Push a heartbeat now (e.g. right after a write) so the master learns new chunk locations quickly. */
    public void triggerNow() {
        executor.execute(this::beat);
    }

    @PreDestroy
    void stop() {
        executor.shutdownNow();
    }

    private void beat() {
        try {
            HeartbeatRequest.Builder req = HeartbeatRequest.newBuilder().setServerId(props.id());
            store.snapshot().forEach((handle, size) ->
                    req.addChunks(ChunkInfo.newBuilder().setChunkHandle(handle).setSizeBytes(size)));

            HeartbeatResponse resp = master
                    .withDeadlineAfter(props.rpcTimeoutMs(), TimeUnit.MILLISECONDS)
                    .heartbeat(req.build());

            if (resp.getIsPrimary() && (!wasPrimary || resp.getLeaseId() != lastLeaseId)) {
                log.info("I am now PRIMARY (lease {})", resp.getLeaseId());
            } else if (!resp.getIsPrimary() && wasPrimary) {
                log.warn("Lost primary role; current primary is '{}'", resp.getPrimaryId());
            }
            wasPrimary = resp.getIsPrimary();
            lastLeaseId = resp.getLeaseId();
        } catch (StatusRuntimeException e) {
            log.warn("Heartbeat to master failed: {}", e.getStatus());
        } catch (RuntimeException e) {
            log.error("Unexpected error during heartbeat", e);
        }
    }
}