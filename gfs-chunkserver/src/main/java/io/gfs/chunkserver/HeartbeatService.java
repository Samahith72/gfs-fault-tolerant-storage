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
 * Sends heartbeats to the master: registration (advertised address), liveness, and a full
 * chunk report with versions. A single-thread executor serializes the periodic beats AND the
 * out-of-band beats triggered after writes/repairs, so the master sees reports in order.
 * Copy orders in the response are handed to the RepairService.
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
    private final RepairService repair;
    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "heartbeat"));

    // touched only from the heartbeat thread
    private boolean wasPrimary = false;
    private long lastLeaseId = 0;

    public HeartbeatService(ChunkserverProperties props, ChunkStore store, RepairService repair) {
        this.props = props;
        this.store = store;
        this.repair = repair;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        log.info("Chunkserver '{}' started; advertising {}:{}; heartbeating to master every {} ms",
                props.id(), props.advertiseHost(), props.advertisePort(), props.heartbeatIntervalMs());
        executor.scheduleWithFixedDelay(this::beat, 0, props.heartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }

    /** Push a heartbeat now (after a write or repair) so the master learns new chunk state quickly. */
    public void triggerNow() {
        executor.execute(this::beat);
    }

    @PreDestroy
    void stop() {
        executor.shutdownNow();
    }

    private void beat() {
        try {
            HeartbeatRequest.Builder req = HeartbeatRequest.newBuilder()
                    .setServerId(props.id())
                    .setHost(props.advertiseHost())
                    .setPort(props.advertisePort());
            store.snapshot().forEach((handle, info) -> req.addChunks(ChunkInfo.newBuilder()
                    .setChunkHandle(handle)
                    .setSizeBytes(info.size())
                    .setVersion(info.version())));

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

            resp.getCopyOrdersList().forEach(order -> repair.submit(order, this::triggerNow));
        } catch (StatusRuntimeException e) {
            log.warn("Heartbeat to master failed: {}", e.getStatus());
        } catch (RuntimeException e) {
            log.error("Unexpected error during heartbeat", e);
        }
    }
}