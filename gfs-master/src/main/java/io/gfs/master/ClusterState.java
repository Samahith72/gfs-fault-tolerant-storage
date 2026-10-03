package io.gfs.master;

import io.gfs.proto.CheckPrimaryResponse;
import io.gfs.proto.ChunkInfo;
import io.gfs.proto.ChunkLocation;
import io.gfs.proto.GetClusterStatusResponse;
import io.gfs.proto.GetPrimaryResponse;
import io.gfs.proto.HeartbeatResponse;
import io.gfs.proto.ServerAddress;
import io.gfs.proto.ServerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The master's entire in-memory state: server liveness, the single write lease,
 * and chunk locations (derived from chunkserver reports, never persisted).
 *
 * Every public method is synchronized. The state is tiny and every operation is
 * O(servers), so one lock gives us trivially correct lease/election semantics.
 *
 * Lease safety rule: a new primary is only granted when the previous lease has
 * EXPIRED (or never existed). So two servers can never hold a valid lease at once.
 */

@Component
public class ClusterState {

    private static final Logger log = LoggerFactory.getLogger(ClusterState.class);

    private final Clock clock;
    private final long heartbeatTimeoutMs;
    private final long leaseDurationMs;

    /** Sorted by id => deterministic "lowest alive id" election. */
    private final Map<String, ServerEntry> servers = new TreeMap<>();

    private String primaryId;      // null = no primary
    private long leaseId;          // monotonic fencing token
    private long leaseExpiryMs;

    public ClusterState(MasterProperties props, Clock clock) {
        this.clock = clock;
        this.heartbeatTimeoutMs = props.heartbeatTimeoutMs();
        this.leaseDurationMs = props.leaseDurationMs();
        for (MasterProperties.ServerConfig cfg : props.servers()) {
            servers.put(cfg.id(), new ServerEntry(cfg.id(), cfg.host(), cfg.port()));
        }
        log.info("Master configured with {} chunkservers {} (heartbeatTimeout={}ms, leaseDuration={}ms)",
                servers.size(), servers.keySet(), heartbeatTimeoutMs, leaseDurationMs);
    }

    // ------------------------------------------------------------------ RPC-facing operations

    public synchronized HeartbeatResponse heartbeat(String serverId, List<ChunkInfo> chunks) {
        long now = clock.millis();
        ServerEntry s = requireServer(serverId);

        s.lastHeartbeatMs = now;
        s.chunks = toMap(chunks);
        if (!s.markedAlive) {
            s.markedAlive = true;
            log.info("Chunkserver {} is ALIVE ({} chunks reported)", serverId, s.chunks.size());
        }

        // Lease renewal piggybacks on the primary's heartbeat. An already-expired lease is
        // never resurrected: reconcileLease() below revokes it and runs a fresh election.
        if (serverId.equals(primaryId) && now < leaseExpiryMs) {
            leaseExpiryMs = now + leaseDurationMs;
        }
        reconcileLease(now);

        return HeartbeatResponse.newBuilder()
                .setIsPrimary(serverId.equals(primaryId))
                .setLeaseId(leaseId)
                .setLeaseExpiryMs(leaseExpiryMs)
                .setPrimaryId(primaryId == null ? "" : primaryId)
                .build();
    }

    public synchronized GetPrimaryResponse getPrimary() {
        long now = clock.millis();
        reconcileLease(now);
        if (primaryId == null) {
            return GetPrimaryResponse.newBuilder().setHasPrimary(false).build();
        }
        return GetPrimaryResponse.newBuilder()
                .setHasPrimary(true)
                .setPrimary(address(servers.get(primaryId)))
                .setLeaseId(leaseId)
                .setLeaseExpiryMs(leaseExpiryMs)
                .build();
    }

    /** The live lease check every chunkserver performs before accepting a write. */
    public synchronized CheckPrimaryResponse checkPrimary(String serverId) {
        long now = clock.millis();
        requireServer(serverId);
        reconcileLease(now);

        boolean isPrimary = serverId.equals(primaryId) && now < leaseExpiryMs;
        CheckPrimaryResponse.Builder b = CheckPrimaryResponse.newBuilder()
                .setIsPrimary(isPrimary)
                .setLeaseId(leaseId)
                .setLeaseExpiryMs(leaseExpiryMs);
        if (isPrimary) {
            for (ServerEntry s : servers.values()) {
                if (!s.id.equals(serverId) && isAlive(s, now)) {
                    b.addSecondaries(address(s));
                }
            }
        }
        return b.build();
    }

    public synchronized List<ServerAddress> locate(String chunkHandle) {
        long now = clock.millis();
        List<ServerAddress> result = new ArrayList<>();
        for (ServerEntry s : servers.values()) {
            if (isAlive(s, now) && s.chunks.containsKey(chunkHandle)) {
                result.add(address(s));
            }
        }
        return result;
    }

    public synchronized GetClusterStatusResponse clusterStatus() {
        long now = clock.millis();
        reconcileLease(now);

        GetClusterStatusResponse.Builder b = GetClusterStatusResponse.newBuilder()
                .setServerTimeMs(now)
                .setLeaseId(leaseId)
                .setLeaseExpiryMs(leaseExpiryMs)
                .setPrimaryId(primaryId == null ? "" : primaryId);

        Map<String, List<String>> locations = new TreeMap<>();
        for (ServerEntry s : servers.values()) {
            boolean alive = isAlive(s, now);
            ServerStatus.Builder sb = ServerStatus.newBuilder()
                    .setServerId(s.id)
                    .setHost(s.host)
                    .setPort(s.port)
                    .setAlive(alive)
                    .setIsPrimary(s.id.equals(primaryId))
                    .setLastHeartbeatMs(s.lastHeartbeatMs);
            for (Map.Entry<String, Long> c : s.chunks.entrySet()) {
                sb.addChunks(ChunkInfo.newBuilder().setChunkHandle(c.getKey()).setSizeBytes(c.getValue()));
                if (alive) {
                    locations.computeIfAbsent(c.getKey(), k -> new ArrayList<>()).add(s.id);
                }
            }
            b.addServers(sb);
        }
        locations.forEach((handle, ids) ->
                b.addChunkLocations(ChunkLocation.newBuilder().setChunkHandle(handle).addAllServerIds(ids)));
        return b.build();
    }

    /** Called periodically by LeaseMonitor: detect dead servers, expire leases, elect. */
    public synchronized void tick() {
        long now = clock.millis();
        for (ServerEntry s : servers.values()) {
            if (s.markedAlive && !isAlive(s, now)) {
                s.markedAlive = false;
                log.warn("Chunkserver {} declared DEAD (no heartbeat for {} ms)", s.id, now - s.lastHeartbeatMs);
            }
        }
        reconcileLease(now);
    }

    // ------------------------------------------------------------------ internals (lock held)

    private void reconcileLease(long now) {
        if (primaryId != null && now >= leaseExpiryMs) {
            log.warn("Lease {} held by primary {} EXPIRED - revoking", leaseId, primaryId);
            primaryId = null;
        }
        if (primaryId == null) {
            for (ServerEntry candidate : servers.values()) {
                if (isAlive(candidate, now)) {
                    // max(prev+1, now): strictly increasing, and still monotonic after a master restart
                    leaseId = Math.max(leaseId + 1, now);
                    primaryId = candidate.id;
                    leaseExpiryMs = now + leaseDurationMs;
                    log.info("Granted lease {} to NEW PRIMARY {} (valid for {} ms)", leaseId, primaryId, leaseDurationMs);
                    return;
                }
            }
        }
    }

    private boolean isAlive(ServerEntry s, long now) {
        return s.lastHeartbeatMs > 0 && now - s.lastHeartbeatMs <= heartbeatTimeoutMs;
    }

    private ServerEntry requireServer(String serverId) {
        ServerEntry s = servers.get(serverId);
        if (s == null) {
            throw new IllegalArgumentException(
                    "Unknown chunkserver '" + serverId + "' (not listed in gfs.master.servers)");
        }
        return s;
    }

    private static ServerAddress address(ServerEntry s) {
        return ServerAddress.newBuilder().setServerId(s.id).setHost(s.host).setPort(s.port).build();
    }

    private static Map<String, Long> toMap(List<ChunkInfo> chunks) {
        Map<String, Long> m = new HashMap<>();
        for (ChunkInfo c : chunks) {
            m.put(c.getChunkHandle(), c.getSizeBytes());
        }
        return m;
    }

    private static final class ServerEntry {
        final String id;
        final String host;
        final int port;
        long lastHeartbeatMs = 0;
        boolean markedAlive = false;
        Map<String, Long> chunks = Map.of();

        ServerEntry(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
        }
    }
}