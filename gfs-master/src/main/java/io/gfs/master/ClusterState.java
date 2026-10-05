package io.gfs.master;

import io.gfs.proto.CheckPrimaryResponse;
import io.gfs.proto.ChunkInfo;
import io.gfs.proto.ChunkLocation;
import io.gfs.proto.CopyOrder;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * The master's entire in-memory state: dynamic server membership, the single write
 * lease, and per-chunk metadata (assigned replicas, committed version, FRESH set).
 *
 * Every public method is synchronized. State is small and every operation is cheap,
 * so one lock gives trivially correct lease / commit / repair-planning semantics.
 *
 * Invariants:
 *  - A new primary is granted only after the previous lease has EXPIRED (no split brain).
 *  - Writes go only to FRESH replicas; reads are served only from FRESH replicas.
 *  - A chunk's fresh set is replaced by the exact set of replicas that acked each commit,
 *    so a replica that missed a write is stale until repaired.
 */
@Component
public class ClusterState {

    private static final Logger log = LoggerFactory.getLogger(ClusterState.class);
    private static final Pattern SERVER_ID = Pattern.compile("[A-Za-z0-9_-]{1,32}");

    private final Clock clock;
    private final long heartbeatTimeoutMs;
    private final long leaseDurationMs;
    private final int replicationFactor;
    private final long startupGraceMs;
    private final long repairQuietMs;
    private final long repairTimeoutMs;
    private final int maxRepairsPerTick;
    private final long startedAtMs;

    /** Sorted by id => deterministic "lowest alive id" election. Filled by heartbeats. */
    private final Map<String, ServerEntry> servers = new TreeMap<>();
    private final Map<String, ChunkMeta> chunks = new TreeMap<>();
    private final Map<String, RepairOrder> inflight = new HashMap<>();       // key = chunk@target
    private final Map<String, List<CopyOrder>> pendingOrders = new HashMap<>(); // target -> orders
    private long readCursor = 0;

    private String primaryId;      // null = no primary
    private long leaseId;          // monotonic fencing token
    private long leaseExpiryMs;

    public ClusterState(MasterProperties props, Clock clock) {
        this.clock = clock;
        this.heartbeatTimeoutMs = props.heartbeatTimeoutMs();
        this.leaseDurationMs = props.leaseDurationMs();
        this.replicationFactor = props.replicationFactor();
        this.startupGraceMs = props.startupGraceMs();
        this.repairQuietMs = props.repairQuietMs();
        this.repairTimeoutMs = props.repairTimeoutMs();
        this.maxRepairsPerTick = props.maxRepairsPerTick();
        this.startedAtMs = clock.millis();
        log.info("Master ready: replicationFactor={}, heartbeatTimeout={}ms, leaseDuration={}ms, "
                        + "startupGrace={}ms (chunkservers register themselves via heartbeat)",
                replicationFactor, heartbeatTimeoutMs, leaseDurationMs, startupGraceMs);
    }

    // ------------------------------------------------------------------ RPC-facing operations

    public synchronized HeartbeatResponse heartbeat(String serverId, String host, int port,
                                                    List<ChunkInfo> reports) {
        long now = clock.millis();
        if (serverId == null || !SERVER_ID.matcher(serverId).matches()) {
            throw new IllegalArgumentException("Invalid server id (allowed: A-Z a-z 0-9 _ -, max 32 chars)");
        }
        if (host == null || host.isBlank() || port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Heartbeat must carry the server's advertised host and port");
        }

        ServerEntry s = servers.get(serverId);
        if (s == null) {
            s = new ServerEntry(serverId, host, port);
            servers.put(serverId, s);
            log.info("Chunkserver {} REGISTERED at {}:{} ({} servers known)", serverId, host, port, servers.size());
        } else if (!s.host.equals(host) || s.port != port) {
            log.info("Chunkserver {} changed address {}:{} -> {}:{}", serverId, s.host, s.port, host, port);
            s.host = host;
            s.port = port;
        }

        s.lastHeartbeatMs = now;
        s.chunks = toMap(reports);
        if (!s.markedAlive) {
            s.markedAlive = true;
            log.info("Chunkserver {} is ALIVE ({} chunks reported)", serverId, s.chunks.size());
        }
        for (ChunkInfo c : reports) {
            absorbReport(s, c);
        }

        // Lease renewal piggybacks on the primary's heartbeat. An already-expired lease is
        // never resurrected: reconcileLease() below revokes it and runs a fresh election.
        if (serverId.equals(primaryId) && now < leaseExpiryMs) {
            leaseExpiryMs = now + leaseDurationMs;
        }
        reconcileLease(now);

        HeartbeatResponse.Builder b = HeartbeatResponse.newBuilder()
                .setIsPrimary(serverId.equals(primaryId))
                .setLeaseId(leaseId)
                .setLeaseExpiryMs(leaseExpiryMs)
                .setPrimaryId(primaryId == null ? "" : primaryId);
        List<CopyOrder> orders = pendingOrders.remove(serverId);
        if (orders != null) {
            b.addAllCopyOrders(orders);
        }
        return b.build();
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

    /**
     * The live lease check a primary performs before every write. If the caller is the
     * primary, this also returns the chunk's replica set and the version the write must carry,
     * placing the chunk on RF servers the first time it is seen.
     */
    public synchronized CheckPrimaryResponse checkPrimary(String serverId, String chunkHandle) {
        long now = clock.millis();
        requireServer(serverId);
        reconcileLease(now);

        boolean isPrimary = serverId.equals(primaryId) && now < leaseExpiryMs;
        CheckPrimaryResponse.Builder b = CheckPrimaryResponse.newBuilder()
                .setIsPrimary(isPrimary)
                .setLeaseId(leaseId)
                .setLeaseExpiryMs(leaseExpiryMs);

        if (isPrimary && chunkHandle != null && !chunkHandle.isEmpty()) {
            ChunkMeta m = chunks.get(chunkHandle);
            List<ServerEntry> targets;
            if (m == null || (m.version == 0 && writeTargets(m, now).isEmpty())) {
                m = place(chunkHandle, serverId, now);
                targets = writeTargets(m, now);
            } else {
                targets = writeTargets(m, now);
                if (targets.isEmpty()) {
                    throw new IllegalStateException("NO_LIVE_REPLICA: every up-to-date replica of chunk '"
                            + chunkHandle + "' (" + m.fresh + ") is down; refusing to write elsewhere");
                }
            }
            m.lastWriteMs = now;
            for (ServerEntry t : targets) {
                b.addReplicas(address(t));
            }
            b.setVersion(m.version + 1);
        }
        return b.build();
    }

    /** The primary reports a fully replicated write: record the new version and who acked it. */
    public synchronized void commitWrite(String serverId, String chunkHandle, long leaseIdArg,
                                         long version, List<String> ackedIds) {
        long now = clock.millis();
        reconcileLease(now);
        if (!(serverId.equals(primaryId) && now < leaseExpiryMs && leaseIdArg == leaseId)) {
            throw new IllegalStateException("NOT_PRIMARY: commit from " + serverId + " rejected (lease "
                    + leaseIdArg + " is not the current valid lease)");
        }
        ChunkMeta m = chunks.get(chunkHandle);
        if (m == null) {
            throw new IllegalStateException("Commit for unknown chunk '" + chunkHandle + "'");
        }
        if (version >= m.version) {
            m.version = version;
            m.fresh.clear();
            m.fresh.addAll(ackedIds);
            m.assigned.addAll(ackedIds);
        } else {
            log.warn("Ignoring commit of chunk {} at version {} (already at {})", chunkHandle, version, m.version);
        }
        m.lastWriteMs = now;
    }

    /** Fresh + alive replicas that hold the chunk, rotated round-robin for read load-balancing. */
    public synchronized List<ServerAddress> locate(String chunkHandle) {
        long now = clock.millis();
        ChunkMeta m = chunks.get(chunkHandle);
        if (m == null) {
            return List.of();
        }
        List<ServerEntry> holders = readableHolders(m, chunkHandle, now);
        if (holders.isEmpty()) {
            return List.of();
        }
        Collections.rotate(holders, -(int) (readCursor++ % holders.size()));
        List<ServerAddress> result = new ArrayList<>();
        for (ServerEntry s : holders) {
            result.add(address(s));
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
                .setReplicationFactor(replicationFactor)
                .setPrimaryId(primaryId == null ? "" : primaryId);

        for (ServerEntry s : servers.values()) {
            ServerStatus.Builder sb = ServerStatus.newBuilder()
                    .setServerId(s.id)
                    .setHost(s.host)
                    .setPort(s.port)
                    .setAlive(isAlive(s, now))
                    .setIsPrimary(s.id.equals(primaryId))
                    .setLastHeartbeatMs(s.lastHeartbeatMs);
            for (Map.Entry<String, ChunkReport> c : s.chunks.entrySet()) {
                sb.addChunks(ChunkInfo.newBuilder()
                        .setChunkHandle(c.getKey())
                        .setSizeBytes(c.getValue().size())
                        .setVersion(c.getValue().version()));
            }
            b.addServers(sb);
        }

        for (Map.Entry<String, ChunkMeta> e : chunks.entrySet()) {
            ChunkMeta m = e.getValue();
            if (m.version == 0) {
                continue; // placed but never successfully written
            }
            ChunkLocation.Builder lb = ChunkLocation.newBuilder()
                    .setChunkHandle(e.getKey())
                    .setVersion(m.version);
            for (ServerEntry s : servers.values()) {
                if (!isAlive(s, now) || !s.chunks.containsKey(e.getKey())) {
                    continue;
                }
                if (m.fresh.contains(s.id)) {
                    lb.addServerIds(s.id);
                } else {
                    lb.addStaleServerIds(s.id);
                }
            }
            b.addChunkLocations(lb);
        }
        return b.build();
    }

    /** Called periodically by LeaseMonitor: detect dead servers, expire leases, elect, plan repairs. */
    public synchronized void tick() {
        long now = clock.millis();
        for (ServerEntry s : servers.values()) {
            if (s.markedAlive && !isAlive(s, now)) {
                s.markedAlive = false;
                pendingOrders.remove(s.id);
                log.warn("Chunkserver {} declared DEAD (no heartbeat for {} ms)", s.id, now - s.lastHeartbeatMs);
            }
        }
        reconcileLease(now);
        planRepairs(now);
    }

    // ------------------------------------------------------------------ internals (lock held)

    private void absorbReport(ServerEntry s, ChunkInfo c) {
        String handle = c.getChunkHandle();
        ChunkMeta m = chunks.computeIfAbsent(handle, h -> new ChunkMeta());
        long r = c.getVersion();
        m.assigned.add(s.id);
        if (r > m.version) {
            // A replica is ahead of everything we know (e.g. master restart): it defines the truth.
            m.version = r;
            m.fresh.clear();
            m.fresh.add(s.id);
        } else if (r == m.version) {
            m.fresh.add(s.id);
        }
        // A lower version never REMOVES a replica from fresh: heartbeat snapshots can lag commits.
        String key = key(handle, s.id);
        RepairOrder o = inflight.get(key);
        if (o != null && r >= o.requiredVersion()) {
            inflight.remove(key);
        }
    }

    /** First write of a chunk: pick RF alive servers, least-loaded first, caller wins ties. */
    private ChunkMeta place(String handle, String callerId, long now) {
        ChunkMeta m = chunks.computeIfAbsent(handle, h -> new ChunkMeta());
        m.assigned.clear();
        m.fresh.clear();
        Map<String, Integer> load = loads();
        List<ServerEntry> candidates = new ArrayList<>();
        for (ServerEntry s : servers.values()) {
            if (isAlive(s, now)) {
                candidates.add(s);
            }
        }
        candidates.sort(Comparator
                .comparingInt((ServerEntry s) -> load.getOrDefault(s.id, 0))
                .thenComparing(s -> s.id.equals(callerId) ? 0 : 1)
                .thenComparing(s -> s.id));
        int n = Math.min(replicationFactor, candidates.size());
        for (int i = 0; i < n; i++) {
            m.assigned.add(candidates.get(i).id);
            m.fresh.add(candidates.get(i).id);
        }
        if (n < replicationFactor) {
            log.warn("Chunk {} placed on only {} servers (replication factor {}); repair will add more",
                    handle, n, replicationFactor);
        }
        log.info("PLACED chunk {} on {} (primary {} {} a replica)", handle, m.fresh, callerId,
                m.fresh.contains(callerId) ? "is" : "is NOT, acts as coordinator only,");
        return m;
    }

    private void planRepairs(long now) {
        if (now < startedAtMs + startupGraceMs) {
            return;
        }
        inflight.values().removeIf(o -> now >= o.expiryMs());
        final Map<String, Integer> load = loads();
        int budget = maxRepairsPerTick;

        for (Map.Entry<String, ChunkMeta> e : chunks.entrySet()) {
            if (budget <= 0) {
                return;
            }
            String handle = e.getKey();
            ChunkMeta m = e.getValue();
            if (m.version == 0 || now - m.lastWriteMs < repairQuietMs) {
                continue;
            }
            List<ServerEntry> sources = readableHolders(m, handle, now);
            if (sources.isEmpty()) {
                continue; // nothing to copy from
            }

            // 1) alive members that are not fresh need a catch-up copy
            List<ServerEntry> targets = new ArrayList<>();
            int covered = 0;
            for (String id : m.assigned) {
                ServerEntry s = servers.get(id);
                if (s == null || !isAlive(s, now)) {
                    continue;
                }
                covered++;
                if (!m.fresh.contains(id)) {
                    targets.add(s);
                }
            }
            for (RepairOrder o : inflight.values()) {
                if (o.chunk().equals(handle) && !m.assigned.contains(o.target())) {
                    covered++;
                }
            }

            // 2) under-replicated: add new servers until RF is met again
            if (covered < replicationFactor) {
                List<ServerEntry> extras = new ArrayList<>();
                for (ServerEntry s : servers.values()) {
                    if (isAlive(s, now) && !m.assigned.contains(s.id) && !inflight.containsKey(key(handle, s.id))) {
                        extras.add(s);
                    }
                }
                extras.sort(Comparator.comparingInt((ServerEntry s) -> load.getOrDefault(s.id, 0))
                        .thenComparing(s -> s.id));
                targets.addAll(extras.subList(0, Math.min(extras.size(), replicationFactor - covered)));
            }

            for (ServerEntry t : targets) {
                if (budget <= 0) {
                    return;
                }
                if (inflight.containsKey(key(handle, t.id)) || targetBusy(t.id)) {
                    continue;
                }
                ServerEntry src = sources.get(0);
                inflight.put(key(handle, t.id), new RepairOrder(handle, t.id, m.version, now + repairTimeoutMs));
                pendingOrders.computeIfAbsent(t.id, k -> new ArrayList<>()).add(CopyOrder.newBuilder()
                        .setChunkHandle(handle)
                        .setSource(address(src))
                        .setRequiredVersion(m.version)
                        .build());
                log.info("REPAIR scheduled: chunk={} version={} copy {} -> {} (rf={}, fresh={})",
                        handle, m.version, src.id, t.id, replicationFactor, m.fresh);
                budget--;
            }
        }
    }

    private boolean targetBusy(String target) {
        for (RepairOrder o : inflight.values()) {
            if (o.target().equals(target)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, Integer> loads() {
        Map<String, Integer> load = new HashMap<>();
        for (ChunkMeta m : chunks.values()) {
            for (String id : m.assigned) {
                load.merge(id, 1, Integer::sum);
            }
        }
        return load;
    }

    /** Alive + fresh: where a write must be applied. */
    private List<ServerEntry> writeTargets(ChunkMeta m, long now) {
        List<ServerEntry> out = new ArrayList<>();
        for (String id : m.fresh) {
            ServerEntry s = servers.get(id);
            if (s != null && isAlive(s, now)) {
                out.add(s);
            }
        }
        return out;
    }

    /** Alive + fresh + actually reported the chunk: where a read (or repair copy) can be served. */
    private List<ServerEntry> readableHolders(ChunkMeta m, String handle, long now) {
        List<ServerEntry> out = new ArrayList<>();
        for (String id : m.fresh) {
            ServerEntry s = servers.get(id);
            if (s != null && isAlive(s, now) && s.chunks.containsKey(handle)) {
                out.add(s);
            }
        }
        return out;
    }

    private void reconcileLease(long now) {
        if (primaryId != null && now >= leaseExpiryMs) {
            log.warn("Lease {} held by primary {} EXPIRED - revoking", leaseId, primaryId);
            primaryId = null;
        }
        if (primaryId == null && now >= startedAtMs + startupGraceMs) {
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
            throw new IllegalArgumentException("Unknown chunkserver '" + serverId + "' (has not registered yet)");
        }
        return s;
    }

    private static String key(String chunk, String target) {
        return chunk + "@" + target;
    }

    private static ServerAddress address(ServerEntry s) {
        return ServerAddress.newBuilder().setServerId(s.id).setHost(s.host).setPort(s.port).build();
    }

    private static Map<String, ChunkReport> toMap(List<ChunkInfo> reports) {
        Map<String, ChunkReport> m = new HashMap<>();
        for (ChunkInfo c : reports) {
            m.put(c.getChunkHandle(), new ChunkReport(c.getSizeBytes(), c.getVersion()));
        }
        return m;
    }

    private record ChunkReport(long size, long version) {
    }

    private record RepairOrder(String chunk, String target, long requiredVersion, long expiryMs) {
    }

    private static final class ChunkMeta {
        long version = 0;                       // last committed version
        long lastWriteMs = 0;
        final Set<String> assigned = new TreeSet<>();   // servers intended to hold this chunk
        final Set<String> fresh = new TreeSet<>();      // subset known to hold `version`
    }

    private static final class ServerEntry {
        final String id;
        String host;
        int port;
        long lastHeartbeatMs = 0;
        boolean markedAlive = false;
        Map<String, ChunkReport> chunks = Map.of();

        ServerEntry(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
        }
    }
}