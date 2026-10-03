package io.gfs.chunkserver;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Fencing token check. Lease ids are strictly increasing, so once this server has
 * seen lease N, any mutation carrying a lease id < N comes from a deposed primary
 * and is rejected. This protects replicas from a stale primary even if that primary
 * still believes (wrongly) that it holds the lease.
 */
@Component
public class LeaseFence {

    private final AtomicLong highestSeen = new AtomicLong(0);

    /** @return true if the lease id is current or newer; false if it is stale. */
    public boolean admit(long leaseId) {
        long previous = highestSeen.getAndUpdate(cur -> Math.max(cur, leaseId));
        return leaseId >= previous;
    }
}