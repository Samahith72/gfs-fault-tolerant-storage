package main.java.io.gfs.master;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Drives failure detection and lease expiry/election even when no RPCs are arriving. */
@Component
public class LeaseMonitor {

    private final ClusterState state;

    public LeaseMonitor(ClusterState state) {
        this.state = state;
    }

    @Scheduled(fixedDelayString = "${gfs.master.election-tick-ms:500}")
    public void tick() {
        state.tick();
    }
}