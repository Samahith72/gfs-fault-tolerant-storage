package io.gfs.master;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "gfs.master")
public record MasterProperties(
        @DefaultValue("4000") long heartbeatTimeoutMs,
        @DefaultValue("5000") long leaseDurationMs,
        @DefaultValue("3") int replicationFactor,
        @DefaultValue("3000") long startupGraceMs,
        @DefaultValue("3000") long repairQuietMs,
        @DefaultValue("30000") long repairTimeoutMs,
        @DefaultValue("4") int maxRepairsPerTick) {

    public MasterProperties {
        if (replicationFactor < 1) {
            throw new IllegalStateException("gfs.master.replication-factor must be >= 1");
        }
    }
}