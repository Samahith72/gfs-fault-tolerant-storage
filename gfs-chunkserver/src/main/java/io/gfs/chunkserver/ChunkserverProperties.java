package io.gfs.chunkserver;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "gfs.chunkserver")
public record ChunkserverProperties(
        String id,
        String dataDir,
        @DefaultValue("1000") long heartbeatIntervalMs,
        @DefaultValue("3000") long rpcTimeoutMs,
        @DefaultValue("5000") long lockTimeoutMs,
        @DefaultValue("67108864") long maxChunkSizeBytes,
        @DefaultValue("2097152") long maxWriteBytes,
        @DefaultValue("true") boolean fsyncOnWrite) {
}