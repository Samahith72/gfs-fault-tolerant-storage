package io.gfs.client;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "gfs.client")
public record ClientProperties(
        @DefaultValue("15") int writeMaxAttempts,
        @DefaultValue("3") int readMaxAttempts,
        @DefaultValue("1000") long retryBackoffMs,
        @DefaultValue("3000") long rpcTimeoutMs,
        @DefaultValue("10000") long writeTimeoutMs) {
}