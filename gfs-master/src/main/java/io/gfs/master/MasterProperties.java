package main.java.io.gfs.master;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.List;

@ConfigurationProperties(prefix = "gfs.master")
public record MasterProperties(
        @DefaultValue("4000") 
        long heartbeatTimeoutMs,

        @DefaultValue("5000") 
        ong leaseDurationMs,
        
        List<ServerConfig> servers) {

    public MasterProperties {
        if (servers == null || servers.isEmpty()) {
            throw new IllegalStateException("gfs.master.servers must list at least one chunkserver");
        }
    }

    public record ServerConfig(String id, String host, int port) {
    }
}