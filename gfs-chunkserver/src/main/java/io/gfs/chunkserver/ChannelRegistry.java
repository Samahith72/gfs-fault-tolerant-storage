package main.java.io.gfs.chunkserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Cache of gRPC channels to peer chunkservers (addresses are only known at runtime, from the master). */
@Component
public class ChannelRegistry {

    private final ConcurrentMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public ManagedChannel get(String host, int port) {
        return channels.computeIfAbsent(host + ":" + port,
                key -> ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    @PreDestroy
    void shutdown() {
        channels.values().forEach(ManagedChannel::shutdown);
    }
}