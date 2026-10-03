package io.gfs.chunkserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ChunkserverApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChunkserverApplication.class, args);
    }
}