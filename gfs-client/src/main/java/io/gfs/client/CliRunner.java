package io.gfs.client;

import io.gfs.proto.ChunkInfo;
import io.gfs.proto.GetClusterStatusResponse;
import io.gfs.proto.ReadChunkResponse;
import io.gfs.proto.ServerStatus;
import io.gfs.proto.WriteChunkResponse;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tiny CLI on top of GfsClient. Usage (positional args after the jar):
 *
 *   status
 *   write      &lt;chunk&gt; &lt;offset&gt; &lt;text...&gt;
 *   read       &lt;chunk&gt; [offset] [length]
 *   write-loop &lt;chunk&gt; &lt;count&gt; &lt;intervalMs&gt; [tag]
 *   raw-write  &lt;host&gt; &lt;port&gt; &lt;chunk&gt; &lt;offset&gt; &lt;text...&gt;   (bypasses discovery; for lease tests)
 */
@Component
public class CliRunner implements ApplicationRunner, ExitCodeGenerator {

    private static final Logger log = LoggerFactory.getLogger(CliRunner.class);
    private static final int RECORD_SIZE = 32;

    private static final String USAGE = """
            Usage:
              status
              write      <chunk> <offset> <text...>
              read       <chunk> [offset] [length]
              write-loop <chunk> <count> <intervalMs> [tag]
              raw-write  <host> <port> <chunk> <offset> <text...>""";

    private static final class UsageException extends RuntimeException {
        UsageException(String message) {
            super(message);
        }
    }

    private final GfsClient client;
    private int exitCode = 0;

    public CliRunner(GfsClient client) {
        this.client = client;
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    @Override
    public void run(ApplicationArguments args) {
        List<String> a = args.getNonOptionArgs();
        try {
            if (a.isEmpty()) {
                throw new UsageException("no command given");
            }
            switch (a.get(0)) {
                case "status" -> status();
                case "write" -> write(a);
                case "read" -> read(a);
                case "write-loop" -> writeLoop(a);
                case "raw-write" -> rawWrite(a);
                default -> throw new UsageException("unknown command '" + a.get(0) + "'");
            }
        } catch (UsageException | NumberFormatException e) {
            log.error("{}\n{}", e.getMessage(), USAGE);
            exitCode = 2;
        } catch (GfsClient.GfsException e) {
            log.error("FAILED: {}", e.getMessage());
            exitCode = 1;
        } catch (StatusRuntimeException e) {
            log.error("FAILED: {}", GfsClient.describe(e));
            exitCode = 1;
        }
    }

    // ------------------------------------------------------------------ commands

    private void status() {
        GetClusterStatusResponse s = client.status();
        long now = s.getServerTimeMs();
        if (s.getPrimaryId().isEmpty()) {
            log.info("PRIMARY: none");
        } else {
            log.info("PRIMARY: {} (lease {}, expires in {} ms)", s.getPrimaryId(), s.getLeaseId(),
                    s.getLeaseExpiryMs() - now);
        }
        for (ServerStatus sv : s.getServersList()) {
            String chunks = sv.getChunksList().stream()
                    .map((ChunkInfo c) -> c.getChunkHandle() + "(" + c.getSizeBytes() + "B)")
                    .collect(Collectors.joining(", "));
            String lastHb = sv.getLastHeartbeatMs() == 0 ? "never" : (now - sv.getLastHeartbeatMs()) + " ms ago";
            log.info("SERVER {} {}:{} alive={} primary={} lastHeartbeat={} chunks=[{}]",
                    sv.getServerId(), sv.getHost(), sv.getPort(), sv.getAlive(), sv.getIsPrimary(), lastHb, chunks);
        }
        s.getChunkLocationsList().forEach(l ->
                log.info("CHUNK {} -> {}", l.getChunkHandle(), l.getServerIdsList()));
    }

    private void write(List<String> a) {
        if (a.size() < 4) {
            throw new UsageException("write needs <chunk> <offset> <text...>");
        }
        String handle = a.get(1);
        long offset = Long.parseLong(a.get(2));
        byte[] data = String.join(" ", a.subList(3, a.size())).getBytes(StandardCharsets.UTF_8);
        WriteChunkResponse r = client.write(handle, offset, data);
        log.info("WRITE ok: chunk={} offset={} bytes={} replicasAcked={} lease={}",
                handle, offset, data.length, r.getReplicasAcked(), r.getLeaseId());
    }

    private void read(List<String> a) {
        if (a.size() < 2 || a.size() > 4) {
            throw new UsageException("read needs <chunk> [offset] [length]");
        }
        long offset = a.size() > 2 ? Long.parseLong(a.get(2)) : 0;
        long length = a.size() > 3 ? Long.parseLong(a.get(3)) : 0;
        ReadChunkResponse r = client.read(a.get(1), offset, length);
        log.info("READ ok: chunk={} servedBy={} bytes={} chunkSize={}\n{}",
                a.get(1), r.getServedBy(), r.getData().size(), r.getChunkSize(),
                r.getData().toString(StandardCharsets.UTF_8));
    }

    /** Writes fixed-size 32-byte records at offset i*32, so every write is idempotent and verifiable. */
    private void writeLoop(List<String> a) {
        if (a.size() < 4 || a.size() > 5) {
            throw new UsageException("write-loop needs <chunk> <count> <intervalMs> [tag]");
        }
        String handle = a.get(1);
        int count = Integer.parseInt(a.get(2));
        long intervalMs = Long.parseLong(a.get(3));
        String tag = a.size() > 4 ? a.get(4) : "rec";

        long started = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            String record = String.format("%-" + (RECORD_SIZE - 1) + "s", tag + "-" + i) + "\n";
            long t0 = System.currentTimeMillis();
            WriteChunkResponse r = client.write(handle, (long) i * RECORD_SIZE,
                    record.getBytes(StandardCharsets.UTF_8));
            log.info("WRITE-LOOP {}/{} ok in {} ms (replicasAcked={}, lease={})",
                    i + 1, count, System.currentTimeMillis() - t0, r.getReplicasAcked(), r.getLeaseId());
            if (intervalMs > 0 && i < count - 1) {
                try {
                    Thread.sleep(intervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new GfsClient.GfsException("interrupted");
                }
            }
        }
        log.info("WRITE-LOOP done: {} records written to '{}' in {} ms",
                count, handle, System.currentTimeMillis() - started);
    }

    private void rawWrite(List<String> a) {
        if (a.size() < 6) {
            throw new UsageException("raw-write needs <host> <port> <chunk> <offset> <text...>");
        }
        String host = a.get(1);
        int port = Integer.parseInt(a.get(2));
        byte[] data = String.join(" ", a.subList(5, a.size())).getBytes(StandardCharsets.UTF_8);
        WriteChunkResponse r = client.writeDirect(host, port, a.get(3), Long.parseLong(a.get(4)), data);
        log.info("RAW-WRITE accepted by {}:{} (replicasAcked={}, lease={})", host, port,
                r.getReplicasAcked(), r.getLeaseId());
    }
}