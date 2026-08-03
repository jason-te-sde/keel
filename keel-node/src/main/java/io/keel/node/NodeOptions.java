package io.keel.node;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * How to run one node.
 *
 * @param nodeId this node's id, which must match its entry in {@code cluster}
 * @param cluster every voter, including this node, mapped to {@code host:port}
 * @param dataDir where the log lives; one directory per node
 * @param tick wall-clock duration of one logical tick. The core counts ticks and never reads a clock,
 *     so this is the only place real time enters the system.
 * @param electionTimeoutTicks ticks without contact from a leader before campaigning
 * @param heartbeatTicks ticks between leader heartbeats
 * @param requestTimeout how long a client request waits before being reported as timed out
 * @param stateMachineDir when present, keep state in RocksDB under this directory instead of on the
 *     heap
 * @param snapshotThresholdEntries take a snapshot and compact the log once this many entries have
 *     accumulated beyond the last snapshot; 0 disables compaction
 */
public record NodeOptions(
        long nodeId,
        Map<Long, String> cluster,
        Path dataDir,
        Duration tick,
        int electionTimeoutTicks,
        int heartbeatTicks,
        Duration requestTimeout,
        Path stateMachineDir,
        int snapshotThresholdEntries) {

    public NodeOptions {
        cluster = new LinkedHashMap<>(new TreeMap<>(cluster));
        if (!cluster.containsKey(nodeId)) {
            throw new IllegalArgumentException(
                    "cluster " + cluster.keySet() + " does not contain this node " + nodeId);
        }
        if (tick.isZero() || tick.isNegative()) {
            throw new IllegalArgumentException("tick must be positive");
        }
        if (electionTimeoutTicks <= heartbeatTicks) {
            throw new IllegalArgumentException(
                    "electionTimeoutTicks must exceed heartbeatTicks");
        }
        if (snapshotThresholdEntries < 0) {
            throw new IllegalArgumentException("snapshotThresholdEntries cannot be negative");
        }
    }

    /**
     * Defaults sized for a real network: a 50ms tick gives a 500ms to 1s election timeout, which is
     * comfortably above a datacentre round trip and fast enough that a failover is not noticed as an
     * outage.
     */
    public static NodeOptions of(long nodeId, Map<Long, String> cluster, Path dataDir) {
        return new NodeOptions(
                nodeId,
                cluster,
                dataDir,
                Duration.ofMillis(50),
                10,
                1,
                Duration.ofSeconds(5),
                null,
                8192);
    }

    public NodeOptions withTick(Duration tick) {
        return new NodeOptions(
                nodeId, cluster, dataDir, tick, electionTimeoutTicks, heartbeatTicks, requestTimeout,
                stateMachineDir, snapshotThresholdEntries);
    }

    public NodeOptions withRequestTimeout(Duration timeout) {
        return new NodeOptions(
                nodeId, cluster, dataDir, tick, electionTimeoutTicks, heartbeatTicks, timeout,
                stateMachineDir, snapshotThresholdEntries);
    }

    public NodeOptions withRocksDb(Path directory) {
        return new NodeOptions(
                nodeId, cluster, dataDir, tick, electionTimeoutTicks, heartbeatTicks, requestTimeout,
                directory, snapshotThresholdEntries);
    }

    public NodeOptions withSnapshotThreshold(int entries) {
        return new NodeOptions(
                nodeId, cluster, dataDir, tick, electionTimeoutTicks, heartbeatTicks, requestTimeout,
                stateMachineDir, entries);
    }

    public Set<Long> voters() {
        return cluster.keySet();
    }

    /** Port this node listens on, parsed from its own cluster entry. */
    public int listenPort() {
        String address = cluster.get(nodeId);
        int colon = address.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("address " + address + " has no port");
        }
        return Integer.parseInt(address.substring(colon + 1));
    }
}
