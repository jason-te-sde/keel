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
 * @param cluster the address book: every node this one may need to reach, including itself, mapped to
 *     {@code host:port}. Addresses of nodes added later arrive in the configuration entries themselves.
 * @param bootstrapVoters the membership to start from, which is <em>not</em> the same thing as the
 *     address book. A node joining an existing cluster knows everyone's address but is not a member
 *     yet, and must not count itself in a quorum until the entry adding it is applied. Empty means
 *     every node in the address book is a voter, which is how a fresh cluster starts.
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
 * @param security TLS and access control; see {@link SecurityOptions} for why the default refuses to
 *     bind a non-loopback address
 */
public record NodeOptions(
        long nodeId,
        Map<Long, String> cluster,
        Set<Long> bootstrapVoters,
        Path dataDir,
        Duration tick,
        int electionTimeoutTicks,
        int heartbeatTicks,
        Duration requestTimeout,
        Path stateMachineDir,
        int snapshotThresholdEntries,
        SecurityOptions security) {

    public NodeOptions {
        cluster = new LinkedHashMap<>(new TreeMap<>(cluster));
        if (!cluster.containsKey(nodeId)) {
            throw new IllegalArgumentException(
                    "the address book " + cluster.keySet() + " has no address for this node " + nodeId);
        }
        bootstrapVoters =
                bootstrapVoters == null || bootstrapVoters.isEmpty()
                        ? Set.copyOf(cluster.keySet())
                        : Set.copyOf(bootstrapVoters);
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
        security = security == null ? SecurityOptions.none() : security;
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
                Set.of(),
                dataDir,
                Duration.ofMillis(50),
                10,
                1,
                Duration.ofSeconds(5),
                null,
                8192,
                SecurityOptions.none());
    }

    /**
     * Options for a node joining an existing cluster: it knows every address but is not a voter yet.
     *
     * @param existingVoters the cluster's current membership, which does not include this node
     */
    public static NodeOptions joining(
            long nodeId, Map<Long, String> addressBook, Set<Long> existingVoters, Path dataDir) {
        return new NodeOptions(
                nodeId,
                addressBook,
                existingVoters,
                dataDir,
                Duration.ofMillis(50),
                10,
                1,
                Duration.ofSeconds(5),
                null,
                8192,
                SecurityOptions.none());
    }

    public NodeOptions withTick(Duration tick) {
        return new NodeOptions(
                nodeId, cluster, bootstrapVoters, dataDir, tick, electionTimeoutTicks, heartbeatTicks,
                requestTimeout, stateMachineDir, snapshotThresholdEntries, security);
    }

    public NodeOptions withRequestTimeout(Duration timeout) {
        return new NodeOptions(
                nodeId, cluster, bootstrapVoters, dataDir, tick, electionTimeoutTicks, heartbeatTicks, timeout,
                stateMachineDir, snapshotThresholdEntries, security);
    }

    public NodeOptions withRocksDb(Path directory) {
        return new NodeOptions(
                nodeId, cluster, bootstrapVoters, dataDir, tick, electionTimeoutTicks, heartbeatTicks,
                requestTimeout, directory, snapshotThresholdEntries, security);
    }

    public NodeOptions withSnapshotThreshold(int entries) {
        return new NodeOptions(
                nodeId, cluster, bootstrapVoters, dataDir, tick, electionTimeoutTicks, heartbeatTicks,
                requestTimeout, stateMachineDir, entries, security);
    }

    /**
     * Largest request this node accepts, in bytes.
     *
     * <p>Fixed rather than configurable for now. It is below gRPC's four megabyte default so an
     * oversized value is refused by this store with a reason, rather than by the transport with a
     * message about frame sizes.
     */
    public int maxRequestBytes() {
        return 2 * 1024 * 1024;
    }

    /** Replaces the security configuration. */
    public NodeOptions withSecurity(SecurityOptions security) {
        return new NodeOptions(
                nodeId, cluster, bootstrapVoters, dataDir, tick, electionTimeoutTicks, heartbeatTicks,
                requestTimeout, stateMachineDir, snapshotThresholdEntries, security);
    }

    /** Host part of this node's own address, for the bind-time security check. */
    public String listenHost() {
        String address = cluster.get(nodeId);
        int colon = address.lastIndexOf(':');
        return colon < 0 ? address : address.substring(0, colon);
    }

    public Set<Long> voters() {
        return bootstrapVoters;
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
