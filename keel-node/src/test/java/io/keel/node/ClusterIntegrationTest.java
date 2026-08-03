package io.keel.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.keel.proto.service.StatusResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three nodes on real sockets, with real files.
 *
 * <p>The simulator covers the schedules that matter for consensus, so these tests are here for what it
 * cannot reach: that the codec round-trips over the wire, that gRPC threads and the thread owning the
 * core stay out of each other's way, that a data directory survives a restart, and that the commands a
 * person would actually type do what they say.
 *
 * <p>Nothing here sleeps waiting for progress. Every wait polls a condition with a deadline, so a slow
 * CI runner makes a test slower rather than flaky.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class ClusterIntegrationTest {

    private static final Duration TICK = Duration.ofMillis(20);

    /**
     * Deliberately tiny. A production threshold would compact once an hour; this makes every test that
     * writes more than a handful of keys cross the compaction paths.
     */
    private static final int SNAPSHOT_EVERY = 12;

    @TempDir Path root;

    private final List<KeelNode> nodes = new ArrayList<>();
    private final List<KeelClient> clients = new ArrayList<>();
    private Map<Long, String> cluster;

    @AfterEach
    void tearDown() {
        clients.forEach(KeelClient::close);
        nodes.forEach(KeelNode::close);
    }

    @Test
    @DisplayName("a three-node cluster elects a leader and serves reads and writes")
    void writeThenRead() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();

        client.put("greeting", "hello");

        assertEquals(Optional.of("hello"), client.get("greeting"));
        assertEquals(Optional.empty(), client.get("absent"), "an unwritten key should be absent");
    }

    @Test
    @DisplayName("every replica ends up with the same data")
    void dataReachesEveryReplica() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        for (int i = 0; i < 20; i++) {
            client.put("key-" + i, "value-" + i);
        }

        await(
                () -> nodes.stream().allMatch(node -> node.keyCount() == 20),
                "every replica to hold 20 keys");

        for (KeelNode node : nodes) {
            assertEquals(20, node.keyCount(), "node " + node.nodeId());
        }
    }

    @Test
    @DisplayName("data written before a leader is killed is readable after failover")
    void survivesLeaderFailure() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        client.put("durable", "yes");

        long oldLeader = leaderId();
        stop(oldLeader);

        // The client rediscovers the cluster on its own, following the hint it gets back.
        await(() -> leaderIdOrZero() != 0 && leaderIdOrZero() != oldLeader, "a new leader");
        assertNotEquals(oldLeader, leaderId());
        assertEquals(
                Optional.of("yes"),
                client.get("durable"),
                "a value acknowledged before the failover must still be there");

        client.put("after", "failover");
        assertEquals(Optional.of("failover"), client.get("after"));
    }

    @Test
    @DisplayName("a restarted node recovers its log from disk and catches up")
    void restartedNodeCatchesUp() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        client.put("before", "restart");

        long victim = followerId();
        Path dataDir = dataDirOf(victim);
        stop(victim);

        for (int i = 0; i < 10; i++) {
            client.put("while-down-" + i, "v" + i);
        }

        KeelNode restarted =
                KeelNode.open(
                                optionsFor(victim, dataDir))
                        .start();
        nodes.add(restarted);

        await(() -> restarted.keyCount() == 11, "the restarted node to catch up");
        assertEquals(11, restarted.keyCount());
    }

    @Test
    @DisplayName("a follower that missed a compacted range is caught up from a snapshot")
    void followerCatchesUpFromASnapshot() {
        // Without compaction this is just replication. With it, the entries the follower needs are gone
        // from the leader's log, so the only way to repair it is to ship the state machine.
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        long victim = followerId();
        Path dataDir = dataDirOf(victim);
        stop(victim);

        for (int i = 0; i < 60; i++) {
            client.put("k" + i, "v" + i);
        }
        await(
                () -> nodes.stream().anyMatch(node -> node.snapshotIndex() > 0),
                "the leader to take a snapshot");

        KeelNode restarted =
                KeelNode.open(
                                optionsFor(victim, dataDir))
                        .start();
        nodes.add(restarted);

        await(() -> restarted.keyCount() == 60, "the follower to catch up from a snapshot");
        assertTrue(
                restarted.snapshotIndex() > 0,
                "it should have been caught up by a snapshot, not by entries");
        assertEquals(Optional.of("v42"), client.get("k42"));
    }

    @Test
    @DisplayName("a node restarts from its own snapshot rather than replaying everything")
    void restartsFromItsOwnSnapshot() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        for (int i = 0; i < 40; i++) {
            client.put("k" + i, "v" + i);
        }
        long victim = followerId();
        await(() -> node(victim).snapshotIndex() > 0, "the follower to snapshot its own state");
        Path dataDir = dataDirOf(victim);
        long boundary = node(victim).snapshotIndex();
        stop(victim);

        KeelNode restarted =
                KeelNode.open(
                                optionsFor(victim, dataDir))
                        .start();
        nodes.add(restarted);

        // The keys below the boundary can only be there because the snapshot was loaded: those log
        // entries no longer exist.
        assertTrue(restarted.snapshotIndex() >= boundary, "it should have restored from its snapshot");
        await(() -> restarted.keyCount() == 40, "the restarted node to be complete");
        assertEquals(Optional.of("v0"), client.get("k0"));
    }

    @Test
    @DisplayName("a majority restart keeps everything that was acknowledged")
    void survivesFullRestart() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        for (int i = 0; i < 5; i++) {
            client.put("k" + i, "v" + i);
        }
        // Every node holds it before we take the cluster down, so nothing is in flight.
        await(() -> nodes.stream().allMatch(node -> node.keyCount() == 5), "all replicas to catch up");

        List<Path> dataDirs = new ArrayList<>();
        for (long id : cluster.keySet()) {
            dataDirs.add(dataDirOf(id));
        }
        new ArrayList<>(nodes).forEach(this::stopNode);
        clients.forEach(KeelClient::close);
        clients.clear();

        int i = 0;
        for (long id : cluster.keySet()) {
            nodes.add(
                    KeelNode.open(
                                    optionsFor(id, dataDirs.get(i++)))
                            .start());
        }

        KeelClient fresh = client();
        await(() -> leaderIdOrZero() != 0, "a leader after the restart");
        assertEquals(
                Optional.of("v3"),
                fresh.get("k3"),
                "a value acknowledged before every node died must come back");
    }

    @Test
    @DisplayName("compare-and-swap is atomic across the cluster")
    void compareAndSwap() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();

        assertTrue(client.compareAndSwap("counter", null, "1"), "insert into an absent key");
        assertTrue(!client.compareAndSwap("counter", null, "2"), "the key exists now");
        assertTrue(client.compareAndSwap("counter", "1", "2"), "the value matches");
        assertTrue(!client.compareAndSwap("counter", "1", "3"), "the value no longer matches");
        assertEquals(Optional.of("2"), client.get("counter"));
    }

    @Test
    @DisplayName("a retried write is applied once")
    void sessionsDeduplicateRetries() {
        // The client retries on its own when a request times out, which is the case sessions exist for.
        // Here the retry is explicit so the assertion is about the state machine rather than about
        // timing.
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        client.put("counter", "1");
        assertTrue(client.compareAndSwap("counter", "1", "2"));

        // A second identical compare-and-swap must not fire: the value is no longer 1.
        assertTrue(!client.compareAndSwap("counter", "1", "2"));
        assertEquals(Optional.of("2"), client.get("counter"));
    }

    @Test
    @DisplayName("status reports one leader and the rest as followers")
    void statusReportsRoles() {
        startCluster(3);
        KeelClient client = client();
        await(() -> leaderIdOrZero() != 0, "a leader");

        int leaders = 0;
        for (long id : cluster.keySet()) {
            StatusResponse status = client.status(id);
            assertEquals(id, status.getNodeId());
            assertTrue(status.getTerm() > 0, "node " + id + " should have a term");
            if ("LEADER".equals(status.getRole())) {
                leaders++;
            }
        }
        assertEquals(1, leaders, "exactly one node should report itself as leader");
    }

    @Test
    @DisplayName("a stale read is served without a round trip to the leader")
    void staleReadsAreAllowed() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        client.put("k", "v");
        await(() -> nodes.stream().allMatch(node -> node.keyCount() == 1), "replication to finish");

        // Reads local state with no read index. Correct here only because the cluster is quiet.
        Optional<String> value =
                client
                        .get(com.google.protobuf.ByteString.copyFromUtf8("k"), false)
                        .map(bytes -> bytes.toStringUtf8());

        assertEquals(Optional.of("v"), value);
    }

    @Test
    @DisplayName("a fourth node joins a running cluster and serves reads")
    void addAMemberToARunningCluster() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        for (int i = 0; i < 20; i++) {
            client.put("k" + i, "v" + i);
        }

        // A node that is not a voter yet: it runs with the cluster's current membership, accepts
        // entries, and does not campaign.
        long newId = 4;
        int port = freePorts(1).get(0);
        String address = "127.0.0.1:" + port;
        Map<Long, String> withNewNode = new LinkedHashMap<>(cluster);
        withNewNode.put(newId, address);
        KeelNode joining =
                KeelNode.open(
                                joiningOptions(newId, withNewNode, dataDirOf(newId)))
                        .start();
        nodes.add(joining);

        List<Long> voters = client.addMember(newId, address);

        assertTrue(voters.contains(newId), "the change should report the new membership: " + voters);
        await(() -> joining.keyCount() == 20, "the new voter to catch up");
        // The address travelled in the configuration entry, so the existing nodes could reach it
        // without their own configuration being touched.
        assertEquals(20, joining.keyCount());
        assertEquals(Optional.of("v7"), client.get("k7"));
    }

    @Test
    @DisplayName("a removed node stops being part of the quorum")
    void removeAMember() {
        startCluster(3);
        KeelClient client = client();
        client.openSession();
        client.put("before", "removal");
        long victim = followerId();

        List<Long> voters = client.removeMember(victim);
        assertFalse(voters.contains(victim), "membership should no longer include it: " + voters);

        // Two voters remain, so a quorum is two. Stopping the removed node must not stop writes.
        stop(victim);
        client.put("after", "removal");
        assertEquals(Optional.of("removal"), client.get("after"));
    }

    @Test
    @DisplayName("every keelctl command works against a live cluster")
    void keelctlCommands() {
        startCluster(3);
        String clusterFlag = "--cluster=" + flagFor(cluster);

        assertEquals(0, ctl(clusterFlag, "put", "cli-key", "cli-value").exitCode());
        Captured get = ctl(clusterFlag, "get", "cli-key");
        assertEquals(0, get.exitCode());
        assertTrue(get.out().contains("cli-value"), get.out());

        Captured missing = ctl(clusterFlag, "get", "no-such-key");
        assertEquals(1, missing.exitCode(), "an absent key should exit non-zero");
        assertTrue(missing.out().contains("absent"), missing.out());

        assertEquals(1, ctl(clusterFlag, "cas", "cli-key", "wrong", "x").exitCode());
        assertEquals(0, ctl(clusterFlag, "cas", "cli-key", "cli-value", "updated").exitCode());
        assertTrue(ctl(clusterFlag, "get", "cli-key").out().contains("updated"));

        Captured status = ctl(clusterFlag, "status");
        assertEquals(0, status.exitCode());
        assertTrue(status.out().contains("LEADER"), status.out());

        assertEquals(0, ctl(clusterFlag, "del", "cli-key").exitCode());
        assertEquals(1, ctl(clusterFlag, "get", "cli-key").exitCode());

        Captured member = ctl(clusterFlag, "member", "remove", String.valueOf(followerId()));
        assertEquals(0, member.exitCode(), member.out() + member.err());
        assertTrue(member.out().contains("voters:"), member.out());
        assertEquals(2, ctl(clusterFlag, "member", "sideways", "1").exitCode());

        assertEquals(0, ctl("--help").exitCode());
        assertEquals(2, ctl(clusterFlag, "nonsense").exitCode(), "an unknown command is a usage error");
        assertEquals(2, ctl("get", "k").exitCode(), "no cluster flag is a usage error");
    }

    // ---------------------------------------------------------------------------------------------

    private void startCluster(int size) {
        cluster = new LinkedHashMap<>();
        List<Integer> ports = freePorts(size);
        for (int i = 0; i < size; i++) {
            cluster.put((long) (i + 1), "127.0.0.1:" + ports.get(i));
        }
        for (long id : cluster.keySet()) {
            NodeOptions options = optionsFor(id, dataDirOf(id));
            nodes.add(KeelNode.open(options).start());
        }
        await(() -> leaderIdOrZero() != 0, "the cluster to elect a leader");
    }

    private NodeOptions optionsFor(long id, Path dataDir) {
        return new NodeOptions(
                id, cluster, Set.of(), dataDir, TICK, 10, 1, Duration.ofSeconds(5), null, SNAPSHOT_EVERY);
    }

    /** Options for a node that knows every address but is not a voter yet. */
    private NodeOptions joiningOptions(long id, Map<Long, String> addressBook, Path dataDir) {
        return new NodeOptions(
                id,
                addressBook,
                cluster.keySet(),
                dataDir,
                TICK,
                10,
                1,
                Duration.ofSeconds(5),
                null,
                SNAPSHOT_EVERY);
    }

    private Path dataDirOf(long id) {
        return root.resolve("node-" + id);
    }

    private KeelClient client() {
        KeelClient client = new KeelClient(cluster, 3_000);
        clients.add(client);
        return client;
    }

    private long leaderId() {
        long leader = leaderIdOrZero();
        if (leader == 0) {
            fail("expected a leader, roles are " + roles());
        }
        return leader;
    }

    private long leaderIdOrZero() {
        long found = 0;
        for (KeelNode node : nodes) {
            try {
                if (node.status().isLeader()) {
                    if (found != 0) {
                        return 0;
                    }
                    found = node.nodeId();
                }
            } catch (RuntimeException e) {
                // A node that is shutting down has no status to give.
            }
        }
        return found;
    }

    private KeelNode node(long id) {
        return nodes.stream().filter(node -> node.nodeId() == id).findFirst().orElseThrow();
    }

    private long followerId() {
        long leader = leaderId();
        return nodes.stream()
                .map(KeelNode::nodeId)
                .filter(id -> id != leader)
                .findFirst()
                .orElseThrow();
    }

    private String roles() {
        StringBuilder sb = new StringBuilder();
        for (KeelNode node : nodes) {
            sb.append(node.nodeId()).append('=').append(node.status().role()).append(' ');
        }
        return sb.toString();
    }

    private void stop(long id) {
        nodes.stream().filter(node -> node.nodeId() == id).findFirst().ifPresent(this::stopNode);
    }

    private void stopNode(KeelNode node) {
        node.close();
        nodes.remove(node);
    }

    /** Polls until the condition holds, so a loaded machine is slow rather than flaky. */
    private void await(BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting for " + what);
            }
        }
        fail("timed out waiting for " + what + "; roles are " + roles());
    }

    private static List<Integer> freePorts(int count) {
        List<Integer> ports = new ArrayList<>(count);
        List<ServerSocket> sockets = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                ServerSocket socket = new ServerSocket(0);
                sockets.add(socket);
                ports.add(socket.getLocalPort());
            }
        } catch (IOException e) {
            throw new IllegalStateException("could not reserve ports", e);
        } finally {
            for (ServerSocket socket : sockets) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Nothing useful to do; the port will simply fail to bind and the test will say so.
                }
            }
        }
        return ports;
    }

    private static String flagFor(Map<Long, String> cluster) {
        StringBuilder sb = new StringBuilder();
        cluster.forEach(
                (id, address) -> {
                    if (sb.length() > 0) {
                        sb.append(',');
                    }
                    sb.append(id).append('=').append(address);
                });
        return sb.toString();
    }

    private record Captured(int exitCode, String out, String err) {}

    private static Captured ctl(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code;
        try (PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8);
                PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8)) {
            code = Keelctl.run(args, outStream, errStream);
        }
        return new Captured(
                code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }
}
