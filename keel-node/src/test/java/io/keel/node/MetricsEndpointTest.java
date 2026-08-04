package io.keel.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The endpoints an operator would actually wire up.
 *
 * <p>The readiness test is the one worth having. Liveness that returns 200 whenever the process is up
 * is hard to get wrong; readiness that returns 200 when the node cannot serve a read is how a rolling
 * restart takes a cluster down, and it looks identical from the outside.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class MetricsEndpointTest {

    private static final Duration TICK = Duration.ofMillis(20);

    @TempDir Path root;

    private final List<KeelNode> nodes = new ArrayList<>();
    private final HttpClient http = HttpClient.newHttpClient();
    private Map<Long, String> cluster;

    @AfterEach
    void tearDown() {
        nodes.forEach(KeelNode::close);
    }

    @Test
    @DisplayName("metrics are exposed in Prometheus text format")
    void metricsAreScrapeable() {
        startCluster(3);
        long leader = leaderId();
        KeelClient client = new KeelClient(cluster, 3_000);
        try {
            client.openSession();
            client.put("k", "v");
        } finally {
            client.close();
        }

        String body = get(node(leader).metricsPort(), "/metrics").body();

        // Every metric must carry its type, or a scraper treats a counter as a gauge and every rate
        // query is quietly wrong.
        assertTrue(body.contains("# TYPE keel_raft_term gauge"), body);
        assertTrue(body.contains("# TYPE keel_writes_accepted_total counter"), body);
        // The role is a labelled gauge so a query can ask for the leader without knowing that some
        // integer means leader.
        assertTrue(body.contains("keel_raft_role{role=\"leader\"} 1"), body);
        assertTrue(body.contains("keel_raft_role{role=\"follower\"} 0"), body);
        assertTrue(body.contains("keel_kv_keys 1"), body);
        assertTrue(parse(body, "keel_writes_accepted_total") >= 1, body);
        assertTrue(parse(body, "keel_entries_applied_total") >= 1, body);
        assertTrue(parse(body, "keel_raft_commit_index") >= 1, body);
    }

    @Test
    @DisplayName("a follower reports the leader and its own lag")
    void followerMetrics() {
        startCluster(3);

        // One scrape of a live cluster is a race. Heartbeats arrive late on a loaded machine, and a
        // follower that has just started its own election is telling the truth when it reports itself
        // a candidate with no leader. Asserting on a single scrape made this the flakiest test in the
        // suite: it was the sole failure on most of the dependency bumps sitting in review, which made
        // every one of them look like a real regression.
        //
        // Waiting for one internally consistent scrape keeps the assertions about what a follower
        // reports rather than about how quickly the cluster happens to settle.
        String body = followerScrape();

        assertTrue(body.contains("keel_raft_role{role=\"follower\"} 1"), body);
        assertEquals(leaderId(), parse(body, "keel_raft_leader_id"), body);
        assertTrue(parse(body, "keel_raft_apply_lag_entries") >= 0, body);
    }

    /**
     * Scrapes a follower until one response describes a follower that names the same leader the cluster
     * does, and returns that body.
     *
     * <p>The leader is re-read on every attempt on purpose. A leader change during the wait is normal
     * behaviour, not a reason to fail, so the condition is about a single response being self
     * consistent rather than about the cluster matching what it looked like when the test started.
     */
    private String followerScrape() {
        StringBuilder last = new StringBuilder("no scrape completed");
        await(
                () -> {
                    long leader = leaderIdOrZero();
                    if (leader == 0) {
                        return false;
                    }
                    long follower =
                            nodes.stream()
                                    .map(KeelNode::nodeId)
                                    .filter(id -> id != leader)
                                    .findFirst()
                                    .orElseThrow();
                    String body = get(node(follower).metricsPort(), "/metrics").body();
                    last.setLength(0);
                    last.append(body);
                    return body.contains("keel_raft_role{role=\"follower\"} 1")
                            && parse(body, "keel_raft_leader_id") == leader;
                },
                () -> "a follower that agrees with the cluster about the leader; last scrape was:\n" + last);
        return last.toString();
    }

    @Test
    @DisplayName("liveness and readiness answer different questions")
    void livenessAndReadinessDiffer() {
        // One node of a three node cluster: the process is fine, and it cannot serve a read because no
        // leader can be elected. Wiring both checks to the same thing hides exactly this state.
        cluster = new LinkedHashMap<>();
        List<Integer> ports = freePorts(6);
        for (int i = 0; i < 3; i++) {
            cluster.put((long) (i + 1), "127.0.0.1:" + ports.get(i));
        }
        KeelNode lonely = start(1, ports.get(3));

        assertEquals(200, get(lonely.metricsPort(), "/healthz").statusCode(), "the process is alive");

        HttpResponse<String> ready = get(lonely.metricsPort(), "/readyz");
        assertEquals(503, ready.statusCode(), "it cannot serve a read, so it is not ready");
        assertTrue(ready.body().contains("no leader"), ready.body());

        // Bring the rest of the cluster up and it becomes ready without anything else changing.
        start(2, ports.get(4));
        start(3, ports.get(5));
        await(() -> get(lonely.metricsPort(), "/readyz").statusCode() == 200, "the node to become ready");
    }

    @Test
    @DisplayName("an unknown path is a 404 that says where to look")
    void unknownPath() {
        startCluster(1);
        HttpResponse<String> response = get(nodes.get(0).metricsPort(), "/nope");

        assertEquals(404, response.statusCode());
        assertTrue(response.body().contains("/metrics"), response.body());
    }

    @Test
    @DisplayName("the endpoints are off unless a port is configured")
    void disabledByDefault() {
        // A fixed default port would collide the moment three nodes run on one host, which is what the
        // local cluster script does.
        cluster = Map.of(1L, "127.0.0.1:" + freePorts(1).get(0));
        KeelNode node =
                KeelNode.open(
                                new NodeOptions(
                                        1,
                                        cluster,
                                        Set.of(),
                                        root.resolve("node-1"),
                                        TICK,
                                        10,
                                        1,
                                        Duration.ofSeconds(5),
                                        null,
                                        16,
                                        0,
                                        SecurityOptions.none()))
                        .start();
        nodes.add(node);

        assertEquals(0, node.metricsPort());
    }

    // ---------------------------------------------------------------------------------------------

    private void startCluster(int size) {
        cluster = new LinkedHashMap<>();
        List<Integer> ports = freePorts(size * 2);
        for (int i = 0; i < size; i++) {
            cluster.put((long) (i + 1), "127.0.0.1:" + ports.get(i));
        }
        for (int i = 0; i < size; i++) {
            start(i + 1, ports.get(size + i));
        }
        await(() -> leaderIdOrZero() != 0, "a leader");
    }

    private KeelNode start(long id, int metricsPort) {
        KeelNode node =
                KeelNode.open(
                                new NodeOptions(
                                        id,
                                        cluster,
                                        Set.of(),
                                        root.resolve("node-" + id),
                                        TICK,
                                        10,
                                        1,
                                        Duration.ofSeconds(5),
                                        null,
                                        16,
                                        metricsPort,
                                        SecurityOptions.none()))
                        .start();
        nodes.add(node);
        return node;
    }

    private HttpResponse<String> get(int port, String path) {
        try {
            return http.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                            .timeout(Duration.ofSeconds(5))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("failed to scrape " + path + " on " + port, e);
        }
    }

    /** Value of a metric line, for assertions that care about the number rather than its presence. */
    private static long parse(String body, String metric) {
        for (String line : body.split("\n")) {
            if (line.startsWith(metric + " ")) {
                return Long.parseLong(line.substring(metric.length() + 1).trim());
            }
        }
        throw new AssertionError("no metric named " + metric + " in:\n" + body);
    }

    private KeelNode node(long id) {
        return nodes.stream().filter(n -> n.nodeId() == id).findFirst().orElseThrow();
    }

    private long leaderId() {
        long leader = leaderIdOrZero();
        if (leader == 0) {
            fail("expected a leader");
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
                // Still starting.
            }
        }
        return found;
    }

    private void await(BooleanSupplier condition, String what) {
        await(condition, () -> what);
    }

    /**
     * Same, with the description built only if the wait fails.
     *
     * <p>Worth the overload because the useful description here is the last response received, which
     * does not exist yet when the wait starts.
     */
    private void await(BooleanSupplier condition, Supplier<String> what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted waiting for " + what.get());
            }
        }
        fail("timed out waiting for " + what.get());
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
                    // The bind will fail and the test will say so.
                }
            }
        }
        return ports;
    }
}
