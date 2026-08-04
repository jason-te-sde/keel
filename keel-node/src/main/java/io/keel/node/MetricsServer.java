package io.keel.node;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.keel.raft.Role;
import io.keel.raft.Status;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prometheus metrics and health endpoints, over the JDK's HTTP server.
 *
 * <p>Hand-written rather than pulled from a client library, because the exposition format is a few
 * lines of text and a metrics library would be the largest dependency in the project. That trade
 * would be wrong the moment histograms or exemplars were needed; it is right for counters and gauges.
 *
 * <p>The distinction between the two health endpoints is the useful part. {@code /healthz} means the
 * process is alive and should not be killed. {@code /readyz} means this node should be sent traffic,
 * which is a different question: a follower replaying a long log is perfectly healthy and will serve
 * stale reads if asked. Wiring both to the same check is how a rolling restart takes a cluster down.
 *
 * <p>Deliberately unauthenticated, and therefore deliberately on its own port. Metrics are not
 * secret, but they are not for the internet either: bind it where your scraper can reach it and your
 * users cannot.
 */
final class MetricsServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsServer.class);

    /**
     * How far behind the commit index a node may be and still call itself ready.
     *
     * <p>Zero would make a node unready during every normal burst of writes. Unbounded would let a
     * node that is thousands of entries behind claim it can serve reads.
     */
    private static final long MAX_READY_LAG = 1_000;

    private final KeelNode node;
    private final NodeMetrics metrics;
    private final HttpServer server;

    MetricsServer(KeelNode node, NodeMetrics metrics, int port) {
        this.node = node;
        this.metrics = metrics;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open the metrics port " + port, e);
        }
        server.createContext("/metrics", this::serveMetrics);
        server.createContext("/healthz", exchange -> respond(exchange, 200, "ok\n"));
        server.createContext("/readyz", this::serveReadiness);
        server.createContext("/", exchange -> respond(exchange, 404, "try /metrics, /healthz or /readyz\n"));
        server.setExecutor(null);
    }

    MetricsServer start() {
        server.start();
        LOG.info("node {} serving metrics on port {}", node.nodeId(), server.getAddress().getPort());
        return this;
    }

    int port() {
        return server.getAddress().getPort();
    }

    private void serveReadiness(HttpExchange exchange) throws IOException {
        Status status;
        try {
            status = node.status();
        } catch (RuntimeException e) {
            respond(exchange, 503, "not ready: cannot read status\n");
            return;
        }
        long lag = Math.max(status.commitIndex() - node.appliedIndex(), 0);

        if (status.leaderId() == 0) {
            // No leader means no linearizable read can be answered, whatever this node's own state is.
            respond(exchange, 503, "not ready: no leader known\n");
            return;
        }
        if (lag > MAX_READY_LAG) {
            respond(
                    exchange,
                    503,
                    "not ready: " + lag + " entries behind the commit index (limit " + MAX_READY_LAG + ")\n");
            return;
        }
        respond(exchange, 200, "ready\n");
    }

    private void serveMetrics(HttpExchange exchange) throws IOException {
        Status status;
        try {
            status = node.status();
        } catch (RuntimeException e) {
            respond(exchange, 503, "# the node is not answering\n");
            return;
        }
        NodeMetrics.Snapshot counters = metrics.snapshot();
        StringBuilder out = new StringBuilder(2048);

        // A role gauge with a label per role, rather than an integer, so a query can say
        // keel_raft_role{role="leader"} == 1 without anybody memorising which number means what.
        help(out, "keel_raft_role", "gauge", "1 for the role this node currently holds");
        for (Role role : Role.values()) {
            out.append("keel_raft_role{role=\"")
                    .append(role.name().toLowerCase(Locale.ROOT))
                    .append("\"} ")
                    .append(status.role() == role ? 1 : 0)
                    .append('\n');
        }

        gauge(out, "keel_raft_term", "current term", status.term());
        gauge(out, "keel_raft_leader_id", "leader this node knows about, 0 for none", status.leaderId());
        gauge(out, "keel_raft_commit_index", "highest index known committed", status.commitIndex());
        gauge(out, "keel_raft_applied_index", "highest index in the state machine", node.appliedIndex());
        gauge(out, "keel_raft_last_index", "highest index in the log", status.lastIndex());
        gauge(
                out,
                "keel_raft_apply_lag_entries",
                "commit index minus applied index; the queue the state machine is behind by",
                Math.max(status.commitIndex() - node.appliedIndex(), 0));
        gauge(out, "keel_raft_snapshot_index", "boundary of the newest snapshot, 0 for none", node.snapshotIndex());
        gauge(
                out,
                "keel_raft_log_entries_since_snapshot",
                "entries the log holds beyond its snapshot boundary; what compaction reclaims",
                Math.max(status.lastIndex() - node.snapshotIndex(), 0));
        gauge(out, "keel_raft_voters", "size of the current membership", status.voters().size());
        gauge(out, "keel_kv_keys", "keys in the state machine", node.keyCount());

        counter(out, "keel_writes_accepted_total", "writes that entered the log", counters.writesAccepted());
        counter(out, "keel_writes_rejected_total", "writes refused before entering the log", counters.writesRejected());
        counter(
                out,
                "keel_writes_overwritten_total",
                "writes whose index was taken by another command, so they never committed",
                counters.writesOverwritten());
        counter(out, "keel_reads_served_total", "reads answered", counters.readsServed());
        counter(out, "keel_reads_failed_total", "reads that could not be answered", counters.readsFailed());
        counter(out, "keel_entries_applied_total", "entries handed to the state machine", counters.entriesApplied());
        counter(out, "keel_snapshots_taken_total", "snapshots this node wrote", counters.snapshotsTaken());
        counter(out, "keel_snapshots_installed_total", "snapshots this node received", counters.snapshotsInstalled());
        counter(out, "keel_snapshots_sent_total", "snapshots this node sent to a follower", counters.snapshotsSent());
        counter(
                out,
                "keel_snapshot_send_failures_total",
                "snapshot transfers that failed",
                counters.snapshotSendFailures());
        counter(out, "keel_membership_changes_total", "applied configuration changes", counters.membershipChanges());
        counter(
                out,
                "keel_peer_send_failures_total",
                "messages that could not be delivered to a peer",
                counters.peerSendFailures());

        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        respond(exchange, 200, out.toString());
    }

    private static void gauge(StringBuilder out, String name, String description, long value) {
        help(out, name, "gauge", description);
        out.append(name).append(' ').append(value).append('\n');
    }

    private static void counter(StringBuilder out, String name, String description, long value) {
        help(out, name, "counter", description);
        out.append(name).append(' ').append(value).append('\n');
    }

    private static void help(StringBuilder out, String name, String type, String description) {
        out.append("# HELP ").append(name).append(' ').append(description).append('\n');
        out.append("# TYPE ").append(name).append(' ').append(type).append('\n');
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
