package io.keel.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ServerSocket;
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
 * A cluster over mutual TLS, with tokens.
 *
 * <p>The tests that matter here are the refusals. Encryption is easy to verify by accident — if the
 * handshake fails nothing works at all — so the interesting cases are a peer whose certificate comes
 * from the wrong authority, a client with no token, and a client with a token that is valid for
 * reading but not for reconfiguring the cluster.
 *
 * <p>Certificates are committed under {@code src/test/resources/certs} and are worthless as secrets;
 * their README says so.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class SecureClusterTest {

    private static final Duration TICK = Duration.ofMillis(20);
    private static final String CLIENT_TOKEN = "client-token-for-tests";
    private static final String ADMIN_TOKEN = "admin-token-for-tests";

    private static final Path CERTS = Path.of("src", "test", "resources", "certs");

    @TempDir Path root;

    private final List<KeelNode> nodes = new ArrayList<>();
    private final List<KeelClient> clients = new ArrayList<>();
    private Map<Long, String> cluster;

    @AfterEach
    void tearDown() {
        clients.forEach(KeelClient::close);
        nodes.forEach(KeelNode::close);
    }

    private static SecurityOptions serverSecurity() {
        return new SecurityOptions(
                CERTS.resolve("node.pem"),
                CERTS.resolve("node.key"),
                CERTS.resolve("ca.pem"),
                CLIENT_TOKEN,
                ADMIN_TOKEN,
                false);
    }

    private static SecurityOptions clientSecurity(String token, String adminToken) {
        return new SecurityOptions(
                CERTS.resolve("client.pem"),
                CERTS.resolve("client.key"),
                CERTS.resolve("ca.pem"),
                token,
                adminToken,
                true);
    }

    @Test
    @DisplayName("a cluster runs over mutual TLS and serves reads and writes")
    void mutualTlsCluster() {
        startCluster(3, serverSecurity());
        KeelClient client = client(clientSecurity(CLIENT_TOKEN, ADMIN_TOKEN));
        client.openSession();

        client.put("secure", "yes");

        assertEquals(Optional.of("yes"), client.get("secure"));
        await(() -> nodes.stream().allMatch(node -> node.keyCount() == 1), "replication over TLS");
    }

    @Test
    @DisplayName("a client with no token is refused")
    void tokenIsRequired() {
        startCluster(3, serverSecurity());
        KeelClient anonymous = client(clientSecurity(null, null));

        IllegalStateException e =
                assertThrows(IllegalStateException.class, () -> anonymous.put("nope", "nope"));

        // Refused once, not retried around the cluster: a bad credential is not transient.
        assertTrue(e.getMessage().toLowerCase().contains("authentication"), e.getMessage());
    }

    @Test
    @DisplayName("a client with the wrong token is refused")
    void wrongTokenIsRefused() {
        startCluster(3, serverSecurity());
        KeelClient impostor = client(clientSecurity("not-the-token", null));

        assertThrows(IllegalStateException.class, () -> impostor.get("anything"));
    }

    @Test
    @DisplayName("a valid client token cannot change the membership")
    void membershipNeedsTheAdminToken() {
        // The split that makes two tokens worth having. This client can read and write, and must not
        // be able to eject a node from the cluster.
        startCluster(3, serverSecurity());
        KeelClient readWrite = client(clientSecurity(CLIENT_TOKEN, null));
        readWrite.openSession();
        readWrite.put("allowed", "yes");

        long victim = followerId();
        IllegalStateException e =
                assertThrows(IllegalStateException.class, () -> readWrite.removeMember(victim));
        assertTrue(e.getMessage().toLowerCase().contains("authentication"), e.getMessage());

        // Still three voters, and the node is still there.
        assertEquals(3, nodes.size());
        assertEquals(Optional.of("yes"), readWrite.get("allowed"));

        // With the admin token, the same call works.
        KeelClient admin = client(clientSecurity(CLIENT_TOKEN, ADMIN_TOKEN));
        assertTrue(admin.removeMember(victim).size() == 2);
    }

    @Test
    @DisplayName("a peer whose certificate comes from another CA cannot join")
    void foreignCertificateCannotJoin() {
        // The certificate is valid, correctly formed, and signed by an authority this cluster does not
        // trust. That is what makes the cluster CA a membership boundary rather than decoration.
        startCluster(3, serverSecurity());
        KeelClient client = client(clientSecurity(CLIENT_TOKEN, ADMIN_TOKEN));
        client.openSession();
        client.put("before", "stranger");

        SecurityOptions stranger =
                new SecurityOptions(
                        CERTS.resolve("stranger.pem"),
                        CERTS.resolve("stranger.key"),
                        CERTS.resolve("other-ca.pem"),
                        CLIENT_TOKEN,
                        ADMIN_TOKEN,
                        false);
        long newId = 4;
        String address = "127.0.0.1:" + freePorts(1).get(0);
        Map<Long, String> addressBook = new LinkedHashMap<>(cluster);
        addressBook.put(newId, address);
        KeelNode outsider =
                KeelNode.open(
                                new NodeOptions(
                                        newId,
                                        addressBook,
                                        cluster.keySet(),
                                        root.resolve("stranger"),
                                        TICK,
                                        10,
                                        1,
                                        Duration.ofSeconds(5),
                                        null,
                                        16,
                                        0,
                                        stranger))
                        .start();
        nodes.add(outsider);

        // It is running and reachable, and it can never take part: the handshake fails before a single
        // Raft message is parsed, so it never learns of a leader.
        boolean joined =
                pollFor(() -> outsider.status().leaderId() != 0, Duration.ofSeconds(6));
        assertTrue(!joined, "a node with a foreign certificate must not be able to join the cluster");
        assertEquals(0, outsider.keyCount());
    }

    @Test
    @DisplayName("a node refuses to listen on a real address without TLS or a token")
    void refusesInsecureNonLoopback() {
        // Secure by default. The check is on the configuration rather than on a successful bind, so
        // the failure is immediate and names the missing piece.
        SecurityOptions nothing = SecurityOptions.none();

        IllegalStateException e =
                assertThrows(IllegalStateException.class, () -> nothing.checkUsableOn("10.0.0.7"));
        assertTrue(e.getMessage().contains("TLS"), e.getMessage());
        assertTrue(e.getMessage().contains("--insecure"), "the message should say how to override it");

        // Loopback is fine, because a laptop should stay one command.
        nothing.checkUsableOn("127.0.0.1");
        nothing.checkUsableOn("localhost");
        // And so is an explicit opt-out.
        SecurityOptions.insecureAnywhere().checkUsableOn("10.0.0.7");
    }

    @Test
    @DisplayName("TLS without a trusted CA is refused as a configuration error")
    void tlsRequiresMutualAuthentication() {
        // Server-only TLS would encrypt the peer protocol while letting anyone who can reach the port
        // take part in it.
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new SecurityOptions(
                                        CERTS.resolve("node.pem"),
                                        CERTS.resolve("node.key"),
                                        null,
                                        null,
                                        null,
                                        false));
        assertTrue(e.getMessage().contains("trusted CA"), e.getMessage());
    }

    // ---------------------------------------------------------------------------------------------

    private void startCluster(int size, SecurityOptions security) {
        cluster = new LinkedHashMap<>();
        List<Integer> ports = freePorts(size);
        for (int i = 0; i < size; i++) {
            cluster.put((long) (i + 1), "127.0.0.1:" + ports.get(i));
        }
        for (long id : cluster.keySet()) {
            nodes.add(
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
                                            0,
                                            security))
                            .start());
        }
        await(() -> leaderIdOrZero() != 0, "a leader over TLS");
    }

    private KeelClient client(SecurityOptions security) {
        KeelClient client = new KeelClient(cluster, security, 3_000);
        clients.add(client);
        return client;
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
                // A node still starting has no status to give.
            }
        }
        return found;
    }

    private long followerId() {
        long leader = leaderIdOrZero();
        return nodes.stream()
                .map(KeelNode::nodeId)
                .filter(id -> id != leader)
                .findFirst()
                .orElseThrow();
    }

    private void await(BooleanSupplier condition, String what) {
        if (!pollFor(condition, Duration.ofSeconds(30))) {
            fail("timed out waiting for " + what);
        }
    }

    private static boolean pollFor(BooleanSupplier condition, Duration limit) {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
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
                    // The port will simply fail to bind and the test will say so.
                }
            }
        }
        return ports;
    }
}
