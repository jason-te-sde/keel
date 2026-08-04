package io.keel.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The backup procedure from {@code docs/operations.md}, run rather than described.
 *
 * <p>A documented recovery procedure that nobody has executed is a guess. The two claims tested here
 * are the ones an operator relies on: a copy of the data directory is a complete backup, and a data
 * directory whose snapshots have been lost fails loudly instead of coming up with a hole in it.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class BackupRestoreTest {

    private static final Duration TICK = Duration.ofMillis(20);
    private static final int SNAPSHOT_EVERY = 8;

    @TempDir Path root;

    private final List<KeelNode> nodes = new ArrayList<>();
    private Map<Long, String> cluster;

    @AfterEach
    void tearDown() {
        nodes.forEach(KeelNode::close);
    }

    @Test
    @DisplayName("a copy of the data directory restores the whole key space")
    void dataDirectoryIsACompleteBackup() throws IOException {
        // Enough writes to force compaction, so the backup has to include both the snapshots and the
        // log after them. A backup of only one would look fine and be missing data.
        KeelNode node = startSingleNode();
        KeelClient client = new KeelClient(cluster, 3_000);
        try {
            client.openSession();
            for (int i = 0; i < 40; i++) {
                client.put("k" + i, "v" + i);
            }
            await(() -> node.snapshotIndex() > 0, "a snapshot to be taken");
        } finally {
            client.close();
        }
        node.close();
        nodes.clear();

        Path backup = root.resolve("backup");
        copyTree(root.resolve("node-1"), backup);
        assertTrue(Files.exists(backup.resolve("wal")), "a backup needs the log");
        assertTrue(Files.exists(backup.resolve("snapshots")), "and the snapshots");

        // Total loss, then restore.
        deleteTree(root.resolve("node-1"));
        copyTree(backup, root.resolve("node-1"));
        KeelNode restored = start(1);

        KeelClient after = new KeelClient(cluster, 3_000);
        try {
            await(() -> restored.status().leaderId() != 0, "the restored node to lead");
            assertEquals(40, restored.keyCount(), "every key should be back");
            // Reads a key from below the snapshot boundary, which can only come from the snapshot,
            // and one from above it, which can only come from the log.
            assertEquals(Optional.of("v0"), after.get("k0"));
            assertEquals(Optional.of("v39"), after.get("k39"));
        } finally {
            after.close();
        }
    }

    @Test
    @DisplayName("a partial restore is refused rather than started with a hole in it")
    void partialRestoreIsRefused() throws IOException {
        // The mistake an operator actually makes: backing up or restoring the log without the
        // snapshots. The log's prefix has been compacted away, so those entries exist nowhere.
        KeelNode node = startSingleNode();
        KeelClient client = new KeelClient(cluster, 3_000);
        try {
            client.openSession();
            for (int i = 0; i < 40; i++) {
                client.put("k" + i, "v" + i);
            }
            await(() -> node.snapshotIndex() > 0, "a snapshot to be taken");
        } finally {
            client.close();
        }
        node.close();
        nodes.clear();

        deleteTree(root.resolve("node-1").resolve("snapshots"));

        IllegalStateException e =
                assertThrows(IllegalStateException.class, () -> KeelNode.open(optionsFor(1)));
        assertTrue(e.getMessage().contains("no snapshot covering it"), e.getMessage());
        // Naming the boundary matters: it tells the operator which snapshot they needed.
        assertTrue(e.getMessage().contains("compacted to index"), e.getMessage());
    }

    // ---------------------------------------------------------------------------------------------

    private KeelNode startSingleNode() {
        cluster = Map.of(1L, "127.0.0.1:" + freePort());
        KeelNode node = start(1);
        // A single node still has to time out and campaign before it leads, and the client's retry
        // budget is not the right place to absorb that.
        await(() -> node.status().leaderId() != 0, "the node to elect itself");
        return node;
    }

    private KeelNode start(long id) {
        KeelNode node = KeelNode.open(optionsFor(id)).start();
        nodes.add(node);
        return node;
    }

    private NodeOptions optionsFor(long id) {
        return new NodeOptions(
                id,
                cluster,
                Set.of(),
                root.resolve("node-" + id),
                TICK,
                10,
                1,
                Duration.ofSeconds(5),
                null,
                SNAPSHOT_EVERY,
                0,
                SecurityOptions.none());
    }

    private static void copyTree(Path from, Path to) throws IOException {
        try (Stream<Path> paths = Files.walk(from)) {
            for (Path path : paths.toList()) {
                Path target = to.resolve(from.relativize(path).toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

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
                fail("interrupted waiting for " + what);
            }
        }
        fail("timed out waiting for " + what);
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("could not reserve a port", e);
        }
    }
}
