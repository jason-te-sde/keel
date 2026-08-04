package io.keel.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Configuration resolution.
 *
 * <p>The precedence rule is the thing worth testing: a file holds what a deployment shares and flags
 * hold what differs per node. Getting the order backwards would mean rebuilding a container image to
 * change a port, and it would fail silently rather than loudly.
 */
class NodeConfigTest {

    @TempDir Path dir;

    @Test
    @DisplayName("options come from a config file alone")
    void fromFileAlone() throws IOException {
        Path file = write("""
                id = 2
                cluster = 1=10.0.0.1:9001,2=10.0.0.2:9001,3=10.0.0.3:9001
                data.dir = /var/lib/keel
                tick.ms = 25
                metrics.port = 9101
                snapshot.threshold = 4096
                client.token = from-the-file
                insecure = true
                """);

        NodeOptions options = new NodeConfig(Map.of("config", file.toString())).toOptions();

        assertEquals(2, options.nodeId());
        assertEquals(3, options.cluster().size());
        assertEquals(Path.of("/var/lib/keel"), options.dataDir());
        assertEquals(Duration.ofMillis(25), options.tick());
        assertEquals(9101, options.metricsPort());
        assertEquals(4096, options.snapshotThresholdEntries());
        assertEquals("from-the-file", options.security().clientToken());
        assertTrue(options.security().insecure());
    }

    @Test
    @DisplayName("a flag overrides the same value from the file")
    void flagsWin() throws IOException {
        // The whole point of the ordering: an image ships the file, and the values that differ per
        // node arrive as flags.
        Path file = write("""
                id = 1
                cluster = 1=127.0.0.1:9001
                metrics.port = 9101
                client.token = from-the-file
                """);

        NodeOptions options =
                new NodeConfig(
                                Map.of(
                                        "config", file.toString(),
                                        "metrics-port", "9999",
                                        "client-token", "from-the-flag"))
                        .toOptions();

        assertEquals(9999, options.metricsPort());
        assertEquals("from-the-flag", options.security().clientToken());
    }

    @Test
    @DisplayName("a hyphenated flag maps to a dotted file key")
    void namesTranslate() throws IOException {
        // Hyphens are what a command line expects and dots are what a properties file expects.
        // Translating is cheaper than making an operator remember which convention applies where.
        Path file = write("""
                id = 1
                cluster = 1=127.0.0.1:9001
                admin.token = secret
                """);

        NodeOptions options = new NodeConfig(Map.of("config", file.toString())).toOptions();

        assertEquals("secret", options.security().adminToken());
    }

    @Test
    @DisplayName("flags alone still work, with no file at all")
    void flagsAlone() {
        NodeOptions options =
                new NodeConfig(Map.of("id", "1", "cluster", "1=127.0.0.1:9001")).toOptions();

        assertEquals(1, options.nodeId());
        assertEquals(Path.of("data/1"), options.dataDir());
        assertEquals(0, options.metricsPort(), "metrics stay off unless asked for");
        assertNull(options.security().clientToken());
        assertFalse(options.security().insecure());
    }

    @Test
    @DisplayName("a missing required value names itself")
    void missingRequiredValue() {
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new NodeConfig(Map.of("id", "1")).toOptions());

        assertTrue(e.getMessage().contains("cluster"), e.getMessage());
    }

    @Test
    @DisplayName("a non-numeric value says which key was wrong")
    void badNumber() throws IOException {
        Path file = write("""
                id = 1
                cluster = 1=127.0.0.1:9001
                tick.ms = soon
                """);

        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new NodeConfig(Map.of("config", file.toString())).toOptions());

        assertTrue(e.getMessage().contains("tick-ms"), e.getMessage());
        assertTrue(e.getMessage().contains("soon"), e.getMessage());
    }

    @Test
    @DisplayName("a malformed cluster entry explains the shape it wanted")
    void badCluster() {
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> NodeConfig.parseCluster("1=127.0.0.1:9001,nonsense"));

        assertTrue(e.getMessage().contains("ID=HOST:PORT"), e.getMessage());
    }

    @Test
    @DisplayName("a missing config file fails immediately rather than starting with defaults")
    void missingFile() {
        assertThrows(
                java.io.UncheckedIOException.class,
                () -> new NodeConfig(Map.of("config", dir.resolve("absent.properties").toString())));
    }

    private Path write(String contents) throws IOException {
        Path file = dir.resolve("keel.properties");
        Files.writeString(file, contents);
        return file;
    }
}
