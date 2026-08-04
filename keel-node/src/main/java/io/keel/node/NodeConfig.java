package io.keel.node;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Resolves node options from a properties file and command line flags.
 *
 * <p>Flags win over the file. That order is what makes a container image useful: the image ships a
 * file with everything a deployment shares, and the one or two values that differ per node arrive as
 * flags or environment variables. The reverse order would mean rebuilding an image to change a port.
 *
 * <p>Flags alone were fine for a demo and wrong for anything a supervisor manages, because a service
 * definition with fifteen flags on one line is a service definition nobody edits correctly.
 */
final class NodeConfig {

    private final Map<String, String> flags;
    private final Properties file = new Properties();

    NodeConfig(Map<String, String> flags) {
        this.flags = flags;
        String path = flags.get("config");
        if (path != null) {
            load(Path.of(path));
        }
    }

    private void load(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            file.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read the config file " + path, e);
        }
    }

    /**
     * A value by its flag name, falling back to the file.
     *
     * <p>Flag {@code --client-token} reads file key {@code client.token}: hyphens on a command line and
     * dots in a properties file are what each convention expects, and translating is cheaper than
     * asking an operator to remember which is which.
     */
    String get(String name) {
        String fromFlag = flags.get(name);
        if (fromFlag != null && !fromFlag.isEmpty()) {
            return fromFlag;
        }
        String value = file.getProperty(name.replace('-', '.'));
        return value == null || value.isBlank() ? null : value.trim();
    }

    String get(String name, String fallback) {
        String value = get(name);
        return value == null ? fallback : value;
    }

    boolean flag(String name) {
        if (flags.containsKey(name)) {
            return true;
        }
        return Boolean.parseBoolean(file.getProperty(name.replace('-', '.'), "false"));
    }

    boolean has(String name) {
        return get(name) != null;
    }

    int getInt(String name, int fallback) {
        String value = get(name);
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " should be a number, got '" + value + "'", e);
        }
    }

    long getLong(String name, long fallback) {
        String value = get(name);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " should be a number, got '" + value + "'", e);
        }
    }

    Path getPath(String name) {
        String value = get(name);
        return value == null ? null : Path.of(value);
    }

    /** Builds the options for this node, or explains what is missing. */
    NodeOptions toOptions() {
        if (!has("id") || !has("cluster")) {
            throw new IllegalArgumentException(
                    "both id and cluster are required, from flags or from a config file");
        }
        long id = getLong("id", 0);
        Map<Long, String> cluster = parseCluster(get("cluster"));
        Path dataDir = Path.of(get("data-dir", "data/" + id));

        NodeOptions options = NodeOptions.of(id, cluster, dataDir);
        if (has("tick-ms")) {
            options = options.withTick(Duration.ofMillis(getLong("tick-ms", 50)));
        }
        if (has("rocksdb")) {
            options = options.withRocksDb(getPath("rocksdb"));
        }
        if (has("metrics-port")) {
            options = options.withMetricsPort(getInt("metrics-port", 0));
        }
        if (has("snapshot-threshold")) {
            options = options.withSnapshotThreshold(getInt("snapshot-threshold", 8192));
        }
        return options.withSecurity(
                new SecurityOptions(
                        getPath("tls-cert"),
                        getPath("tls-key"),
                        getPath("tls-ca"),
                        get("client-token"),
                        get("admin-token"),
                        flag("insecure")));
    }

    static Map<Long, String> parseCluster(String spec) {
        Map<Long, String> cluster = new LinkedHashMap<>();
        for (String member : spec.split(",")) {
            String trimmed = member.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int equals = trimmed.indexOf('=');
            if (equals < 0) {
                throw new IllegalArgumentException(
                        "cluster member '" + trimmed + "' should look like ID=HOST:PORT");
            }
            cluster.put(
                    Long.parseLong(trimmed.substring(0, equals).trim()),
                    trimmed.substring(equals + 1).trim());
        }
        if (cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is empty");
        }
        return cluster;
    }
}
