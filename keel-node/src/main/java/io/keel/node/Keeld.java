package io.keel.node;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The server entry point.
 *
 * <pre>
 *   keeld --id=1 --cluster=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 --data-dir=data/1
 * </pre>
 *
 * <p>There is no separate bootstrap step. A node reads its own log, learns the term and vote it had
 * before, and joins in; an empty log simply means it has never run. That keeps the first start and
 * every later start on the same path, so recovery is exercised every time rather than only after a
 * crash.
 */
public final class Keeld {

    private Keeld() {}

    public static void main(String[] args) {
        Map<String, String> flags = Flags.parse(args);
        if (flags.containsKey("help") || !flags.containsKey("id") || !flags.containsKey("cluster")) {
            usage();
            System.exit(flags.containsKey("help") ? 0 : 2);
        }

        long id = Long.parseLong(flags.get("id"));
        Map<Long, String> cluster = Flags.parseCluster(flags.get("cluster"));
        Path dataDir = Path.of(flags.getOrDefault("data-dir", "data/" + id));

        NodeOptions options = NodeOptions.of(id, cluster, dataDir);
        if (flags.containsKey("tick-ms")) {
            options = options.withTick(Duration.ofMillis(Long.parseLong(flags.get("tick-ms"))));
        }
        if (flags.containsKey("rocksdb")) {
            options = options.withRocksDb(Path.of(flags.get("rocksdb")));
        }

        KeelNode node = KeelNode.open(options).start();
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    System.out.println("shutting down node " + id);
                                    node.close();
                                },
                                "keel-shutdown"));

        System.out.printf(
                "node %d listening on %s, cluster %s%n", id, cluster.get(id), cluster.keySet());
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void usage() {
        System.out.println(
                """
                keeld: run one keel node

                  --id=N                 this node's id, which must appear in --cluster
                  --cluster=ID=HOST:PORT,...
                                         every voter, including this node
                  --data-dir=PATH        where the log lives (default data/<id>)
                  --rocksdb=PATH         keep state in RocksDB instead of on the heap
                  --tick-ms=N            logical tick in milliseconds (default 50)

                example:
                  keeld --id=1 --cluster=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 \\
                        --data-dir=data/1
                """);
    }

    /** Minimal flag parsing, so the binaries need no dependency for it. */
    static final class Flags {

        private Flags() {}

        static Map<String, String> parse(String[] args) {
            Map<String, String> flags = new LinkedHashMap<>();
            for (String arg : args) {
                if (!arg.startsWith("--")) {
                    continue;
                }
                String body = arg.substring(2);
                int equals = body.indexOf('=');
                if (equals < 0) {
                    flags.put(body, "");
                } else {
                    flags.put(body.substring(0, equals), body.substring(equals + 1));
                }
            }
            return flags;
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
}
