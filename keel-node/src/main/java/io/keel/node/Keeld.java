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
        if (flags.containsKey("help")) {
            usage();
            System.exit(0);
        }

        NodeOptions options;
        try {
            options = new NodeConfig(flags).toOptions();
        } catch (RuntimeException e) {
            System.err.println("error: " + e.getMessage());
            usage();
            System.exit(2);
            return;
        }
        long id = options.nodeId();
        Map<Long, String> cluster = options.cluster();

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

                  --config=PATH          properties file; any flag below may live in it instead,
                                         with dots for hyphens (client.token, tls.cert, ...).
                                         Flags override the file.
                  --id=N                 this node's id, which must appear in --cluster
                  --cluster=ID=HOST:PORT,...
                                         every voter, including this node
                  --data-dir=PATH        where the log lives (default data/<id>)
                  --rocksdb=PATH         keep state in RocksDB instead of on the heap
                  --tick-ms=N            logical tick in milliseconds (default 50)
                  --metrics-port=N       serve /metrics, /healthz and /readyz on this port

                security (required to listen on anything but loopback):
                  --tls-cert=PATH        PEM certificate chain this node presents
                  --tls-key=PATH         PEM private key
                  --tls-ca=PATH          PEM CA that peers and clients are verified against
                  --client-token=TOKEN   required on client calls
                  --admin-token=TOKEN    required for membership changes; without it, membership
                                         changes fall back to the client token
                  --insecure             allow a non-loopback address with no TLS and no token

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

    }
}
