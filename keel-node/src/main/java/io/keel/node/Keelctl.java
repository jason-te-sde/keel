package io.keel.node;

import com.google.protobuf.ByteString;
import io.keel.proto.service.StatusResponse;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The command line client.
 *
 * <pre>
 *   keelctl --cluster=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 put greeting hello
 *   keelctl --cluster=... get greeting
 *   keelctl --cluster=... status
 * </pre>
 *
 * <p>Reads are linearizable unless {@code --stale} is passed. A fast answer that might be wrong is a
 * poor default for a tool someone is using to work out what the cluster thinks.
 *
 * <p>{@link #run} returns the exit code instead of calling {@code System.exit}, so the commands can be
 * tested against a real cluster rather than only run by hand.
 */
public final class Keelctl {

    private Keelctl() {}

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /**
     * Runs one command.
     *
     * @return 0 on success, 1 when a key is absent or a compare-and-swap was rejected, 2 for a usage
     *     error
     */
    public static int run(String[] args, PrintStream out, PrintStream err) {
        Map<String, String> flags = Keeld.Flags.parse(args);
        List<String> positional = Arrays.stream(args).filter(arg -> !arg.startsWith("--")).toList();

        if (flags.containsKey("help")) {
            usage(out);
            return 0;
        }
        if (positional.isEmpty() || !flags.containsKey("cluster")) {
            usage(err);
            return 2;
        }

        Map<Long, String> cluster;
        try {
            cluster = NodeConfig.parseCluster(flags.get("cluster"));
        } catch (RuntimeException e) {
            err.println("error: " + rootCause(e));
            return 2;
        }

        String command = positional.get(0);
        SecurityOptions security;
        try {
            security = securityFrom(flags);
        } catch (RuntimeException e) {
            err.println("error: " + rootCause(e));
            return 2;
        }
        try (KeelClient client = new KeelClient(cluster, security, 5_000)) {
            return switch (command) {
                case "get" -> get(client, positional, flags, out, err);
                case "put" -> put(client, positional, out, err);
                case "del" -> delete(client, positional, out, err);
                case "cas" -> compareAndSwap(client, positional, out, err);
                case "status" -> status(client, cluster, out);
                case "member" -> member(client, positional, out, err);
                default -> {
                    err.println("unknown command: " + command);
                    usage(err);
                    yield 2;
                }
            };
        } catch (RuntimeException e) {
            err.println("error: " + rootCause(e));
            return 1;
        }
    }

    private static int get(
            KeelClient client,
            List<String> positional,
            Map<String, String> flags,
            PrintStream out,
            PrintStream err) {
        if (positional.size() < 2) {
            err.println("usage: keelctl --cluster=... get KEY");
            return 2;
        }
        Optional<String> value =
                client
                        .get(ByteString.copyFromUtf8(positional.get(1)), !flags.containsKey("stale"))
                        .map(ByteString::toStringUtf8);
        out.println(value.orElse("(absent)"));
        return value.isPresent() ? 0 : 1;
    }

    private static int put(KeelClient client, List<String> positional, PrintStream out, PrintStream err) {
        if (positional.size() < 3) {
            err.println("usage: keelctl --cluster=... put KEY VALUE");
            return 2;
        }
        client.openSession();
        client.put(positional.get(1), positional.get(2));
        out.println("ok");
        return 0;
    }

    private static int delete(
            KeelClient client, List<String> positional, PrintStream out, PrintStream err) {
        if (positional.size() < 2) {
            err.println("usage: keelctl --cluster=... del KEY");
            return 2;
        }
        client.openSession();
        boolean found = client.delete(positional.get(1));
        out.println(found ? "deleted" : "(absent)");
        return found ? 0 : 1;
    }

    private static int compareAndSwap(
            KeelClient client, List<String> positional, PrintStream out, PrintStream err) {
        if (positional.size() < 4) {
            err.println("usage: keelctl --cluster=... cas KEY EXPECTED|- VALUE");
            return 2;
        }
        client.openSession();
        String expected = "-".equals(positional.get(2)) ? null : positional.get(2);
        boolean applied = client.compareAndSwap(positional.get(1), expected, positional.get(3));
        out.println(applied ? "applied" : "rejected");
        return applied ? 0 : 1;
    }

    private static int member(
            KeelClient client, List<String> positional, PrintStream out, PrintStream err) {
        if (positional.size() < 3) {
            err.println("usage: keelctl --cluster=... member add ID=HOST:PORT | member remove ID");
            return 2;
        }
        String action = positional.get(1);
        String argument = positional.get(2);
        switch (action) {
            case "add" -> {
                int equals = argument.indexOf('=');
                if (equals < 0) {
                    err.println("usage: keelctl --cluster=... member add ID=HOST:PORT");
                    return 2;
                }
                List<Long> voters =
                        client.addMember(
                                Long.parseLong(argument.substring(0, equals).trim()),
                                argument.substring(equals + 1).trim());
                out.println("voters: " + voters);
                return 0;
            }
            case "remove" -> {
                List<Long> voters = client.removeMember(Long.parseLong(argument.trim()));
                out.println("voters: " + voters);
                return 0;
            }
            default -> {
                err.println("unknown member action: " + action);
                return 2;
            }
        }
    }

    private static int status(KeelClient client, Map<Long, String> cluster, PrintStream out) {
        for (long id : cluster.keySet()) {
            try {
                StatusResponse status = client.status(id);
                out.printf(
                        "node %d  %-9s term=%-4d leader=%-3d commit=%-6d applied=%-6d keys=%d%n",
                        status.getNodeId(),
                        status.getRole(),
                        status.getTerm(),
                        status.getLeaderId(),
                        status.getCommitIndex(),
                        status.getAppliedIndex(),
                        status.getKeys());
            } catch (RuntimeException e) {
                out.printf("node %d  unreachable (%s)%n", id, rootCause(e));
            }
        }
        return 0;
    }

    private static SecurityOptions securityFrom(Map<String, String> flags) {
        java.nio.file.Path cert =
                flags.containsKey("tls-cert") ? java.nio.file.Path.of(flags.get("tls-cert")) : null;
        java.nio.file.Path key =
                flags.containsKey("tls-key") ? java.nio.file.Path.of(flags.get("tls-key")) : null;
        java.nio.file.Path ca =
                flags.containsKey("tls-ca") ? java.nio.file.Path.of(flags.get("tls-ca")) : null;
        return new SecurityOptions(
                cert, key, ca, flags.get("token"), flags.get("admin-token"), true);
    }

    private static String rootCause(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.toString() : cause.getMessage();
    }

    private static void usage(PrintStream out) {
        out.println(
                """
                keelctl: talk to a keel cluster

                  --cluster=ID=HOST:PORT,...   cluster to talk to (required)
                  --stale                      read local state without a read index
                  --tls-cert=PATH              PEM client certificate
                  --tls-key=PATH               PEM client private key
                  --tls-ca=PATH                PEM CA to verify the server against
                  --token=TOKEN                client token
                  --admin-token=TOKEN          token for member add and member remove

                commands:
                  get KEY                      read a key
                  put KEY VALUE                write a key
                  del KEY                      remove a key
                  cas KEY EXPECTED|- VALUE     write only if the current value matches
                                               ('-' means the key must be absent)
                  status                       per-node role, term, and progress
                  member add ID=HOST:PORT      add a voter
                  member remove ID             remove a voter

                exit status is 1 when a key is absent or a compare-and-swap is rejected.
                """);
    }
}
