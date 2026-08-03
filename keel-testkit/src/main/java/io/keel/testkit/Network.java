package io.keel.testkit;

import io.keel.raft.RaftMessage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 * A network that misbehaves on purpose, deterministically.
 *
 * <p>Messages are held in a priority queue ordered by delivery tick and then by a send sequence
 * number. The sequence number is not decoration: two messages scheduled for the same tick have to be
 * delivered in a defined order, or the run stops being reproducible from its seed.
 *
 * <p>Reachability is evaluated at delivery time rather than at send time. That is the more interesting
 * model, because it produces the case where a message is sent before a partition, arrives after it
 * heals, and lands in a cluster that has moved on.
 */
final class Network {

    private record InFlight(long deliverAt, long sequence, RaftMessage message) {}

    private static final Comparator<InFlight> ORDER =
            Comparator.comparingLong(InFlight::deliverAt).thenComparingLong(InFlight::sequence);

    private final PriorityQueue<InFlight> queue = new PriorityQueue<>(ORDER);
    private final Map<Long, Integer> group = new HashMap<>();

    private long sequence;
    private long sent;
    private long delivered;
    private long dropped;
    private long duplicated;

    /** Queues a message, possibly dropping it or scheduling a second copy. */
    void send(long now, RaftMessage message, Random random, SimConfig config) {
        sent++;
        if (random.nextDouble() < config.dropProbability()) {
            dropped++;
            return;
        }
        queue.add(new InFlight(now + latency(random, config), sequence++, message));
        if (random.nextDouble() < config.duplicateProbability()) {
            // A retransmission that was not needed. A correct implementation has to be idempotent
            // against it, and this is where that gets exercised.
            duplicated++;
            queue.add(new InFlight(now + latency(random, config), sequence++, message));
        }
    }

    private static long latency(Random random, SimConfig config) {
        int span = config.maxLatencyTicks() - config.minLatencyTicks() + 1;
        return config.minLatencyTicks() + random.nextInt(span);
    }

    /** Messages due at or before {@code now} that can still reach their destination. */
    List<RaftMessage> due(long now) {
        List<RaftMessage> out = new ArrayList<>();
        while (!queue.isEmpty() && queue.peek().deliverAt() <= now) {
            RaftMessage message = queue.poll().message();
            if (connected(message.from(), message.to())) {
                delivered++;
                out.add(message);
            } else {
                dropped++;
            }
        }
        return out;
    }

    /** Splits the cluster so that only nodes in the same group can talk. */
    void partition(List<Set<Long>> groups) {
        group.clear();
        for (int i = 0; i < groups.size(); i++) {
            for (long node : groups.get(i)) {
                group.put(node, i);
            }
        }
    }

    void heal() {
        group.clear();
    }

    boolean partitioned() {
        return !group.isEmpty();
    }

    boolean connected(long from, long to) {
        if (group.isEmpty()) {
            return true;
        }
        Integer a = group.get(from);
        Integer b = group.get(to);
        return a != null && a.equals(b);
    }

    /** Discards traffic to and from a node that has just crashed. */
    void forget(long node) {
        queue.removeIf(f -> f.message().to() == node || f.message().from() == node);
    }

    int inFlight() {
        return queue.size();
    }

    long sent() {
        return sent;
    }

    long delivered() {
        return delivered;
    }

    long dropped() {
        return dropped;
    }

    long duplicated() {
        return duplicated;
    }
}
