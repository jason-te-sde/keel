package io.keel.testkit;

import com.google.protobuf.ByteString;
import io.keel.kv.Commands;
import io.keel.raft.ReadState;
import io.keel.testkit.linz.KvModel;
import io.keel.testkit.linz.Op;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;

/**
 * Clients issuing reads and writes against a {@link Sim}, recording what they observed.
 *
 * <p>The output is a history that {@link io.keel.testkit.linz.Linearizability} can check, which is a
 * different question from the one the invariants answer. Invariants confirm replicas agree with each
 * other; a history confirms that what clients were told could have happened at all.
 *
 * <p>Each client keeps one operation outstanding, which is what a session-based client does anyway
 * since only the newest response is remembered. Timestamps are simulated ticks, so the ordering
 * information in the history is exact rather than approximate.
 *
 * <p>Reads go through the real path: ask for a read index, wait for a quorum-confirmed answer, wait for
 * the local state machine to reach it, then read. Skipping any of those steps is how a store produces
 * a stale read, and {@code SimLinearizabilityTest} does exactly that on purpose to show the checker
 * catches it.
 */
public final class Workload {

    /**
     * @param clients number of concurrent clients
     * @param keys size of the key space; fewer keys means more contention per key and a stricter check
     * @param readFraction share of operations that are reads
     * @param timeoutTicks how long a client waits before giving up and recording an unknown outcome
     */
    public record Config(int clients, int keys, double readFraction, int timeoutTicks) {

        public static Config defaults() {
            return new Config(4, 6, 0.5, 60);
        }
    }

    /** Whether a read waits for the index it was given, or ignores it. */
    public enum ReadMode {
        /** The correct path: wait for a confirmed read index and for the state machine to reach it. */
        LINEARIZABLE,
        /**
         * Read local state immediately, ignoring the read index entirely.
         *
         * <p>Deliberately broken, and only used to prove the checker rejects the histories a real
         * stale-read bug produces.
         */
        IGNORE_READ_INDEX
    }

    private sealed interface Pending {
        record Write(String key, ByteString value, long index, long node, long invoked) implements Pending {}

        record Read(String key, long requestId, long node, long invoked, long readIndex) implements Pending {}
    }

    private final Sim sim;
    private final Config config;
    private final Random random;
    private final ReadMode readMode;

    private final Map<Integer, Pending> inFlight = new HashMap<>();
    private final Map<Long, Long> grantedReads = new HashMap<>();
    private final List<Op<KvModel.In, KvModel.Out>> history = new ArrayList<>();

    private int nextOpId = 1;
    private long nextRequestId = 1;
    private int writeCounter;

    public Workload(Sim sim, Config config, long seed) {
        this(sim, config, seed, ReadMode.LINEARIZABLE);
    }

    public Workload(Sim sim, Config config, long seed, ReadMode readMode) {
        this.sim = sim;
        this.config = config;
        this.random = new Random(seed);
        this.readMode = readMode;
    }

    /** Runs the workload for {@code ticks} simulated ticks and returns the recorded history. */
    public List<Op<KvModel.In, KvModel.Out>> run(int ticks) {
        for (int i = 0; i < ticks; i++) {
            collectGrantedReads();
            for (int client = 0; client < config.clients(); client++) {
                advance(client);
            }
            sim.step();
        }
        // Anything still outstanding when the run ends has an outcome its client never learned.
        for (Map.Entry<Integer, Pending> entry : inFlight.entrySet()) {
            recordUnknown(entry.getValue());
        }
        inFlight.clear();
        return List.copyOf(history);
    }

    private void collectGrantedReads() {
        for (long id : sim.ids()) {
            if (!sim.isDown(id)) {
                for (ReadState state : sim.drainReads(id)) {
                    grantedReads.put(state.requestId(), state.readIndex());
                }
            }
        }
    }

    private void advance(int client) {
        Pending pending = inFlight.get(client);
        if (pending == null) {
            start(client);
            return;
        }
        switch (pending) {
            case Pending.Write write -> advanceWrite(client, write);
            case Pending.Read read -> advanceRead(client, read);
        }
    }

    private void start(int client) {
        String key = "key-" + random.nextInt(config.keys());
        if (random.nextDouble() < config.readFraction()) {
            startRead(client, key);
        } else {
            startWrite(client, key);
        }
    }

    private void startWrite(int client, String key) {
        OptionalLong leader = sim.leader();
        if (leader.isEmpty()) {
            return;
        }
        ByteString value = Commands.utf8("v" + (writeCounter++));
        long index = sim.propose(Commands.put(Commands.NO_SESSION, Commands.utf8(key), value));
        if (index < 0) {
            return;
        }
        inFlight.put(client, new Pending.Write(key, value, index, leader.getAsLong(), sim.tick()));
    }

    private void advanceWrite(int client, Pending.Write write) {
        Optional<ByteString> applied = sim.appliedDataAt(write.node(), write.index());
        if (applied.isPresent()) {
            ByteString expected = Commands.put(Commands.NO_SESSION, Commands.utf8(write.key()), write.value());
            if (applied.get().equals(expected)) {
                history.add(
                        new Op<>(
                                nextOpId++,
                                write.key(),
                                new KvModel.In.Put(write.value()),
                                new KvModel.Out.Ok(),
                                write.invoked(),
                                sim.tick()));
            } else {
                // A different command took that index, so this write was overwritten before it
                // committed. The client cannot tell whether it ever applied.
                recordUnknown(write);
            }
            inFlight.remove(client);
            return;
        }
        if (sim.tick() - write.invoked() > config.timeoutTicks()) {
            recordUnknown(write);
            inFlight.remove(client);
        }
    }

    private void startRead(int client, String key) {
        List<Long> ids = List.copyOf(sim.ids());
        long node = ids.get(random.nextInt(ids.size()));
        if (sim.isDown(node)) {
            return;
        }
        long requestId = nextRequestId++;

        if (readMode == ReadMode.IGNORE_READ_INDEX) {
            // No read index, no waiting: whatever this replica happens to hold right now.
            long invoked = sim.tick();
            Optional<ByteString> value = sim.stateMachine(node).get(Commands.utf8(key));
            history.add(
                    new Op<>(
                            nextOpId++,
                            key,
                            new KvModel.In.Get(),
                            new KvModel.Out.Value(value),
                            invoked,
                            sim.tick()));
            return;
        }

        if (sim.requestRead(node, requestId) < 0) {
            return;
        }
        inFlight.put(client, new Pending.Read(key, requestId, node, sim.tick(), -1));
    }

    private void advanceRead(int client, Pending.Read read) {
        long readIndex = read.readIndex();
        if (readIndex < 0) {
            Long granted = grantedReads.remove(read.requestId());
            if (granted == null) {
                if (sim.tick() - read.invoked() > config.timeoutTicks()) {
                    inFlight.remove(client);
                }
                return;
            }
            readIndex = granted;
            inFlight.put(
                    client,
                    new Pending.Read(read.key(), read.requestId(), read.node(), read.invoked(), readIndex));
        }

        if (sim.isDown(read.node())) {
            inFlight.remove(client);
            return;
        }
        // The step that makes the read index mean anything: local state has to have caught up to it.
        if (sim.appliedIndex(read.node()) >= readIndex) {
            Optional<ByteString> value = sim.stateMachine(read.node()).get(Commands.utf8(read.key()));
            history.add(
                    new Op<>(
                            nextOpId++,
                            read.key(),
                            new KvModel.In.Get(),
                            new KvModel.Out.Value(value),
                            read.invoked(),
                            sim.tick()));
            inFlight.remove(client);
            return;
        }
        if (sim.tick() - read.invoked() > config.timeoutTicks()) {
            inFlight.remove(client);
        }
    }

    /** Records an operation whose outcome the client never learned. */
    private void recordUnknown(Pending pending) {
        if (pending instanceof Pending.Write write) {
            history.add(
                    new Op<>(
                            nextOpId++,
                            write.key(),
                            new KvModel.In.Put(write.value()),
                            new KvModel.Out.Ok(),
                            write.invoked(),
                            Op.UNKNOWN));
        }
        // An unresolved read is simply not in the history: it observed nothing, so it constrains
        // nothing, and including it would only enlarge the search.
    }
}
