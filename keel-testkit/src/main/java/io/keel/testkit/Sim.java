package io.keel.testkit;

import com.google.protobuf.ByteString;
import io.keel.kv.MemoryStateMachine;
import io.keel.proto.log.Entry;
import io.keel.proto.log.EntryType;
import io.keel.raft.MemoryLogStore;
import io.keel.raft.NotLeaderException;
import io.keel.raft.ProposalDroppedException;
import io.keel.raft.RaftConfig;
import io.keel.raft.RaftMessage;
import io.keel.raft.RaftNode;
import io.keel.raft.ReadState;
import io.keel.raft.Ready;
import io.keel.raft.Role;
import io.keel.proto.log.SnapshotMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * A whole cluster in one thread, with a clock that only moves when asked.
 *
 * <p>This exists because the bugs worth finding live in schedules a handwritten integration test never
 * produces: a leader crashing between persisting an entry and acknowledging it, a partition healing
 * halfway through an election, a duplicated message arriving after the term it belonged to is over.
 * Starting a real cluster and waiting explores one arbitrary schedule per run and cannot reproduce
 * whatever it finds.
 *
 * <p>Here, a run is a pure function of {@link SimConfig}. The seed drives message latency, every fault
 * decision, and each node's randomized election timeout, so a failure at one seed is still there at
 * that seed tomorrow. Nodes are always visited in id order for the same reason.
 *
 * <p>The simulator also plays the driver, which means it obeys the {@link Ready} contract: hard state
 * and entries are written and synced before that batch's messages are released. Getting that wrong here
 * would quietly make every crash test meaningless, since the whole point of a crash test is that an
 * acknowledgement implies durability.
 */
public final class Sim {

    /** One simulated node: consensus core, its disk, and its state machine. */
    private static final class Node {
        final long id;
        final MemoryLogStore store = new MemoryLogStore();
        final List<Entry> applied = new ArrayList<>();
        final List<ReadState> reads = new ArrayList<>();
        MemoryStateMachine stateMachine = new MemoryStateMachine();
        RaftNode raft;
        boolean down;

        /** The newest snapshot this node has taken, and its payload. */
        SnapshotMetadata snapshotMeta = SnapshotMetadata.getDefaultInstance();
        byte[] snapshotPayload = new byte[0];

        /**
         * Index and term of the highest entry applied, as a pair.
         *
         * <p>They have to come from the same entry. Taking the index from the state machine, which
         * only sees client commands, and the term from the last entry applied of any kind produced a
         * snapshot whose advertised term did not match its own index whenever a no-op or
         * configuration entry followed a command across a term boundary.
         */
        long lastAppliedIndex;

        long lastAppliedTerm;

        Node(long id) {
            this.id = id;
        }
    }

    private final SimConfig config;
    private final Random random;
    private final Map<Long, Node> nodes = new LinkedHashMap<>();
    private final Network network = new Network();
    private final Invariants invariants;

    private long tick;
    private long traceHash = 0xcbf29ce484222325L;
    private long proposals;
    private long crashes;
    private long partitions;
    private long snapshotsTaken;
    private long snapshotsInstalled;

    /**
     * Snapshot payloads in flight, by recipient.
     *
     * <p>Recorded when the leader emits InstallSnapshot and consumed when the follower installs it.
     * The metadata message travels through the network and can be delayed, dropped, or duplicated like
     * anything else; only the bulk payload takes this shortcut, which is the same split the real
     * transport makes.
     *
     * <p>Keyed by recipient <em>and</em> boundary index, not by recipient alone. Two leaders can have a
     * snapshot in flight to the same node at different boundaries, and pairing a payload with the wrong
     * metadata gives a node a state machine that disagrees with its own log. That was a real bug here,
     * and it presented as a compaction request past the end of the log several hundred ticks later.
     */
    private final Map<String, byte[]> snapshotsInFlight = new LinkedHashMap<>();

    public static Sim of(SimConfig config) {
        return new Sim(config);
    }

    private Sim(SimConfig config) {
        this.config = config;
        this.random = new Random(config.seed());
        this.invariants = new Invariants(config.seed());
        for (long id = 1; id <= config.nodes(); id++) {
            nodes.put(id, new Node(id));
        }
        Set<Long> voters = config.voterIds();
        for (Node node : nodes.values()) {
            // A distinct derived seed per node, so two nodes do not pick identical election timeouts
            // for the whole run and split every vote.
            node.raft =
                    RaftNode.restore(
                            configFor(node.id, voters),
                            node.store,
                            new Random(config.seed() * 1_000_003L + node.id));
        }
    }

    private RaftConfig configFor(long id, Set<Long> voters) {
        return RaftConfig.builder(id)
                .voters(voters)
                .electionTimeoutTicks(config.electionTimeoutTicks())
                .heartbeatTicks(config.heartbeatTicks())
                .build();
    }

    // ---------------------------------------------------------------------------------------------
    // Running
    // ---------------------------------------------------------------------------------------------

    /**
     * Advances the simulation by one tick.
     *
     * <p>Order matters and is fixed: injecting faults, delivering what is due, ticking the nodes,
     * draining their output, then checking every invariant. Delivering before ticking means a message
     * and the reply it provokes land in the same tick, which keeps runs short without making them any
     * less adversarial.
     */
    public void step() {
        tick++;
        injectFaults();
        for (RaftMessage message : network.due(tick)) {
            Node target = nodes.get(message.to());
            if (target != null && !target.down) {
                mixTrace(message.to(), message.getClass().getSimpleName().hashCode(), message.term());
                target.raft.step(message);
            }
        }
        for (Node node : nodes.values()) {
            if (!node.down) {
                node.raft.tick();
            }
        }
        for (Node node : nodes.values()) {
            if (!node.down) {
                drive(node);
            }
        }
        invariants.check(tick, views());
    }

    public void run(long ticks) {
        for (long i = 0; i < ticks; i++) {
            step();
        }
    }

    /** Runs until {@code condition} holds. Returns false if it never did. */
    public boolean runUntil(java.util.function.BooleanSupplier condition, long maxTicks) {
        for (long i = 0; i < maxTicks; i++) {
            if (condition.getAsBoolean()) {
                return true;
            }
            step();
        }
        return condition.getAsBoolean();
    }

    /** Runs until some node is leader. */
    public boolean runUntilLeader(long maxTicks) {
        return runUntil(() -> leader().isPresent(), maxTicks);
    }

    /**
     * One driver cycle: persist, sync, send, apply, acknowledge.
     *
     * <p>Exactly the order {@link Ready} documents. Sending before syncing would mean acknowledging an
     * entry that a crash could still lose, and the leader counts acknowledgements toward a quorum.
     */
    private void drive(Node node) {
        Ready ready = node.raft.ready();
        if (ready.isEmpty()) {
            return;
        }
        // The snapshot goes first. Entries in the same batch are the ones that follow its boundary,
        // and appending them into a log that still starts lower down would leave a gap. This ordering
        // is not obvious from the Ready contract, which is why it is spelled out there too.
        if (ready.hasSnapshotToInstall()) {
            installSnapshot(node, ready.snapshotToInstall());
        }
        if (ready.hasHardState()) {
            node.store.saveHardState(ready.hardState());
        }
        if (!ready.entriesToPersist().isEmpty()) {
            node.store.append(ready.entriesToPersist());
        }
        node.store.sync();

        for (RaftMessage message : ready.messages()) {
            if (message instanceof RaftMessage.InstallSnapshot snapshot) {
                // Hand the payload over out of band, as the real transport does with its streaming RPC.
                snapshotsInFlight.put(
                        payloadKey(snapshot.to(), snapshot.meta().getLastIndex()), node.snapshotPayload);
            }
            network.send(tick, message, random, config);
        }
        for (Entry entry : ready.committedEntries()) {
            node.applied.add(entry);
            node.lastAppliedIndex = entry.getIndex();
            node.lastAppliedTerm = entry.getTerm();
            if (entry.getType() == EntryType.ENTRY_TYPE_NORMAL) {
                node.stateMachine.apply(entry.getIndex(), entry.getData());
            } else if (entry.getType() == EntryType.ENTRY_TYPE_CONF_CHANGE) {
                // The change takes effect on apply, not on append. A driver that skips this leaves the
                // node disagreeing with the cluster about who votes.
                node.raft.applyConfChange(RaftNode.decodeConfChange(entry));
            }
        }
        node.reads.addAll(ready.readStates());
        node.raft.advance(ready);
        maybeSnapshot(node);
        mixTrace(node.id, ready.entriesToPersist().size(), ready.committedEntries().size());
    }

    /**
     * Takes a snapshot and compacts the log once it has grown past the threshold.
     *
     * <p>Ordering is the whole point: the state machine snapshot is serialized first and the log is
     * compacted second. Compacting first would leave a window where the entries are gone and nothing
     * else holds their effects.
     */
    private void maybeSnapshot(Node node) {
        int threshold = config.snapshotThresholdEntries();
        if (threshold == 0) {
            return;
        }
        long applied = node.lastAppliedIndex;
        if (applied == 0 || applied - node.snapshotMeta.getLastIndex() < threshold) {
            return;
        }
        // The boundary index and its term come from the same applied entry. A snapshot that
        // misdescribes its own boundary makes a receiver discard entries it has acknowledged.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        node.stateMachine.snapshot(out);
        SnapshotMetadata meta =
                SnapshotMetadata.newBuilder()
                        .setLastIndex(applied)
                        .setLastTerm(node.lastAppliedTerm)
                        .setSizeBytes(out.size())
                        // The membership has to travel with the snapshot: a node restoring from it
                        // cannot recover the configuration from the log, because the entries that
                        // carried the changes are exactly what the snapshot replaced.
                        .setConf(node.raft.confState())
                        .build();
        node.snapshotPayload = out.toByteArray();
        node.snapshotMeta = meta;
        node.store.compact(meta);
        snapshotsTaken++;
        mixTrace(node.id, -3, applied);
    }

    private static String payloadKey(long to, long lastIndex) {
        return to + ":" + lastIndex;
    }

    private void installSnapshot(Node node, SnapshotMetadata meta) {
        byte[] payload = snapshotsInFlight.remove(payloadKey(node.id, meta.getLastIndex()));
        if (payload == null) {
            throw new IllegalStateException(
                    "node " + node.id + " accepted a snapshot at " + meta.getLastIndex()
                            + " with no payload in flight");
        }
        node.stateMachine.restore(new ByteArrayInputStream(payload));
        node.store.installSnapshot(meta);
        node.snapshotMeta = meta;
        node.snapshotPayload = payload;
        node.lastAppliedIndex = meta.getLastIndex();
        node.lastAppliedTerm = meta.getLastTerm();
        // The pre-snapshot applied list is not a record of this node's state any more: the snapshot
        // replaced it wholesale. Invariants compare by log index, so the shorter list is fine.
        node.applied.clear();
        snapshotsInstalled++;
        mixTrace(node.id, -4, meta.getLastIndex());
    }

    // ---------------------------------------------------------------------------------------------
    // Faults
    // ---------------------------------------------------------------------------------------------

    private void injectFaults() {
        if (roll(config.healProbability()) && network.partitioned()) {
            network.heal();
        }
        if (roll(config.partitionProbability())) {
            splitCluster();
        }
        if (roll(config.crashProbability())) {
            crashSomeone();
        }
        if (roll(config.restartProbability())) {
            restartSomeone();
        }
    }

    private boolean roll(double probability) {
        return probability > 0 && random.nextDouble() < probability;
    }

    /** Splits the cluster into two arbitrary groups, which may leave neither side with a quorum. */
    private void splitCluster() {
        List<Long> ids = new ArrayList<>(nodes.keySet());
        Set<Long> left = new HashSet<>();
        Set<Long> right = new HashSet<>();
        for (long id : ids) {
            if (random.nextBoolean()) {
                left.add(id);
            } else {
                right.add(id);
            }
        }
        if (left.isEmpty() || right.isEmpty()) {
            return;
        }
        network.partition(List.of(left, right));
        partitions++;
        mixTrace(0, left.size(), right.size());
    }

    /** Kills a node, discarding everything it had not synced and its whole state machine. */
    public void crash(long id) {
        Node node = node(id);
        if (node.down) {
            return;
        }
        node.down = true;
        node.store.crash();
        // An in-memory state machine does not survive either. Recovery replays the durable log, and if
        // the replay produced anything different the invariant checks would say so.
        node.stateMachine.close();
        node.stateMachine = new MemoryStateMachine();
        node.applied.clear();
        node.reads.clear();
        // The snapshot file survives a crash, so recovery restores from it and replays the log after
        // it. Without this the node would replay from index 1 into a log whose prefix is gone.
        if (node.snapshotMeta.getLastIndex() > 0) {
            node.stateMachine.restore(new ByteArrayInputStream(node.snapshotPayload));
            node.lastAppliedIndex = node.snapshotMeta.getLastIndex();
            node.lastAppliedTerm = node.snapshotMeta.getLastTerm();
        } else {
            node.lastAppliedIndex = 0;
            node.lastAppliedTerm = 0;
        }
        network.forget(id);
        crashes++;
        mixTrace(id, -1, 0);
    }

    /** Brings a crashed node back, rebuilt from what survived on its disk. */
    public void restart(long id) {
        Node node = node(id);
        if (!node.down) {
            return;
        }
        node.raft =
                RaftNode.restore(
                        configFor(id, config.voterIds()),
                        node.store,
                        new Random(config.seed() * 1_000_003L + id + tick));
        node.down = false;
        mixTrace(id, -2, 0);
    }

    private void crashSomeone() {
        List<Long> up = nodes.values().stream().filter(n -> !n.down).map(n -> n.id).toList();
        if (up.isEmpty()) {
            return;
        }
        crash(up.get(random.nextInt(up.size())));
    }

    private void restartSomeone() {
        List<Long> down = nodes.values().stream().filter(n -> n.down).map(n -> n.id).toList();
        if (down.isEmpty()) {
            return;
        }
        restart(down.get(random.nextInt(down.size())));
    }

    public void heal() {
        network.heal();
    }

    public void isolate(long id) {
        Set<Long> alone = Set.of(id);
        Set<Long> rest = new HashSet<>(nodes.keySet());
        rest.remove(id);
        network.partition(List.of(alone, rest));
        partitions++;
    }

    // ---------------------------------------------------------------------------------------------
    // Client operations
    // ---------------------------------------------------------------------------------------------

    /** The single leader, or empty when there is none or more than one. */
    public OptionalLong leader() {
        long found = 0;
        for (Node node : nodes.values()) {
            if (!node.down && node.raft.role() == Role.LEADER) {
                if (found != 0) {
                    return OptionalLong.empty();
                }
                found = node.id;
            }
        }
        return found == 0 ? OptionalLong.empty() : OptionalLong.of(found);
    }

    /**
     * Offers a command to whichever node is currently leader.
     *
     * @return the log index it was assigned, or -1 when there was nobody to accept it
     */
    public long propose(ByteString command) {
        OptionalLong leader = leader();
        if (leader.isEmpty()) {
            return -1;
        }
        try {
            long index = node(leader.getAsLong()).raft.propose(command.toByteArray());
            proposals++;
            return index;
        } catch (NotLeaderException | ProposalDroppedException e) {
            // Both are ordinary outcomes here: leadership can change between the check and the call,
            // and a leader with no quorum is supposed to refuse.
            return -1;
        }
    }

    /** Asks a specific node for a read index. Returns -1 if it cannot be asked. */
    public long requestRead(long nodeId, long requestId) {
        Node node = node(nodeId);
        if (node.down) {
            return -1;
        }
        try {
            node.raft.requestRead(requestId);
            return requestId;
        } catch (NotLeaderException e) {
            return -1;
        }
    }

    /** Read indexes granted to a node since the last call, and forgets them. */
    public List<ReadState> drainReads(long nodeId) {
        Node node = node(nodeId);
        List<ReadState> out = List.copyOf(node.reads);
        node.reads.clear();
        return out;
    }

    public MemoryStateMachine stateMachine(long id) {
        return node(id).stateMachine;
    }

    public long appliedIndex(long id) {
        Node node = node(id);
        return node.applied.isEmpty() ? 0 : node.applied.get(node.applied.size() - 1).getIndex();
    }

    /**
     * The command a node applied at {@code index}, if it applied that index at all.
     *
     * <p>A client that proposed at an index and then finds a different command there has learned that
     * its write was overwritten before it committed, which is the case where the outcome is genuinely
     * unknown rather than merely slow.
     */
    public java.util.Optional<ByteString> appliedDataAt(long id, long index) {
        for (Entry entry : node(id).applied) {
            if (entry.getIndex() == index) {
                return java.util.Optional.of(entry.getData());
            }
        }
        return java.util.Optional.empty();
    }

    /**
     * Proposes adding a voter, on whichever node is leader.
     *
     * @return the index the change was assigned, or -1 if there was nobody to accept it
     */
    public long addVoter(long id) {
        return proposeConfChange(id, io.keel.proto.log.ConfChange.Type.TYPE_ADD_VOTER);
    }

    /** Proposes removing a voter. */
    public long removeVoter(long id) {
        return proposeConfChange(id, io.keel.proto.log.ConfChange.Type.TYPE_REMOVE_VOTER);
    }

    private long proposeConfChange(long id, io.keel.proto.log.ConfChange.Type type) {
        OptionalLong leader = leader();
        if (leader.isEmpty()) {
            return -1;
        }
        try {
            return node(leader.getAsLong())
                    .raft
                    .proposeConfChange(
                            io.keel.proto.log.ConfChange.newBuilder().setType(type).setNodeId(id).build());
        } catch (RuntimeException e) {
            return -1;
        }
    }

    /** The membership a node currently believes in. */
    public Set<Long> voters(long id) {
        return node(id).raft.voters();
    }

    /** The snapshot boundary a node has compacted to. */
    public long snapshotIndex(long id) {
        return node(id).snapshotMeta.getLastIndex();
    }

    public boolean isDown(long id) {
        return node(id).down;
    }

    public Set<Long> ids() {
        return nodes.keySet();
    }

    // ---------------------------------------------------------------------------------------------
    // Observation
    // ---------------------------------------------------------------------------------------------

    public List<NodeView> views() {
        List<NodeView> out = new ArrayList<>(nodes.size());
        for (Node node : nodes.values()) {
            out.add(
                    new NodeView(
                            node.id,
                            node.raft.role(),
                            node.raft.term(),
                            node.raft.commitIndex(),
                            node.applied,
                            node.store.durableEntries(),
                            node.down));
        }
        return out;
    }

    public long tick() {
        return tick;
    }

    public Invariants invariants() {
        return invariants;
    }

    public SimConfig config() {
        return config;
    }

    /**
     * A hash of every significant event in the run so far.
     *
     * <p>Two runs of the same config must produce the same value. Comparing a single number is enough
     * to catch a stray {@code HashMap} iteration or an unseeded {@code Random} sneaking into the code
     * under test, and it costs nothing to carry.
     */
    public long traceHash() {
        return traceHash;
    }

    private void mixTrace(long a, long b, long c) {
        traceHash = mix(mix(mix(mix(traceHash, tick), a), b), c);
    }

    private static long mix(long hash, long value) {
        long h = hash ^ value;
        h *= 0x100000001b3L;
        return h ^ (h >>> 29);
    }

    /** Counters for asserting that a run actually did something. */
    public SimStats stats() {
        return new SimStats(
                tick,
                proposals,
                crashes,
                partitions,
                network.sent(),
                network.delivered(),
                network.dropped(),
                network.duplicated(),
                invariants.checkCount(),
                snapshotsTaken,
                snapshotsInstalled);
    }

    /** Aggregate counters from a run. */
    public record SimStats(
            long ticks,
            long proposals,
            long crashes,
            long partitions,
            long messagesSent,
            long messagesDelivered,
            long messagesDropped,
            long messagesDuplicated,
            long invariantChecks,
            long snapshotsTaken,
            long snapshotsInstalled) {}

    public String describe() {
        StringBuilder sb = new StringBuilder("sim at tick " + tick + " (seed " + config.seed() + ")\n");
        for (NodeView view : views()) {
            sb.append("  ")
                    .append(view.down() ? "[down] " : "       ")
                    .append("node ")
                    .append(view.id())
                    .append(' ')
                    .append(view.role())
                    .append(" term=")
                    .append(view.term())
                    .append(" commit=")
                    .append(view.commitIndex())
                    .append(" applied=")
                    .append(view.applied().size())
                    .append(" durable=")
                    .append(view.durable().size())
                    .append('\n');
        }
        sb.append("  inFlight=").append(network.inFlight()).append(" partitioned=").append(network.partitioned());
        return sb.toString();
    }

    private Node node(long id) {
        Node node = nodes.get(id);
        if (node == null) {
            throw new IllegalArgumentException("no such node: " + id);
        }
        return node;
    }
}
