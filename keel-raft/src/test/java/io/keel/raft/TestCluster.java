package io.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.keel.proto.log.Entry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * A cluster of {@link RaftNode}s wired together in one thread, with no clock and no sockets.
 *
 * <p>Everything happens because a test asked for it: {@link #tick()} advances logical time, and
 * {@link #settle()} delivers messages until the cluster stops producing any. There is nothing to
 * wait for, so there is no flakiness to tolerate and no sleep to tune.
 *
 * <p>The harness also plays the part of the driver, which means it enforces the {@link Ready}
 * contract: hard state and entries are written and synced before the batch's messages are released.
 * A test that breaks that ordering would be testing a system nobody is going to build.
 */
final class TestCluster {

    /** One node, plus the durable state and applied entries a driver would own. */
    static final class Member {
        final long id;
        final MemoryLogStore store = new MemoryLogStore();
        final List<Entry> applied = new ArrayList<>();
        RaftNode raft;
        boolean crashed;

        Member(long id) {
            this.id = id;
        }
    }

    private final Map<Long, Member> members = new LinkedHashMap<>();
    private final Deque<RaftMessage> wire = new ArrayDeque<>();
    private final Set<Long> isolated = new HashSet<>();
    private final Random random;
    private final long seed;
    private RaftConfig.Builder configTemplate = RaftConfig.builder(1);
    private boolean started;

    /** Messages dropped because the recipient was unreachable, for assertions about partitions. */
    private int dropped;

    private TestCluster(long seed, Set<Long> voters) {
        this.seed = seed;
        this.random = new Random(seed);
        for (long id : voters) {
            members.put(id, new Member(id));
        }
    }

    /** A cluster of {@code n} voters with ids 1..n, using a fixed seed. */
    static TestCluster of(int n) {
        return of(n, 42L);
    }

    static TestCluster of(int n, long seed) {
        Set<Long> voters = new java.util.LinkedHashSet<>();
        for (long i = 1; i <= n; i++) {
            voters.add(i);
        }
        return new TestCluster(seed, voters);
    }

    /** Adjusts the configuration every node is built with. Call before {@link #start()}. */
    TestCluster configure(java.util.function.Consumer<RaftConfig.Builder> tweak) {
        requireNotStarted();
        this.configTemplate = RaftConfig.builder(1);
        tweak.accept(configTemplate);
        return this;
    }

    /**
     * Pre-populates a node's durable log, for tests that need nodes to start out disagreeing.
     *
     * @param termsByIndex term of each entry, starting at index 1
     */
    TestCluster seedLog(long id, long... termsByIndex) {
        requireNotStarted();
        Member m = member(id);
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < termsByIndex.length; i++) {
            entries.add(Entries.normal(i + 1, termsByIndex[i], new byte[] {(byte) i}));
        }
        if (!entries.isEmpty()) {
            m.store.append(entries);
        }
        m.store.saveHardState(
                io.keel.proto.log.HardState.newBuilder()
                        .setTerm(termsByIndex.length == 0 ? 0 : termsByIndex[termsByIndex.length - 1])
                        .build());
        m.store.sync();
        return this;
    }

    TestCluster start() {
        requireNotStarted();
        for (Member m : members.values()) {
            m.raft = RaftNode.restore(configFor(m.id), m.store, new Random(seed * 31 + m.id));
        }
        started = true;
        return this;
    }

    private RaftConfig configFor(long id) {
        // Rebuild per node so each gets its own id but shares the tuned settings.
        RaftConfig template = configTemplate.voters(members.keySet()).build();
        return new RaftConfig(
                id,
                members.keySet(),
                template.electionTimeoutTicks(),
                template.heartbeatTicks(),
                template.maxEntriesPerAppend(),
                template.maxBytesPerAppend(),
                template.maxUncommittedEntries(),
                template.preVote(),
                template.checkQuorum());
    }

    // ---------------------------------------------------------------------------------------------
    // Driving
    // ---------------------------------------------------------------------------------------------

    /** Advances every live node by one tick, then delivers the resulting traffic. */
    void tick() {
        for (Member m : members.values()) {
            if (!m.crashed) {
                m.raft.tick();
            }
        }
        settle();
    }

    void tick(int times) {
        for (int i = 0; i < times; i++) {
            tick();
        }
    }

    /**
     * Runs the cluster until it has nothing left to do.
     *
     * <p>Bounded, because a livelock here means a bug in the core: a leader and follower that keep
     * exchanging messages without converging would otherwise hang the test suite instead of failing
     * it.
     */
    void settle() {
        for (int round = 0; round < 10_000; round++) {
            boolean progressed = false;
            for (Member m : members.values()) {
                if (!m.crashed && drive(m)) {
                    progressed = true;
                }
            }
            if (!wire.isEmpty()) {
                deliverOne();
                progressed = true;
            }
            if (!progressed) {
                return;
            }
        }
        fail("cluster did not settle after 10000 rounds (seed " + seed + "): livelock in the core");
    }

    /** Performs one driver cycle for a node. Returns true if it had anything to do. */
    private boolean drive(Member m) {
        Ready rd = m.raft.ready();
        if (rd.isEmpty()) {
            return false;
        }
        // Exactly the order Ready documents. Persist, sync, and only then let messages go.
        if (rd.hasHardState()) {
            m.store.saveHardState(rd.hardState());
        }
        if (!rd.entriesToPersist().isEmpty()) {
            m.store.append(rd.entriesToPersist());
        }
        m.store.sync();
        wire.addAll(rd.messages());
        m.applied.addAll(rd.committedEntries());
        m.raft.advance(rd);
        return true;
    }

    private void deliverOne() {
        RaftMessage m = wire.poll();
        if (m == null) {
            return;
        }
        Member to = members.get(m.to());
        if (to == null || to.crashed || !reachable(m.from(), m.to())) {
            dropped++;
            return;
        }
        to.raft.step(m);
    }

    /** Delivers in-flight messages in a seed-dependent order, to shake out order assumptions. */
    void settleShuffled() {
        for (int round = 0; round < 10_000; round++) {
            boolean progressed = false;
            List<Member> order = new ArrayList<>(members.values());
            Collections.shuffle(order, random);
            for (Member m : order) {
                if (!m.crashed && drive(m)) {
                    progressed = true;
                }
            }
            if (!wire.isEmpty()) {
                List<RaftMessage> batch = new ArrayList<>(wire);
                wire.clear();
                Collections.shuffle(batch, random);
                for (RaftMessage msg : batch) {
                    wire.add(msg);
                }
                deliverOne();
                progressed = true;
            }
            if (!progressed) {
                return;
            }
        }
        fail("cluster did not settle after 10000 rounds (seed " + seed + ")");
    }

    /** Ticks until {@code condition} holds, failing with context if it never does. */
    void runUntil(BooleanSupplier condition, int maxTicks, String what) {
        for (int i = 0; i < maxTicks; i++) {
            if (condition.getAsBoolean()) {
                return;
            }
            tick();
        }
        if (!condition.getAsBoolean()) {
            fail(
                    "gave up waiting for "
                            + what
                            + " after "
                            + maxTicks
                            + " ticks (seed "
                            + seed
                            + ")\n"
                            + describe());
        }
    }

    /** Ticks until some node is leader. */
    long electLeader() {
        runUntil(() -> leaderId() != 0, 200, "a leader to be elected");
        return leaderId();
    }

    // ---------------------------------------------------------------------------------------------
    // Faults
    // ---------------------------------------------------------------------------------------------

    /** Cuts a node off from every other node. In-flight messages to and from it are dropped. */
    void isolate(long id) {
        isolated.add(id);
    }

    void heal() {
        isolated.clear();
    }

    private boolean reachable(long from, long to) {
        return !isolated.contains(from) && !isolated.contains(to);
    }

    /**
     * Kills a node, discarding everything it had not synced.
     *
     * <p>Its applied entries are kept, standing in for a state machine that had already been written
     * out; snapshot-based recovery arrives with snapshots.
     */
    void crash(long id) {
        Member m = member(id);
        m.crashed = true;
        m.store.crash();
        wire.removeIf(msg -> msg.to() == id || msg.from() == id);
    }

    /**
     * Brings a crashed node back, rebuilding it from what survived on disk.
     *
     * <p>The applied list is cleared because an in-memory state machine does not survive a crash
     * either: recovery replays the durable log from the start, and if that replay produced anything
     * different from what the node applied before, {@link #assertAppliedPrefixesAgree()} would say
     * so.
     */
    void restart(long id) {
        Member m = member(id);
        m.applied.clear();
        m.raft = RaftNode.restore(configFor(id), m.store, new Random(seed * 31 + id));
        m.crashed = false;
    }

    // ---------------------------------------------------------------------------------------------
    // Inspection and assertions
    // ---------------------------------------------------------------------------------------------

    Member member(long id) {
        Member m = members.get(id);
        if (m == null) {
            throw new IllegalArgumentException("no such node: " + id);
        }
        return m;
    }

    RaftNode node(long id) {
        return member(id).raft;
    }

    Set<Long> ids() {
        return members.keySet();
    }

    int droppedMessages() {
        return dropped;
    }

    /** The single leader's id, or 0 when there is no leader or more than one. */
    long leaderId() {
        long found = 0;
        for (Member m : members.values()) {
            if (!m.crashed && m.raft.role() == Role.LEADER) {
                if (found != 0) {
                    return 0;
                }
                found = m.id;
            }
        }
        return found;
    }

    /** Proposes on the current leader, ticking first if there is not one yet. */
    long proposeOnLeader(String value) {
        long leader = electLeader();
        long index = node(leader).propose(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        settle();
        return index;
    }

    /** Commands applied by a node, decoded as UTF-8, skipping no-ops. */
    List<String> appliedCommands(long id) {
        List<String> out = new ArrayList<>();
        for (Entry e : member(id).applied) {
            if (e.getType() == io.keel.proto.log.EntryType.ENTRY_TYPE_NORMAL) {
                out.add(e.getData().toStringUtf8());
            }
        }
        return out;
    }

    /**
     * Election safety: no term ever has two leaders.
     *
     * <p>Checked across the whole run rather than at one instant, so a leader that existed only
     * briefly still counts.
     */
    void assertOneLeaderPerTerm() {
        Map<Long, Long> leaderByTerm = new HashMap<>();
        for (Member m : members.values()) {
            if (m.crashed || m.raft.role() != Role.LEADER) {
                continue;
            }
            Long existing = leaderByTerm.put(m.raft.term(), m.id);
            if (existing != null) {
                fail(
                        "two leaders in term "
                                + m.raft.term()
                                + ": nodes "
                                + existing
                                + " and "
                                + m.id
                                + " (seed "
                                + seed
                                + ")");
            }
        }
    }

    /**
     * Log matching: if two nodes hold the same term at an index, their logs agree up to it.
     *
     * <p>Only the durable logs are compared, since that is what survives and what other nodes have
     * been told about.
     */
    void assertLogsMatch() {
        List<Member> live = new ArrayList<>();
        for (Member m : members.values()) {
            if (!m.crashed) {
                live.add(m);
            }
        }
        for (int i = 0; i < live.size(); i++) {
            for (int j = i + 1; j < live.size(); j++) {
                List<Entry> a = live.get(i).store.durableEntries();
                List<Entry> b = live.get(j).store.durableEntries();
                int shared = Math.min(a.size(), b.size());
                for (int k = shared - 1; k >= 0; k--) {
                    if (a.get(k).getTerm() == b.get(k).getTerm()) {
                        for (int p = 0; p <= k; p++) {
                            assertEquals(
                                    a.get(p).getTerm(),
                                    b.get(p).getTerm(),
                                    "log mismatch at index "
                                            + (p + 1)
                                            + " between nodes "
                                            + live.get(i).id
                                            + " and "
                                            + live.get(j).id);
                            assertEquals(
                                    a.get(p).getData(),
                                    b.get(p).getData(),
                                    "log data mismatch at index "
                                            + (p + 1)
                                            + " between nodes "
                                            + live.get(i).id
                                            + " and "
                                            + live.get(j).id);
                        }
                        break;
                    }
                }
            }
        }
    }

    /** State machine safety: no two nodes applied different commands at the same position. */
    void assertAppliedPrefixesAgree() {
        List<Member> live = new ArrayList<>(members.values());
        for (int i = 0; i < live.size(); i++) {
            for (int j = i + 1; j < live.size(); j++) {
                List<Entry> a = live.get(i).applied;
                List<Entry> b = live.get(j).applied;
                int shared = Math.min(a.size(), b.size());
                for (int k = 0; k < shared; k++) {
                    assertEquals(
                            a.get(k).getIndex(),
                            b.get(k).getIndex(),
                            "nodes "
                                    + live.get(i).id
                                    + " and "
                                    + live.get(j).id
                                    + " applied different indexes at position "
                                    + k);
                    assertEquals(
                            a.get(k).getData(),
                            b.get(k).getData(),
                            "nodes "
                                    + live.get(i).id
                                    + " and "
                                    + live.get(j).id
                                    + " applied different commands at index "
                                    + a.get(k).getIndex());
                }
            }
        }
    }

    String describe() {
        StringBuilder sb = new StringBuilder("cluster state:\n");
        for (Member m : members.values()) {
            sb.append("  ")
                    .append(m.crashed ? "[down] " : "       ")
                    .append(m.raft)
                    .append(" applied=")
                    .append(m.applied.size())
                    .append('\n');
        }
        sb.append("  inFlight=").append(wire.size()).append(" dropped=").append(dropped);
        return sb.toString();
    }

    private void requireNotStarted() {
        if (started) {
            throw new IllegalStateException("cluster already started");
        }
    }
}
