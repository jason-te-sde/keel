package io.keel.raft;

import io.keel.proto.log.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Drives a single {@link RaftNode} with no peers attached, so a test can hand it exactly the
 * messages it wants and inspect what comes back.
 *
 * <p>{@link TestCluster} is the right tool for anything involving real interaction. This one exists
 * for states a real cluster passes through too quickly to observe, such as a leader holding entries
 * from a previous term that a majority has stored but that are still not committed.
 */
final class TestDriver {

    final MemoryLogStore store = new MemoryLogStore();
    final List<Entry> applied = new ArrayList<>();
    RaftNode raft;

    private final RaftConfig cfg;
    private final long seed;

    TestDriver(RaftConfig cfg, long seed) {
        this.cfg = cfg;
        this.seed = seed;
        this.raft = RaftNode.restore(cfg, store, new Random(seed));
    }

    /** Rebuilds the node from the store, for use after seeding the log directly. */
    void reopen() {
        applied.clear();
        raft = RaftNode.restore(cfg, store, new Random(seed));
    }

    /** Runs the driver loop to quiescence and returns everything the node wanted to send. */
    List<RaftMessage> pump() {
        List<RaftMessage> sent = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Ready rd = raft.ready();
            if (rd.isEmpty()) {
                return sent;
            }
            if (rd.hasHardState()) {
                store.saveHardState(rd.hardState());
            }
            if (!rd.entriesToPersist().isEmpty()) {
                store.append(rd.entriesToPersist());
            }
            store.sync();
            sent.addAll(rd.messages());
            applied.addAll(rd.committedEntries());
            raft.advance(rd);
        }
        throw new IllegalStateException("node never went quiet: " + raft);
    }

    /** Makes this node leader by granting it the votes it asks for. */
    void electSelf() {
        raft.campaign();
        for (int round = 0; round < 4; round++) {
            List<RaftMessage> sent = pump();
            if (raft.role() == Role.LEADER) {
                return;
            }
            for (RaftMessage m : sent) {
                if (m instanceof RaftMessage.Vote v) {
                    raft.step(
                            new RaftMessage.VoteReply(
                                    v.to(), v.from(), v.term(), v.preVote(), true));
                }
            }
        }
        pump();
        if (raft.role() != Role.LEADER) {
            throw new IllegalStateException("node did not become leader: " + raft);
        }
    }

    /** Messages of one kind from a batch, for concise assertions. */
    static <T extends RaftMessage> List<T> only(List<RaftMessage> msgs, Class<T> type) {
        List<T> out = new ArrayList<>();
        for (RaftMessage m : msgs) {
            if (type.isInstance(m)) {
                out.add(type.cast(m));
            }
        }
        return out;
    }
}
