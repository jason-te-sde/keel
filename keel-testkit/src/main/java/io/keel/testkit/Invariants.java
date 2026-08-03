package io.keel.testkit;

import io.keel.proto.log.Entry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The safety properties from the Raft paper, checked after every step of a run.
 *
 * <p>Checking continuously rather than at the end is the point. A cluster that elects two leaders in
 * one term and then recovers looks perfectly healthy by the time a run finishes, and an end-state
 * assertion would pass. Several of these properties are also about history rather than about the
 * current instant, which is why this class is stateful: it remembers what it has already seen so it
 * can notice a value going backwards or a committed prefix being rewritten.
 *
 * <p>The properties, in the paper's terms:
 *
 * <ul>
 *   <li><b>Election Safety</b> (5.2): at most one leader per term.
 *   <li><b>Log Matching</b> (5.3): if two logs hold the same term at the same index, they are
 *       identical up to it.
 *   <li><b>State Machine Safety</b> (5.4.3): no two replicas apply different commands at the same
 *       index, and no replica ever rewrites what it has already applied.
 *   <li><b>Monotonicity</b>: a node's term and commit index never move backwards, including across a
 *       crash. This is not in the paper's list, but a violation means durability is broken, and it
 *       fails much closer to the cause than the properties above would.
 * </ul>
 */
public final class Invariants {

    private final long seed;

    private final Map<Long, Long> leaderByTerm = new HashMap<>();
    private final Map<Long, Long> highestTerm = new HashMap<>();
    private final Map<Long, Long> highestCommit = new HashMap<>();

    /**
     * Commands applied at each index, by anyone, ever.
     *
     * <p>Keyed by log index rather than by position in a node's history, which matters once snapshots
     * exist: a node that catches up from a snapshot starts applying at a higher index, so its history
     * no longer lines up positionally with what it applied before. Indexes always line up.
     */
    private final Map<Long, Entry> committedByIndex = new HashMap<>();

    private long checks;

    public Invariants(long seed) {
        this.seed = seed;
    }

    /** Number of times {@link #check} has run, for a sanity check that a suite did any work. */
    public long checkCount() {
        return checks;
    }

    /**
     * Verifies every property against a snapshot of the cluster.
     *
     * @throws InvariantViolation on the first property that fails
     */
    public void check(long tick, List<NodeView> views) {
        checks++;
        electionSafety(tick, views);
        monotonicity(tick, views);
        logMatching(tick, views);
        stateMachineSafety(tick, views);
    }

    private void electionSafety(long tick, List<NodeView> views) {
        for (NodeView view : views) {
            if (!view.isLeader()) {
                continue;
            }
            Long existing = leaderByTerm.putIfAbsent(view.term(), view.id());
            if (existing != null && existing != view.id()) {
                throw new InvariantViolation(
                        "Election Safety",
                        seed,
                        tick,
                        "term " + view.term() + " has two leaders: nodes " + existing + " and " + view.id());
            }
        }
    }

    private void monotonicity(long tick, List<NodeView> views) {
        for (NodeView view : views) {
            if (view.down()) {
                // A node that is down has no state to report; its durable state is checked when it
                // comes back, which is exactly where a durability bug shows up.
                continue;
            }
            Long term = highestTerm.get(view.id());
            if (term != null && view.term() < term) {
                throw new InvariantViolation(
                        "Term Monotonicity",
                        seed,
                        tick,
                        "node "
                                + view.id()
                                + " went from term "
                                + term
                                + " back to "
                                + view.term()
                                + ", which means a persisted term was lost");
            }
            highestTerm.put(view.id(), view.term());

            Long commit = highestCommit.get(view.id());
            if (commit != null && view.commitIndex() < commit) {
                throw new InvariantViolation(
                        "Commit Monotonicity",
                        seed,
                        tick,
                        "node "
                                + view.id()
                                + " went from commit index "
                                + commit
                                + " back to "
                                + view.commitIndex());
            }
            highestCommit.put(view.id(), view.commitIndex());
        }
    }

    /**
     * Log Matching, compared by log index rather than by position in the list.
     *
     * <p>Once compaction exists, two nodes' durable logs start at different indexes, and a positional
     * comparison silently compares index 5 on one node against index 1 on another. Only the range both
     * logs actually hold can be compared; entries below a node's snapshot boundary are committed and
     * gone, which is not a disagreement.
     */
    private void logMatching(long tick, List<NodeView> views) {
        for (int i = 0; i < views.size(); i++) {
            for (int j = i + 1; j < views.size(); j++) {
                NodeView a = views.get(i);
                NodeView b = views.get(j);
                if (a.durable().isEmpty() || b.durable().isEmpty()) {
                    continue;
                }
                long lo = Math.max(firstIndexOf(a), firstIndexOf(b));
                long hi = Math.min(lastIndexOf(a), lastIndexOf(b));
                if (lo > hi) {
                    continue;
                }
                // Walk down to the highest index where the terms agree. From there the paper's
                // induction says the prefixes must be identical.
                for (long index = hi; index >= lo; index--) {
                    if (entryAt(a, index).getTerm() != entryAt(b, index).getTerm()) {
                        continue;
                    }
                    for (long check = lo; check <= index; check++) {
                        Entry left = entryAt(a, check);
                        Entry right = entryAt(b, check);
                        if (left.getTerm() != right.getTerm()
                                || !left.getData().equals(right.getData())) {
                            throw new InvariantViolation(
                                    "Log Matching",
                                    seed,
                                    tick,
                                    "nodes "
                                            + a.id()
                                            + " and "
                                            + b.id()
                                            + " agree at index "
                                            + index
                                            + " in term "
                                            + entryAt(a, index).getTerm()
                                            + " but differ at index "
                                            + check);
                        }
                    }
                    break;
                }
            }
        }
    }

    private static long firstIndexOf(NodeView view) {
        return view.durable().get(0).getIndex();
    }

    private static long lastIndexOf(NodeView view) {
        return view.durable().get(view.durable().size() - 1).getIndex();
    }

    /** Entries are contiguous, so an index maps straight to a position. */
    private static Entry entryAt(NodeView view, long index) {
        return view.durable().get((int) (index - firstIndexOf(view)));
    }

    private void stateMachineSafety(long tick, List<NodeView> views) {
        for (NodeView view : views) {
            for (Entry entry : view.applied()) {
                Entry agreed = committedByIndex.putIfAbsent(entry.getIndex(), entry);
                if (agreed != null && !sameCommand(agreed, entry)) {
                    throw new InvariantViolation(
                            "State Machine Safety",
                            seed,
                            tick,
                            "index "
                                    + entry.getIndex()
                                    + " was applied as term "
                                    + agreed.getTerm()
                                    + " with "
                                    + agreed.getData().size()
                                    + " bytes, and node "
                                    + view.id()
                                    + " applied it as term "
                                    + entry.getTerm()
                                    + " with "
                                    + entry.getData().size()
                                    + " bytes");
                }
            }
        }
    }

    private static boolean sameCommand(Entry a, Entry b) {
        return a.getIndex() == b.getIndex()
                && a.getTerm() == b.getTerm()
                && a.getData().equals(b.getData());
    }
}
