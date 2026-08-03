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

    /** Everything each node has ever applied, so a rewritten prefix can be spotted. */
    private final Map<Long, List<Entry>> appliedHistory = new HashMap<>();

    /** Commands agreed at each index, so two replicas cannot disagree about one. */
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

    private void logMatching(long tick, List<NodeView> views) {
        for (int i = 0; i < views.size(); i++) {
            for (int j = i + 1; j < views.size(); j++) {
                List<Entry> a = views.get(i).durable();
                List<Entry> b = views.get(j).durable();
                int shared = Math.min(a.size(), b.size());
                // Walk down from the highest shared index to the first agreement on term. From there
                // the paper's induction says the prefixes must be identical.
                for (int k = shared - 1; k >= 0; k--) {
                    if (a.get(k).getTerm() != b.get(k).getTerm()) {
                        continue;
                    }
                    for (int p = 0; p <= k; p++) {
                        Entry left = a.get(p);
                        Entry right = b.get(p);
                        if (left.getTerm() != right.getTerm() || !left.getData().equals(right.getData())) {
                            throw new InvariantViolation(
                                    "Log Matching",
                                    seed,
                                    tick,
                                    "nodes "
                                            + views.get(i).id()
                                            + " and "
                                            + views.get(j).id()
                                            + " agree at index "
                                            + (k + 1)
                                            + " in term "
                                            + a.get(k).getTerm()
                                            + " but differ at index "
                                            + (p + 1));
                        }
                    }
                    break;
                }
            }
        }
    }

    private void stateMachineSafety(long tick, List<NodeView> views) {
        for (NodeView view : views) {
            List<Entry> applied = view.applied();
            List<Entry> before = appliedHistory.get(view.id());

            if (before != null && !view.down()) {
                // A node may restart and replay from the start, so a shorter list is legitimate. What
                // is not legitimate is a different command at a position that was already applied.
                int shared = Math.min(before.size(), applied.size());
                for (int i = 0; i < shared; i++) {
                    if (!sameCommand(before.get(i), applied.get(i))) {
                        throw new InvariantViolation(
                                "State Machine Safety",
                                seed,
                                tick,
                                "node "
                                        + view.id()
                                        + " applied index "
                                        + applied.get(i).getIndex()
                                        + " a second time with different content: was term "
                                        + before.get(i).getTerm()
                                        + ", now term "
                                        + applied.get(i).getTerm());
                    }
                }
                if (applied.size() >= before.size()) {
                    appliedHistory.put(view.id(), applied);
                }
            } else if (!view.down()) {
                appliedHistory.put(view.id(), applied);
            }

            for (Entry entry : applied) {
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
                                    + " by one replica and term "
                                    + entry.getTerm()
                                    + " by node "
                                    + view.id());
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
