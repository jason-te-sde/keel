package io.keel.testkit.linz;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Checks whether a history of concurrent operations could have happened in some sequential order.
 *
 * <p>This is the property the invariant checks cannot see. Those verify that replicas agree with each
 * other, which a store can do perfectly while still returning a value to a client that no valid
 * ordering allows, for instance by serving a read from a leader that has already been replaced.
 *
 * <p>Two ideas make the search affordable.
 *
 * <p><b>Decomposition.</b> A key-value store is a composition of independent registers, and
 * linearizability is compositional: a history is linearizable if and only if each per-key subhistory
 * is. One intractable search becomes many small ones. This is the difference between checking a
 * thousand operations and checking six at a time.
 *
 * <p><b>Memoization.</b> Within a subhistory the search is Wing and Gong's: repeatedly try to
 * linearize an operation that could come next, recurse, and backtrack. Without memoization that
 * revisits the same (state, remaining operations) pair along every permutation that reaches it, which
 * is factorial. With it, each distinct pair is explored once.
 *
 * <p>Operations whose outcome the client never learned are tried both ways: applied at that point, and
 * never applied at all. A timed-out request really might have done either, and a checker that assumed
 * one would reject correct systems or accept broken ones.
 */
public final class Linearizability {

    private Linearizability() {}

    /** What the checker concluded, and enough context to start a diagnosis if the answer is no. */
    public record Result(boolean linearizable, String detail, int operationsChecked, long statesExplored) {

        public static Result ok(int operations, long states) {
            return new Result(true, "linearizable", operations, states);
        }

        /** Throws if the history was not linearizable, with the counterexample in the message. */
        public void assertLinearizable() {
            if (!linearizable) {
                throw new AssertionError(detail);
            }
        }
    }

    /** Upper bound on states explored per partition, so a pathological history fails loudly. */
    private static final long STATE_BUDGET = 2_000_000L;

    /**
     * Checks a history against a model.
     *
     * @throws IllegalStateException if the search exceeds its budget, which means the history is too
     *     concurrent to check rather than that it is wrong
     */
    public static <S, I, O> Result check(Model<S, I, O> model, List<Op<I, O>> history) {
        Map<Object, List<Op<I, O>>> byPartition = new LinkedHashMap<>();
        for (Op<I, O> op : history) {
            byPartition.computeIfAbsent(op.partition(), k -> new ArrayList<>()).add(op);
        }

        long states = 0;
        for (Map.Entry<Object, List<Op<I, O>>> entry : byPartition.entrySet()) {
            Search<S, I, O> search = new Search<>(model, entry.getValue());
            Optional<String> failure = search.run();
            states += search.statesExplored;
            if (failure.isPresent()) {
                return new Result(
                        false,
                        "partition "
                                + entry.getKey()
                                + " is not linearizable: "
                                + failure.get(),
                        history.size(),
                        states);
            }
        }
        return Result.ok(history.size(), states);
    }

    /** Checks a single partition's subhistory, ignoring the partition field. */
    public static <S, I, O> Result checkWithoutDecomposition(Model<S, I, O> model, List<Op<I, O>> history) {
        Search<S, I, O> search = new Search<>(model, history);
        Optional<String> failure = search.run();
        return failure
                .map(detail -> new Result(false, detail, history.size(), search.statesExplored))
                .orElseGet(() -> Result.ok(history.size(), search.statesExplored));
    }

    /** The backtracking search over one partition. */
    private static final class Search<S, I, O> {

        /** A memoized dead end: this state with these operations left cannot be completed. */
        private record DeadEnd(Object state, BitSet remaining) {}

        private final Model<S, I, O> model;
        private final List<Op<I, O>> ops;
        private final Set<DeadEnd> visited = new HashSet<>();

        private long statesExplored;
        private int deepest;
        private List<Op<I, O>> deepestPrefix = List.of();

        Search(Model<S, I, O> model, List<Op<I, O>> ops) {
            this.model = model;
            this.ops = new ArrayList<>(ops);
            // A stable order keeps the search deterministic, which matters when a failure has to be
            // reproducible from a seed.
            this.ops.sort(Comparator.comparingLong(Op<I, O>::invoked).thenComparingInt(Op::id));
        }

        Optional<String> run() {
            BitSet remaining = new BitSet(ops.size());
            remaining.set(0, ops.size());
            if (linearize(model.initial(), remaining, new ArrayList<>())) {
                return Optional.empty();
            }
            return Optional.of(describeFailure());
        }

        /**
         * Tries to order the remaining operations.
         *
         * <p>Cost is driven by how many operations overlap rather than by how many there are. At each
         * step only operations that could come next are candidates, which for a workload where each
         * client has one request outstanding is a handful regardless of history length.
         *
         * @param remaining operations not yet placed; mutated and restored around each branch
         * @param placed the order chosen so far, kept for the failure report
         */
        private boolean linearize(S state, BitSet remaining, List<Op<I, O>> placed) {
            if (remaining.isEmpty()) {
                return true;
            }
            if (placed.size() > deepest) {
                deepest = placed.size();
                deepestPrefix = List.copyOf(placed);
            }
            if (++statesExplored > STATE_BUDGET) {
                throw new IllegalStateException(
                        "gave up after "
                                + STATE_BUDGET
                                + " states on a partition of "
                                + ops.size()
                                + " operations; the history is too concurrent to check");
            }
            if (!visited.add(new DeadEnd(state, (BitSet) remaining.clone()))) {
                // Already proved unreachable from here. Without this the search revisits the same
                // position along every permutation that reaches it, which is factorial.
                return false;
            }

            // An operation can only go next if nothing still pending completed before it was invoked.
            long earliestCompletion = Long.MAX_VALUE;
            for (int i = remaining.nextSetBit(0); i >= 0; i = remaining.nextSetBit(i + 1)) {
                earliestCompletion = Math.min(earliestCompletion, ops.get(i).completionBound());
            }

            for (int i = remaining.nextSetBit(0); i >= 0; i = remaining.nextSetBit(i + 1)) {
                Op<I, O> op = ops.get(i);
                if (op.invoked() > earliestCompletion) {
                    continue;
                }
                remaining.clear(i);
                placed.add(op);

                Optional<S> next =
                        op.outcomeKnown()
                                ? model.apply(state, op.input(), op.output())
                                : model.applyUnknown(state, op.input());
                if (next.isPresent() && linearize(next.get(), remaining, placed)) {
                    return true;
                }
                // The other possibility for a request that timed out: it never applied at all.
                // Dropping it is legitimate, and a checker that insisted on placing it would reject
                // correct systems.
                if (!op.outcomeKnown() && linearize(state, remaining, placed)) {
                    return true;
                }

                placed.remove(placed.size() - 1);
                remaining.set(i);
            }
            return false;
        }

        private String describeFailure() {
            StringBuilder sb = new StringBuilder();
            sb.append("no sequential order explains these ")
                    .append(ops.size())
                    .append(" operations.\n");
            sb.append("  longest prefix that could be linearized (")
                    .append(deepestPrefix.size())
                    .append(" of ")
                    .append(ops.size())
                    .append("):\n");
            for (Op<I, O> op : deepestPrefix) {
                sb.append("    ").append(op).append('\n');
            }
            Set<Integer> placedIds = new HashSet<>();
            for (Op<I, O> op : deepestPrefix) {
                placedIds.add(op.id());
            }
            sb.append("  could not place any of:\n");
            for (Op<I, O> op : ops) {
                if (!placedIds.contains(op.id())) {
                    sb.append("    ").append(op).append('\n');
                }
            }
            sb.append("  explored ").append(statesExplored).append(" states");
            return sb.toString();
        }
    }
}
