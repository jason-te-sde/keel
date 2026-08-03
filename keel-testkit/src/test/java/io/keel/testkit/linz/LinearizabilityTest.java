package io.keel.testkit.linz;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the checker.
 *
 * <p>The important half is the negative cases. A checker that accepts everything passes every
 * positive test ever written, so each broken history here is one a real bug produces: a read that
 * returns a value the timeline does not allow, a write that vanishes, a compare-and-swap that reports
 * the wrong outcome.
 */
class LinearizabilityTest {

    private static final KvModel MODEL = new KvModel();

    @Test
    @DisplayName("a sequential history is linearizable")
    void sequentialHistory() {
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(put(1, "a", "1", 0, 1), get(2, "a", "1", 2, 3), get(3, "a", "1", 4, 5));

        Linearizability.Result result = Linearizability.check(MODEL, history);

        assertTrue(result.linearizable(), result.detail());
        assertEquals(3, result.operationsChecked());
    }

    @Test
    @DisplayName("concurrent operations can be ordered either way")
    void concurrentHistory() {
        // Two writes overlap, then a read sees one of them. Both orders are legal, and the checker only
        // has to find one.
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(put(1, "a", "1", 0, 10), put(2, "a", "2", 1, 11), get(3, "a", "1", 12, 13));

        assertTrue(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("a read of a value nothing wrote is rejected")
    void inventedValueRejected() {
        List<Op<KvModel.In, KvModel.Out>> history = List.of(put(1, "a", "1", 0, 1), get(2, "a", "9", 2, 3));

        Linearizability.Result result = Linearizability.check(MODEL, history);

        assertFalse(result.linearizable());
        assertTrue(result.detail().contains("key-a") || result.detail().contains("a"), result.detail());
    }

    @Test
    @DisplayName("a stale read is rejected")
    void staleReadRejected() {
        // The bug ReadIndex exists to prevent, in history form. The write completed before the read
        // began, so no ordering allows the read to return the older value.
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(put(1, "a", "old", 0, 1), put(2, "a", "new", 2, 3), get(3, "a", "old", 4, 5));

        assertFalse(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("a lost write is rejected")
    void lostWriteRejected() {
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(put(1, "a", "1", 0, 1), get(2, "a", "1", 2, 3), get(3, "a", null, 4, 5));

        assertFalse(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("a compare-and-swap reporting the wrong outcome is rejected")
    void wrongCasOutcomeRejected() {
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(
                        put(1, "a", "1", 0, 1),
                        // Claims to have applied while expecting a value the key does not hold.
                        new Op<>(
                                2,
                                "a",
                                new KvModel.In.Cas(bytes("wrong"), false, bytes("2")),
                                new KvModel.Out.Applied(true),
                                2,
                                3));

        assertFalse(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("an operation with an unknown outcome may have applied")
    void unknownOutcomeMayHaveApplied() {
        // The client timed out on the write, then read the value back. That is consistent only if the
        // write is allowed to have applied despite the timeout.
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(
                        new Op<>(1, "a", new KvModel.In.Put(bytes("1")), new KvModel.Out.Ok(), 0, Op.UNKNOWN),
                        get(2, "a", "1", 5, 6));

        assertTrue(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("an operation with an unknown outcome may not have applied")
    void unknownOutcomeMayNotHaveApplied() {
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(
                        new Op<>(1, "a", new KvModel.In.Put(bytes("1")), new KvModel.Out.Ok(), 0, Op.UNKNOWN),
                        get(2, "a", null, 5, 6));

        assertTrue(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("an unknown outcome does not excuse an impossible read")
    void unknownOutcomeIsNotAWildcard() {
        // Being generous about timeouts must not become being generous about everything.
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(
                        new Op<>(1, "a", new KvModel.In.Put(bytes("1")), new KvModel.Out.Ok(), 0, Op.UNKNOWN),
                        get(2, "a", "something-else", 5, 6));

        assertFalse(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("real time order is respected")
    void realTimeOrderIsRespected() {
        // The write completes at 1 and the read starts at 2, so the read cannot be ordered first even
        // though returning absent would otherwise be consistent.
        List<Op<KvModel.In, KvModel.Out>> history = List.of(put(1, "a", "1", 0, 1), get(2, "a", null, 2, 3));

        assertFalse(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("operations on different keys do not constrain each other")
    void keysAreIndependent() {
        List<Op<KvModel.In, KvModel.Out>> history =
                List.of(put(1, "a", "1", 0, 1), get(2, "b", null, 0, 1), put(3, "b", "2", 2, 3), get(4, "a", "1", 2, 3));

        assertTrue(Linearizability.check(MODEL, history).linearizable());
    }

    @Test
    @DisplayName("decomposing by key gives the same verdict as not decomposing")
    void decompositionAgreesWithTheFullSearch() {
        // The compositionality argument, checked rather than assumed. If these ever disagreed, the
        // optimisation that makes the checker usable would be unsound.
        List<Op<KvModel.In, KvModel.Out>> good =
                List.of(put(1, "a", "1", 0, 2), get(2, "a", "1", 3, 4), put(3, "b", "9", 1, 5), get(4, "b", "9", 6, 7));
        List<Op<KvModel.In, KvModel.Out>> bad =
                List.of(put(1, "a", "1", 0, 2), get(2, "a", "wrong", 3, 4), put(3, "b", "9", 1, 5));

        assertEquals(
                Linearizability.check(MODEL, good).linearizable(),
                singleKeyVerdict(good),
                "verdicts must agree on a linearizable history");
        assertEquals(
                Linearizability.check(MODEL, bad).linearizable(),
                singleKeyVerdict(bad),
                "verdicts must agree on a broken history");
    }

    @Test
    @DisplayName("a wide concurrent history is still checkable")
    void memoizationKeepsTheSearchFeasible() {
        // Twelve overlapping writes on one key is 12! orderings. It completes because the search
        // memoizes on (state, remaining), not because the history is small.
        List<Op<KvModel.In, KvModel.Out>> history = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            history.add(put(i + 1, "a", "v" + i, 0, 100));
        }
        history.add(get(100, "a", "v11", 101, 102));

        Linearizability.Result result = Linearizability.check(MODEL, history);

        assertTrue(result.linearizable(), result.detail());
        assertTrue(result.statesExplored() < 500_000, "explored " + result.statesExplored() + " states");
    }

    /** Runs the same history with every operation on one partition, defeating decomposition. */
    private static boolean singleKeyVerdict(List<Op<KvModel.In, KvModel.Out>> history) {
        List<Op<KvModel.In, KvModel.Out>> perKey = new ArrayList<>();
        for (Op<KvModel.In, KvModel.Out> op : history) {
            if (op.partition().equals("a")) {
                perKey.add(op);
            }
        }
        return Linearizability.checkWithoutDecomposition(MODEL, perKey).linearizable();
    }

    private static Op<KvModel.In, KvModel.Out> put(
            int id, String key, String value, long invoked, long completed) {
        return new Op<>(id, key, new KvModel.In.Put(bytes(value)), new KvModel.Out.Ok(), invoked, completed);
    }

    private static Op<KvModel.In, KvModel.Out> get(
            int id, String key, String value, long invoked, long completed) {
        Optional<ByteString> observed = value == null ? Optional.empty() : Optional.of(bytes(value));
        return new Op<>(id, key, new KvModel.In.Get(), new KvModel.Out.Value(observed), invoked, completed);
    }

    private static ByteString bytes(String s) {
        return ByteString.copyFromUtf8(s);
    }
}
