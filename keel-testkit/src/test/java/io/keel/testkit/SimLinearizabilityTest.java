package io.keel.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.testkit.linz.KvModel;
import io.keel.testkit.linz.Linearizability;
import io.keel.testkit.linz.Op;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The two halves of the test strategy pointed at the same run.
 *
 * <p>The invariant checks confirm the replicas agree with each other. This confirms that what the
 * clients were told could have happened at all, which is a different claim and the one a user of the
 * store actually cares about.
 */
class SimLinearizabilityTest {

    private static final KvModel MODEL = new KvModel();

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 7, 11, 13, 17})
    @DisplayName("client histories are linearizable under faults")
    void historiesAreLinearizable(long seed) {
        Sim sim = Sim.of(SimConfig.chaotic(5, seed));
        Workload workload = new Workload(sim, Workload.Config.defaults(), seed);

        List<Op<KvModel.In, KvModel.Out>> history = workload.run(1200);

        assertTrue(history.size() > 5, "the workload barely ran: " + history.size() + " operations");
        Linearizability.Result result = Linearizability.check(MODEL, history);
        assertTrue(result.linearizable(), result.detail());
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {21, 34})
    @DisplayName("histories stay linearizable when faults barely heal")
    void brutalHistoriesAreLinearizable(long seed) {
        Sim sim = Sim.of(SimConfig.brutal(5, seed));
        Workload workload = new Workload(sim, Workload.Config.defaults(), seed);

        List<Op<KvModel.In, KvModel.Out>> history = workload.run(1500);

        Linearizability.Result result = Linearizability.check(MODEL, history);
        assertTrue(result.linearizable(), result.detail());
    }

    @Test
    @DisplayName("the checker rejects a history produced by skipping the read index")
    void skippingTheReadIndexProducesAnUnlinearizableHistory() {
        // The test that makes every other result in this class mean something. If the checker cannot
        // detect a deliberately broken read path, its verdict on the correct one is worthless.
        //
        // The break is exactly the shortcut a naive implementation takes: read whatever this replica
        // holds right now, without asking the leader for a read index and without waiting for the
        // local state machine to reach it. That is the bug ReadIndex exists to prevent.
        boolean anyRejected = false;
        StringBuilder verdicts = new StringBuilder();

        for (long seed : new long[] {1, 2, 3, 4, 5, 6, 7, 8}) {
            Sim sim = Sim.of(SimConfig.chaotic(5, seed));
            Workload workload =
                    new Workload(
                            sim,
                            new Workload.Config(4, 3, 0.6, 60),
                            seed,
                            Workload.ReadMode.IGNORE_READ_INDEX);

            List<Op<KvModel.In, KvModel.Out>> history = workload.run(1000);
            Linearizability.Result result = Linearizability.check(MODEL, history);
            verdicts.append("  seed ").append(seed).append(": ").append(result.linearizable() ? "accepted" : "REJECTED").append('\n');
            if (!result.linearizable()) {
                anyRejected = true;
            }
        }

        assertTrue(
                anyRejected,
                "a read path that ignores the read index must produce a history the checker rejects, "
                        + "at least at some seed:\n"
                        + verdicts);
    }

    @Test
    @DisplayName("a quiet cluster produces a linearizable history with plenty of operations")
    void quietClusterHistory() {
        Sim sim = Sim.of(SimConfig.quiet(3, 99));
        Workload workload = new Workload(sim, new Workload.Config(4, 4, 0.5, 40), 99);

        List<Op<KvModel.In, KvModel.Out>> history = workload.run(1200);

        // With no faults the workload should complete a healthy number of operations, which is what
        // makes a positive verdict worth having.
        assertTrue(history.size() > 40, "only " + history.size() + " operations completed");
        assertTrue(history.stream().anyMatch(op -> op.input() instanceof KvModel.In.Get));
        assertTrue(history.stream().anyMatch(op -> op.input() instanceof KvModel.In.Put));
        // The only unresolved operations should be the ones still outstanding when the run ended,
        // which is at most one per client.
        long unknown = history.stream().filter(op -> !op.outcomeKnown()).count();
        assertTrue(unknown <= 4, unknown + " operations had an unknown outcome in a quiet cluster");
        Linearizability.check(MODEL, history).assertLinearizable();
    }
}
