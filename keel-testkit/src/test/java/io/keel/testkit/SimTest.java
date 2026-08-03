package io.keel.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.kv.Commands;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The simulator: that it is deterministic, that it makes progress, and that safety holds. */
class SimTest {

    @Test
    @DisplayName("the same seed produces exactly the same run")
    void sameSeedSameRun() {
        // The property the whole approach rests on. If this fails, an unseeded Random or a hash
        // iteration has crept into the code under test and no reported seed means anything.
        Sim first = Sim.of(SimConfig.chaotic(5, 1234));
        Sim second = Sim.of(SimConfig.chaotic(5, 1234));

        workload(first, 600);
        workload(second, 600);

        assertEquals(first.traceHash(), second.traceHash(), "identical configs must produce identical runs");
        assertEquals(first.stats(), second.stats());
    }

    @Test
    @DisplayName("different seeds explore different schedules")
    void differentSeedsDiffer() {
        Sim a = Sim.of(SimConfig.chaotic(5, 1));
        Sim b = Sim.of(SimConfig.chaotic(5, 2));
        workload(a, 400);
        workload(b, 400);

        assertNotEquals(a.traceHash(), b.traceHash());
    }

    @Test
    @DisplayName("a healthy cluster elects a leader and commits")
    void quietClusterMakesProgress() {
        // Without this, every safety test below could be passing because nothing ever happened.
        Sim sim = Sim.of(SimConfig.quiet(3, 7));

        assertTrue(sim.runUntilLeader(200), "no leader was elected\n" + sim.describe());
        long index = -1;
        for (int i = 0; i < 50 && index < 0; i++) {
            index = sim.propose(Commands.put(Commands.NO_SESSION, "k", "v"));
            sim.step();
        }
        assertTrue(index > 0, "nothing could be proposed\n" + sim.describe());

        long committed = index;
        assertTrue(
                sim.runUntil(() -> sim.ids().stream().allMatch(id -> sim.appliedIndex(id) >= committed), 200),
                "not every replica applied the command\n" + sim.describe());
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5, 8, 13, 21, 42, 99, 256, 1001})
    @DisplayName("safety holds under partitions, crashes, drops, and duplicates")
    void safetyUnderChaos(long seed) {
        Sim sim = Sim.of(SimConfig.chaotic(5, seed));

        workload(sim, 1500);

        Sim.SimStats stats = sim.stats();
        // Assert the run was actually hostile, so a green result means the checks had something to
        // check rather than that the cluster sat idle.
        assertTrue(stats.messagesDropped() > 0, "no messages were dropped: " + stats);
        assertTrue(stats.invariantChecks() >= 1500, "invariants were not checked every step: " + stats);
        assertTrue(stats.proposals() > 0, "nothing was ever proposed: " + stats);
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {11, 22, 33, 44})
    @DisplayName("safety holds when faults barely heal")
    void safetyUnderBrutalFaults(long seed) {
        Sim sim = Sim.of(SimConfig.brutal(5, seed));

        workload(sim, 1500);

        assertTrue(sim.stats().crashes() > 0, "expected at least one crash: " + sim.stats());
    }

    @Test
    @DisplayName("committed data survives every node crashing at once")
    void committedDataSurvivesAFullRestart() {
        Sim sim = Sim.of(SimConfig.quiet(3, 31));
        assertTrue(sim.runUntilLeader(200));
        for (int i = 0; i < 10; i++) {
            sim.propose(Commands.put(Commands.NO_SESSION, "k" + i, "v" + i));
            sim.run(3);
        }
        long committedBefore = sim.views().stream().mapToLong(NodeView::commitIndex).max().orElseThrow();
        assertTrue(committedBefore > 0);

        // Everything dies. Only what was synced comes back, and the invariant checks carry their
        // memory of what was applied across the restart, so a replica that came back with different
        // content at a committed index would fail State Machine Safety.
        for (long id : sim.ids()) {
            sim.crash(id);
        }
        sim.run(5);
        for (long id : sim.ids()) {
            sim.restart(id);
        }

        assertTrue(sim.runUntilLeader(300), "the cluster did not recover\n" + sim.describe());
        assertTrue(
                sim.runUntil(
                        () -> sim.views().stream().anyMatch(v -> v.commitIndex() >= committedBefore), 300),
                "committed data did not survive the restart\n" + sim.describe());
    }

    @Test
    @DisplayName("a follower that misses a lot catches up when it returns")
    void crashedFollowerCatchesUp() {
        Sim sim = Sim.of(SimConfig.quiet(3, 77));
        assertTrue(sim.runUntilLeader(200));
        long leader = sim.leader().orElseThrow();
        long victim = sim.ids().stream().filter(id -> id != leader).findFirst().orElseThrow();

        sim.crash(victim);
        for (int i = 0; i < 30; i++) {
            sim.propose(Commands.put(Commands.NO_SESSION, "k" + i, "v" + i));
            sim.run(2);
        }
        long target = sim.appliedIndex(leader);
        sim.restart(victim);

        assertTrue(
                sim.runUntil(() -> sim.appliedIndex(victim) >= target, 400),
                "the follower never caught up\n" + sim.describe());
        assertEquals(
                sim.stateMachine(leader).size(),
                sim.stateMachine(victim).size(),
                "and its state machine should hold the same keys");
    }

    @Test
    @DisplayName("a linearizable read is only ever answered by a confirmed leader")
    void readsUnderPartition() {
        Sim sim = Sim.of(SimConfig.quiet(3, 5));
        assertTrue(sim.runUntilLeader(200));
        long leader = sim.leader().orElseThrow();
        sim.propose(Commands.put(Commands.NO_SESSION, "k", "v"));
        sim.run(5);

        sim.isolate(leader);
        sim.requestRead(leader, 1);
        sim.run(30);

        assertTrue(
                sim.drainReads(leader).isEmpty(),
                "an isolated leader must not be handed a read index\n" + sim.describe());
    }

    /** Proposes a command every few ticks, so the log keeps growing while faults happen. */
    private static void workload(Sim sim, int ticks) {
        int written = 0;
        for (int i = 0; i < ticks; i++) {
            if (i % 5 == 0) {
                long index =
                        sim.propose(Commands.put(Commands.NO_SESSION, "key-" + (written % 16), "value-" + written));
                if (index > 0) {
                    written++;
                }
            }
            if (i % 37 == 0) {
                List<Long> ids = List.copyOf(sim.ids());
                sim.requestRead(ids.get(i % ids.size()), i);
            }
            sim.step();
        }
    }
}
