package io.keel.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.kv.Commands;
import java.util.Locale;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The wide seed sweep.
 *
 * <p>The per-pull-request suite runs a fixed dozen seeds so it stays under a second. That is enough to
 * catch a regression but not enough to find a rare interleaving, which is what this is for. It runs a
 * small number of seeds by default and a large number when asked:
 *
 * <pre>
 *   mvn test -Dkeel.sim.seeds=2000 -Dtest=SoakTest -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>A failure prints the seed, and that seed reproduces the run exactly, so the first step of any
 * diagnosis is a single-seed rerun rather than an attempt to make the failure happen again.
 */
class SoakTest {

    private static final int DEFAULT_SEEDS = 30;
    private static final int TICKS_PER_SEED = 1200;

    @Test
    @DisplayName("safety holds across a sweep of seeds")
    void sweep() {
        int seeds = Integer.getInteger("keel.sim.seeds", DEFAULT_SEEDS);
        long start = System.nanoTime();
        long totalTicks = 0;
        long totalChecks = 0;
        long committed = 0;
        long snapshots = 0;

        for (int seed = 1; seed <= seeds; seed++) {
            // Alternate profiles: chaotic makes progress often enough to build long logs, brutal
            // spends most of its time partitioned and is where liveness edge cases surface.
            SimConfig config =
                    (seed % 2 == 0)
                            ? SimConfig.chaotic(5, seed)
                            : SimConfig.brutal(5, seed);
            Sim sim = Sim.of(config);

            int written = 0;
            for (int i = 0; i < TICKS_PER_SEED; i++) {
                if (i % 4 == 0) {
                    long index =
                            sim.propose(
                                    Commands.put(
                                            Commands.NO_SESSION, "k" + (written % 32), "v" + written));
                    if (index > 0) {
                        written++;
                    }
                }
                sim.step();
            }

            Sim.SimStats stats = sim.stats();
            totalTicks += stats.ticks();
            totalChecks += stats.invariantChecks();
            snapshots += stats.snapshotsTaken();
            committed += sim.views().stream().mapToLong(NodeView::commitIndex).max().orElse(0);
        }

        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf(
                Locale.ROOT,
                "soak: %d seeds, %,d ticks, %,d invariant checks, %,d snapshots in %.1fs (%,.0f ticks/s)%n",
                seeds,
                totalTicks,
                totalChecks,
                snapshots,
                seconds,
                totalTicks / seconds);

        // A sweep where nothing ever committed would pass every safety check for the wrong reason.
        assertTrue(committed > 0, "no seed in the sweep committed anything");
        // A sweep where nothing ever compacted would leave the snapshot paths untested.
        assertTrue(snapshots > 0, "no seed in the sweep took a snapshot");
    }
}
