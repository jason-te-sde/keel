package io.keel.testkit;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.kv.Commands;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Seeds that once failed, replayed on every run.
 *
 * <p>A soak sweep is only worth running if what it finds stays found. Each row here is a seed that
 * produced a violation, kept with a note saying what it caught, so that removing the fix fails the
 * fast suite rather than the nightly job.
 */
class RegressionSeedTest {

    @ParameterizedTest(name = "seed {0} ({2})")
    @CsvSource({
        // InstallSnapshot discarded the receiver's whole log, including entries above the snapshot
        // boundary that it had already acknowledged. A committed entry then survived on fewer than a
        // majority, and a node without it won the next election. Paper figure 13, step 6.
        "1695, brutal, install-snapshot discarded acknowledged entries",
        // A heartbeat's commit index was clamped to the receiver's own last index, so a node committed
        // whichever entry happened to sit there instead of the one the leader meant. Paper section 5.3.
        "1537, brutal, heartbeat committed an unmatched entry",
        // Snapshot metadata took its boundary index and its term from different entries. The term it
        // advertised then failed the receiver's match check, which discarded acknowledged entries.
        "2626, chaotic, snapshot metadata misdescribed its own boundary",
    })
    @DisplayName("a seed that once produced a violation still passes")
    void replay(long seed, String profile, String what) {
        SimConfig config =
                "brutal".equals(profile) ? SimConfig.brutal(5, seed) : SimConfig.chaotic(5, seed);
        Sim sim = Sim.of(config);

        int written = 0;
        for (int i = 0; i < 1200; i++) {
            if (i % 4 == 0) {
                long index =
                        sim.propose(Commands.put(Commands.NO_SESSION, "k" + (written % 32), "v" + written));
                if (index > 0) {
                    written++;
                }
            }
            sim.step();
        }

        // The invariant checks run inside step(), so reaching here is the assertion. This one only
        // confirms the run was substantial enough to have re-entered the code path.
        assertTrue(sim.stats().snapshotsTaken() > 0, "no snapshot was taken: " + sim.stats());
    }
}
