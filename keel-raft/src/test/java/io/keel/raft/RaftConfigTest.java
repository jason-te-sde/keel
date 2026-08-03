package io.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Configuration validation.
 *
 * <p>These are cheap tests for a reason worth stating: a heartbeat interval accidentally set above
 * the election timeout produces a cluster that elects a new leader every few ticks and never makes
 * progress, and the symptom looks nothing like the cause.
 */
class RaftConfigTest {

    @Test
    @DisplayName("quorum is a strict majority")
    void quorumSizes() {
        assertEquals(1, RaftConfig.builder(1).voters(1).build().quorum());
        assertEquals(2, RaftConfig.builder(1).voters(1, 2).build().quorum());
        assertEquals(2, RaftConfig.builder(1).voters(1, 2, 3).build().quorum());
        assertEquals(3, RaftConfig.builder(1).voters(1, 2, 3, 4).build().quorum());
        assertEquals(3, RaftConfig.builder(1).voters(1, 2, 3, 4, 5).build().quorum());
    }

    @Test
    @DisplayName("a node must be one of its own voters")
    void nodeMustBeAVoter() {
        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> RaftConfig.builder(9).voters(1, 2, 3).build());
        assertEquals(true, e.getMessage().contains("must contain this node"), e.getMessage());
    }

    @Test
    @DisplayName("the voter set is copied, so a later change cannot reconfigure a running node")
    void votersAreCopied() {
        Set<Long> mutable = new java.util.HashSet<>(Set.of(1L, 2L, 3L));
        RaftConfig cfg = RaftConfig.builder(1).voters(mutable).build();

        mutable.add(4L);

        assertEquals(3, cfg.initialVoters().size());
    }

    @ParameterizedTest(name = "election={0} heartbeat={1} is rejected")
    @CsvSource({"1, 1", "5, 5", "2, 10"})
    @DisplayName("the election timeout must exceed the heartbeat interval")
    void heartbeatMustBeShorterThanElection(int election, int heartbeat) {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        RaftConfig.builder(1)
                                .electionTimeoutTicks(election)
                                .heartbeatTicks(heartbeat)
                                .build());
    }

    @Test
    @DisplayName("non-positive limits are rejected")
    void limitsMustBePositive() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RaftConfig.builder(1).maxEntriesPerAppend(0).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> RaftConfig.builder(1).maxBytesPerAppend(0).build());
        assertThrows(
                IllegalArgumentException.class,
                () -> RaftConfig.builder(1).maxUncommittedEntries(0).build());
        assertThrows(IllegalArgumentException.class, () -> RaftConfig.builder(0).build());
    }
}
