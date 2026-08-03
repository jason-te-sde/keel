package io.keel.testkit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.kv.Commands;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Single-node membership changes, paper section 4.3.
 *
 * <p>Exercised through the simulator rather than with hand-fed messages, because the interesting part
 * is what the rest of the cluster does afterwards: whether the new voter is counted, whether the quorum
 * shrinks, and whether the change survives a restart. All of the safety invariants are checked after
 * every step of these runs, which is the actual assertion.
 */
class MembershipTest {

    @Test
    @DisplayName("a new voter is added, catches up, and counts toward the quorum")
    void addAVoter() {
        // Four nodes exist; three are voters. The fourth is what a node waiting to join looks like.
        Sim sim = Sim.of(SimConfig.quiet(4, 11).withInitialVoters(3));
        assertTrue(sim.runUntilLeader(200));
        for (int i = 0; i < 5; i++) {
            sim.propose(Commands.put(Commands.NO_SESSION, "k" + i, "v" + i));
            sim.run(2);
        }
        assertEquals(Set.of(1L, 2L, 3L), sim.voters(sim.leader().orElseThrow()));

        assertTrue(sim.addVoter(4) > 0, "the change should have been accepted");
        assertTrue(
                sim.runUntil(() -> sim.voters(sim.leader().orElseThrow()).contains(4L), 300),
                "the leader never applied the change\n" + sim.describe());
        assertTrue(
                sim.runUntil(() -> sim.stateMachine(4).size() == sim.stateMachine(1).size(), 400),
                "the new voter never caught up\n" + sim.describe());

        // Every node should agree on the membership once the entry has been applied everywhere.
        assertTrue(
                sim.runUntil(() -> sim.ids().stream().allMatch(id -> sim.voters(id).contains(4L)), 300),
                "not every node applied the change\n" + sim.describe());
    }

    @Test
    @DisplayName("a removed voter stops counting, and the quorum shrinks with it")
    void removeAVoter() {
        Sim sim = Sim.of(SimConfig.quiet(5, 12));
        assertTrue(sim.runUntilLeader(200));
        long leader = sim.leader().orElseThrow();
        long victim = sim.ids().stream().filter(id -> id != leader).findFirst().orElseThrow();

        assertTrue(sim.removeVoter(victim) > 0);
        assertTrue(
                sim.runUntil(() -> !sim.voters(leader).contains(victim), 300),
                "the leader never applied the removal\n" + sim.describe());

        // Four voters remain, so a quorum is three. Killing the removed node changes nothing, and
        // writes must keep committing.
        sim.crash(victim);
        long index = -1;
        for (int i = 0; i < 40 && index < 0; i++) {
            index = sim.propose(Commands.put(Commands.NO_SESSION, "after", "removal"));
            sim.step();
        }
        assertTrue(index > 0, "writes should continue without the removed node\n" + sim.describe());
        long committed = index;
        assertTrue(
                sim.runUntil(() -> sim.appliedIndex(leader) >= committed, 200),
                "the write never committed\n" + sim.describe());
    }

    @Test
    @DisplayName("a leader that removes itself steps down")
    void leaderRemovesItself() {
        Sim sim = Sim.of(SimConfig.quiet(3, 13));
        assertTrue(sim.runUntilLeader(200));
        long leader = sim.leader().orElseThrow();

        assertTrue(sim.removeVoter(leader) > 0);

        // It cannot keep leading a cluster it is not a member of: it could not count its own vote
        // toward anything.
        assertTrue(
                sim.runUntil(() -> sim.leader().isPresent() && sim.leader().getAsLong() != leader, 400),
                "the cluster never replaced the departing leader\n" + sim.describe());
        assertFalse(sim.voters(sim.leader().orElseThrow()).contains(leader));
    }

    @Test
    @DisplayName("a second change is refused while the first is unapplied")
    void oneChangeAtATime() {
        // Two changes in flight can produce two disjoint majorities, which is how a cluster elects two
        // leaders in one term. Refusing is what makes single-node changes safe without joint consensus.
        Sim sim = Sim.of(SimConfig.quiet(5, 14).withInitialVoters(3));
        assertTrue(sim.runUntilLeader(200));

        assertTrue(sim.addVoter(4) > 0);
        assertEquals(-1, sim.addVoter(5), "the second change must be refused while the first is pending");

        assertTrue(
                sim.runUntil(() -> sim.voters(sim.leader().orElseThrow()).contains(4L), 300),
                "the first change never applied\n" + sim.describe());
        assertTrue(sim.addVoter(5) > 0, "and allowed once the first has been applied");
    }

    @Test
    @DisplayName("membership survives a crash and comes back from the log")
    void membershipSurvivesRestart() {
        Sim sim = Sim.of(SimConfig.quiet(4, 15).withInitialVoters(3).withoutSnapshots());
        assertTrue(sim.runUntilLeader(200));
        assertTrue(sim.addVoter(4) > 0);
        assertTrue(
                sim.runUntil(() -> sim.ids().stream().allMatch(id -> sim.voters(id).contains(4L)), 300),
                "the change never reached everyone\n" + sim.describe());

        long victim = 2;
        sim.crash(victim);
        sim.run(5);
        sim.restart(victim);
        sim.run(30);

        // Replaying the log has to reproduce the membership, or this node disagrees with the cluster
        // about who can vote.
        assertTrue(
                sim.voters(victim).contains(4L),
                "the restarted node forgot the membership change: " + sim.voters(victim));
    }

    @Test
    @DisplayName("membership travels with a snapshot")
    void membershipTravelsWithSnapshots() {
        // A node catching up from a snapshot cannot learn the configuration from the log: the entries
        // that carried the changes are exactly what the snapshot replaced.
        Sim sim = Sim.of(SimConfig.quiet(4, 16).withInitialVoters(3));
        assertTrue(sim.runUntilLeader(200));
        assertTrue(sim.addVoter(4) > 0);
        assertTrue(sim.runUntil(() -> sim.voters(1).contains(4L), 300));

        long victim = 4;
        sim.crash(victim);
        for (int i = 0; i < 60; i++) {
            sim.propose(Commands.put(Commands.NO_SESSION, "k" + i, "v" + i));
            sim.run(2);
        }
        assertTrue(sim.snapshotIndex(sim.leader().orElseThrow()) > 0, "the leader should have compacted");
        sim.restart(victim);

        assertTrue(
                sim.runUntil(() -> sim.snapshotIndex(victim) > 0, 600),
                "the node never received a snapshot\n" + sim.describe());
        assertTrue(
                sim.voters(victim).contains(4L),
                "the snapshot did not carry the membership: " + sim.voters(victim));
    }
}
