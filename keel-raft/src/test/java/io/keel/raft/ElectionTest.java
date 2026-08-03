package io.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Leader election: safety rules from paper sections 5.2 and 5.4.1, plus pre-vote from 9.6. */
class ElectionTest {

    @Test
    @DisplayName("a single-node cluster elects itself without sending anything")
    void singleNodeElectsItself() {
        TestCluster c = TestCluster.of(1).start();
        c.node(1).campaign();
        c.settle();

        assertSame(Role.LEADER, c.node(1).role());
        assertEquals(1, c.node(1).leaderId());
        // The no-op appended on election is committed immediately: one node is already a majority.
        assertEquals(1, c.node(1).commitIndex());
    }

    @Test
    @DisplayName("a three-node cluster elects exactly one leader")
    void threeNodesElectOneLeader() {
        TestCluster c = TestCluster.of(3).start();
        long leader = c.electLeader();

        assertTrue(leader != 0, "expected a leader\n" + c.describe());
        c.assertOneLeaderPerTerm();
        for (long id : c.ids()) {
            assertEquals(leader, c.node(id).leaderId(), "node " + id + " disagrees on the leader");
        }
    }

    @Test
    @DisplayName("a leader emerges without anyone being told to campaign")
    void electionHappensOnTimeoutAlone() {
        TestCluster c = TestCluster.of(5).start();
        c.runUntil(() -> c.leaderId() != 0, 300, "an election to happen on its own");
        c.assertOneLeaderPerTerm();
    }

    @Test
    @DisplayName("a node with a shorter log cannot win (election restriction, 5.4.1)")
    void staleNodeCannotWin() {
        // Nodes 1 and 2 have entries nobody would want to lose; node 3 has none.
        TestCluster c = TestCluster.of(3).seedLog(1, 1, 1, 1).seedLog(2, 1, 1, 1).start();

        c.node(3).campaign();
        c.settle();
        assertNotEquals(
                Role.LEADER, c.node(3).role(), "a node missing committed entries must not win");

        long leader = c.electLeader();
        assertTrue(leader == 1 || leader == 2, "leader should be a node with the full log");
    }

    @Test
    @DisplayName("a leader that loses its quorum steps down (check-quorum, 6.2)")
    void leaderWithoutQuorumStepsDown() {
        TestCluster c = TestCluster.of(3).start();
        long leader = c.electLeader();

        c.isolate(leader);
        c.tick(80);

        // It steps down to follower and then, still hearing nothing, starts campaigning. Any role
        // but leader satisfies the property under test: it has stopped answering as one.
        assertNotSame(
                Role.LEADER,
                c.node(leader).role(),
                "an unreachable leader must stop acting as one\n" + c.describe());
        assertEquals(
                0,
                c.node(leader).leaderId(),
                "and it should no longer claim to know of a leader\n" + c.describe());
    }

    @Test
    @DisplayName("the majority side elects a new leader when the old one is cut off")
    void majoritySideElectsNewLeader() {
        TestCluster c = TestCluster.of(3).start();
        long old = c.electLeader();
        long termBefore = c.node(old).term();

        c.isolate(old);
        c.runUntil(
                () -> {
                    for (long id : c.ids()) {
                        if (id != old && c.node(id).role() == Role.LEADER) {
                            return true;
                        }
                    }
                    return false;
                },
                200,
                "the majority side to elect a new leader");

        long fresh = 0;
        for (long id : c.ids()) {
            if (id != old && c.node(id).role() == Role.LEADER) {
                fresh = id;
            }
        }
        assertTrue(fresh != 0);
        assertTrue(
                c.node(fresh).term() > termBefore,
                "a new leader must be in a later term than the one it replaced");
    }

    @Test
    @DisplayName("a deposed leader steps down once it can talk to the cluster again")
    void deposedLeaderStepsDownAfterHeal() {
        TestCluster c = TestCluster.of(3).start();
        long old = c.electLeader();

        c.isolate(old);
        c.runUntil(
                () -> {
                    for (long id : c.ids()) {
                        if (id != old && c.node(id).role() == Role.LEADER) {
                            return true;
                        }
                    }
                    return false;
                },
                200,
                "a replacement leader");
        c.heal();
        c.tick(20);

        assertSame(Role.FOLLOWER, c.node(old).role(), "the old leader should have stepped down");
        c.assertOneLeaderPerTerm();
        c.assertLogsMatch();
    }

    @Nested
    @DisplayName("pre-vote")
    class PreVote {

        @Test
        @DisplayName("keeps an isolated node from raising its term")
        void isolatedNodeDoesNotRaiseTerm() {
            TestCluster c = TestCluster.of(3).configure(b -> b.preVote(true)).start();
            long leader = c.electLeader();
            long stableTerm = c.node(leader).term();
            long follower = firstFollower(c, leader);

            c.isolate(follower);
            c.tick(200);

            assertEquals(
                    stableTerm,
                    c.node(follower).term(),
                    "a node that cannot win an election must not keep incrementing its term");
        }

        @Test
        @DisplayName("is what prevents that, shown by turning it off")
        void withoutPreVoteTheTermRunsAway() {
            TestCluster c = TestCluster.of(3).configure(b -> b.preVote(false)).start();
            long leader = c.electLeader();
            long stableTerm = c.node(leader).term();
            long follower = firstFollower(c, leader);

            c.isolate(follower);
            c.tick(200);

            assertTrue(
                    c.node(follower).term() > stableTerm + 5,
                    "without pre-vote an isolated node climbs terms; got "
                            + c.node(follower).term()
                            + " against a cluster at "
                            + stableTerm);
        }

        @Test
        @DisplayName("does not stop a legitimate election")
        void preVoteStillAllowsElections() {
            TestCluster c = TestCluster.of(3).configure(b -> b.preVote(true)).start();
            assertTrue(c.electLeader() != 0);
        }
    }

    @Test
    @DisplayName("vote requests are emitted in ascending recipient order")
    void voteRequestsAreOrdered() {
        // Guards the same property from the other side: given sorted membership, the messages a
        // campaign produces have to come out in a defined order for a simulated run to be replayable.
        RaftConfig cfg = RaftConfig.builder(1).voters(java.util.Set.of(5L, 1L, 3L, 9L)).preVote(false).build();
        TestDriver d = new TestDriver(cfg, 11);

        d.raft.campaign();
        java.util.List<Long> recipients =
                TestDriver.only(d.pump(), RaftMessage.Vote.class).stream().map(RaftMessage.Vote::to).toList();

        assertEquals(java.util.List.of(3L, 5L, 9L), recipients);
    }

    private static long firstFollower(TestCluster c, long leader) {
        for (long id : c.ids()) {
            if (id != leader) {
                return id;
            }
        }
        throw new IllegalStateException("no follower");
    }
}
