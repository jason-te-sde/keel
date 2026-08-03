package io.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Linearizable reads, paper section 6.4.
 *
 * <p>The interesting cases are all about what must <em>not</em> be answered. A read that comes back
 * with a plausible-looking index from a leader that has already been replaced is the exact bug this
 * mechanism exists to prevent, and it is invisible unless a test partitions the leader and checks
 * that nothing was served.
 */
class ReadIndexTest {

    @Test
    @DisplayName("a leader answers a read once a quorum has confirmed it is still the leader")
    void leaderServesReadAfterQuorumConfirms() {
        TestCluster c = TestCluster.of(3).start();
        long leader = c.electLeader();
        long committed = c.node(leader).commitIndex();

        c.node(leader).requestRead(77);
        c.settle();

        assertEquals(1, c.reads(leader).size(), "expected exactly one answer\n" + c.describe());
        ReadState state = c.reads(leader).get(0);
        assertEquals(77, state.requestId());
        assertEquals(committed, state.readIndex());
    }

    @Test
    @DisplayName("a read is answered at an index that includes every acknowledged write")
    void readIndexCoversPriorWrites() {
        TestCluster c = TestCluster.of(3).start();
        long leader = c.electLeader();
        c.proposeOnLeader("a");
        long writeIndex = c.node(leader).commitIndex();

        c.node(leader).requestRead(1);
        c.settle();

        assertEquals(
                writeIndex,
                c.reads(leader).get(0).readIndex(),
                "a read must not be given an index behind a write that already returned");
    }

    @Test
    @DisplayName("reads append nothing to the log")
    void readsDoNotGrowTheLog() {
        TestCluster c = TestCluster.of(3).start();
        long leader = c.electLeader();
        long lastIndex = c.node(leader).lastIndex();

        for (int i = 0; i < 25; i++) {
            c.node(leader).requestRead(i);
            c.settle();
        }

        assertEquals(25, c.reads(leader).size());
        assertEquals(
                lastIndex,
                c.node(leader).lastIndex(),
                "ReadIndex confirms leadership with heartbeats, so the log must not grow");
    }

    @Test
    @DisplayName("a leader that cannot reach a quorum answers nothing")
    void partitionedLeaderAnswersNothing() {
        // The bug this whole mechanism exists to prevent. Without the quorum round, this leader would
        // happily return its own commit index and the value that goes with it, which a newer leader
        // may already have overwritten.
        TestCluster c = TestCluster.of(3).configure(b -> b.checkQuorum(false)).start();
        long leader = c.electLeader();
        for (long id : c.ids()) {
            if (id != leader) {
                c.isolate(id);
            }
        }

        c.node(leader).requestRead(5);
        c.tick(50);

        assertTrue(
                c.reads(leader).isEmpty(),
                "a deposed or unreachable leader must not serve a read\n" + c.describe());
    }

    @Test
    @DisplayName("a read taken before the leader commits its no-op waits for it")
    void readBeforeTheTermIsEstablishedWaits() {
        // A fresh leader does not know its own committed prefix until one of its own entries commits.
        // Answering from the commit index it inherited would be answering from an unknown state.
        RaftConfig cfg = RaftConfig.builder(1).voters(1, 2, 3).preVote(false).build();
        TestDriver d = new TestDriver(cfg, 3);
        d.raft.campaign();
        d.pump();
        d.raft.step(new RaftMessage.VoteReply(2, 1, 1, false, true));
        d.pump();
        assertEquals(Role.LEADER, d.raft.role());
        assertEquals(0, d.raft.commitIndex(), "the no-op is appended but not yet committed");

        d.raft.requestRead(42);
        d.pump();
        assertTrue(d.reads.isEmpty(), "there is no safe index to give out yet");

        // A follower stores the no-op, which commits it and releases the held read into a round.
        d.raft.step(new RaftMessage.AppendReply(2, 1, 1, true, 1, 0, 0));
        List<RaftMessage> afterCommit = d.pump();
        assertTrue(d.reads.isEmpty(), "released, but still waiting on its own quorum round");

        d.raft.step(new RaftMessage.HeartbeatReply(2, 1, 1, readRoundOf(afterCommit)));
        d.pump();

        assertEquals(1, d.reads.size(), "the read should be answered once the round is confirmed");
        assertEquals(42, d.reads.get(0).requestId());
        assertEquals(1, d.reads.get(0).readIndex());
    }

    @Test
    @DisplayName("a heartbeat response from an older round does not answer a newer read")
    void staleRoundDoesNotConfirmANewerRead() {
        // Why heartbeats carry a round token at all. If any response could count toward the round in
        // flight, a leader could satisfy a read using confirmation it obtained before that read even
        // existed, which is confirmation of nothing.
        RaftConfig cfg = RaftConfig.builder(1).voters(1, 2, 3).preVote(false).build();
        TestDriver d = new TestDriver(cfg, 4);
        d.electSelf();
        d.raft.step(new RaftMessage.AppendReply(2, 1, 1, true, 1, 0, 0));
        d.pump();

        d.raft.requestRead(1);
        List<RaftMessage> firstRound = d.pump();
        long roundOne = readRoundOf(firstRound);
        d.raft.step(new RaftMessage.HeartbeatReply(2, 1, 1, roundOne));
        d.pump();
        assertEquals(1, d.reads.size());

        d.raft.requestRead(2);
        d.pump();
        // Replay the previous round's token. It is a real response from a real follower, just not for
        // this round.
        d.raft.step(new RaftMessage.HeartbeatReply(2, 1, 1, roundOne));
        d.pump();

        assertEquals(1, d.reads.size(), "the second read must still be waiting for its own round");
    }

    @Test
    @DisplayName("a follower forwards a read and answers it from its own state machine")
    void followerForwardsTheRequest() {
        TestCluster c = TestCluster.of(3).start();
        long leader = c.electLeader();
        c.proposeOnLeader("value");
        long follower = c.ids().stream().filter(id -> id != leader).findFirst().orElseThrow();

        c.node(follower).requestRead(9);
        c.settle();

        assertEquals(1, c.reads(follower).size(), "the follower should have been given an index");
        ReadState state = c.reads(follower).get(0);
        assertEquals(9, state.requestId());
        assertTrue(
                state.readIndex() >= c.node(leader).commitIndex(),
                "the index must cover everything committed when the leader answered");
        assertTrue(c.reads(leader).isEmpty(), "the leader answers the forwarder, not itself");
    }

    @Test
    @DisplayName("a follower with no known leader has nobody to ask")
    void followerWithoutALeaderFails() {
        TestCluster c = TestCluster.of(3).start();

        NotLeaderException e =
                assertThrows(NotLeaderException.class, () -> c.node(1).requestRead(1));
        assertEquals(0, e.leaderHint());
    }

    @Test
    @DisplayName("a single-node cluster is its own quorum")
    void singleNodeNeedsNoRound() {
        TestCluster c = TestCluster.of(1).start();
        c.node(1).campaign();
        c.settle();

        c.node(1).requestRead(1);
        c.settle();

        assertEquals(1, c.reads(1).size());
        assertEquals(c.node(1).commitIndex(), c.reads(1).get(0).readIndex());
    }

    @Test
    @DisplayName("a read in flight is abandoned rather than answered late")
    void readsAreDroppedOnStepDown() {
        // Check-quorum is off so the old leader keeps believing it is one, which is the dangerous
        // shape: it has a read outstanding, it is about to be replaced, and the answer must never
        // arrive.
        TestCluster c = TestCluster.of(3).configure(b -> b.checkQuorum(false)).start();
        long old = c.electLeader();

        c.isolate(old);
        c.node(old).requestRead(1);
        c.tick(5);
        assertTrue(c.reads(old).isEmpty(), "an isolated leader gets no confirmation");

        c.runUntil(
                () -> c.ids().stream().anyMatch(id -> id != old && c.node(id).role() == Role.LEADER),
                300,
                "the majority side to elect a replacement");
        c.heal();
        c.runUntil(() -> c.node(old).role() != Role.LEADER, 100, "the old leader to step down");
        c.tick(20);

        assertFalse(
                c.reads(old).stream().anyMatch(r -> r.requestId() == 1),
                "a read must never be answered by a node that has lost leadership");
    }

    private static long readRoundOf(List<RaftMessage> messages) {
        for (RaftMessage m : messages) {
            if (m instanceof RaftMessage.Heartbeat h && h.readSeq() != 0) {
                return h.readSeq();
            }
        }
        throw new AssertionError("no read-carrying heartbeat was sent: " + messages);
    }
}
