package io.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.proto.log.Entry;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Log replication and the commit rule, paper sections 5.3 and 5.4.2. */
class ReplicationTest {

    @Test
    @DisplayName("a committed command reaches every node's state machine")
    void commandIsAppliedEverywhere() {
        TestCluster c = TestCluster.of(3).start();
        c.proposeOnLeader("alpha");
        c.proposeOnLeader("beta");
        c.tick(5);

        for (long id : c.ids()) {
            assertEquals(
                    List.of("alpha", "beta"),
                    c.appliedCommands(id),
                    "node " + id + " applied the wrong commands");
        }
        c.assertAppliedPrefixesAgree();
        c.assertLogsMatch();
    }

    @Test
    @DisplayName("a single-node cluster commits its own writes")
    void singleNodeCommitsWithoutReplies() {
        // A lone voter is its own majority, so nothing ever replies to it. maybeCommit used to run
        // only when a reply arrived, which meant a one-node cluster committed its election no-op and
        // then nothing else, forever. Found by writing a backup test against a single node.
        TestCluster c = TestCluster.of(1).start();
        c.node(1).campaign();
        c.settle();

        c.node(1).propose(bytes("alone"));
        c.settle();

        assertEquals(2, c.node(1).commitIndex(), "the no-op and the write should both be committed");
        assertEquals(List.of("alone"), c.appliedCommands(1));
    }

    @Test
    @DisplayName("a write offered to a follower is refused with a hint")
    void followerRefusesWrites() {
        TestCluster c = TestCluster.of(3).start();
        long leader = c.electLeader();
        long follower = c.ids().stream().filter(id -> id != leader).findFirst().orElseThrow();

        NotLeaderException e =
                assertThrows(NotLeaderException.class, () -> c.node(follower).propose(bytes("x")));
        assertEquals(leader, e.leaderHint(), "the hint should point at the current leader");
    }

    @Test
    @DisplayName("a follower that was down catches up on everything it missed")
    void laggingFollowerCatchesUp() {
        TestCluster c = TestCluster.of(3).start();
        long leader = c.electLeader();
        long lagging = c.ids().stream().filter(id -> id != leader).findFirst().orElseThrow();

        c.crash(lagging);
        for (int i = 0; i < 20; i++) {
            c.proposeOnLeader("v" + i);
        }
        c.restart(lagging);
        c.runUntil(
                () -> c.appliedCommands(lagging).size() == 20, 100, "the follower to catch up");

        assertEquals(c.appliedCommands(leader), c.appliedCommands(lagging));
        c.assertLogsMatch();
    }

    @Test
    @DisplayName("a divergent suffix on a follower is overwritten, not merged")
    void divergentSuffixIsOverwritten() {
        // Node 3 comes up with two entries that no majority ever accepted. They are at the same
        // indexes the new leader is about to use, so they have to go.
        TestCluster c =
                TestCluster.of(3).seedLog(1, 1, 1).seedLog(2, 1, 1).seedLog(3, 1, 1, 1, 1).start();
        c.crash(3);

        long leader = c.electLeader();
        c.proposeOnLeader("x");
        c.proposeOnLeader("y");

        c.restart(3);
        c.runUntil(
                () -> c.node(3).commitIndex() == c.node(leader).commitIndex(),
                100,
                "node 3 to be repaired");

        List<Entry> leaderLog = c.member(leader).store.durableEntries();
        List<Entry> repaired = c.member(3).store.durableEntries();
        assertEquals(leaderLog.size(), repaired.size(), "logs should be the same length");
        for (int i = 0; i < leaderLog.size(); i++) {
            assertEquals(
                    leaderLog.get(i).getTerm(),
                    repaired.get(i).getTerm(),
                    "term mismatch at index " + (i + 1));
            assertEquals(
                    leaderLog.get(i).getData(),
                    repaired.get(i).getData(),
                    "data mismatch at index " + (i + 1));
        }
        c.assertLogsMatch();
    }

    @Test
    @DisplayName("entries from an earlier term are not committed on a vote count alone (5.4.2)")
    void earlierTermEntriesNeedACurrentTermCommit() {
        // This is the figure 8 scenario. The acknowledgements below are synthesized because a real
        // cluster passes through this state in a single round trip, and the point is to hold it
        // still: a majority has stored the old entries, and they must still not be committed.
        RaftConfig cfg = RaftConfig.builder(1).voters(1, 2, 3).preVote(false).build();
        TestDriver d = new TestDriver(cfg, 7);
        d.store.append(List.of(Entries.normal(1, 1, bytes("a")), Entries.normal(2, 1, bytes("b"))));
        d.store.saveHardState(io.keel.proto.log.HardState.newBuilder().setTerm(1).build());
        d.store.sync();
        d.reopen();

        d.electSelf();
        assertEquals(2, d.raft.term());
        assertEquals(3, d.raft.lastIndex(), "the leader should have appended a no-op at index 3");
        assertEquals(0, d.raft.commitIndex());

        // Both followers confirm the two old entries, so a majority holds them.
        d.raft.step(new RaftMessage.AppendReply(2, 1, 2, true, 2, 0, 0));
        d.raft.step(new RaftMessage.AppendReply(3, 1, 2, true, 2, 0, 0));
        d.pump();
        assertEquals(
                0,
                d.raft.commitIndex(),
                "index 2 is from term 1; committing it on a count alone is what figure 8 forbids");

        // Now a follower confirms the leader's own no-op, which is from the current term.
        d.raft.step(new RaftMessage.AppendReply(2, 1, 2, true, 3, 0, 0));
        d.pump();
        assertEquals(
                3,
                d.raft.commitIndex(),
                "committing an entry from this term carries the earlier ones with it");
    }

    @Test
    @DisplayName("a leader that cannot reach a majority does not commit")
    void noQuorumMeansNoCommit() {
        TestCluster c = TestCluster.of(5).configure(b -> b.checkQuorum(false)).start();
        long leader = c.electLeader();
        long committedBefore = c.node(leader).commitIndex();

        for (long id : c.ids()) {
            if (id != leader) {
                c.isolate(id);
            }
        }
        c.node(leader).propose(bytes("lonely"));
        c.tick(30);

        assertEquals(
                committedBefore,
                c.node(leader).commitIndex(),
                "an entry only this node holds must not be committed");
        assertTrue(c.node(leader).lastIndex() > committedBefore, "but it is still in the log");
    }

    @Test
    @DisplayName("a leader refuses writes once too many entries are uncommitted")
    void backpressureWhenUncommittedGrows() {
        TestCluster c =
                TestCluster.of(3)
                        .configure(b -> b.maxUncommittedEntries(4).checkQuorum(false))
                        .start();
        long leader = c.electLeader();
        for (long id : c.ids()) {
            if (id != leader) {
                c.isolate(id);
            }
        }

        for (int i = 0; i < 4; i++) {
            c.node(leader).propose(bytes("v" + i));
        }
        assertThrows(
                ProposalDroppedException.class,
                () -> c.node(leader).propose(bytes("one too many")),
                "a leader with no quorum must stop accepting writes rather than buffer forever");
    }

    @Test
    @DisplayName("replication converges regardless of the order messages arrive in")
    void convergesUnderReordering() {
        TestCluster c = TestCluster.of(5, 12345L).start();
        long leader = c.electLeader();
        for (int i = 0; i < 10; i++) {
            c.node(leader).propose(bytes("v" + i));
            c.settleShuffled();
        }
        c.runUntil(() -> c.appliedCommands(leader).size() == 10, 100, "everything to commit");
        c.tick(10);

        for (long id : c.ids()) {
            assertEquals(c.appliedCommands(leader), c.appliedCommands(id), "node " + id);
        }
        c.assertLogsMatch();
        c.assertAppliedPrefixesAgree();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
