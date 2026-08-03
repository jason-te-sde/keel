package io.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.proto.log.SnapshotMetadata;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Snapshots and log compaction, paper section 7.
 *
 * <p>The core's part is narrow on purpose: it decides when a follower needs a snapshot rather than
 * entries, and which boundary that snapshot establishes. Moving the payload is the driver's job,
 * because the core has no I/O. These tests cover the decisions, and the simulator covers what happens
 * when the payload actually moves.
 */
class SnapshotTest {

    @Test
    @DisplayName("a leader sends a snapshot when a follower needs entries it has discarded")
    void snapshotInsteadOfEntries() {
        RaftConfig cfg = RaftConfig.builder(1).voters(1, 2, 3).preVote(false).build();
        TestDriver d = new TestDriver(cfg, 1);
        d.electSelf();
        for (int i = 0; i < 5; i++) {
            d.raft.propose(("v" + i).getBytes(StandardCharsets.UTF_8));
        }
        d.pump();
        // Node 2 acknowledges, which is a majority with the leader, so everything commits and is safe
        // to compact. Node 3 has acknowledged nothing: it is the one that will need a snapshot.
        long last = d.raft.lastIndex();
        d.raft.step(new RaftMessage.AppendReply(2, 1, 1, true, last, 0, 0));
        d.pump();
        assertEquals(last, d.raft.commitIndex());

        d.store.compact(
                SnapshotMetadata.newBuilder().setLastIndex(last).setLastTerm(1).build());
        // Node 3 now reports it is far behind, at an index that no longer exists in the log.
        d.raft.step(new RaftMessage.AppendReply(3, 1, 1, false, 0, 1, 0));
        List<RaftMessage> sent = d.pump();

        List<RaftMessage.InstallSnapshot> snapshots =
                TestDriver.only(sent, RaftMessage.InstallSnapshot.class);
        assertEquals(1, snapshots.size(), "expected a snapshot to be sent, got " + sent);
        assertEquals(3, snapshots.get(0).to());
        assertEquals(last, snapshots.get(0).meta().getLastIndex());
        assertEquals(1, snapshots.get(0).meta().getLastTerm());
    }

    @Test
    @DisplayName("nothing further is sent to a follower while its snapshot is in flight")
    void snapshotPausesTheFollower() {
        RaftConfig cfg = RaftConfig.builder(1).voters(1, 2, 3).preVote(false).build();
        TestDriver d = new TestDriver(cfg, 2);
        d.electSelf();
        d.raft.propose("a".getBytes(StandardCharsets.UTF_8));
        d.pump();
        d.raft.step(new RaftMessage.AppendReply(2, 1, 1, true, d.raft.lastIndex(), 0, 0));
        d.pump();
        d.store.compact(
                SnapshotMetadata.newBuilder()
                        .setLastIndex(d.raft.commitIndex())
                        .setLastTerm(1)
                        .build());
        d.raft.step(new RaftMessage.AppendReply(3, 1, 1, false, 0, 1, 0));
        d.pump();

        // More writes while the snapshot is outstanding. Sending appends now would only be rejected,
        // and the rejections would rewind progress the snapshot is about to fix.
        d.raft.propose("b".getBytes(StandardCharsets.UTF_8));
        List<RaftMessage> sent = d.pump();

        assertTrue(
                TestDriver.only(sent, RaftMessage.Append.class).stream().noneMatch(a -> a.to() == 3),
                "node 3 should be paused while its snapshot is in flight: " + sent);
    }

    @Test
    @DisplayName("a follower accepting a snapshot reports it for installation before acknowledging")
    void followerRestoresFromSnapshot() {
        RaftConfig cfg = RaftConfig.builder(2).voters(1, 2, 3).build();
        TestDriver d = new TestDriver(cfg, 3);
        SnapshotMetadata meta =
                SnapshotMetadata.newBuilder().setLastIndex(40).setLastTerm(4).build();

        d.raft.step(new RaftMessage.InstallSnapshot(1, 2, 4, meta));
        Ready ready = d.raft.ready();

        assertNotNull(ready.snapshotToInstall(), "the driver has to be told to install it");
        assertEquals(40, ready.snapshotToInstall().getLastIndex());
        assertEquals(40, d.raft.commitIndex(), "the boundary is committed by definition");
        assertEquals(4, d.raft.term());

        // Installing it, as a driver must, before acknowledging the batch.
        d.store.installSnapshot(meta);
        d.raft.advance(ready);
        assertNull(d.raft.ready().snapshotToInstall(), "it should not be offered twice");
        assertEquals(40, d.raft.lastIndex());
        assertEquals(4, d.raft.termAt(40), "the boundary's term has to survive, for the next append");
    }

    @Test
    @DisplayName("a snapshot older than what the follower already has is acknowledged, not applied")
    void staleSnapshotIsAcknowledged() {
        RaftConfig cfg = RaftConfig.builder(2).voters(1, 2, 3).build();
        TestDriver d = new TestDriver(cfg, 4);
        SnapshotMetadata newer =
                SnapshotMetadata.newBuilder().setLastIndex(50).setLastTerm(5).build();
        d.raft.step(new RaftMessage.InstallSnapshot(1, 2, 5, newer));
        Ready first = d.raft.ready();
        d.store.installSnapshot(newer);
        d.raft.advance(first);

        SnapshotMetadata older =
                SnapshotMetadata.newBuilder().setLastIndex(20).setLastTerm(2).build();
        d.raft.step(new RaftMessage.InstallSnapshot(1, 2, 5, older));
        Ready second = d.raft.ready();

        assertNull(second.snapshotToInstall(), "going backwards would throw state away");
        List<RaftMessage.InstallSnapshotReply> replies =
                TestDriver.only(second.messages(), RaftMessage.InstallSnapshotReply.class);
        assertEquals(1, replies.size());
        assertTrue(replies.get(0).success());
        assertEquals(
                50,
                replies.get(0).matchIndex(),
                "reporting the higher index stops the leader resending the same snapshot");
    }

    @Test
    @DisplayName("a follower whose snapshot covers prevLogIndex points the leader at its boundary")
    void appendBelowTheBoundaryIsRedirected() {
        // Without this the follower treats entries it has already committed as conflicting, and refuses
        // to continue because they sit at or below its commit index.
        MemoryLogStore store = new MemoryLogStore();
        SnapshotMetadata meta =
                SnapshotMetadata.newBuilder().setLastIndex(30).setLastTerm(3).build();
        store.installSnapshot(meta);
        RaftLog log = new RaftLog(store, 30);

        RaftLog.AppendOutcome outcome =
                log.maybeAppend(5, 1, 5, List.of(Entries.normal(6, 1, new byte[] {1})));

        assertFalse(outcome.accepted());
        assertEquals(31, outcome.conflictIndex(), "one past the snapshot boundary");
    }

    @Test
    @DisplayName("compaction refuses to run past the end of the log")
    void compactionIsBounded() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(List.of(Entries.normal(1, 1, new byte[] {1}), Entries.normal(2, 1, new byte[] {2})));

        assertThrows(
                IllegalArgumentException.class,
                () -> store.compact(SnapshotMetadata.newBuilder().setLastIndex(9).setLastTerm(1).build()));
    }

    @Test
    @DisplayName("compaction keeps the boundary term readable and the tail intact")
    void compactionKeepsWhatIsNeeded() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(
                List.of(
                        Entries.normal(1, 1, new byte[] {1}),
                        Entries.normal(2, 1, new byte[] {2}),
                        Entries.normal(3, 2, new byte[] {3}),
                        Entries.normal(4, 2, new byte[] {4})));
        store.sync();

        store.compact(SnapshotMetadata.newBuilder().setLastIndex(2).setLastTerm(1).build());

        assertEquals(3, store.firstIndex());
        assertEquals(4, store.lastIndex());
        // The term at the boundary is what the next AppendEntries needs for its prevLogTerm.
        assertEquals(1, store.term(2));
        assertEquals(2, store.term(3));
        assertThrows(RaftStorage.CompactedException.class, () -> store.term(1));
        assertEquals(2, store.entries(3, 5, Long.MAX_VALUE).size());
    }
}
