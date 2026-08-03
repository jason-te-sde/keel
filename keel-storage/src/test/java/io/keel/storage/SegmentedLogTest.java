package io.keel.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.proto.log.Entry;
import io.keel.proto.log.HardState;
import io.keel.raft.Entries;
import io.keel.raft.RaftStorage;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The durable log, including what it does with the mess a crash leaves behind. */
class SegmentedLogTest {

    @TempDir Path dir;

    @Test
    @DisplayName("an empty log starts at index 1 with nothing in it")
    void emptyLog() {
        try (SegmentedLog log = open()) {
            assertEquals(1, log.firstIndex());
            assertEquals(0, log.lastIndex());
            assertEquals(0, log.term(0));
            assertEquals(1, log.segmentCount());
            assertEquals(HardState.getDefaultInstance(), log.hardState());
        }
    }

    @Test
    @DisplayName("entries and hard state survive a reopen")
    void reopenKeepsEverything() {
        try (SegmentedLog log = open()) {
            log.append(entries(1, 1, 1, 2, 2));
            log.saveHardState(HardState.newBuilder().setTerm(2).setVote(3).setCommit(4).build());
            log.sync();
        }

        try (SegmentedLog log = open()) {
            assertEquals(5, log.lastIndex());
            assertEquals(2, log.term(4));
            assertEquals(2, log.hardState().getTerm());
            assertEquals(3, log.hardState().getVote());
            assertEquals(4, log.hardState().getCommit());
            assertEquals("i3", data(log.entries(3, 4, Long.MAX_VALUE).get(0)));
        }
    }

    @Test
    @DisplayName("segments roll over and the log still reads as one sequence")
    void rollsOverSegments() {
        try (SegmentedLog log = openWithSegmentBytes(256)) {
            for (int i = 1; i <= 40; i++) {
                log.append(List.of(Entries.normal(i, 1, ("value-" + i).getBytes(StandardCharsets.UTF_8))));
            }
            log.sync();
            assertTrue(log.segmentCount() > 1, "expected more than one segment file");
        }

        try (SegmentedLog log = open()) {
            assertEquals(40, log.lastIndex());
            List<Entry> all = log.entries(1, 41, Long.MAX_VALUE);
            assertEquals(40, all.size());
            for (int i = 0; i < 40; i++) {
                assertEquals("value-" + (i + 1), data(all.get(i)));
            }
        }
    }

    @Test
    @DisplayName("a superseding append replaces the suffix, and the replacement survives a reopen")
    void supersedingAppendSurvivesReopen() {
        try (SegmentedLog log = open()) {
            log.append(entries(1, 1, 1, 1));
            log.sync();
            log.append(List.of(Entries.normal(2, 9, "replaced".getBytes(StandardCharsets.UTF_8))));
            log.sync();
            assertEquals(2, log.lastIndex());
        }

        try (SegmentedLog log = open()) {
            // The original records for indexes 2 through 4 are still on disk. Replay has to let the
            // later record win, or a restart would resurrect entries that were overwritten.
            assertEquals(2, log.lastIndex());
            assertEquals(9, log.term(2));
            assertEquals("replaced", data(log.entries(2, 3, Long.MAX_VALUE).get(0)));
        }
    }

    @Test
    @DisplayName("a superseding append does not roll back the hard state")
    void supersedingAppendKeepsHardState() {
        // The safety case behind the append-only design. If overwriting a suffix could remove an
        // already-recorded vote, a crash here would resurrect an older term and let this node vote
        // twice in one term, which is how a cluster ends up with two leaders.
        try (SegmentedLog log = open()) {
            log.append(entries(1, 1, 1, 1));
            log.saveHardState(HardState.newBuilder().setTerm(5).setVote(2).build());
            log.sync();

            log.append(List.of(Entries.normal(2, 6, "from the new leader".getBytes(StandardCharsets.UTF_8))));
            log.sync();
        }

        try (SegmentedLog log = open()) {
            assertEquals(5, log.hardState().getTerm(), "the recorded term must not go backwards");
            assertEquals(2, log.hardState().getVote(), "nor the recorded vote");
            assertEquals(2, log.lastIndex());
        }
    }

    @Test
    @DisplayName("the newest hard state wins")
    void latestHardStateWins() {
        try (SegmentedLog log = open()) {
            log.saveHardState(HardState.newBuilder().setTerm(1).setVote(1).build());
            log.saveHardState(HardState.newBuilder().setTerm(2).setVote(0).build());
            log.saveHardState(HardState.newBuilder().setTerm(2).setVote(3).build());
            log.sync();
        }
        try (SegmentedLog log = open()) {
            assertEquals(2, log.hardState().getTerm());
            assertEquals(3, log.hardState().getVote());
        }
    }

    @Test
    @DisplayName("writes that were never synced are simply absent")
    void unsyncedWritesAreNotPromised() {
        try (SegmentedLog log = open()) {
            log.append(entries(1, 1));
            log.sync();
            log.append(entriesFrom(3, 1, 1));
            // No sync, and close() deliberately does not sync either.
        }

        try (SegmentedLog log = open()) {
            // The data may or may not be there depending on what the page cache did. What must hold
            // is that the log is a prefix and reopens cleanly.
            assertTrue(log.lastIndex() >= 2, "the synced prefix must be intact");
            assertTrue(log.lastIndex() <= 4);
        }
    }

    @Nested
    @DisplayName("recovery from damage")
    class Recovery {

        @Test
        @DisplayName("an unfinished record at the tail is discarded")
        void tornTailIsDiscarded() throws IOException {
            try (SegmentedLog log = open()) {
                log.append(entries(1, 1, 1));
                log.sync();
            }
            // A header promising far more bytes than follow it is what an interrupted write leaves.
            Path segment = onlySegment();
            long before = Files.size(segment);
            try (RandomAccessFile raf = new RandomAccessFile(segment.toFile(), "rw")) {
                raf.seek(before);
                ByteBuffer header = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                header.putInt(4096).putInt(0);
                raf.write(header.array());
                raf.write(new byte[16]);
            }

            try (SegmentedLog log = open()) {
                assertEquals(3, log.lastIndex(), "the complete records must survive");
                assertEquals(24, log.discardedBytesOnRecovery());
                assertEquals(before, Files.size(segment), "the tail should have been truncated away");
            }
        }

        @Test
        @DisplayName("a damaged last record is discarded")
        void damagedTailIsDiscarded() throws IOException {
            try (SegmentedLog log = open()) {
                log.append(entries(1, 1, 1));
                log.sync();
            }
            flipByteAt(onlySegment(), Files.size(onlySegment()) - 1);

            try (SegmentedLog log = open()) {
                assertEquals(2, log.lastIndex(), "the last record is unreadable, the rest is fine");
                assertTrue(log.discardedBytesOnRecovery() > 0);
            }
        }

        @Test
        @DisplayName("a damaged record with valid records after it fails recovery")
        void damageInTheMiddleIsFatal() throws IOException {
            try (SegmentedLog log = open()) {
                log.append(entries(1, 1, 1, 1, 1));
                log.sync();
            }
            // Offset 8 is the first payload byte of the first record. Everything after it is intact,
            // which no crash can produce: this is corruption, and quietly dropping it would quietly
            // drop entries a quorum may have committed.
            flipByteAt(onlySegment(), 8);

            CorruptLogException e = assertThrows(CorruptLogException.class, SegmentedLogTest.this::open);
            assertTrue(e.getMessage().contains("checksum"), e.getMessage());
        }

        @Test
        @DisplayName("damage in an older segment fails recovery even if the newest one is clean")
        void damageInAnOlderSegmentIsFatal() throws IOException {
            try (SegmentedLog log = openWithSegmentBytes(128)) {
                for (int i = 1; i <= 20; i++) {
                    log.append(List.of(Entries.normal(i, 1, ("v" + i).getBytes(StandardCharsets.UTF_8))));
                }
                log.sync();
                assertTrue(log.segmentCount() > 2);
            }
            flipByteAt(segmentFiles().get(0), 8);

            assertThrows(CorruptLogException.class, SegmentedLogTest.this::open);
        }
    }

    @Nested
    @DisplayName("compaction")
    class Compaction {

        @Test
        @DisplayName("survives a reopen, and the boundary term with it")
        void compactionIsDurable() {
            try (SegmentedLog log = open()) {
                log.append(entries(1, 1, 2, 2, 3));
                log.sync();
                log.compact(boundary(3, 2));
                assertEquals(4, log.firstIndex());
            }

            try (SegmentedLog log = open()) {
                assertEquals(4, log.firstIndex(), "the compaction marker has to survive");
                assertEquals(5, log.lastIndex());
                // The next AppendEntries to a follower needs prevLogTerm at the boundary.
                assertEquals(2, log.term(3));
                assertEquals(3, log.term(5));
                assertThrows(RaftStorage.CompactedException.class, () -> log.term(2));
                assertEquals(3, log.snapshotMetadata().getLastIndex());
            }
        }

        @Test
        @DisplayName("frees whole segments once nothing points into them")
        void compactionDeletesSegments() {
            try (SegmentedLog log = openWithSegmentBytes(128)) {
                for (int i = 1; i <= 40; i++) {
                    log.append(List.of(Entries.normal(i, 1, ("v" + i).getBytes(StandardCharsets.UTF_8))));
                }
                log.sync();
                int before = log.segmentCount();
                assertTrue(before > 3, "expected several segments, got " + before);

                log.compact(boundary(30, 1));

                assertTrue(
                        log.segmentCount() < before,
                        "compaction should have freed segments: " + before + " -> " + log.segmentCount());
                assertEquals(10, log.entries(31, 41, Long.MAX_VALUE).size());
            }
        }

        @Test
        @DisplayName("installing a snapshot discards the whole log, not just the prefix")
        void installDiscardsEverything() {
            // A follower catching up may hold entries above the boundary that came from a leader that
            // lost, so they are not known to be valid either.
            try (SegmentedLog log = open()) {
                log.append(entries(1, 1, 1, 1));
                log.sync();
                log.installSnapshot(boundary(10, 4));

                assertEquals(11, log.firstIndex());
                assertEquals(10, log.lastIndex());
                assertEquals(4, log.term(10));
            }

            try (SegmentedLog log = open()) {
                assertEquals(11, log.firstIndex(), "and it survives a reopen");
                assertEquals(10, log.lastIndex());
                assertEquals(4, log.snapshotMetadata().getLastTerm());
            }
        }

        @Test
        @DisplayName("entries appended after a snapshot are kept on replay")
        void entriesAfterASnapshotSurvive() {
            try (SegmentedLog log = open()) {
                log.installSnapshot(boundary(10, 4));
                log.append(entriesFrom(11, 5, 5));
                log.sync();
            }

            try (SegmentedLog log = open()) {
                assertEquals(11, log.firstIndex());
                assertEquals(12, log.lastIndex());
                assertEquals(5, log.term(12));
            }
        }

        @Test
        @DisplayName("compacting backwards is refused")
        void compactingBackwardsIsRefused() {
            try (SegmentedLog log = open()) {
                log.append(entries(1, 1, 1, 1));
                log.compact(boundary(3, 1));

                assertThrows(IllegalArgumentException.class, () -> log.compact(boundary(2, 1)));
            }
        }

        private io.keel.proto.log.SnapshotMetadata boundary(long index, long term) {
            return io.keel.proto.log.SnapshotMetadata.newBuilder()
                    .setLastIndex(index)
                    .setLastTerm(term)
                    .build();
        }
    }

    @Test
    @DisplayName("a gap is refused")
    void gapIsRefused() {
        try (SegmentedLog log = open()) {
            log.append(entries(1));
            IllegalArgumentException e =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> log.append(List.of(Entries.normal(7, 1, new byte[] {1}))));
            assertTrue(e.getMessage().contains("gap"), e.getMessage());
        }
    }

    @Test
    @DisplayName("reads past the end are refused")
    void readPastEndIsRefused() {
        try (SegmentedLog log = open()) {
            log.append(entries(1, 1));
            assertThrows(IllegalArgumentException.class, () -> log.term(5));
            assertThrows(IllegalArgumentException.class, () -> log.entries(1, 5, Long.MAX_VALUE));
        }
    }

    @Test
    @DisplayName("the byte budget caps a batch but always returns one entry")
    void byteBudget() {
        try (SegmentedLog log = open()) {
            for (int i = 1; i <= 5; i++) {
                log.append(List.of(Entries.normal(i, 1, new byte[500])));
            }
            assertEquals(1, log.entries(1, 6, 1).size());
            assertEquals(5, log.entries(1, 6, Long.MAX_VALUE).size());
            assertTrue(log.entries(1, 6, 1200).size() < 5);
        }
    }

    @Test
    @DisplayName("an entry larger than a segment is still written")
    void oversizedEntryStillFits() {
        try (SegmentedLog log = openWithSegmentBytes(64)) {
            log.append(List.of(Entries.normal(1, 1, new byte[4096])));
            log.sync();
            assertEquals(1, log.lastIndex());
        }
        try (SegmentedLog log = open()) {
            assertEquals(4096, log.entries(1, 2, Long.MAX_VALUE).get(0).getData().size());
        }
    }

    @Test
    @DisplayName("asking for a compacted index is a distinct failure")
    void compactedIndexIsDistinct() {
        try (SegmentedLog log = open()) {
            log.append(entries(1, 1));
            // Nothing has been compacted yet, so index 0 is the boundary and reads as term 0.
            assertEquals(0, log.term(0));
            assertThrows(RaftStorage.CompactedException.class, () -> log.entries(0, 2, Long.MAX_VALUE));
        }
    }

    // ---------------------------------------------------------------------------------------------

    private SegmentedLog open() {
        return SegmentedLog.open(LogOptions.of(dir));
    }

    private SegmentedLog openWithSegmentBytes(long bytes) {
        return SegmentedLog.open(LogOptions.of(dir).withMaxSegmentBytes(bytes));
    }

    private List<Path> segmentFiles() {
        try (var stream = Files.list(dir)) {
            return stream.filter(p -> Segment.parseSequence(p) >= 0)
                    .sorted(Comparator.comparingLong(Segment::parseSequence))
                    .toList();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private Path onlySegment() {
        List<Path> files = segmentFiles();
        assertEquals(1, files.size(), "expected a single segment: " + files);
        return files.get(0);
    }

    private static void flipByteAt(Path file, long offset) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 0x20);
        }
    }

    private static List<Entry> entries(long... terms) {
        return entriesFrom(1, terms);
    }

    private static List<Entry> entriesFrom(long start, long... terms) {
        List<Entry> out = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            long index = start + i;
            out.add(Entries.normal(index, terms[i], ("i" + index).getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }

    private static String data(Entry e) {
        return e.getData().toStringUtf8();
    }
}
