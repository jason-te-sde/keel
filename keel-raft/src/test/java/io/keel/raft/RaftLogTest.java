package io.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.proto.log.Entry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The log's index arithmetic, where the durable prefix meets the in-memory tail. */
class RaftLogTest {

    @Test
    @DisplayName("an empty log reports index 0 with term 0")
    void emptyLog() {
        RaftLog log = logWithDurableTerms();
        assertEquals(1, log.firstIndex());
        assertEquals(0, log.lastIndex());
        assertEquals(0, log.term(0));
        assertEquals(0, log.lastTerm());
    }

    @Test
    @DisplayName("slices span the durable prefix and the staged tail")
    void sliceCrossesTheBoundary() {
        MemoryLogStore store = storeWithTerms(1, 1, 2);
        RaftLog log = new RaftLog(store, 0);
        log.append(List.of(Entries.normal(4, 3, data("d")), Entries.normal(5, 3, data("e"))));

        assertEquals(5, log.lastIndex());
        assertEquals(3, log.lastTerm());
        assertEquals(4, log.unstableOffset());

        List<Entry> all = log.slice(1, 6, Long.MAX_VALUE);
        assertEquals(5, all.size());
        assertEquals(List.of(1L, 1L, 2L, 3L, 3L), all.stream().map(Entry::getTerm).toList());
    }

    @Test
    @DisplayName("staged entries win over storage that still holds the old ones")
    void stagedTailShadowsStorage() {
        // Storage physically holds three entries. The follower then accepts a leader's version that
        // diverges at index 2, which is staged but not yet written back.
        MemoryLogStore store = storeWithTerms(1, 1, 1);
        RaftLog log = new RaftLog(store, 1);

        RaftLog.AppendOutcome outcome =
                log.maybeAppend(
                        1, 1, 0, List.of(Entries.normal(2, 5, data("new")), Entries.normal(3, 5, data("newer"))));

        assertTrue(outcome.accepted());
        assertEquals(3, log.lastIndex());
        assertEquals(5, log.term(2), "the staged entry must win, not the one still on disk");
        assertEquals(5, log.term(3));
        assertEquals(2, log.unstableOffset());
    }

    @Test
    @DisplayName("marking entries durable drops them from the staged tail")
    void stableToDropsPrefix() {
        RaftLog log = logWithDurableTerms();
        log.append(
                List.of(
                        Entries.normal(1, 1, data("a")),
                        Entries.normal(2, 1, data("b")),
                        Entries.normal(3, 1, data("c"))));
        assertEquals(3, log.unstableEntries().size());

        log.stableTo(2);
        assertEquals(1, log.unstableEntries().size());
        assertEquals(3, log.unstableEntries().get(0).getIndex());
        assertEquals(3, log.lastIndex());
    }

    @Test
    @DisplayName("an append past the end of the log asks for the gap to be filled")
    void rejectsGap() {
        RaftLog log = new RaftLog(storeWithTerms(1, 1), 0);

        RaftLog.AppendOutcome outcome = log.maybeAppend(5, 1, 0, List.of());

        assertFalse(outcome.accepted());
        assertEquals(3, outcome.conflictIndex(), "one past our end");
        assertEquals(0, outcome.conflictTerm(), "no term to report: our log simply stops earlier");
    }

    @Test
    @DisplayName("a term mismatch reports the first index of the conflicting term")
    void rejectionSkipsAWholeTerm() {
        // Terms 1,1,2,2,2: a leader probing at index 5 with term 3 should be sent back to index 3,
        // not to index 4, so the whole run of term 2 is skipped in one round trip.
        RaftLog log = new RaftLog(storeWithTerms(1, 1, 2, 2, 2), 0);

        RaftLog.AppendOutcome outcome = log.maybeAppend(5, 3, 0, List.of());

        assertFalse(outcome.accepted());
        assertEquals(2, outcome.conflictTerm());
        assertEquals(3, outcome.conflictIndex());
    }

    @Test
    @DisplayName("matching entries are not rewritten")
    void identicalEntriesAreLeftAlone() {
        RaftLog log = new RaftLog(storeWithTerms(1, 1, 1), 0);

        RaftLog.AppendOutcome outcome =
                log.maybeAppend(1, 1, 3, List.of(termEntry(2, 1), termEntry(3, 1)));

        assertTrue(outcome.accepted());
        assertEquals(3, outcome.lastNewIndex());
        assertTrue(log.unstableEntries().isEmpty(), "nothing needed staging");
        assertEquals(3, log.committed());
    }

    @Test
    @DisplayName("overwriting a committed entry is refused rather than accepted")
    void refusesToOverwriteCommitted() {
        RaftLog log = new RaftLog(storeWithTerms(1, 1, 1), 3);

        // Index 2 is committed. A leader claiming a different term there means two leaders committed
        // different entries at the same index, and there is no safe way to continue.
        IllegalStateException e =
                assertThrows(
                        IllegalStateException.class,
                        () -> log.maybeAppend(1, 1, 3, List.of(termEntry(2, 9))));
        assertTrue(e.getMessage().contains("committed"), e.getMessage());
    }

    @Test
    @DisplayName("the commit index cannot pass the end of the log")
    void commitCannotOutrunTheLog() {
        RaftLog log = new RaftLog(storeWithTerms(1, 1), 0);
        assertThrows(IllegalStateException.class, () -> log.commitTo(5));
    }

    @Test
    @DisplayName("the applied index cannot pass the commit index")
    void appliedCannotOutrunCommit() {
        RaftLog log = new RaftLog(storeWithTerms(1, 1), 1);
        assertThrows(IllegalStateException.class, () -> log.appliedTo(2));
    }

    @Test
    @DisplayName("a single entry over the byte budget is still returned")
    void byteBudgetNeverStarves() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(List.of(Entries.normal(1, 1, new byte[4096])));
        store.sync();
        RaftLog log = new RaftLog(store, 0);

        List<Entry> got = log.slice(1, 2, 1);

        assertEquals(1, got.size(), "one oversized entry must not wedge replication forever");
    }

    @Test
    @DisplayName("committed entries are handed out once, in order")
    void nextCommittedEntriesDrains() {
        RaftLog log = new RaftLog(storeWithTerms(1, 1, 1), 3);

        List<Entry> first = log.nextCommittedEntries();
        assertEquals(3, first.size());
        log.appliedTo(3);
        assertTrue(log.nextCommittedEntries().isEmpty());
    }

    @ParameterizedTest(name = "candidate at ({0},{1}) against local ({2}) is up to date: {3}")
    @CsvSource({
        // candidateIndex, candidateTerm, localTerms, expected
        "3, 2, '1 1 2', true", // identical
        "4, 2, '1 1 2', true", // longer, same term
        "2, 2, '1 1 2', false", // shorter, same term
        "1, 3, '1 1 2', true", // higher term always wins, however short
        "9, 1, '1 1 2', false", // longer but from an older term
    })
    @DisplayName("election restriction compares term first, then index")
    void upToDateComparison(long index, long term, String localTerms, boolean expected) {
        long[] terms = parseTerms(localTerms);
        RaftLog log = new RaftLog(storeWithTerms(terms), 0);

        assertEquals(expected, log.isUpToDate(index, term));
    }

    // -----------------------------------------------------------------------------------------

    private static RaftLog logWithDurableTerms(long... terms) {
        return new RaftLog(storeWithTerms(terms), 0);
    }

    private static MemoryLogStore storeWithTerms(long... terms) {
        MemoryLogStore store = new MemoryLogStore();
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            entries.add(termEntry(i + 1, terms[i]));
        }
        if (!entries.isEmpty()) {
            store.append(entries);
        }
        store.sync();
        return store;
    }

    private static Entry termEntry(long index, long term) {
        return Entries.normal(index, term, data("i" + index));
    }

    private static long[] parseTerms(String spec) {
        String[] parts = spec.trim().split("\\s+");
        long[] out = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Long.parseLong(parts[i]);
        }
        return out;
    }

    private static byte[] data(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
