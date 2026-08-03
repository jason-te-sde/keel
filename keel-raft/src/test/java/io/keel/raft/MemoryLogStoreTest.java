package io.keel.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.keel.proto.log.Entry;
import io.keel.proto.log.HardState;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The in-memory store, including the part that makes crash tests meaningful: unsynced writes are
 * supposed to disappear.
 */
class MemoryLogStoreTest {

    @Test
    @DisplayName("entries read back in order")
    void appendAndRead() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(entries(1, 1, 1, 2, 2));

        assertEquals(1, store.firstIndex());
        assertEquals(5, store.lastIndex());
        assertEquals(2, store.term(4));
        assertEquals(3, store.entries(1, 4, Long.MAX_VALUE).size());
    }

    @Test
    @DisplayName("a crash loses everything that was not synced")
    void crashDropsUnsyncedWrites() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(entries(1, 1));
        store.sync();
        store.append(entriesFrom(3, 1, 1));

        assertEquals(4, store.lastIndex());
        store.crash();

        assertEquals(2, store.lastIndex(), "only the synced prefix should survive");
        assertEquals(2, store.durableEntries().size());
    }

    @Test
    @DisplayName("a crash reverts the hard state too")
    void crashRevertsHardState() {
        MemoryLogStore store = new MemoryLogStore();
        store.saveHardState(HardState.newBuilder().setTerm(3).setVote(2).build());
        store.sync();
        store.saveHardState(HardState.newBuilder().setTerm(4).setVote(1).build());

        store.crash();

        assertEquals(3, store.hardState().getTerm(), "an unsynced vote must not survive a crash");
        assertEquals(2, store.hardState().getVote());
    }

    @Test
    @DisplayName("a crash after a sync keeps everything")
    void crashAfterSyncKeepsEverything() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(entries(1, 1, 2));
        store.saveHardState(HardState.newBuilder().setTerm(2).setCommit(3).build());
        store.sync();

        store.crash();

        assertEquals(3, store.lastIndex());
        assertEquals(3, store.hardState().getCommit());
    }

    @Test
    @DisplayName("appending at an existing index replaces the tail")
    void appendTruncatesConflictingSuffix() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(entries(1, 1, 1, 1));
        store.sync();

        store.append(List.of(Entries.normal(2, 7, data("replaced"))));

        assertEquals(2, store.lastIndex(), "indexes 3 and 4 are gone");
        assertEquals(7, store.term(2));
    }

    @Test
    @DisplayName("a truncation that was never synced is undone by a crash")
    void unsyncedTruncationIsUndone() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(entries(1, 1, 1, 1));
        store.sync();

        store.append(List.of(Entries.normal(2, 7, data("replaced"))));
        assertEquals(2, store.lastIndex());
        store.crash();

        assertEquals(4, store.lastIndex(), "the disk still held the original entries");
        assertEquals(1, store.term(2));
    }

    @Test
    @DisplayName("a gap in the log is refused")
    void gapIsRefused() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(entries(1));

        IllegalArgumentException e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> store.append(List.of(Entries.normal(5, 1, data("x")))));
        assertTrue(e.getMessage().contains("gap"), e.getMessage());
    }

    @Test
    @DisplayName("non-contiguous entries in one batch are refused")
    void nonContiguousIsRefused() {
        MemoryLogStore store = new MemoryLogStore();

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        store.append(
                                List.of(
                                        Entries.normal(1, 1, data("a")),
                                        Entries.normal(3, 1, data("c")))));
    }

    @Test
    @DisplayName("reads past the end of the log are refused")
    void readPastEndIsRefused() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(entries(1, 1));

        assertThrows(IllegalArgumentException.class, () -> store.term(9));
        assertThrows(IllegalArgumentException.class, () -> store.entries(1, 9, Long.MAX_VALUE));
    }

    @Test
    @DisplayName("the byte budget caps a batch but always returns something")
    void byteBudget() {
        MemoryLogStore store = new MemoryLogStore();
        List<Entry> big = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            big.add(Entries.normal(i, 1, new byte[1000]));
        }
        store.append(big);

        assertEquals(1, store.entries(1, 6, 1).size());
        assertEquals(2, store.entries(1, 6, 2100).size());
        assertEquals(5, store.entries(1, 6, Long.MAX_VALUE).size());
    }

    @Test
    @DisplayName("the term before the first index is 0")
    void termBeforeStart() {
        MemoryLogStore store = new MemoryLogStore();
        store.append(entries(4));

        assertEquals(0, store.term(0));
    }

    private static List<Entry> entries(long... terms) {
        return entriesFrom(1, terms);
    }

    private static List<Entry> entriesFrom(long start, long... terms) {
        List<Entry> out = new ArrayList<>();
        for (int i = 0; i < terms.length; i++) {
            out.add(Entries.normal(start + i, terms[i], data("i" + (start + i))));
        }
        return out;
    }

    private static byte[] data(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
