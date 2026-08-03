package io.keel.raft;

import io.keel.proto.log.Entry;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The log as the core sees it: a durable prefix in {@link RaftStorage} plus a tail in memory that
 * the driver has not persisted yet.
 *
 * <p>The split exists because the core has to serve reads of entries it has only just created, while
 * still refusing to pretend they are durable. Keeping the whole log in memory would be simpler and
 * is what several implementations do, at the cost of a memory footprint that grows with the log
 * rather than with the snapshot interval.
 *
 * <p>One invariant carries most of the weight here: the in-memory tail always wins. Once entries are
 * staged at index {@code i}, nothing above {@code i} may be read from storage, even though storage
 * may still physically hold the entries that are being replaced. Every read routes through
 * {@link #unstableOffset()} for that reason.
 */
final class RaftLog {

    private static final Logger LOG = LoggerFactory.getLogger(RaftLog.class);

    private final RaftStorage storage;
    private final List<Entry> unstable = new ArrayList<>();

    private long committed;
    private long applied;

    RaftLog(RaftStorage storage, long committed) {
        this.storage = storage;
        long last = storage.lastIndex();
        if (committed > last) {
            // Recovery would otherwise silently accept a commit index for entries that are not on
            // disk, which is indistinguishable from data loss.
            throw new IllegalStateException(
                    "recovered commit index " + committed + " exceeds last log index " + last);
        }
        this.committed = committed;
        // Entries at or below the snapshot boundary are already in the state machine; everything
        // above it is replayed by the driver on startup. For a log that has never been compacted
        // this is 0, so the whole log is replayed.
        this.applied = storage.firstIndex() - 1;
    }

    /** Index of the first in-memory entry, or one past the durable end when there are none. */
    long unstableOffset() {
        return unstable.isEmpty() ? storage.lastIndex() + 1 : unstable.get(0).getIndex();
    }

    long firstIndex() {
        return storage.firstIndex();
    }

    long lastIndex() {
        return unstable.isEmpty()
                ? storage.lastIndex()
                : unstable.get(unstable.size() - 1).getIndex();
    }

    long committed() {
        return committed;
    }

    long applied() {
        return applied;
    }

    long lastTerm() {
        return term(lastIndex());
    }

    /**
     * Term at {@code index}.
     *
     * @throws RaftStorage.CompactedException if the index predates the snapshot
     * @throws IllegalArgumentException if the index is past the end of the log
     */
    long term(long index) {
        if (index == 0) {
            return 0;
        }
        long offset = unstableOffset();
        if (index >= offset) {
            if (index > lastIndex()) {
                throw new IllegalArgumentException(
                        "index " + index + " is past the last index " + lastIndex());
            }
            return unstable.get((int) (index - offset)).getTerm();
        }
        return storage.term(index);
    }

    /** True when the log holds an entry at {@code index} whose term is {@code term}. */
    boolean matchTerm(long index, long term) {
        if (index > lastIndex()) {
            return false;
        }
        try {
            return term(index) == term;
        } catch (RaftStorage.CompactedException e) {
            // The entry is older than the snapshot, so it is already known to be committed and
            // cannot be in conflict. Treating it as a mismatch is the safe answer: the caller
            // retries at a higher index rather than assuming agreement it cannot verify.
            return false;
        }
    }

    /**
     * Election restriction, paper 5.4.1: a candidate may only win if its log is at least as up to
     * date as the voter's, compared by last term first and index second.
     */
    boolean isUpToDate(long index, long term) {
        long myTerm = lastTerm();
        return term > myTerm || (term == myTerm && index >= lastIndex());
    }

    /**
     * Entries in {@code [lo, hi)}, spanning the durable prefix and the in-memory tail, capped at
     * {@code maxBytes} of entry data but never empty for a non-empty range.
     */
    List<Entry> slice(long lo, long hi, long maxBytes) {
        if (lo >= hi) {
            return List.of();
        }
        if (hi > lastIndex() + 1) {
            throw new IllegalArgumentException(
                    "range [" + lo + "," + hi + ") extends past the last index " + lastIndex());
        }
        long offset = unstableOffset();
        List<Entry> out = new ArrayList<>((int) (hi - lo));
        if (lo < offset) {
            long storageHi = Math.min(hi, offset);
            out.addAll(storage.entries(lo, storageHi, maxBytes));
            if (out.size() < storageHi - lo) {
                // Storage stopped early on the byte budget; do not skip ahead past the gap.
                return out;
            }
        }
        long bytes = Entries.byteSize(out);
        for (long i = Math.max(lo, offset); i < hi; i++) {
            Entry e = unstable.get((int) (i - offset));
            if (!out.isEmpty() && bytes + e.getSerializedSize() > maxBytes) {
                break;
            }
            bytes += e.getSerializedSize();
            out.add(e);
        }
        return out;
    }

    /** Stages leader-created entries. Indexes must continue from {@link #lastIndex()}. */
    void append(List<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        long expected = lastIndex() + 1;
        if (entries.get(0).getIndex() != expected) {
            throw new IllegalArgumentException(
                    "append must start at " + expected + " but starts at "
                            + entries.get(0).getIndex());
        }
        unstable.addAll(entries);
    }

    /** Outcome of a follower's attempt to accept an AppendEntries message. */
    record AppendOutcome(boolean accepted, long lastNewIndex, long conflictIndex, long conflictTerm) {

        static AppendOutcome accepted(long lastNewIndex) {
            return new AppendOutcome(true, lastNewIndex, 0, 0);
        }

        static AppendOutcome rejected(long conflictIndex, long conflictTerm) {
            return new AppendOutcome(false, 0, conflictIndex, conflictTerm);
        }
    }

    /**
     * Follower side of AppendEntries: verify the previous entry matches, overwrite any conflicting
     * suffix, and advance the commit index.
     *
     * <p>On rejection the outcome carries where to retry. Reporting the first index of the
     * conflicting term rather than just "no" lets the leader skip an entire term per round trip,
     * which is the difference between converging in two messages and one message per index.
     */
    AppendOutcome maybeAppend(
            long prevIndex, long prevTerm, long leaderCommit, List<Entry> entries) {
        if (prevIndex > lastIndex()) {
            // Our log ends before the leader's previous entry. Ask for the gap to be filled.
            return AppendOutcome.rejected(lastIndex() + 1, 0);
        }
        if (!matchTerm(prevIndex, prevTerm)) {
            long conflictTerm;
            try {
                conflictTerm = term(prevIndex);
            } catch (RaftStorage.CompactedException e) {
                return AppendOutcome.rejected(firstIndex(), 0);
            }
            return AppendOutcome.rejected(firstIndexOfTerm(prevIndex, conflictTerm), conflictTerm);
        }

        long lastNewIndex = prevIndex + entries.size();
        long conflict = findConflict(entries);
        if (conflict != 0) {
            if (conflict <= committed) {
                // Two leaders would have to have committed different entries at the same index for
                // this to happen. There is no safe way to continue.
                throw new IllegalStateException(
                        "leader is overwriting committed entry at index "
                                + conflict
                                + " (commit index is "
                                + committed
                                + ")");
            }
            truncateAndStage(entries, conflict);
        }
        commitTo(Math.min(leaderCommit, lastNewIndex));
        return AppendOutcome.accepted(lastNewIndex);
    }

    /**
     * Index of the first entry whose term differs from ours, or 0 when every entry already matches.
     */
    private long findConflict(List<Entry> entries) {
        for (Entry e : entries) {
            if (!matchTerm(e.getIndex(), e.getTerm())) {
                if (e.getIndex() <= lastIndex()) {
                    LOG.debug(
                            "log divergence at index {}: local term {}, leader term {}",
                            e.getIndex(),
                            termOrUnknown(e.getIndex()),
                            e.getTerm());
                }
                return e.getIndex();
            }
        }
        return 0;
    }

    /** Replaces everything from {@code from} onward with the matching tail of {@code entries}. */
    private void truncateAndStage(List<Entry> entries, long from) {
        long offset = unstableOffset();
        if (from < offset) {
            // The divergence is inside the durable prefix. Drop the in-memory tail entirely and
            // stage from `from`; storage is rewritten when the driver persists this batch, because
            // append is defined as truncate-then-append.
            unstable.clear();
        } else {
            unstable.subList((int) (from - offset), unstable.size()).clear();
        }
        for (Entry e : entries) {
            if (e.getIndex() >= from) {
                unstable.add(e);
            }
        }
    }

    /** Walks back to the first index belonging to {@code term}, not going below the log start. */
    private long firstIndexOfTerm(long index, long term) {
        long i = index;
        while (i > firstIndex()) {
            try {
                if (term(i - 1) != term) {
                    break;
                }
            } catch (RaftStorage.CompactedException e) {
                break;
            }
            i--;
        }
        return i;
    }

    private Object termOrUnknown(long index) {
        try {
            return term(index);
        } catch (RuntimeException e) {
            return "unknown";
        }
    }

    /** Advances the commit index. Never moves backwards, and never past the end of the log. */
    boolean commitTo(long index) {
        if (index <= committed) {
            return false;
        }
        if (index > lastIndex()) {
            throw new IllegalStateException(
                    "commit index " + index + " exceeds last log index " + lastIndex());
        }
        committed = index;
        return true;
    }

    void appliedTo(long index) {
        if (index < applied) {
            throw new IllegalStateException(
                    "applied index moved backwards from " + applied + " to " + index);
        }
        if (index > committed) {
            throw new IllegalStateException(
                    "applied index " + index + " exceeds commit index " + committed);
        }
        applied = index;
    }

    /** Entries the driver has not persisted yet. */
    List<Entry> unstableEntries() {
        return List.copyOf(unstable);
    }

    /** Marks entries up to {@code index} as durable, dropping them from the in-memory tail. */
    void stableTo(long index) {
        long offset = unstableOffset();
        if (unstable.isEmpty() || index < offset) {
            return;
        }
        int upTo = (int) Math.min(index - offset + 1, unstable.size());
        unstable.subList(0, upTo).clear();
    }

    /** Committed entries not yet handed to the state machine. */
    List<Entry> nextCommittedEntries() {
        if (applied >= committed) {
            return List.of();
        }
        return slice(applied + 1, committed + 1, Long.MAX_VALUE);
    }

    @Override
    public String toString() {
        return "RaftLog[first="
                + firstIndex()
                + " last="
                + lastIndex()
                + " committed="
                + committed
                + " applied="
                + applied
                + " unstableFrom="
                + unstableOffset()
                + "]";
    }
}
