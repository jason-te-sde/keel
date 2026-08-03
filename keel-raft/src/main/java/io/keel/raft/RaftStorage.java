package io.keel.raft;

import io.keel.proto.log.Entry;
import io.keel.proto.log.SnapshotMetadata;
import java.util.List;

/**
 * Read access to the durable log, as the consensus core sees it.
 *
 * <p>Deliberately read-only. Writes are the driver's job: the core reports what must be persisted in
 * a {@link Ready} batch and cannot itself reach the disk. Handing the core a mutable storage
 * reference would make the persist-before-send ordering a matter of discipline rather than of
 * structure.
 *
 * <p>Indexes are 1-based and contiguous. Index 0 is the position before the first entry and always
 * has term 0.
 */
public interface RaftStorage {

    /**
     * Index of the oldest entry still available, which is one past the last index covered by a
     * snapshot. Equals {@code lastIndex() + 1} when the log is empty.
     */
    long firstIndex();

    /** Index of the newest entry, or {@code firstIndex() - 1} when the log is empty. */
    long lastIndex();

    /**
     * Term of the entry at {@code index}.
     *
     * @throws CompactedException if the index is below {@link #firstIndex()} and is not the index
     *     covered by the current snapshot
     * @throws IllegalArgumentException if the index is above {@link #lastIndex()}
     */
    long term(long index);

    /**
     * Entries in {@code [lo, hi)}, truncated to at most {@code maxBytes} of serialized entry data.
     * At least one entry is returned when the range is non-empty, even if that single entry exceeds
     * {@code maxBytes}: refusing to return anything would stall replication.
     *
     * @throws CompactedException if {@code lo} is below {@link #firstIndex()}
     */
    List<Entry> entries(long lo, long hi, long maxBytes);

    /**
     * The snapshot the log has been compacted to, or a zeroed message when nothing has been
     * compacted.
     *
     * <p>The core needs this for one specific reason: after compaction, the entry just below
     * {@link #firstIndex()} is gone, but its term is still needed to send the next AppendEntries and
     * to answer a vote. The snapshot boundary is where that term comes from.
     */
    SnapshotMetadata snapshotMetadata();

    /** Thrown when a caller asks for log data that has been discarded by compaction. */
    final class CompactedException extends RuntimeException {
        private final long requestedIndex;
        private final long firstAvailableIndex;

        public CompactedException(long requestedIndex, long firstAvailableIndex) {
            super(
                    "index "
                            + requestedIndex
                            + " has been compacted; first available is "
                            + firstAvailableIndex);
            this.requestedIndex = requestedIndex;
            this.firstAvailableIndex = firstAvailableIndex;
        }

        public long requestedIndex() {
            return requestedIndex;
        }

        public long firstAvailableIndex() {
            return firstAvailableIndex;
        }
    }
}
