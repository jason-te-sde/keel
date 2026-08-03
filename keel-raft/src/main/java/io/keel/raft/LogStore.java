package io.keel.raft;

import io.keel.proto.log.Entry;
import io.keel.proto.log.HardState;
import io.keel.proto.log.SnapshotMetadata;
import java.util.List;

/**
 * The durable side of the log, used by whatever drives the consensus core.
 *
 * <p>The core is given only the {@link RaftStorage} view of this, so it can read but not write. The
 * driver writes what a {@link Ready} batch asks it to write, in this order:
 *
 * <ol>
 *   <li>{@link #saveHardState} if the batch carries one
 *   <li>{@link #append} for the batch's entries
 *   <li>{@link #sync}
 *   <li>only then, send the batch's messages
 * </ol>
 *
 * <p>Getting that order wrong is the classic way to lose a committed entry: acknowledge an append,
 * crash before the data reaches the disk, come back with a log that is missing an entry a quorum
 * counted, and a committed value disappears.
 */
public interface LogStore extends RaftStorage, AutoCloseable {

    /**
     * Appends entries, first discarding anything at or above {@code entries.get(0).getIndex()}.
     *
     * <p>Truncate-then-append is one operation rather than two because a follower overwriting a
     * conflicting suffix must not be able to observe, or crash into, a state where the old tail is
     * gone and the new one is not yet there.
     *
     * @throws IllegalArgumentException if the entries are not contiguous and ascending, or if they
     *     would leave a gap above {@link #lastIndex()}
     */
    void append(List<Entry> entries);

    /** Records the term, vote, and commit index. Not durable until {@link #sync()}. */
    void saveHardState(HardState state);

    /** The hard state as last written, or a zeroed state for a fresh log. */
    HardState hardState();

    /** Makes every preceding write durable. */
    void sync();

    /**
     * Discards entries at or below {@code meta.getLastIndex()}, keeping everything above.
     *
     * <p>Called after the state machine has written a snapshot covering that index. The snapshot must
     * be durable first: this is the point of no return, and losing both the snapshot and the entries
     * it replaced loses committed data.
     *
     * @throws IllegalArgumentException if the index is above the last index, or below the current
     *     snapshot boundary
     */
    void compact(SnapshotMetadata meta);

    /**
     * Replaces the entire log with a snapshot boundary, for a follower catching up from one.
     *
     * <p>Unlike {@link #compact}, nothing is kept: the receiving node's log may have diverged from the
     * leader's, so entries above the snapshot are not necessarily valid and are discarded too.
     */
    void installSnapshot(SnapshotMetadata meta);

    /**
     * Releases any resources held by the store. Implementations backed by memory need do nothing.
     *
     * <p>Deliberately does not sync. Closing without syncing loses unsynced writes, which is the same
     * outcome as a crash and is exactly what a test needs to be able to arrange. It also narrows
     * {@link AutoCloseable#close()} to throw nothing, so callers are not forced into catch blocks
     * that can never fire.
     */
    @Override
    default void close() {}
}
