package io.keel.raft;

import io.keel.proto.log.Entry;
import io.keel.proto.log.HardState;
import io.keel.proto.log.SnapshotMetadata;
import java.util.List;

/**
 * Everything the core wants done, as one batch.
 *
 * <p>The order is not advisory. A driver must:
 *
 * <ol>
 *   <li>install {@link #snapshotToInstall()} if present, <em>before</em> anything else: the entries in
 *       this same batch are the ones that follow its boundary, and appending them into a log that
 *       still starts lower down leaves a gap
 *   <li>write {@link #hardState()} if present
 *   <li>write {@link #entriesToPersist()}
 *   <li>make both durable
 *   <li>send {@link #messages()}
 *   <li>apply {@link #committedEntries()}
 *   <li>call {@link RaftNode#advance(Ready)}
 * </ol>
 *
 * <p>Steps 3 and 4 are the ones that matter. An acknowledgement is a promise that the entry will
 * survive a crash, and a leader counts acknowledgements toward a quorum. Sending before syncing
 * turns that promise into a guess, and a majority restarting at the wrong moment then loses an entry
 * that was reported committed.
 *
 * <p>Applying after sending is a latency choice rather than a safety one: followers learn the new
 * commit index a round trip sooner.
 *
 * @param hardState term, vote, and commit index to persist, or {@code null} when unchanged
 * @param entriesToPersist entries to write, with truncate-then-append semantics: anything at or
 *     above the first index here is being replaced
 * @param committedEntries entries to hand to the state machine, in index order
 * @param messages messages to put on the wire, valid only after the writes above are durable
 * @param readStates reads that have been given a safe index; each may be answered once the state
 *     machine has applied at least that index
 * @param snapshotToInstall a snapshot this node has accepted from the leader, or {@code null}. When
 *     present it must be installed into the state machine and the log <em>before</em>
 *     {@link RaftNode#advance(Ready)}: the core has already moved its commit index to the snapshot
 *     boundary, so a driver that acknowledges without installing has claimed state it does not have.
 */
public record Ready(
        HardState hardState,
        List<Entry> entriesToPersist,
        List<Entry> committedEntries,
        List<RaftMessage> messages,
        List<ReadState> readStates,
        SnapshotMetadata snapshotToInstall) {

    public Ready {
        entriesToPersist = List.copyOf(entriesToPersist);
        committedEntries = List.copyOf(committedEntries);
        messages = List.copyOf(messages);
        readStates = List.copyOf(readStates);
    }

    /** True when the core has nothing for the driver to do. */
    public boolean isEmpty() {
        return hardState == null
                && entriesToPersist.isEmpty()
                && committedEntries.isEmpty()
                && messages.isEmpty()
                && readStates.isEmpty()
                && snapshotToInstall == null;
    }

    public boolean hasHardState() {
        return hardState != null;
    }

    public boolean hasSnapshotToInstall() {
        return snapshotToInstall != null;
    }

    @Override
    public String toString() {
        return "Ready[hardState="
                + (hardState == null
                        ? "unchanged"
                        : "term=" + hardState.getTerm() + ",vote=" + hardState.getVote()
                                + ",commit=" + hardState.getCommit())
                + " persist="
                + entriesToPersist.size()
                + " apply="
                + committedEntries.size()
                + " send="
                + messages.size()
                + " reads="
                + readStates.size()
                + (snapshotToInstall == null
                        ? ""
                        : " snapshot=" + snapshotToInstall.getLastIndex())
                + "]";
    }
}
