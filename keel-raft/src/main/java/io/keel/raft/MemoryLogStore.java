package io.keel.raft;

import io.keel.proto.log.Entry;
import io.keel.proto.log.HardState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An in-memory {@link LogStore} that models a disk, including the part where unsynced writes
 * disappear.
 *
 * <p>It keeps two versions of the log: what a reader currently sees, and what would survive a power
 * cut. {@link #sync()} promotes the first to the second and {@link #crash()} throws the first away.
 * A store that simply kept one copy would make every crash test pass for the wrong reason, because
 * the persist-before-acknowledge ordering the core relies on would be unobservable.
 *
 * <p>Not thread safe, and not meant to be: a single thread owns the core and its storage.
 */
public final class MemoryLogStore implements LogStore {

    /** Log index of {@code live.get(0)}. Rises past 1 only after compaction. */
    private long offset = 1;

    private final List<Entry> live = new ArrayList<>();
    private final List<Entry> durable = new ArrayList<>();

    private HardState liveState = HardState.getDefaultInstance();
    private HardState durableState = HardState.getDefaultInstance();

    /**
     * Lowest log index modified since the last sync, or {@link Long#MAX_VALUE} when clean. Tracking
     * it keeps sync and crash proportional to what actually changed instead of to the log length.
     */
    private long dirtyFrom = Long.MAX_VALUE;

    /** Whether the hard state has been written since the last sync. Tracked apart from entries. */
    private boolean stateDirty;

    @Override
    public long firstIndex() {
        return offset;
    }

    @Override
    public long lastIndex() {
        return offset + live.size() - 1;
    }

    @Override
    public long term(long index) {
        if (index == offset - 1) {
            // The position just before the log: either the start of time or the last index a
            // snapshot covered. Term 0 is correct for a fresh log.
            return 0;
        }
        if (index < offset) {
            throw new CompactedException(index, offset);
        }
        if (index > lastIndex()) {
            throw new IllegalArgumentException(
                    "index " + index + " is past the last index " + lastIndex());
        }
        return live.get((int) (index - offset)).getTerm();
    }

    @Override
    public List<Entry> entries(long lo, long hi, long maxBytes) {
        if (lo < offset) {
            throw new CompactedException(lo, offset);
        }
        if (hi > lastIndex() + 1) {
            throw new IllegalArgumentException(
                    "range [" + lo + "," + hi + ") extends past the last index " + lastIndex());
        }
        if (lo >= hi) {
            return List.of();
        }
        List<Entry> out = new ArrayList<>((int) (hi - lo));
        long bytes = 0;
        for (long i = lo; i < hi; i++) {
            Entry e = live.get((int) (i - offset));
            // Always return the first entry: a single oversized entry must still be replicable, or
            // one large value would wedge the cluster forever.
            if (!out.isEmpty() && bytes + e.getSerializedSize() > maxBytes) {
                break;
            }
            bytes += e.getSerializedSize();
            out.add(e);
        }
        return out;
    }

    @Override
    public void append(List<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        long first = entries.get(0).getIndex();
        if (first < offset) {
            throw new IllegalArgumentException(
                    "append at " + first + " is below the first index " + offset);
        }
        if (first > lastIndex() + 1) {
            throw new IllegalArgumentException(
                    "append at " + first + " would leave a gap after " + lastIndex());
        }
        long expected = first;
        for (Entry e : entries) {
            if (e.getIndex() != expected) {
                throw new IllegalArgumentException(
                        "entries must be contiguous and ascending; expected index "
                                + expected
                                + " but got "
                                + e.getIndex());
            }
            expected++;
        }

        markDirty(first);
        int from = (int) (first - offset);
        // Truncate anything at or above `first`, then append. One operation, so a reader can never
        // see the gap in between.
        live.subList(from, live.size()).clear();
        live.addAll(entries);
    }

    @Override
    public void saveHardState(HardState state) {
        this.liveState = state;
        this.stateDirty = true;
    }

    @Override
    public HardState hardState() {
        return liveState;
    }

    @Override
    public void sync() {
        // Entries and hard state become durable together. A real log needs the same property: if
        // the commit index survives a crash but the entries it refers to do not, recovery cannot
        // tell the difference between that and losing committed data.
        if (dirtyFrom != Long.MAX_VALUE) {
            int keep = unchangedPrefixLength();
            durable.subList(keep, durable.size()).clear();
            durable.addAll(live.subList(keep, live.size()));
            dirtyFrom = Long.MAX_VALUE;
        }
        if (stateDirty) {
            durableState = liveState;
            stateDirty = false;
        }
    }

    /**
     * Discards everything not yet synced, as a power cut would.
     *
     * <p>The store is usable afterwards and reads what the disk would hold on restart.
     */
    public void crash() {
        if (dirtyFrom != Long.MAX_VALUE) {
            int keep = unchangedPrefixLength();
            live.subList(keep, live.size()).clear();
            live.addAll(durable.subList(keep, durable.size()));
            dirtyFrom = Long.MAX_VALUE;
        }
        liveState = durableState;
        stateDirty = false;
    }

    /**
     * Number of leading entries that both versions share, which is everything below the first index
     * modified since the last sync.
     *
     * <p>The durable list always reaches at least this far: entries below {@code dirtyFrom} have not
     * been touched since they were synced. If that ever stops holding, the two versions have drifted
     * and any repair would be a guess, so this fails loudly instead.
     */
    private int unchangedPrefixLength() {
        int keep = (int) Math.max(dirtyFrom - offset, 0);
        if (keep > durable.size() || keep > live.size()) {
            throw new IllegalStateException(
                    "dirty boundary "
                            + dirtyFrom
                            + " is past the end of the log (live="
                            + live.size()
                            + ", durable="
                            + durable.size()
                            + ", offset="
                            + offset
                            + ")");
        }
        return keep;
    }

    /** Entries that are durable, for assertions about what a crash would leave behind. */
    public List<Entry> durableEntries() {
        return Collections.unmodifiableList(durable);
    }

    private void markDirty(long index) {
        dirtyFrom = Math.min(dirtyFrom, index);
    }

    @Override
    public String toString() {
        return "MemoryLogStore[first="
                + firstIndex()
                + " last="
                + lastIndex()
                + " durableLast="
                + (offset + durable.size() - 1)
                + " term="
                + liveState.getTerm()
                + " vote="
                + liveState.getVote()
                + "]";
    }
}
