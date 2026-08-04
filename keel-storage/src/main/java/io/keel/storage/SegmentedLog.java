package io.keel.storage;

import io.keel.proto.log.Entry;
import io.keel.proto.log.HardState;
import io.keel.proto.log.Record;
import io.keel.proto.log.SnapshotMetadata;
import io.keel.raft.LogStore;
import io.keel.raft.RaftStorage.CompactedException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A segmented, checksummed, append-only log.
 *
 * <p>Nothing is ever rewritten in place. Overwriting a conflicting suffix appends the replacement
 * entries and lets them supersede the earlier ones during replay, so a crash can only ever cost a
 * suffix of the file. The alternative, truncating the file and then writing, has a window where a
 * crash leaves the old entries gone and the new ones absent.
 *
 * <p>That choice matters most for the hard state, which shares this log so that a term, a vote, and
 * the entries they relate to all become durable in one fsync. If a suffix truncation could remove an
 * already-written vote record, a crash at the wrong moment would resurrect an older term and let the
 * node vote twice in one term, which is precisely how two leaders happen. Append-only replay makes
 * that unrepresentable.
 *
 * <p>The cost is space: a log that is overwritten repeatedly holds dead records until the next
 * snapshot lets whole segments go. Conflicting suffixes only occur on leader changes, so in practice
 * this is a rounding error.
 *
 * <p>Not thread safe. One thread owns the log, as one thread owns the core.
 */
public final class SegmentedLog implements LogStore {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentedLog.class);

    /** Where one entry lives, plus its term so that reading a term never touches the disk. */
    private record Loc(Segment segment, long offset, int framedLength, long term) {}

    private final LogOptions options;
    private final List<Segment> segments = new ArrayList<>();
    private final ArrayList<Loc> locations = new ArrayList<>();

    private long firstIndex = 1;
    private long nextSequence = 1;
    private SnapshotMetadata snapshot = SnapshotMetadata.getDefaultInstance();
    private HardState state = HardState.getDefaultInstance();
    private boolean dirty;
    private long discardedBytes;

    private SegmentedLog(LogOptions options) {
        this.options = options;
    }

    /** Opens, or creates, the log in {@code options.directory()} and recovers it. */
    public static SegmentedLog open(LogOptions options) {
        SegmentedLog log = new SegmentedLog(options);
        try {
            Files.createDirectories(options.directory());
            log.recover();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open log in " + options.directory(), e);
        }
        return log;
    }

    // ---------------------------------------------------------------------------------------------
    // Recovery
    // ---------------------------------------------------------------------------------------------

    private void recover() throws IOException {
        List<Path> files = segmentFiles();
        if (files.isEmpty()) {
            segments.add(Segment.create(options.directory(), nextSequence++, 1));
            syncDirectory();
            LOG.info("initialised an empty log in {}", options.directory());
            return;
        }

        firstIndex = Segment.parseBaseIndex(files.get(0));
        for (int i = 0; i < files.size(); i++) {
            boolean isNewest = i == files.size() - 1;
            Segment segment = Segment.open(files.get(i));
            segments.add(segment);
            nextSequence = Math.max(nextSequence, segment.sequence() + 1);
            replay(segment, isNewest);
        }

        LOG.info(
                "recovered {} segments from {}: indexes [{},{}], term={} vote={} commit={}{}",
                segments.size(),
                options.directory(),
                firstIndex,
                lastIndex(),
                state.getTerm(),
                state.getVote(),
                state.getCommit(),
                discardedBytes == 0 ? "" : ", discarded " + discardedBytes + " unfinished bytes");
    }

    /** Replays one segment, stopping at a torn tail and failing on anything worse. */
    private void replay(Segment segment, boolean isNewest) {
        ByteBuffer buf = segment.readAll();
        long validBytes = 0;

        while (buf.hasRemaining()) {
            int start = buf.position();
            RecordCodec.ReadResult result = RecordCodec.read(buf, options.maxRecordBytes());

            if (result instanceof RecordCodec.ReadResult.Ok ok) {
                apply(ok.record(), segment, start, ok.framedBytes());
                validBytes = buf.position();
                continue;
            }

            String detail;
            boolean tolerable;
            if (result instanceof RecordCodec.ReadResult.Truncated truncated) {
                // A record that does not fit in the file it is in was never finished being written.
                detail = truncated.detail();
                tolerable = isNewest;
            } else {
                RecordCodec.ReadResult.Damaged damaged = (RecordCodec.ReadResult.Damaged) result;
                detail = damaged.detail();
                // A complete record that fails its checksum is only excusable as the very last thing
                // in the newest segment, where a partial write can leave a full-length record with
                // garbage in it. With valid data after it, the bytes were corrupted in place.
                tolerable = isNewest && start + damaged.framedBytes() >= segment.size();
            }

            if (!tolerable) {
                throw new CorruptLogException(segment.path(), start, detail);
            }
            discardedBytes += segment.size() - validBytes;
            LOG.warn(
                    "discarding {} unfinished bytes at the end of {}: {}",
                    segment.size() - validBytes,
                    segment.path().getFileName(),
                    detail);
            segment.truncateTo(validBytes);
            segment.sync();
            return;
        }
    }

    /** Folds one recovered record into the in-memory view. */
    private void apply(Record record, Segment segment, long offset, int framedLength) {
        if (record.hasState()) {
            state = record.getState();
            return;
        }
        if (record.hasSnapshot()) {
            // Entries at or below the boundary are covered by a state machine snapshot, so the ones
            // already replayed are void. Entries *above* it are not: compaction keeps the tail, and
            // those records were written before the marker. Dropping them here would silently lose
            // committed entries on every restart after a compaction.
            SnapshotMetadata meta = record.getSnapshot();
            long newFirst = meta.getLastIndex() + 1;
            if (newFirst > firstIndex) {
                int drop = (int) Math.min(newFirst - firstIndex, locations.size());
                locations.subList(0, drop).clear();
                firstIndex = newFirst;
            }
            snapshot = meta;
            return;
        }
        if (!record.hasEntry()) {
            // An unknown record kind from a future version. Refusing is safer than ignoring: this
            // build cannot know whether skipping it changes the meaning of the log.
            throw new CorruptLogException(
                    segment.path(), offset, "record has no known payload; written by a newer build?");
        }

        Entry entry = record.getEntry();
        long expected = firstIndex + locations.size();
        if (entry.getIndex() == expected) {
            locations.add(new Loc(segment, offset, framedLength, entry.getTerm()));
            return;
        }
        if (entry.getIndex() < expected && entry.getIndex() >= firstIndex) {
            // A replacement written after a conflict. It supersedes this index and everything above.
            locations.subList((int) (entry.getIndex() - firstIndex), locations.size()).clear();
            locations.add(new Loc(segment, offset, framedLength, entry.getTerm()));
            return;
        }
        throw new CorruptLogException(
                segment.path(),
                offset,
                "entry index " + entry.getIndex() + " leaves a gap after " + (expected - 1));
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(options.directory())) {
            // Ordered by creation sequence, which is the order the records were written in.
            // Sorting by base index instead would replay a superseded entry after its replacement.
            return files.filter(p -> Segment.parseSequence(p) >= 0)
                    .sorted(Comparator.comparingLong(Segment::parseSequence))
                    .toList();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Reads
    // ---------------------------------------------------------------------------------------------

    @Override
    public long firstIndex() {
        return firstIndex;
    }

    @Override
    public long lastIndex() {
        return firstIndex + locations.size() - 1;
    }

    @Override
    public SnapshotMetadata snapshotMetadata() {
        return snapshot;
    }

    @Override
    public long term(long index) {
        if (index == firstIndex - 1) {
            // The snapshot boundary. A zeroed snapshot reports term 0, which is right for a log that
            // has never been compacted.
            return snapshot.getLastTerm();
        }
        if (index < firstIndex) {
            throw new CompactedException(index, firstIndex);
        }
        if (index > lastIndex()) {
            throw new IllegalArgumentException(
                    "index " + index + " is past the last index " + lastIndex());
        }
        return locations.get((int) (index - firstIndex)).term();
    }

    @Override
    public List<Entry> entries(long lo, long hi, long maxBytes) {
        if (lo < firstIndex) {
            throw new CompactedException(lo, firstIndex);
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
            Loc loc = locations.get((int) (i - firstIndex));
            Entry entry = RecordCodec.decode(loc.segment().read(loc.offset(), loc.framedLength())).getEntry();
            // Always return one entry, however large: refusing would stall replication permanently.
            if (!out.isEmpty() && bytes + entry.getSerializedSize() > maxBytes) {
                break;
            }
            bytes += entry.getSerializedSize();
            out.add(entry);
        }
        return out;
    }

    @Override
    public HardState hardState() {
        return state;
    }

    // ---------------------------------------------------------------------------------------------
    // Writes
    // ---------------------------------------------------------------------------------------------

    @Override
    public void append(List<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        long first = entries.get(0).getIndex();
        if (first < firstIndex) {
            throw new IllegalArgumentException(
                    "append at " + first + " is below the first index " + firstIndex);
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

        if (first <= lastIndex()) {
            // Drop the superseded tail from the index. The records stay on disk and are skipped on
            // the next replay, because the replacements written below appear after them.
            locations.subList((int) (first - firstIndex), locations.size()).clear();
        }
        for (Entry entry : entries) {
            byte[] framed = RecordCodec.encode(Record.newBuilder().setEntry(entry).build());
            Segment target = segmentFor(framed.length);
            long offset = target.append(framed);
            locations.add(new Loc(target, offset, framed.length, entry.getTerm()));
        }
        dirty = true;
    }

    @Override
    public void saveHardState(HardState newState) {
        byte[] framed = RecordCodec.encode(Record.newBuilder().setState(newState).build());
        segmentFor(framed.length).append(framed);
        this.state = newState;
        dirty = true;
    }

    @Override
    public void compact(SnapshotMetadata meta) {
        if (meta.getLastIndex() < firstIndex - 1) {
            throw new IllegalArgumentException(
                    "snapshot at "
                            + meta.getLastIndex()
                            + " is behind the current boundary "
                            + (firstIndex - 1));
        }
        if (meta.getLastIndex() > lastIndex()) {
            throw new IllegalArgumentException(
                    "cannot compact to " + meta.getLastIndex() + " past the last index " + lastIndex());
        }
        if (meta.getLastIndex() >= firstIndex && meta.getLastIndex() <= lastIndex()) {
            long actual = locations.get((int) (meta.getLastIndex() - firstIndex)).term();
            if (actual != meta.getLastTerm()) {
                // A follower receiving this snapshot compares the term at the boundary to decide
                // whether it may keep its own entries. A wrong term makes it discard entries it has
                // already acknowledged, which can leave a committed entry on fewer than a majority.
                throw new IllegalArgumentException(
                        "snapshot claims term "
                                + meta.getLastTerm()
                                + " at index "
                                + meta.getLastIndex()
                                + " but the log holds term "
                                + actual);
            }
        }

        // The marker is a normal appended record, so a crash either has it or does not; there is no
        // state where the log is compacted but does not say so. It is synced before any segment is
        // deleted, because deleting first would leave a log whose entries are gone and whose
        // boundary still claims they should be there.
        byte[] framed = RecordCodec.encode(Record.newBuilder().setSnapshot(meta).build());
        segmentFor(framed.length).append(framed);
        dirty = true;
        sync();

        int drop = (int) (meta.getLastIndex() - firstIndex + 1);
        locations.subList(0, drop).clear();
        firstIndex = meta.getLastIndex() + 1;
        snapshot = meta;
        deleteUnreferencedSegments();
    }

    @Override
    public void installSnapshot(SnapshotMetadata meta) {
        // A follower catching up from a snapshot discards its whole log, not just the prefix: its
        // entries above the boundary may have come from a leader that lost, so they are not known to
        // be valid either.
        locations.clear();
        for (Segment segment : segments) {
            segment.delete();
        }
        segments.clear();
        firstIndex = meta.getLastIndex() + 1;
        snapshot = meta;
        try {
            segments.add(Segment.create(options.directory(), nextSequence++, firstIndex));
            syncDirectory();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start a segment after installing a snapshot", e);
        }
        byte[] framed = RecordCodec.encode(Record.newBuilder().setSnapshot(meta).build());
        active().append(framed);
        dirty = true;
        sync();
    }

    /**
     * Deletes segments no live entry points into.
     *
     * <p>The rule is emptiness rather than base index, because a superseding append can put a low
     * index into a segment created later: a segment's name is a hint, and the location index is the
     * only authority on what is still reachable.
     */
    private void deleteUnreferencedSegments() {
        java.util.Set<Segment> referenced =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (Loc loc : locations) {
            referenced.add(loc.segment());
        }
        Segment active = active();
        java.util.Iterator<Segment> it = segments.iterator();
        int removed = 0;
        while (it.hasNext()) {
            Segment segment = it.next();
            if (segment != active && !referenced.contains(segment)) {
                segment.delete();
                it.remove();
                removed++;
            }
        }
        if (removed > 0) {
            LOG.info(
                    "compacted to index {}, deleted {} segment{}",
                    snapshot.getLastIndex(),
                    removed,
                    removed == 1 ? "" : "s");
        }
    }

    @Override
    public void sync() {
        if (!dirty) {
            return;
        }
        active().sync();
        dirty = false;
    }

    @Override
    public void close() {
        for (Segment segment : segments) {
            segment.close();
        }
        segments.clear();
    }

    // ---------------------------------------------------------------------------------------------
    // Segments
    // ---------------------------------------------------------------------------------------------

    private Segment active() {
        return segments.get(segments.size() - 1);
    }

    /**
     * The segment a record of {@code recordBytes} should go in, rotating first if it would not fit.
     *
     * <p>An empty segment always accepts the record even if it is over the cap, so one large entry
     * cannot become unwritable.
     */
    private Segment segmentFor(int recordBytes) {
        Segment current = active();
        if (current.size() > 0 && current.size() + recordBytes > options.maxSegmentBytes()) {
            // Sync before rotating so the only segment that can be dirty is the active one.
            current.sync();
            long nextIndex = firstIndex + locations.size();
            try {
                Segment fresh = Segment.create(options.directory(), nextSequence++, nextIndex);
                segments.add(fresh);
                syncDirectory();
                LOG.debug("rotated to segment {}", fresh.path().getFileName());
                return fresh;
            } catch (IOException e) {
                throw new UncheckedIOException("failed to roll a new segment", e);
            }
        }
        return current;
    }

    /**
     * Syncs the directory so a newly created segment file is itself durable.
     *
     * <p>Syncing a file does not make its directory entry durable, so without this a crash can leave
     * records that were written to a file that no longer exists.
     */
    private void syncDirectory() {
        try (FileChannel dir = FileChannel.open(options.directory(), StandardOpenOption.READ)) {
            dir.force(true);
        } catch (IOException e) {
            // Not every platform allows opening a directory as a channel. Losing this sync risks a
            // freshly created segment vanishing in a crash, which recovery would see as a shorter
            // log, so it is worth a warning but not a failed write.
            LOG.warn("could not sync the log directory: {}", e.getMessage());
        }
    }

    /** Number of segment files currently open. */
    public int segmentCount() {
        return segments.size();
    }

    /** Bytes dropped from the tail during recovery because they were never finished. */
    public long discardedBytesOnRecovery() {
        return discardedBytes;
    }

    @Override
    public String toString() {
        return "SegmentedLog["
                + options.directory()
                + " indexes=["
                + firstIndex
                + ","
                + lastIndex()
                + "] segments="
                + segments.size()
                + "]";
    }
}
