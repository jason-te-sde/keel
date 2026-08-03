package io.keel.raft;

import com.google.protobuf.ByteString;
import io.keel.proto.log.Entry;
import io.keel.proto.log.EntryType;
import java.util.List;

/** Factories for log entries, so callers do not repeat builder boilerplate. */
public final class Entries {

    private Entries() {}

    /** An entry carrying a client command for the state machine. */
    public static Entry normal(long index, long term, byte[] data) {
        return Entry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setType(EntryType.ENTRY_TYPE_NORMAL)
                .setData(ByteString.copyFrom(data))
                .build();
    }

    /**
     * The empty entry a new leader appends on election.
     *
     * <p>It exists so the leader can commit something in its own term. Until it does, the commit
     * rule in paper section 5.4.2 forbids advancing the commit index over entries inherited from
     * earlier terms, and ReadIndex cannot establish a safe read point.
     */
    public static Entry noop(long index, long term) {
        return Entry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setType(EntryType.ENTRY_TYPE_NOOP)
                .build();
    }

    /** An entry carrying a serialized {@code ConfChange}. */
    public static Entry confChange(long index, long term, byte[] data) {
        return Entry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setType(EntryType.ENTRY_TYPE_CONF_CHANGE)
                .setData(ByteString.copyFrom(data))
                .build();
    }

    /** Total serialized size of {@code entries}, used for the per-message byte budget. */
    public static long byteSize(List<Entry> entries) {
        long total = 0;
        for (Entry e : entries) {
            total += e.getSerializedSize();
        }
        return total;
    }

    /** Index of the last entry, or {@code 0} for an empty list. */
    public static long lastIndex(List<Entry> entries) {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).getIndex();
    }
}
