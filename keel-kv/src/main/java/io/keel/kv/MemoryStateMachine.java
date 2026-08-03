package io.keel.kv;

import com.google.protobuf.ByteString;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * An in-memory state machine, ordered by key.
 *
 * <p>Used by the simulator, where thousands of replicas are created and destroyed and a native
 * database would dominate the runtime. A {@link TreeMap} with an explicit comparator rather than a
 * hash map because snapshots have to serialize in the same order on every replica, and "whatever
 * order the hash table happens to be in" is not that.
 */
public final class MemoryStateMachine extends AbstractStateMachine {

    /**
     * Unsigned byte-wise order, which is what RocksDB's default comparator does.
     *
     * <p>Spelled out rather than borrowed so the two backends provably agree: snapshots are compared
     * byte for byte across replicas, and a signed comparison would order any key with a high bit set
     * differently.
     */
    private static final Comparator<ByteString> BY_BYTES =
            (a, b) -> {
                int shared = Math.min(a.size(), b.size());
                for (int i = 0; i < shared; i++) {
                    int cmp = Integer.compare(a.byteAt(i) & 0xFF, b.byteAt(i) & 0xFF);
                    if (cmp != 0) {
                        return cmp;
                    }
                }
                return Integer.compare(a.size(), b.size());
            };

    private static final int DEFAULT_MAX_SESSIONS = 4096;

    private final TreeMap<ByteString, ByteString> data = new TreeMap<>(BY_BYTES);

    public MemoryStateMachine() {
        this(DEFAULT_MAX_SESSIONS);
    }

    public MemoryStateMachine(int maxSessions) {
        super(maxSessions);
    }

    @Override
    protected Optional<ByteString> read(ByteString key) {
        return Optional.ofNullable(data.get(key));
    }

    @Override
    protected void write(ByteString key, ByteString value) {
        data.put(key, value);
    }

    @Override
    protected void remove(ByteString key) {
        data.remove(key);
    }

    @Override
    protected void clear() {
        data.clear();
    }

    @Override
    protected Iterable<Map.Entry<ByteString, ByteString>> pairs() {
        return data.entrySet();
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public void close() {
        data.clear();
    }

    @Override
    public String toString() {
        return "MemoryStateMachine[keys=" + data.size() + " applied=" + appliedIndex() + "]";
    }
}
