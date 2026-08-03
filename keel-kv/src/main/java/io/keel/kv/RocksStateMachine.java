package io.keel.kv;

import com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A state machine backed by RocksDB, for a server whose key space does not fit in the heap.
 *
 * <p>Durability here is not load bearing, which is worth being explicit about. The Raft log is the
 * source of truth: on startup the node restores the newest snapshot into this store and replays the
 * log after it, and {@link #restore} clears everything first. Whatever survived in the RocksDB
 * directory is therefore discarded rather than trusted. That is what makes it safe for this store to
 * be behind the log, and it is why the interesting durability work lives in the write-ahead log
 * instead of here.
 *
 * <p>Keys are iterated in byte order, which matches {@link MemoryStateMachine}, so a snapshot taken by
 * one is byte-identical to a snapshot taken by the other from the same log.
 */
public final class RocksStateMachine extends AbstractStateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(RocksStateMachine.class);
    private static final int DEFAULT_MAX_SESSIONS = 4096;

    static {
        RocksDB.loadLibrary();
    }

    private final Path directory;
    private final Options options;
    private final RocksDB db;

    public static RocksStateMachine open(Path directory) {
        return new RocksStateMachine(directory, DEFAULT_MAX_SESSIONS);
    }

    public static RocksStateMachine open(Path directory, int maxSessions) {
        return new RocksStateMachine(directory, maxSessions);
    }

    private RocksStateMachine(Path directory, int maxSessions) {
        super(maxSessions);
        this.directory = directory;
        this.options = new Options().setCreateIfMissing(true);
        try {
            this.db = RocksDB.open(options, directory.toString());
        } catch (RocksDBException e) {
            options.close();
            throw new StateMachineFailure("failed to open RocksDB in " + directory, e);
        }
        LOG.debug("opened RocksDB state machine in {}", directory);
    }

    @Override
    protected Optional<ByteString> read(ByteString key) {
        try {
            byte[] value = db.get(key.toByteArray());
            return value == null ? Optional.empty() : Optional.of(ByteString.copyFrom(value));
        } catch (RocksDBException e) {
            throw new StateMachineFailure("failed to read a key", e);
        }
    }

    @Override
    protected void write(ByteString key, ByteString value) {
        try {
            db.put(key.toByteArray(), value.toByteArray());
        } catch (RocksDBException e) {
            throw new StateMachineFailure("failed to write a key", e);
        }
    }

    @Override
    protected void remove(ByteString key) {
        try {
            db.delete(key.toByteArray());
        } catch (RocksDBException e) {
            throw new StateMachineFailure("failed to delete a key", e);
        }
    }

    @Override
    protected void clear() {
        List<byte[]> keys = new ArrayList<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                keys.add(it.key());
            }
        }
        try {
            for (byte[] key : keys) {
                db.delete(key);
            }
        } catch (RocksDBException e) {
            throw new StateMachineFailure("failed to clear the store", e);
        }
    }

    @Override
    protected Iterable<Map.Entry<ByteString, ByteString>> pairs() {
        // Materialized rather than streamed: a snapshot is serialized into one protobuf message
        // anyway, and holding an iterator open across that serialization would pin a RocksDB
        // snapshot for no benefit.
        List<Map.Entry<ByteString, ByteString>> out = new ArrayList<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                out.add(
                        new AbstractMap.SimpleImmutableEntry<>(
                                ByteString.copyFrom(it.key()), ByteString.copyFrom(it.value())));
            }
        }
        return out;
    }

    @Override
    public int size() {
        // RocksDB has no cheap exact count. Iterating is fine for status output and tests, and this is
        // not on any hot path.
        int count = 0;
        try (RocksIterator it = db.newIterator()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void close() {
        db.close();
        options.close();
    }

    @Override
    public String toString() {
        return "RocksStateMachine[" + directory + " applied=" + appliedIndex() + "]";
    }

    /** Thrown when the backing store fails in a way the state machine cannot resolve. */
    public static final class StateMachineFailure extends RuntimeException {
        StateMachineFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
