package io.keel.kv;

import com.google.protobuf.ByteString;
import io.keel.proto.kv.CommandResult;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

/**
 * The replicated state machine.
 *
 * <p>Implementations have one hard obligation: {@link #apply} must be a deterministic, total function
 * of the current state and the command. Given the same log, every replica must reach byte-identical
 * state. That rules out reading a clock, consulting anything outside the state machine, iterating a
 * collection whose order is not defined, and failing in a way that depends on local conditions.
 *
 * <p>Applying is also not allowed to refuse. A command that has been committed will be applied on
 * every replica whether this one likes it or not, so a precondition that fails has to be represented
 * as a result ({@code applied = false}), never as an exception. Throwing would leave this replica's
 * state different from the others, which is the one thing consensus is supposed to prevent.
 */
public interface StateMachine extends AutoCloseable {

    /**
     * Applies one committed command.
     *
     * @param index the log index of the command, which is a deterministic source of identity for
     *     anything that needs one, such as a new client id
     * @param command the serialized {@code keel.kv.v1.Command}
     * @return what to return to the client; the same result is returned again for a retry of the same
     *     client sequence number
     */
    CommandResult apply(long index, ByteString command);

    /**
     * Reads a key from local state.
     *
     * <p>No consistency is promised here. Whoever calls it decides whether local state is safe to
     * read, which for a linearizable read means waiting until the applied index has reached an index
     * obtained from {@code RaftNode.requestRead}.
     */
    Optional<ByteString> get(ByteString key);

    /** Highest log index applied so far. */
    long appliedIndex();

    /** Number of keys, for status output and tests. */
    int size();

    /** Writes a complete snapshot, including the session table. */
    void snapshot(OutputStream out);

    /** Replaces all state with the contents of a snapshot. */
    void restore(InputStream in);

    @Override
    void close();
}
