package io.keel.kv;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.keel.proto.kv.CasCommand;
import io.keel.proto.kv.Command;
import io.keel.proto.kv.CommandResult;
import io.keel.proto.kv.KvSnapshot;
import io.keel.proto.kv.PutCommand;
import io.keel.proto.kv.Session;
import io.keel.proto.kv.SnapshotPair;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Command interpretation and session handling, shared by every storage backend.
 *
 * <p>Splitting it this way keeps the part that has to be deterministic in one place. A backend only
 * has to store bytes and iterate them in key order; it has no say in what a command means, which
 * removes the possibility of two backends disagreeing about a compare-and-swap.
 */
abstract class AbstractStateMachine implements StateMachine {

    private final SessionTable sessions;
    private long appliedIndex;

    AbstractStateMachine(int maxSessions) {
        this.sessions = new SessionTable(maxSessions);
    }

    // --- backend contract ---------------------------------------------------------------------

    protected abstract Optional<ByteString> read(ByteString key);

    protected abstract void write(ByteString key, ByteString value);

    protected abstract void remove(ByteString key);

    protected abstract void clear();

    /** Every pair, in ascending key order, so snapshots are byte-identical across replicas. */
    protected abstract Iterable<Map.Entry<ByteString, ByteString>> pairs();

    // --- state machine ------------------------------------------------------------------------

    @Override
    public final CommandResult apply(long index, ByteString commandBytes) {
        Command command;
        try {
            command = Command.parseFrom(commandBytes);
        } catch (InvalidProtocolBufferException e) {
            // This command is in the log and every replica will see it. Refusing to apply would make
            // this replica diverge, so the only safe response is a deterministic rejection.
            appliedIndex = index;
            return CommandResult.newBuilder()
                    .setApplied(false)
                    .setMessage("command at index " + index + " did not parse: " + e.getMessage())
                    .build();
        }

        Session session = command.getSession();
        long clientId = session.getClientId();
        long sequence = session.getSequence();

        if (clientId != 0 && !command.hasRegisterClient()) {
            if (!sessions.isRegistered(clientId)) {
                // Applying a command from a session this replica has forgotten would defeat the point
                // of having sessions: the retry that follows would be applied a second time.
                appliedIndex = index;
                return CommandResult.newBuilder()
                        .setApplied(false)
                        .setMessage("session " + clientId + " is not open; register again")
                        .build();
            }
            Optional<CommandResult> replay = sessions.replayOf(clientId, sequence);
            if (replay.isPresent()) {
                appliedIndex = index;
                return replay.get();
            }
        }

        CommandResult result = interpret(index, command);
        if (clientId != 0 && !command.hasRegisterClient()) {
            sessions.record(clientId, sequence, index, result);
        }
        appliedIndex = index;
        return result;
    }

    private CommandResult interpret(long index, Command command) {
        return switch (command.getKindCase()) {
            case PUT -> put(command.getPut());
            case DELETE -> delete(command.getDelete().getKey());
            case CAS -> compareAndSwap(command.getCas());
            case REGISTER_CLIENT -> registerClient(index);
            case KIND_NOT_SET ->
                    CommandResult.newBuilder()
                            .setApplied(false)
                            .setMessage("command carried no operation")
                            .build();
        };
    }

    private CommandResult put(PutCommand put) {
        write(put.getKey(), put.getValue());
        return CommandResult.newBuilder().setApplied(true).build();
    }

    private CommandResult delete(ByteString key) {
        boolean existed = read(key).isPresent();
        remove(key);
        return CommandResult.newBuilder().setApplied(true).setFound(existed).build();
    }

    private CommandResult compareAndSwap(CasCommand cas) {
        Optional<ByteString> current = read(cas.getKey());
        boolean matches =
                cas.getExpectAbsent() ? current.isEmpty() : current.filter(cas.getExpected()::equals).isPresent();

        if (!matches) {
            // A failed precondition is an outcome, not an error. Every replica computes the same
            // answer from the same state, which is exactly what applying has to be.
            CommandResult.Builder rejected =
                    CommandResult.newBuilder().setApplied(false).setFound(current.isPresent());
            current.ifPresent(rejected::setValue);
            return rejected.build();
        }
        if (cas.getDeleteOnMatch()) {
            remove(cas.getKey());
        } else {
            write(cas.getKey(), cas.getValue());
        }
        return CommandResult.newBuilder().setApplied(true).setFound(current.isPresent()).build();
    }

    /**
     * Opens a session, using the command's own log index as the client id.
     *
     * <p>A counter would work too, and would be one more thing that has to be snapshotted and kept
     * identical on every replica. The log index is already unique, already agreed, and already
     * durable.
     */
    private CommandResult registerClient(long index) {
        sessions.register(index, index);
        return CommandResult.newBuilder().setApplied(true).setClientId(index).build();
    }

    @Override
    public final Optional<ByteString> get(ByteString key) {
        return read(key);
    }

    @Override
    public final long appliedIndex() {
        return appliedIndex;
    }

    /** Open sessions, for status output and tests. */
    public final int sessionCount() {
        return sessions.size();
    }

    @Override
    public final void snapshot(OutputStream out) {
        KvSnapshot.Builder snapshot = KvSnapshot.newBuilder().setLastAppliedIndex(appliedIndex);
        for (Map.Entry<ByteString, ByteString> pair : pairs()) {
            snapshot.addPairs(
                    SnapshotPair.newBuilder().setKey(pair.getKey()).setValue(pair.getValue()));
        }
        snapshot.addAllSessions(sessions.toSnapshot());
        try {
            snapshot.build().writeTo(out);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write a state machine snapshot", e);
        }
    }

    @Override
    public final void restore(InputStream in) {
        KvSnapshot snapshot;
        try {
            snapshot = KvSnapshot.parseFrom(in);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read a state machine snapshot", e);
        }
        clear();
        for (SnapshotPair pair : snapshot.getPairsList()) {
            write(pair.getKey(), pair.getValue());
        }
        sessions.restore(List.copyOf(snapshot.getSessionsList()));
        appliedIndex = snapshot.getLastAppliedIndex();
    }
}
