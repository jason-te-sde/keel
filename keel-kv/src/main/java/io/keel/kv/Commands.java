package io.keel.kv;

import com.google.protobuf.ByteString;
import io.keel.proto.kv.CasCommand;
import io.keel.proto.kv.Command;
import io.keel.proto.kv.DeleteCommand;
import io.keel.proto.kv.PutCommand;
import io.keel.proto.kv.RegisterClientCommand;
import io.keel.proto.kv.Session;
import java.nio.charset.StandardCharsets;

/**
 * Builders for the commands that go into the Raft log.
 *
 * <p>Everything here returns serialized bytes, because that is what a log entry holds. Callers on both
 * sides of the wire build commands the same way, so there is one place where the encoding is decided.
 */
public final class Commands {

    private Commands() {}

    /** No session: the command is applied once with no retry protection. */
    public static final Session NO_SESSION = Session.getDefaultInstance();

    public static Session session(long clientId, long sequence) {
        return Session.newBuilder().setClientId(clientId).setSequence(sequence).build();
    }

    public static ByteString put(Session session, ByteString key, ByteString value) {
        return Command.newBuilder()
                .setSession(session)
                .setPut(PutCommand.newBuilder().setKey(key).setValue(value))
                .build()
                .toByteString();
    }

    public static ByteString put(Session session, String key, String value) {
        return put(session, utf8(key), utf8(value));
    }

    public static ByteString delete(Session session, ByteString key) {
        return Command.newBuilder()
                .setSession(session)
                .setDelete(DeleteCommand.newBuilder().setKey(key))
                .build()
                .toByteString();
    }

    public static ByteString delete(Session session, String key) {
        return delete(session, utf8(key));
    }

    /** Writes {@code value} only if the key currently holds {@code expected}. */
    public static ByteString compareAndSwap(
            Session session, ByteString key, ByteString expected, ByteString value) {
        return Command.newBuilder()
                .setSession(session)
                .setCas(
                        CasCommand.newBuilder()
                                .setKey(key)
                                .setExpected(expected)
                                .setValue(value))
                .build()
                .toByteString();
    }

    /** Writes {@code value} only if the key is absent, which is how an insert is expressed. */
    public static ByteString compareAndSwapIfAbsent(
            Session session, ByteString key, ByteString value) {
        return Command.newBuilder()
                .setSession(session)
                .setCas(
                        CasCommand.newBuilder()
                                .setKey(key)
                                .setExpectAbsent(true)
                                .setValue(value))
                .build()
                .toByteString();
    }

    /** Removes the key only if it currently holds {@code expected}. */
    public static ByteString compareAndDelete(Session session, ByteString key, ByteString expected) {
        return Command.newBuilder()
                .setSession(session)
                .setCas(
                        CasCommand.newBuilder()
                                .setKey(key)
                                .setExpected(expected)
                                .setDeleteOnMatch(true))
                .build()
                .toByteString();
    }

    /** Opens a session. The client id comes back in the result. */
    public static ByteString registerClient() {
        return Command.newBuilder()
                .setRegisterClient(RegisterClientCommand.getDefaultInstance())
                .build()
                .toByteString();
    }

    public static ByteString utf8(String s) {
        return ByteString.copyFrom(s, StandardCharsets.UTF_8);
    }
}
