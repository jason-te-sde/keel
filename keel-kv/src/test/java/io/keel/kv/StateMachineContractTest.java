package io.keel.kv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.keel.proto.kv.CommandResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * One suite run against every backend.
 *
 * <p>Written as a contract rather than as two test classes on purpose. The in-memory store and the
 * RocksDB store are applied to the same log by different replicas of the same cluster, so any
 * behavioural difference between them is a correctness bug, not an implementation detail. A shared
 * suite makes that difference impossible to introduce quietly.
 */
class StateMachineContractTest {

    @TempDir Path dir;

    private StateMachine open(String backend) {
        return switch (backend) {
            case "memory" -> new MemoryStateMachine();
            case "rocks" -> RocksStateMachine.open(dir.resolve("rocks"));
            default -> throw new IllegalArgumentException(backend);
        };
    }

    private StateMachine open(String backend, int maxSessions) {
        return switch (backend) {
            case "memory" -> new MemoryStateMachine(maxSessions);
            case "rocks" -> RocksStateMachine.open(dir.resolve("rocks-" + maxSessions), maxSessions);
            default -> throw new IllegalArgumentException(backend);
        };
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void putThenGet(String backend) {
        try (StateMachine sm = open(backend)) {
            CommandResult result = sm.apply(1, Commands.put(Commands.NO_SESSION, "k", "v"));

            assertTrue(result.getApplied());
            assertEquals("v", utf8(sm.get(Commands.utf8("k")).orElseThrow()));
            assertEquals(1, sm.appliedIndex());
            assertEquals(1, sm.size());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void deleteReportsWhetherTheKeyWasThere(String backend) {
        try (StateMachine sm = open(backend)) {
            sm.apply(1, Commands.put(Commands.NO_SESSION, "k", "v"));

            assertTrue(sm.apply(2, Commands.delete(Commands.NO_SESSION, "k")).getFound());
            assertFalse(sm.apply(3, Commands.delete(Commands.NO_SESSION, "k")).getFound());
            assertTrue(sm.get(Commands.utf8("k")).isEmpty());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void compareAndSwapDistinguishesItsThreeOutcomes(String backend) {
        try (StateMachine sm = open(backend)) {
            // Absent, so an insert succeeds and a second insert does not.
            assertTrue(
                    sm.apply(1, Commands.compareAndSwapIfAbsent(Commands.NO_SESSION, key("k"), val("first")))
                            .getApplied());
            CommandResult second =
                    sm.apply(2, Commands.compareAndSwapIfAbsent(Commands.NO_SESSION, key("k"), val("again")));
            assertFalse(second.getApplied(), "the key exists, so expect-absent must fail");
            assertEquals("first", utf8(second.getValue()), "and the caller learns the current value");

            // Matching value, so the swap goes through.
            assertTrue(
                    sm.apply(3, Commands.compareAndSwap(Commands.NO_SESSION, key("k"), val("first"), val("second")))
                            .getApplied());
            assertEquals("second", utf8(sm.get(key("k")).orElseThrow()));

            // Mismatched value, so it does not.
            CommandResult mismatch =
                    sm.apply(4, Commands.compareAndSwap(Commands.NO_SESSION, key("k"), val("first"), val("third")));
            assertFalse(mismatch.getApplied());
            assertEquals("second", utf8(sm.get(key("k")).orElseThrow()), "state must be untouched");
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void compareAndDelete(String backend) {
        try (StateMachine sm = open(backend)) {
            sm.apply(1, Commands.put(Commands.NO_SESSION, "k", "v"));

            assertFalse(
                    sm.apply(2, Commands.compareAndDelete(Commands.NO_SESSION, key("k"), val("other")))
                            .getApplied());
            assertTrue(sm.get(key("k")).isPresent());

            assertTrue(
                    sm.apply(3, Commands.compareAndDelete(Commands.NO_SESSION, key("k"), val("v")))
                            .getApplied());
            assertTrue(sm.get(key("k")).isEmpty());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void aRetriedRequestIsAppliedOnce(String backend) {
        try (StateMachine sm = open(backend)) {
            long clientId = sm.apply(1, Commands.registerClient()).getClientId();
            assertEquals(1, clientId, "the client id is the log index of the registration");

            // A counter, incremented by compare-and-swap. Applying the retry twice would be visible.
            sm.apply(2, Commands.put(Commands.session(clientId, 1), "n", "1"));
            ByteString increment =
                    Commands.compareAndSwap(Commands.session(clientId, 2), key("n"), val("1"), val("2"));

            CommandResult first = sm.apply(3, increment);
            CommandResult retry = sm.apply(4, increment);

            assertTrue(first.getApplied());
            assertTrue(retry.getApplied(), "a retry returns the original answer, not a failure");
            assertEquals("2", utf8(sm.get(key("n")).orElseThrow()), "and must not have applied twice");
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void aRetryOfAFailedPreconditionReturnsTheSameFailure(String backend) {
        try (StateMachine sm = open(backend)) {
            long clientId = sm.apply(1, Commands.registerClient()).getClientId();
            ByteString doomed =
                    Commands.compareAndSwap(Commands.session(clientId, 1), key("k"), val("expected"), val("v"));

            CommandResult first = sm.apply(2, doomed);
            CommandResult retry = sm.apply(3, doomed);

            assertFalse(first.getApplied());
            assertEquals(first, retry, "the stored answer is returned verbatim");
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void anUnknownSessionIsRefused(String backend) {
        try (StateMachine sm = open(backend)) {
            CommandResult result = sm.apply(1, Commands.put(Commands.session(999, 1), "k", "v"));

            // Applying it would defeat the point of sessions: the retry that follows would apply a
            // second time.
            assertFalse(result.getApplied());
            assertTrue(result.getMessage().contains("session"), result.getMessage());
            assertEquals(0, sm.size());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void snapshotAndRestoreRoundTripsKeysAndSessions(String backend) {
        byte[] snapshot;
        long clientId;
        try (StateMachine sm = open(backend)) {
            clientId = sm.apply(1, Commands.registerClient()).getClientId();
            sm.apply(2, Commands.put(Commands.session(clientId, 1), "a", "1"));
            sm.apply(3, Commands.put(Commands.session(clientId, 2), "b", "2"));
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            sm.snapshot(out);
            snapshot = out.toByteArray();
        }

        // Reopened from scratch, and for RocksDB that means the old directory is still there.
        try (StateMachine restored = open(backend)) {
            restored.apply(1, Commands.put(Commands.NO_SESSION, "junk", "junk"));
            restored.restore(new ByteArrayInputStream(snapshot));

            assertEquals(2, restored.size(), "restore replaces state rather than merging into it");
            assertEquals("1", utf8(restored.get(key("a")).orElseThrow()));
            assertEquals(3, restored.appliedIndex());

            // The session came back too, so a retry across the handover is still deduplicated.
            CommandResult retry =
                    restored.apply(4, Commands.put(Commands.session(clientId, 2), "b", "different"));
            assertTrue(retry.getApplied());
            assertEquals(
                    "2",
                    utf8(restored.get(key("b")).orElseThrow()),
                    "a session table that did not survive the snapshot is not a session table");
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void replayingTheSameLogTwiceGivesIdenticalSnapshots(String backend) {
        List<ByteString> log = new ArrayList<>();
        log.add(Commands.registerClient());
        for (int i = 0; i < 40; i++) {
            log.add(Commands.put(Commands.session(1, i + 1), "key-" + (i % 7), "value-" + i));
        }
        log.add(Commands.delete(Commands.session(1, 41), "key-3"));

        byte[] first = replay(backend, "replay-a", log);
        byte[] second = replay(backend, "replay-b", log);

        // The property every replica depends on: same log in, same bytes out.
        assertArrayEquals(first, second);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"memory", "rocks"})
    void sessionEvictionIsAFunctionOfTheLogAlone(String backend) {
        // Eviction has to be deterministic or two replicas will forget different sessions and then
        // disagree about which retries to deduplicate.
        List<ByteString> log = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            log.add(Commands.registerClient());
        }

        byte[] first = replay(backend, "evict-a", log, 4);
        byte[] second = replay(backend, "evict-b", log, 4);

        assertArrayEquals(first, second);
    }

    private byte[] replay(String backend, String tag, List<ByteString> log) {
        return replay(backend, tag, log, 4096);
    }

    private byte[] replay(String backend, String tag, List<ByteString> log, int maxSessions) {
        StateMachine sm =
                switch (backend) {
                    case "memory" -> new MemoryStateMachine(maxSessions);
                    case "rocks" -> RocksStateMachine.open(dir.resolve(tag), maxSessions);
                    default -> throw new IllegalArgumentException(backend);
                };
        try {
            for (int i = 0; i < log.size(); i++) {
                sm.apply(i + 1, log.get(i));
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            sm.snapshot(out);
            return out.toByteArray();
        } finally {
            sm.close();
        }
    }

    private static ByteString key(String s) {
        return Commands.utf8(s);
    }

    private static ByteString val(String s) {
        return Commands.utf8(s);
    }

    private static String utf8(ByteString b) {
        return b.toStringUtf8();
    }
}
