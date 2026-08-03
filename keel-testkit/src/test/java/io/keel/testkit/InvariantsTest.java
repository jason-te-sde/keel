package io.keel.testkit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.keel.proto.log.Entry;
import io.keel.proto.log.EntryType;
import io.keel.raft.Role;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the checker itself.
 *
 * <p>A checker nobody has tested is a checker that reports success. Each case here fabricates a state
 * that violates exactly one property and confirms the right one is named, which is why
 * {@link Invariants} works on {@link NodeView} records rather than on the simulator's internals:
 * these states are ones no real run would ever produce.
 */
class InvariantsTest {

    @Test
    @DisplayName("a healthy cluster passes")
    void healthyClusterPasses() {
        Invariants invariants = new Invariants(1);
        List<Entry> log = List.of(entry(1, 1, "a"), entry(2, 1, "b"));

        assertDoesNotThrow(
                () ->
                        invariants.check(
                                1,
                                List.of(
                                        node(1, Role.LEADER, 1, 2, log, log),
                                        node(2, Role.FOLLOWER, 1, 2, log, log),
                                        node(3, Role.FOLLOWER, 1, 2, log, log))));
        assertTrue(invariants.checkCount() > 0);
    }

    @Test
    @DisplayName("two leaders in one term is caught")
    void twoLeadersInOneTerm() {
        Invariants invariants = new Invariants(1);

        InvariantViolation e =
                assertThrows(
                        InvariantViolation.class,
                        () ->
                                invariants.check(
                                        7,
                                        List.of(
                                                node(1, Role.LEADER, 4, 0, List.of(), List.of()),
                                                node(2, Role.LEADER, 4, 0, List.of(), List.of()))));

        assertTrue(e.getMessage().contains("Election Safety"), e.getMessage());
        assertTrue(e.getMessage().contains("seed 1"), "the seed has to be in the message");
        assertTrue(e.getMessage().contains("tick 7"));
    }

    @Test
    @DisplayName("two leaders in different terms is fine")
    void twoLeadersInDifferentTermsIsFine() {
        // A deposed leader that has not noticed yet is normal, and a checker that flagged it would fire
        // on every partition.
        Invariants invariants = new Invariants(1);

        assertDoesNotThrow(
                () ->
                        invariants.check(
                                1,
                                List.of(
                                        node(1, Role.LEADER, 4, 0, List.of(), List.of()),
                                        node(2, Role.LEADER, 5, 0, List.of(), List.of()))));
    }

    @Test
    @DisplayName("a term going backwards is caught")
    void termGoingBackwards() {
        Invariants invariants = new Invariants(2);
        invariants.check(1, List.of(node(1, Role.FOLLOWER, 9, 0, List.of(), List.of())));

        InvariantViolation e =
                assertThrows(
                        InvariantViolation.class,
                        () ->
                                invariants.check(
                                        2, List.of(node(1, Role.FOLLOWER, 8, 0, List.of(), List.of()))));

        // This means a persisted term was lost, which is a durability bug rather than a consensus one.
        assertTrue(e.getMessage().contains("Term Monotonicity"), e.getMessage());
    }

    @Test
    @DisplayName("a commit index going backwards is caught")
    void commitGoingBackwards() {
        Invariants invariants = new Invariants(3);
        List<Entry> log = List.of(entry(1, 1, "a"), entry(2, 1, "b"));
        invariants.check(1, List.of(node(1, Role.FOLLOWER, 1, 2, log, log)));

        InvariantViolation e =
                assertThrows(
                        InvariantViolation.class,
                        () -> invariants.check(2, List.of(node(1, Role.FOLLOWER, 1, 1, log, log))));

        assertTrue(e.getMessage().contains("Commit Monotonicity"), e.getMessage());
    }

    @Test
    @DisplayName("two replicas applying different commands at one index is caught")
    void divergentApply() {
        Invariants invariants = new Invariants(4);

        InvariantViolation e =
                assertThrows(
                        InvariantViolation.class,
                        () ->
                                invariants.check(
                                        5,
                                        List.of(
                                                node(1, Role.LEADER, 2, 1, List.of(entry(1, 1, "a")), List.of()),
                                                node(2, Role.FOLLOWER, 2, 1, List.of(entry(1, 2, "b")), List.of()))));

        assertTrue(e.getMessage().contains("State Machine Safety"), e.getMessage());
    }

    @Test
    @DisplayName("a replica rewriting what it already applied is caught")
    void rewrittenApplyHistory() {
        Invariants invariants = new Invariants(5);
        invariants.check(1, List.of(node(1, Role.FOLLOWER, 1, 1, List.of(entry(1, 1, "a")), List.of())));

        InvariantViolation e =
                assertThrows(
                        InvariantViolation.class,
                        () ->
                                invariants.check(
                                        2,
                                        List.of(node(1, Role.FOLLOWER, 2, 1, List.of(entry(1, 2, "different")), List.of()))));

        assertTrue(e.getMessage().contains("State Machine Safety"), e.getMessage());
    }

    @Test
    @DisplayName("a replica replaying its log after a restart is not a violation")
    void replayAfterRestartIsFine() {
        // A restarted node applies from the beginning again. Identical content at the same index is
        // exactly what recovery is supposed to produce.
        Invariants invariants = new Invariants(6);
        List<Entry> log = List.of(entry(1, 1, "a"), entry(2, 1, "b"));
        invariants.check(1, List.of(node(1, Role.FOLLOWER, 1, 2, log, log)));
        invariants.check(2, List.of(downNode(1, log)));

        assertDoesNotThrow(
                () -> invariants.check(3, List.of(node(1, Role.FOLLOWER, 1, 2, List.of(entry(1, 1, "a")), log))));
    }

    @Test
    @DisplayName("logs that agree on a term but differ below it is caught")
    void logMatchingViolation() {
        Invariants invariants = new Invariants(7);
        List<Entry> a = List.of(entry(1, 1, "a"), entry(2, 2, "shared"));
        List<Entry> b = List.of(entry(1, 1, "DIFFERENT"), entry(2, 2, "shared"));

        InvariantViolation e =
                assertThrows(
                        InvariantViolation.class,
                        () ->
                                invariants.check(
                                        9,
                                        List.of(
                                                node(1, Role.LEADER, 2, 2, List.of(), a),
                                                node(2, Role.FOLLOWER, 2, 2, List.of(), b))));

        assertTrue(e.getMessage().contains("Log Matching"), e.getMessage());
    }

    private static NodeView node(
            long id, Role role, long term, long commit, List<Entry> applied, List<Entry> durable) {
        return new NodeView(id, role, term, commit, applied, durable, false);
    }

    private static NodeView downNode(long id, List<Entry> durable) {
        return new NodeView(id, Role.FOLLOWER, 1, 2, List.of(), durable, true);
    }

    private static Entry entry(long index, long term, String data) {
        return Entry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setType(EntryType.ENTRY_TYPE_NORMAL)
                .setData(ByteString.copyFromUtf8(data))
                .build();
    }
}
