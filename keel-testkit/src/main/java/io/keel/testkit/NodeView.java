package io.keel.testkit;

import io.keel.proto.log.Entry;
import io.keel.raft.Role;
import java.util.List;

/**
 * What one node looks like from outside at one instant.
 *
 * <p>The invariant checks work on these rather than on the simulator's own objects, which means they
 * can be pointed at a hand-built state that no real run would produce. That is how a checker gets
 * tested: fabricate two leaders in one term and confirm it complains, instead of hoping a seed
 * eventually produces the bug the checker is supposed to catch.
 *
 * @param applied entries handed to the state machine, in order
 * @param durable entries that would survive a crash
 */
public record NodeView(
        long id,
        Role role,
        long term,
        long commitIndex,
        List<Entry> applied,
        List<Entry> durable,
        boolean down) {

    public NodeView {
        applied = List.copyOf(applied);
        durable = List.copyOf(durable);
    }

    public boolean isLeader() {
        return !down && role == Role.LEADER;
    }
}
