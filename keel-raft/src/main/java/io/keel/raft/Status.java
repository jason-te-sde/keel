package io.keel.raft;

import java.util.Set;

/**
 * A snapshot of one node's consensus state, for tests, invariant checks, and the status command.
 *
 * @param nodeId this node
 * @param role what this node believes it is
 * @param term current term
 * @param leaderId the leader this node knows about, or 0 for none
 * @param commitIndex highest index known to be committed
 * @param appliedIndex highest index handed to the state machine
 * @param lastIndex highest index in the log, durable or not
 * @param voters current membership
 */
public record Status(
        long nodeId,
        Role role,
        long term,
        long leaderId,
        long commitIndex,
        long appliedIndex,
        long lastIndex,
        Set<Long> voters) {

    public Status {
        voters = Set.copyOf(voters);
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }
}
