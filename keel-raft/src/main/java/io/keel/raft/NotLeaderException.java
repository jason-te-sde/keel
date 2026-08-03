package io.keel.raft;

/**
 * Thrown when a write is offered to a node that is not the leader.
 *
 * <p>Carries a hint rather than nothing, so a client can retry against the right node instead of
 * rediscovering the cluster. The hint is what this node believed when it rejected the request, which
 * may already be out of date; treat it as a suggestion, not an answer.
 */
public final class NotLeaderException extends RuntimeException {

    private final long leaderHint;

    public NotLeaderException(long leaderHint) {
        super(leaderHint == 0 ? "no leader is known" : "not the leader; try node " + leaderHint);
        this.leaderHint = leaderHint;
    }

    /** The leader this node knew about, or 0 if it knew of none. */
    public long leaderHint() {
        return leaderHint;
    }
}
