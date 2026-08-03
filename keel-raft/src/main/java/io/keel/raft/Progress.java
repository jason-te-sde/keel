package io.keel.raft;

/**
 * A leader's view of one follower's log.
 *
 * <p>{@code match} is what the follower has confirmed; {@code next} is what to send it. The gap
 * between them is the leader's optimism, and the state machine below controls how much optimism is
 * allowed.
 */
final class Progress {

    /**
     * PROBE keeps one message outstanding at a time. It is the state a follower is in when the
     * leader does not yet know where their logs diverge, and sending a pipeline of guesses would
     * just be wasted bandwidth. REPLICATE pipelines: the leader advances {@code next} on send
     * without waiting, because the follower has confirmed a match and is expected to accept.
     */
    enum State {
        PROBE,
        REPLICATE,
        /**
         * A snapshot is being sent. The follower needs entries this leader no longer has, so nothing
         * is sent to it until the snapshot lands: further appends would only be rejected, and the
         * rejections would rewind progress that the snapshot is about to fix.
         */
        SNAPSHOT
    }

    long match;
    long next;
    State state = State.PROBE;

    /** True when a probe is outstanding, so PROBE sends one message and then waits. */
    boolean probeSent;

    /** Index of the snapshot in flight, or 0 when none is. */
    long pendingSnapshotIndex;

    /**
     * Set when anything is received from this follower, cleared each time check-quorum samples it.
     * A leader that finds too few active followers has lost its quorum and steps down (paper 6.2),
     * which is what stops it from serving reads nobody else can confirm.
     */
    boolean recentActive;

    Progress(long next) {
        this.next = next;
    }

    /** Records a confirmed match. Returns false for a stale or duplicated acknowledgement. */
    boolean maybeUpdate(long confirmedIndex) {
        if (confirmedIndex <= match) {
            return false;
        }
        match = confirmedIndex;
        next = Math.max(next, confirmedIndex + 1);
        return true;
    }

    void becomeReplicate() {
        state = State.REPLICATE;
        probeSent = false;
        next = match + 1;
    }

    void becomeProbe(long nextIndex) {
        state = State.PROBE;
        probeSent = false;
        pendingSnapshotIndex = 0;
        next = Math.max(nextIndex, match + 1);
    }

    void becomeSnapshot(long snapshotIndex) {
        state = State.SNAPSHOT;
        probeSent = false;
        pendingSnapshotIndex = snapshotIndex;
    }

    /**
     * Called when a snapshot in flight is known to have landed, or to have failed.
     *
     * <p>Either way the follower leaves SNAPSHOT state. On success its match index is at least the
     * snapshot boundary; on failure the leader retries, which will discover a snapshot is still needed.
     */
    void snapshotFinished(boolean landed) {
        if (landed) {
            match = Math.max(match, pendingSnapshotIndex);
        }
        becomeProbe(match + 1);
    }

    /** True when this follower should not be sent another message right now. */
    boolean paused() {
        return state == State.SNAPSHOT || (state == State.PROBE && probeSent);
    }

    @Override
    public String toString() {
        return "Progress[match=" + match + " next=" + next + " " + state + "]";
    }
}
