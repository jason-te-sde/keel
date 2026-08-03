package io.keel.raft;

/**
 * The role a node currently believes it holds.
 *
 * <p>{@link #PRE_CANDIDATE} is not in the original paper's figure 4. It is the extra state needed
 * for pre-vote (paper section 9.6): a node in this state is polling whether an election would
 * succeed, and has deliberately <em>not</em> incremented its term yet.
 */
public enum Role {
    FOLLOWER,
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
