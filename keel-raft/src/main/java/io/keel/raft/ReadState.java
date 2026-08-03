package io.keel.raft;

/**
 * A read request that has been given a safe index to read at.
 *
 * <p>The index is not a promise that the state machine has caught up to it. The driver must wait
 * until {@code applied >= readIndex} before reading, or it will answer from a state machine that is
 * missing writes the read is supposed to see.
 *
 * @param requestId the caller's identifier, echoed back so a reply can be matched to its request
 * @param readIndex read only once the state machine has applied at least this index
 */
public record ReadState(long requestId, long readIndex) {}
