package io.keel.raft;

/**
 * Thrown when a leader refuses a write because too many entries are already appended and not yet
 * committed.
 *
 * <p>Without this bound, a leader that has lost contact with its followers keeps accepting writes it
 * can never commit, and the only limit is the heap. Refusing early turns an eventual crash into an
 * error the caller can retry.
 */
public final class ProposalDroppedException extends RuntimeException {

    public ProposalDroppedException(long uncommitted, int limit) {
        super("too many uncommitted entries: " + uncommitted + " (limit " + limit + ")");
    }
}
