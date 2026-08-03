package io.keel.testkit.linz;

/**
 * One operation as a client observed it.
 *
 * <p>The two timestamps are the whole reason a history can be checked at all. An operation that
 * completed before another was invoked must be ordered before it in any valid explanation; operations
 * whose intervals overlap may be ordered either way. That is the only ordering information a client
 * genuinely has.
 *
 * @param id unique within a history, used for reporting and for memoization keys
 * @param partition operations on different partitions are independent; for a key-value store this is
 *     the key, which is what makes checking tractable
 * @param input what was asked for
 * @param output what came back, ignored when the outcome is unknown
 * @param invoked logical time the request was issued
 * @param completed logical time the response arrived, or {@link #UNKNOWN} when the client never found
 *     out. A timed-out request may have applied or not, so a checker has to try it both ways.
 */
public record Op<I, O>(int id, Object partition, I input, O output, long invoked, long completed) {

    /** Marks an operation whose outcome the client never learned. */
    public static final long UNKNOWN = -1L;

    public Op {
        if (completed != UNKNOWN && completed < invoked) {
            throw new IllegalArgumentException(
                    "operation " + id + " completed at " + completed + " before it was invoked at " + invoked);
        }
    }

    public boolean outcomeKnown() {
        return completed != UNKNOWN;
    }

    /** Latest point this operation can be linearized at, for pruning the search. */
    public long completionBound() {
        return completed == UNKNOWN ? Long.MAX_VALUE : completed;
    }

    @Override
    public String toString() {
        return "op"
                + id
                + "["
                + partition
                + "] "
                + input
                + " -> "
                + (outcomeKnown() ? output : "unknown")
                + " @["
                + invoked
                + ","
                + (outcomeKnown() ? completed : "?")
                + "]";
    }
}
