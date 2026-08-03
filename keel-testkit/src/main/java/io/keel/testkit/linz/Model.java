package io.keel.testkit.linz;

import java.util.Optional;

/**
 * A sequential specification: what the thing being checked would do if only one client existed.
 *
 * <p>The checker's job is to find some order of the concurrent operations that this model accepts. The
 * model therefore has to be a pure function, and it has to be strict: a model that accepts too much
 * turns the checker into a rubber stamp.
 *
 * @param <S> state, which must have value semantics for {@code equals} and {@code hashCode} because
 *     the search memoizes on it
 */
public interface Model<S, I, O> {

    /** State before anything has happened. */
    S initial();

    /**
     * Applies {@code input} to {@code state} and checks the result matches {@code output}.
     *
     * @return the resulting state, or empty if this model would never have produced that output
     */
    Optional<S> apply(S state, I input, O output);

    /**
     * Applies {@code input} when the client never learned the outcome.
     *
     * <p>The default accepts whatever the model does with the input and ignores the response, which is
     * right for a store where an operation either happened or did not. Override when an unknown
     * outcome should be constrained more tightly.
     */
    default Optional<S> applyUnknown(S state, I input) {
        return applyIgnoringOutput(state, input);
    }

    /** The state after {@code input}, without checking any response. */
    Optional<S> applyIgnoringOutput(S state, I input);
}
