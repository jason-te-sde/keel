package io.keel.testkit.linz;

import com.google.protobuf.ByteString;
import java.util.Optional;

/**
 * The sequential specification of a single key: a register holding a value or nothing.
 *
 * <p>Strict on purpose. Every rule here is a chance for the checker to reject something, and a model
 * that shrugs at an unexpected response makes the whole exercise decorative. A read must return
 * exactly the current value, and a compare-and-swap must report exactly whether its precondition held.
 */
public final class KvModel implements Model<Optional<ByteString>, KvModel.In, KvModel.Out> {

    /** Operations a client can perform on one key. */
    public sealed interface In {

        /** Read the current value. */
        record Get() implements In {
            @Override
            public String toString() {
                return "get";
            }
        }

        /** Overwrite unconditionally. */
        record Put(ByteString value) implements In {
            @Override
            public String toString() {
                return "put(" + value.toStringUtf8() + ")";
            }
        }

        record Delete() implements In {
            @Override
            public String toString() {
                return "delete";
            }
        }

        /** Write only if the current value matches, or if the key is absent. */
        record Cas(ByteString expected, boolean expectAbsent, ByteString value) implements In {
            @Override
            public String toString() {
                return "cas("
                        + (expectAbsent ? "absent" : expected.toStringUtf8())
                        + " -> "
                        + value.toStringUtf8()
                        + ")";
            }
        }
    }

    /** Responses a client can observe. */
    public sealed interface Out {

        /** The value a read returned, empty when the key was absent. */
        record Value(Optional<ByteString> value) implements Out {
            @Override
            public String toString() {
                return value.map(ByteString::toStringUtf8).orElse("absent");
            }
        }

        /** An unconditional write succeeded. */
        record Ok() implements Out {
            @Override
            public String toString() {
                return "ok";
            }
        }

        /** Whether a conditional operation's precondition held. */
        record Applied(boolean applied) implements Out {
            @Override
            public String toString() {
                return applied ? "applied" : "rejected";
            }
        }
    }

    @Override
    public Optional<ByteString> initial() {
        return Optional.empty();
    }

    @Override
    public Optional<Optional<ByteString>> apply(
            Optional<ByteString> state, In input, Out output) {
        return switch (input) {
            case In.Get ignored ->
                    // The one rule that catches a stale read: the response has to be the current value,
                    // not merely a value the key held at some point.
                    output instanceof Out.Value value && value.value().equals(state)
                            ? Optional.of(state)
                            : Optional.empty();
            case In.Put put ->
                    output instanceof Out.Ok ? Optional.of(Optional.of(put.value())) : Optional.empty();
            case In.Delete ignored ->
                    output instanceof Out.Applied applied && applied.applied() == state.isPresent()
                            ? Optional.of(Optional.empty())
                            : Optional.empty();
            case In.Cas cas -> {
                boolean matches = preconditionHolds(state, cas);
                if (!(output instanceof Out.Applied applied) || applied.applied() != matches) {
                    yield Optional.empty();
                }
                yield Optional.of(matches ? Optional.of(cas.value()) : state);
            }
        };
    }

    @Override
    public Optional<Optional<ByteString>> applyIgnoringOutput(Optional<ByteString> state, In input) {
        return switch (input) {
            case In.Get ignored -> Optional.of(state);
            case In.Put put -> Optional.of(Optional.of(put.value()));
            case In.Delete ignored -> Optional.of(Optional.empty());
            case In.Cas cas ->
                    Optional.of(preconditionHolds(state, cas) ? Optional.of(cas.value()) : state);
        };
    }

    private static boolean preconditionHolds(Optional<ByteString> state, In.Cas cas) {
        return cas.expectAbsent()
                ? state.isEmpty()
                : state.filter(cas.expected()::equals).isPresent();
    }
}
