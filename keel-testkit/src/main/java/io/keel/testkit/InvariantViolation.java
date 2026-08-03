package io.keel.testkit;

/**
 * Thrown when a safety property fails.
 *
 * <p>Carries the seed and tick, because a violation that cannot be replayed is a rumour. Every
 * message is written to name the property, the nodes involved, and the values that broke it, so the
 * failure output is a starting point for the diagnosis rather than a prompt to add print statements.
 */
public final class InvariantViolation extends RuntimeException {

    private final String property;
    private final long seed;
    private final long tick;

    public InvariantViolation(String property, long seed, long tick, String detail) {
        super(
                property
                        + " violated at tick "
                        + tick
                        + " (seed "
                        + seed
                        + "): "
                        + detail
                        + "\nreplay with: Sim.of(config.withSeed("
                        + seed
                        + ")).run("
                        + tick
                        + ")");
        this.property = property;
        this.seed = seed;
        this.tick = tick;
    }

    public String property() {
        return property;
    }

    public long seed() {
        return seed;
    }

    public long tick() {
        return tick;
    }
}
