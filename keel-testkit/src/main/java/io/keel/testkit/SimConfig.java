package io.keel.testkit;

/**
 * Everything a simulated run needs, including how hostile it should be.
 *
 * <p>A run is a pure function of this record. Two runs with the same config produce the same trace,
 * which is what makes a failure worth reporting: the seed and the config are the whole reproduction.
 *
 * @param nodes cluster size; ids are 1..nodes
 * @param seed drives message latency, fault injection, and each node's election timeout
 * @param minLatencyTicks lower bound on delivery delay
 * @param maxLatencyTicks upper bound on delivery delay; must be below the election timeout most of
 *     the time, or the cluster spends the run electing
 * @param dropProbability chance a message is discarded at send
 * @param duplicateProbability chance a message is delivered twice, at different times
 * @param partitionProbability chance per tick of splitting the cluster into two groups
 * @param healProbability chance per tick of removing any partition
 * @param crashProbability chance per tick of killing a node, losing everything it had not synced
 * @param restartProbability chance per tick of bringing a crashed node back
 * @param electionTimeoutTicks base election timeout handed to every node
 * @param heartbeatTicks leader heartbeat interval
 * @param initialVoters how many of the nodes start as voters; 0 means all of them. The rest exist and
 *     run but are not members yet, which is what a node waiting to join a cluster looks like.
 * @param snapshotThresholdEntries take a snapshot and compact once a node's log holds this many
 *     entries beyond its snapshot boundary; 0 disables compaction entirely. A small value is
 *     deliberately unrealistic: it makes every run cross the paths where a follower has to be caught
 *     up from a snapshot, which a production threshold would reach once an hour.
 */
public record SimConfig(
        int nodes,
        long seed,
        int minLatencyTicks,
        int maxLatencyTicks,
        double dropProbability,
        double duplicateProbability,
        double partitionProbability,
        double healProbability,
        double crashProbability,
        double restartProbability,
        int electionTimeoutTicks,
        int heartbeatTicks,
        int initialVoters,
        int snapshotThresholdEntries) {

    public SimConfig {
        if (nodes <= 0) {
            throw new IllegalArgumentException("nodes must be positive");
        }
        if (minLatencyTicks < 1 || maxLatencyTicks < minLatencyTicks) {
            throw new IllegalArgumentException(
                    "latency range [" + minLatencyTicks + "," + maxLatencyTicks + "] is not usable");
        }
        checkProbability("dropProbability", dropProbability);
        checkProbability("duplicateProbability", duplicateProbability);
        checkProbability("partitionProbability", partitionProbability);
        checkProbability("healProbability", healProbability);
        checkProbability("crashProbability", crashProbability);
        checkProbability("restartProbability", restartProbability);
        if (snapshotThresholdEntries < 0) {
            throw new IllegalArgumentException("snapshotThresholdEntries cannot be negative");
        }
        if (initialVoters < 0 || initialVoters > nodes) {
            throw new IllegalArgumentException(
                    "initialVoters " + initialVoters + " is not in [0," + nodes + "]");
        }
    }

    private static void checkProbability(String name, double value) {
        if (value < 0 || value > 1) {
            throw new IllegalArgumentException(name + " must be in [0,1], got " + value);
        }
    }

    /**
     * A healthy cluster: no faults at all.
     *
     * <p>Used for the progress checks. A safety suite that only ever runs under chaos can pass because
     * the cluster never got anything done, which is the classic false negative.
     */
    public static SimConfig quiet(int nodes, long seed) {
        return new SimConfig(nodes, seed, 1, 2, 0, 0, 0, 0, 0, 0, 10, 1, 0, 16);
    }

    /**
     * Partitions, crashes, drops, duplicates, and reordering, tuned so the cluster still makes
     * progress often enough for the safety checks to have something to check.
     */
    public static SimConfig chaotic(int nodes, long seed) {
        return new SimConfig(nodes, seed, 1, 6, 0.03, 0.02, 0.01, 0.05, 0.005, 0.05, 10, 1, 0, 12);
    }

    /** Faults with no recovery pressure: partitions and crashes heal rarely. */
    public static SimConfig brutal(int nodes, long seed) {
        return new SimConfig(nodes, seed, 1, 12, 0.10, 0.05, 0.04, 0.03, 0.02, 0.03, 10, 1, 0, 8);
    }

    public SimConfig withSeed(long seed) {
        return new SimConfig(
                nodes,
                seed,
                minLatencyTicks,
                maxLatencyTicks,
                dropProbability,
                duplicateProbability,
                partitionProbability,
                healProbability,
                crashProbability,
                restartProbability,
                electionTimeoutTicks,
                heartbeatTicks,
                initialVoters,
                snapshotThresholdEntries);
    }

    public SimConfig withNodes(int nodes) {
        return new SimConfig(
                nodes,
                seed,
                minLatencyTicks,
                maxLatencyTicks,
                dropProbability,
                duplicateProbability,
                partitionProbability,
                healProbability,
                crashProbability,
                restartProbability,
                electionTimeoutTicks,
                heartbeatTicks,
                initialVoters,
                snapshotThresholdEntries);
    }

    /** Disables compaction, for tests that want an uncompacted log. */
    public SimConfig withoutSnapshots() {
        return new SimConfig(
                nodes,
                seed,
                minLatencyTicks,
                maxLatencyTicks,
                dropProbability,
                duplicateProbability,
                partitionProbability,
                healProbability,
                crashProbability,
                restartProbability,
                electionTimeoutTicks,
                heartbeatTicks,
                initialVoters,
                0);
    }

    /** Starts with only the first {@code count} nodes as voters; the rest wait to be added. */
    public SimConfig withInitialVoters(int count) {
        return new SimConfig(
                nodes,
                seed,
                minLatencyTicks,
                maxLatencyTicks,
                dropProbability,
                duplicateProbability,
                partitionProbability,
                healProbability,
                crashProbability,
                restartProbability,
                electionTimeoutTicks,
                heartbeatTicks,
                count,
                snapshotThresholdEntries);
    }

    /** The nodes that start as voters. */
    public java.util.Set<Long> voterIds() {
        java.util.Set<Long> ids = new java.util.TreeSet<>();
        int count = initialVoters == 0 ? nodes : initialVoters;
        for (long id = 1; id <= count; id++) {
            ids.add(id);
        }
        return ids;
    }
}
