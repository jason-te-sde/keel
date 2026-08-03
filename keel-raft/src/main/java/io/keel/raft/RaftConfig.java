package io.keel.raft;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Static configuration for one node.
 *
 * <p>Timeouts are counted in logical ticks rather than milliseconds. The core never reads a clock:
 * the driver decides what a tick is worth, which is what lets the simulator run a cluster for a
 * simulated hour in a few milliseconds and get exactly the same behaviour every time.
 *
 * @param nodeId this node's id; ids are positive, so 0 is available as "no node"
 * @param initialVoters the bootstrap membership, including this node
 * @param electionTimeoutTicks base ticks without contact from a leader before campaigning; the
 *     effective timeout is randomized in {@code [t, 2t)} to break up split votes
 * @param heartbeatTicks ticks between leader heartbeats; must be well below the election timeout
 * @param maxEntriesPerAppend cap on entries in one AppendEntries message
 * @param maxBytesPerAppend cap on the serialized size of entries in one AppendEntries message
 * @param maxUncommittedEntries cap on entries appended but not yet committed, which bounds how much
 *     a leader that has lost its quorum can buffer
 * @param preVote run the pre-vote round before a real election
 * @param checkQuorum make a leader step down when it stops hearing from a quorum
 */
public record RaftConfig(
        long nodeId,
        Set<Long> initialVoters,
        int electionTimeoutTicks,
        int heartbeatTicks,
        int maxEntriesPerAppend,
        long maxBytesPerAppend,
        int maxUncommittedEntries,
        boolean preVote,
        boolean checkQuorum) {

    public RaftConfig {
        if (nodeId <= 0) {
            throw new IllegalArgumentException("nodeId must be positive, got " + nodeId);
        }
        initialVoters = Set.copyOf(initialVoters);
        if (!initialVoters.contains(nodeId)) {
            throw new IllegalArgumentException(
                    "initialVoters " + initialVoters + " must contain this node " + nodeId);
        }
        if (electionTimeoutTicks <= heartbeatTicks) {
            // Otherwise followers time out faster than the leader can reach them and the cluster
            // never holds a stable leader.
            throw new IllegalArgumentException(
                    "electionTimeoutTicks ("
                            + electionTimeoutTicks
                            + ") must exceed heartbeatTicks ("
                            + heartbeatTicks
                            + ")");
        }
        if (heartbeatTicks <= 0) {
            throw new IllegalArgumentException("heartbeatTicks must be positive");
        }
        if (maxEntriesPerAppend <= 0) {
            throw new IllegalArgumentException("maxEntriesPerAppend must be positive");
        }
        if (maxBytesPerAppend <= 0) {
            throw new IllegalArgumentException("maxBytesPerAppend must be positive");
        }
        if (maxUncommittedEntries <= 0) {
            throw new IllegalArgumentException("maxUncommittedEntries must be positive");
        }
    }

    /** Number of votes that constitutes a majority of the bootstrap membership. */
    public int quorum() {
        return initialVoters.size() / 2 + 1;
    }

    public static Builder builder(long nodeId) {
        return new Builder(nodeId);
    }

    /**
     * Defaults chosen for the simulator's tick granularity: an election takes ten ticks and a
     * heartbeat one. Real deployments set a tick to a few milliseconds through the node layer.
     */
    public static final class Builder {
        private final long nodeId;
        private Set<Long> voters;
        private int electionTimeoutTicks = 10;
        private int heartbeatTicks = 1;
        private int maxEntriesPerAppend = 64;
        private long maxBytesPerAppend = 1L << 20;
        private int maxUncommittedEntries = 1 << 14;
        private boolean preVote = true;
        private boolean checkQuorum = true;

        private Builder(long nodeId) {
            this.nodeId = nodeId;
            this.voters = Set.of(nodeId);
        }

        public Builder voters(Set<Long> voters) {
            this.voters = voters;
            return this;
        }

        public Builder voters(long... voters) {
            Set<Long> set = new LinkedHashSet<>();
            for (long v : voters) {
                set.add(v);
            }
            this.voters = set;
            return this;
        }

        public Builder voters(List<Long> voters) {
            this.voters = new LinkedHashSet<>(voters);
            return this;
        }

        public Builder electionTimeoutTicks(int ticks) {
            this.electionTimeoutTicks = ticks;
            return this;
        }

        public Builder heartbeatTicks(int ticks) {
            this.heartbeatTicks = ticks;
            return this;
        }

        public Builder maxEntriesPerAppend(int max) {
            this.maxEntriesPerAppend = max;
            return this;
        }

        public Builder maxBytesPerAppend(long max) {
            this.maxBytesPerAppend = max;
            return this;
        }

        public Builder maxUncommittedEntries(int max) {
            this.maxUncommittedEntries = max;
            return this;
        }

        public Builder preVote(boolean enabled) {
            this.preVote = enabled;
            return this;
        }

        public Builder checkQuorum(boolean enabled) {
            this.checkQuorum = enabled;
            return this;
        }

        public RaftConfig build() {
            return new RaftConfig(
                    nodeId,
                    voters,
                    electionTimeoutTicks,
                    heartbeatTicks,
                    maxEntriesPerAppend,
                    maxBytesPerAppend,
                    maxUncommittedEntries,
                    preVote,
                    checkQuorum);
        }
    }
}
