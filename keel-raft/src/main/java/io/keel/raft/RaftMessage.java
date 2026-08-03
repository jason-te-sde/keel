package io.keel.raft;

import io.keel.proto.log.Entry;
import io.keel.proto.log.SnapshotMetadata;
import java.util.List;

/**
 * The peer-to-peer message set, closed by design.
 *
 * <p>These are the core's own types rather than the generated protobuf messages. The reason is
 * exhaustiveness: because the interface is sealed, a {@code switch} over messages that forgets a
 * case does not compile. A protobuf {@code oneof} gives no such guarantee, and a message silently
 * falling through a dispatch is exactly the class of bug that is hardest to find in a consensus
 * implementation.
 *
 * <p>The transport converts between these and {@code keel.raft.v1} on the wire. That boundary also
 * keeps the simulator free of serialization: it hands these records straight from one node to
 * another.
 */
public sealed interface RaftMessage {

    /** Sender id. */
    long from();

    /** Recipient id. */
    long to();

    /** Sender's term, as understood when the message was created. */
    long term();

    /** A copy of this message addressed to a different recipient. */
    RaftMessage withTo(long to);

    /**
     * Request for a vote, or for a pre-vote poll.
     *
     * <p>When {@code preVote} is set, {@code term} is the term the candidate <em>would</em> campaign
     * in, one above its current term. Nobody increments a term on account of a pre-vote, which is
     * the whole point of the round.
     */
    record Vote(
            long from, long to, long term, boolean preVote, long lastLogIndex, long lastLogTerm)
            implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new Vote(from, to, term, preVote, lastLogIndex, lastLogTerm);
        }
    }

    /**
     * Response to {@link Vote}.
     *
     * <p>A granted response carries the term of the request; a rejection carries the responder's own
     * term, which is how a candidate discovers it is behind.
     */
    record VoteReply(long from, long to, long term, boolean preVote, boolean granted)
            implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new VoteReply(from, to, term, preVote, granted);
        }
    }

    /** AppendEntries. An empty {@code entries} list is a log-consistency probe, not a heartbeat. */
    record Append(
            long from,
            long to,
            long term,
            long prevLogIndex,
            long prevLogTerm,
            List<Entry> entries,
            long leaderCommit)
            implements RaftMessage {

        public Append {
            entries = List.copyOf(entries);
        }

        @Override
        public RaftMessage withTo(long to) {
            return new Append(from, to, term, prevLogIndex, prevLogTerm, entries, leaderCommit);
        }
    }

    /**
     * Response to {@link Append}.
     *
     * @param matchIndex highest index known to agree with the leader; meaningful when accepted
     * @param conflictIndex on rejection, the index the leader should try next: either the first
     *     index of the follower's conflicting term, or one past the end of a log that is too short
     * @param conflictTerm on rejection, the term found at {@code prevLogIndex}, or 0 if the
     *     follower's log simply ends before it
     */
    record AppendReply(
            long from,
            long to,
            long term,
            boolean accepted,
            long matchIndex,
            long conflictIndex,
            long conflictTerm)
            implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new AppendReply(
                    from, to, term, accepted, matchIndex, conflictIndex, conflictTerm);
        }
    }

    /**
     * Heartbeat. Carries no entries and never causes a log write.
     *
     * @param readSeq the leader's read-round token, echoed back by the follower. ReadIndex needs to
     *     count responses that belong to one specific round; without a token, a late response from
     *     an earlier round could be counted toward the current one and a stale read would be served.
     *     0 means this heartbeat is not part of a read round.
     */
    record Heartbeat(long from, long to, long term, long leaderCommit, long readSeq)
            implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new Heartbeat(from, to, term, leaderCommit, readSeq);
        }
    }

    /** Response to {@link Heartbeat}, echoing {@code readSeq}. */
    record HeartbeatReply(long from, long to, long term, long readSeq) implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new HeartbeatReply(from, to, term, readSeq);
        }
    }

    /**
     * A follower asking the leader for an index it can safely read at.
     *
     * <p>Forwarding the request rather than the read itself is the point: the follower then serves the
     * read from its own state machine, so reads scale with the cluster instead of piling onto the
     * leader, and they are still linearizable because the index came from a leader that confirmed
     * itself.
     */
    record ReadIndexRequest(long from, long to, long term, long requestId) implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new ReadIndexRequest(from, to, term, requestId);
        }
    }

    /**
     * Tells a follower to catch up from a snapshot instead of from entries.
     *
     * <p>Carries only the metadata. Moving the payload is the driver's job, because a snapshot is
     * arbitrarily large and the core has no I/O: it decides <em>when</em> a snapshot is needed and
     * <em>which boundary</em> it establishes, and nothing more. A driver must have the payload durable
     * before it steps this message into the receiving core.
     */
    record InstallSnapshot(long from, long to, long term, SnapshotMetadata meta)
            implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new InstallSnapshot(from, to, term, meta);
        }
    }

    /** Response to {@link InstallSnapshot}. */
    record InstallSnapshotReply(
            long from, long to, long term, boolean success, long matchIndex)
            implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new InstallSnapshotReply(from, to, term, success, matchIndex);
        }
    }

    /** The leader's answer to {@link ReadIndexRequest}. */
    record ReadIndexReply(long from, long to, long term, long requestId, long readIndex)
            implements RaftMessage {
        @Override
        public RaftMessage withTo(long to) {
            return new ReadIndexReply(from, to, term, requestId, readIndex);
        }
    }
}
