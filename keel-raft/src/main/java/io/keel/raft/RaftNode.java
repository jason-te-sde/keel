package io.keel.raft;

import com.google.protobuf.InvalidProtocolBufferException;
import io.keel.proto.log.Entry;
import io.keel.proto.log.ConfChange;
import io.keel.proto.log.ConfState;
import io.keel.proto.log.HardState;
import io.keel.proto.log.SnapshotMetadata;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One node's consensus state machine.
 *
 * <p>This class owns no threads, reads no clock, and touches no disk or socket. Time arrives through
 * {@link #tick()}, messages through {@link #step(RaftMessage)}, and everything the node wants done
 * leaves through {@link #ready()}. Randomness comes from an injected {@link Random}.
 *
 * <p>That is the whole design argument. A cluster's behaviour becomes a pure function of its seed and
 * its schedule, so a failure found at seed 8123 is still there at seed 8123 tomorrow. It also means
 * there is nothing to synchronize: exactly one thread ever calls into an instance, and the class is
 * documented as not thread safe rather than defensively locked.
 *
 * <p>Implemented from the extended Raft paper (Ongaro and Ousterhout, 2014). Section references in
 * the comments point at the argument being relied on, not at a paraphrase of it.
 */
public final class RaftNode {

    private static final Logger LOG = LoggerFactory.getLogger(RaftNode.class);

    /** Reserved id meaning "no node": ids are positive, so 0 is free. */
    public static final long NO_NODE = 0L;

    private final RaftConfig cfg;
    private final RaftStorage storage;
    private final RaftLog log;
    private final Random random;

    private final Set<Long> voters;
    private final Map<Long, Progress> peers = new LinkedHashMap<>();

    /** Votes collected in the current election round, keyed by voter. */
    private final Map<Long, Boolean> ballots = new HashMap<>();

    private final List<RaftMessage> outbound = new ArrayList<>();

    private Role role = Role.FOLLOWER;
    private long term;
    private long vote = NO_NODE;
    private long leaderId = NO_NODE;

    private int electionElapsed;
    private int heartbeatElapsed;
    private int electionTimeout;

    /** Set when term, vote, or commit index changed and has not been persisted yet. */
    private boolean hardStateDirty;

    /**
     * Index of the newest configuration entry this leader has appended, or 0.
     *
     * <p>A second configuration change must not be appended while an earlier one is still
     * unapplied. With one change in flight the old and new majorities always overlap, which is what
     * makes single-node changes safe without a joint configuration; two in flight breaks that, and
     * two disjoint majorities can elect two leaders in the same term.
     */
    private long pendingConfIndex;

    /**
     * Reads waiting for a heartbeat round to confirm this node is still the leader. Each carries the
     * commit index as it stood when the round was sent, which is what makes it safe to read at.
     */
    private final Deque<PendingRead> pendingReads = new ArrayDeque<>();

    /**
     * Reads that arrived before this leader had committed anything in its own term, so there is no
     * safe index to give them yet. Released once its no-op commits.
     */
    private final List<PendingRead> deferredReads = new ArrayList<>();

    /** Acknowledgements per heartbeat round, including this node's own. */
    private final Map<Long, Integer> readRoundAcks = new HashMap<>();

    private long readRound;

    private final List<ReadState> readStates = new ArrayList<>();

    /**
     * A read waiting to be answered.
     *
     * @param origin the node that wants the answer; this node when the client asked it directly
     * @param requestId the origin's identifier for the request
     * @param readIndex commit index recorded when the round was started
     * @param round heartbeat round that must be confirmed before this read is safe
     */
    private record PendingRead(long origin, long requestId, long readIndex, long round) {}

    /**
     * Restores a node from persisted state.
     *
     * @param persisted the hard state as recovered from disk; a zeroed state for a new node
     */
    public RaftNode(RaftConfig cfg, RaftStorage storage, HardState persisted, Random random) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.random = Objects.requireNonNull(random, "random");
        this.storage = Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(persisted, "persisted");

        // Sorted so every iteration of the membership, and therefore the order messages are
        // emitted in, is a function of the ids alone.
        this.voters = new TreeSet<>(cfg.initialVoters());
        this.term = persisted.getTerm();
        this.vote = persisted.getVote();
        this.log = new RaftLog(storage, persisted.getCommit());
        this.electionTimeout = nextElectionTimeout();

        LOG.debug(
                "node {} restored: term={} vote={} commit={} lastIndex={}",
                cfg.nodeId(),
                term,
                vote,
                log.committed(),
                log.lastIndex());
    }

    /** Restores a node from the hard state already in {@code store}. */
    public static RaftNode restore(RaftConfig cfg, LogStore store, Random random) {
        return new RaftNode(cfg, store, store.hardState(), random);
    }

    // ---------------------------------------------------------------------------------------------
    // Driver API
    // ---------------------------------------------------------------------------------------------

    /**
     * Advances the logical clock by one tick.
     *
     * <p>A leader sends heartbeats and, with check-quorum enabled, samples whether it can still
     * reach a majority. Everyone else counts down to an election.
     */
    public void tick() {
        if (role == Role.LEADER) {
            heartbeatElapsed++;
            electionElapsed++;
            if (cfg.checkQuorum() && electionElapsed >= cfg.electionTimeoutTicks()) {
                electionElapsed = 0;
                if (!quorumRecentlyActive()) {
                    // Paper 6.2. A leader that cannot reach a majority has to stop acting like one,
                    // or it will keep answering reads that no quorum can confirm.
                    LOG.info("node {} stepping down in term {}: lost quorum", cfg.nodeId(), term);
                    becomeFollower(term, NO_NODE);
                    return;
                }
            }
            if (heartbeatElapsed >= cfg.heartbeatTicks()) {
                heartbeatElapsed = 0;
                broadcastHeartbeat(0);
            }
            return;
        }

        electionElapsed++;
        if (promotable() && electionElapsed >= electionTimeout) {
            electionElapsed = 0;
            campaign();
        }
    }

    /**
     * Handles one incoming message.
     *
     * @throws IllegalArgumentException if the message is addressed to another node
     */
    public void step(RaftMessage m) {
        Objects.requireNonNull(m, "m");
        if (m.to() != cfg.nodeId()) {
            throw new IllegalArgumentException(
                    "message addressed to " + m.to() + " delivered to " + cfg.nodeId());
        }

        if (m.term() > term) {
            if (!ignoresTermBump(m)) {
                long newLeader = (m instanceof RaftMessage.Append || m instanceof RaftMessage.Heartbeat)
                        ? m.from()
                        : NO_NODE;
                LOG.debug(
                        "node {} sees term {} from node {}, was term {}",
                        cfg.nodeId(),
                        m.term(),
                        m.from(),
                        term);
                becomeFollower(m.term(), newLeader);
            }
        } else if (m.term() < term) {
            handleStaleTerm(m);
            return;
        }

        switch (m) {
            case RaftMessage.Vote v -> handleVote(v);
            case RaftMessage.VoteReply v -> handleVoteReply(v);
            case RaftMessage.Append a -> handleAppend(a);
            case RaftMessage.AppendReply a -> {
                if (role == Role.LEADER) {
                    handleAppendReply(a);
                }
            }
            case RaftMessage.Heartbeat h -> handleHeartbeat(h);
            case RaftMessage.HeartbeatReply h -> {
                if (role == Role.LEADER) {
                    handleHeartbeatReply(h);
                }
            }
            case RaftMessage.InstallSnapshot s -> handleInstallSnapshot(s);
            case RaftMessage.InstallSnapshotReply s -> {
                if (role == Role.LEADER) {
                    handleInstallSnapshotReply(s);
                }
            }
            case RaftMessage.ReadIndexRequest r -> {
                if (role == Role.LEADER) {
                    beginRead(r.from(), r.requestId());
                }
                // A follower that is asked for a read index simply drops it. The requester is waiting
                // on a timeout and will retry once it learns who the leader is.
            }
            case RaftMessage.ReadIndexReply r -> readStates.add(new ReadState(r.requestId(), r.readIndex()));
        }
    }

    /**
     * Appends a client command to the log and starts replicating it.
     *
     * @return the index the command was assigned
     * @throws NotLeaderException if this node is not the leader
     * @throws ProposalDroppedException if too many entries are uncommitted
     */
    public long propose(byte[] data) {
        Objects.requireNonNull(data, "data");
        if (role != Role.LEADER) {
            throw new NotLeaderException(leaderId);
        }
        long uncommitted = log.lastIndex() - log.committed();
        if (uncommitted >= cfg.maxUncommittedEntries()) {
            throw new ProposalDroppedException(uncommitted, cfg.maxUncommittedEntries());
        }
        long index = log.lastIndex() + 1;
        log.append(List.of(Entries.normal(index, term, data)));
        broadcastAppend();
        return index;
    }

    /**
     * Appends a membership change.
     *
     * <p>One voter at a time, which is a decision rather than a limitation: with a single change in
     * flight the old and new majorities always overlap, so no joint configuration is needed (paper 4.3).
     * Joint consensus handles arbitrary reconfiguration and roughly doubles the state every safety
     * argument has to account for.
     *
     * <p>The change takes effect when the entry is <em>applied</em>, not when it is appended. A driver
     * must call {@link #applyConfChange(ConfChange)} for every applied configuration entry, including
     * ones it replays on startup, or the node will disagree with the cluster about who votes.
     *
     * @return the index the change was assigned
     * @throws NotLeaderException if this node is not the leader
     * @throws IllegalArgumentException if the change is a no-op, or would empty the cluster
     * @throws IllegalStateException if an earlier configuration change has not been applied yet
     */
    public long proposeConfChange(ConfChange change) {
        Objects.requireNonNull(change, "change");
        if (role != Role.LEADER) {
            throw new NotLeaderException(leaderId);
        }
        if (pendingConfIndex > log.applied()) {
            throw new IllegalStateException(
                    "a configuration change at index "
                            + pendingConfIndex
                            + " has not been applied yet (applied is "
                            + log.applied()
                            + ")");
        }
        validate(change);

        long index = log.lastIndex() + 1;
        log.append(List.of(Entries.confChange(index, term, change.toByteArray())));
        pendingConfIndex = index;
        LOG.info(
                "node {} proposing to {} node {} at index {}",
                cfg.nodeId(),
                change.getType() == ConfChange.Type.TYPE_ADD_VOTER ? "add" : "remove",
                change.getNodeId(),
                index);
        broadcastAppend();
        return index;
    }

    private void validate(ConfChange change) {
        boolean present = voters.contains(change.getNodeId());
        switch (change.getType()) {
            case TYPE_ADD_VOTER -> {
                if (present) {
                    throw new IllegalArgumentException("node " + change.getNodeId() + " is already a voter");
                }
            }
            case TYPE_REMOVE_VOTER -> {
                if (!present) {
                    throw new IllegalArgumentException("node " + change.getNodeId() + " is not a voter");
                }
                if (voters.size() == 1) {
                    throw new IllegalArgumentException("cannot remove the last voter");
                }
            }
            case TYPE_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("configuration change has no type");
        }
    }

    /**
     * Applies a membership change that has been committed and applied to the state machine.
     *
     * <p>Called by the driver for every applied configuration entry, replays included. The core cannot
     * do it itself: it hands committed entries out and does not see when they are applied, and applying
     * a change early would mean counting a vote from a node that is not a member yet.
     *
     * @return the membership after the change
     */
    public ConfState applyConfChange(ConfChange change) {
        Objects.requireNonNull(change, "change");
        switch (change.getType()) {
            case TYPE_ADD_VOTER -> {
                if (voters.add(change.getNodeId()) && role == Role.LEADER) {
                    // Start from the end of the log rather than the beginning: a new voter that is
                    // actually far behind will say so, and be rewound or sent a snapshot.
                    peers.put(change.getNodeId(), new Progress(log.lastIndex() + 1));
                }
            }
            case TYPE_REMOVE_VOTER -> {
                voters.remove(change.getNodeId());
                peers.remove(change.getNodeId());
            }
            case TYPE_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("configuration change has no type");
        }
        LOG.info("node {} now sees voters {}", cfg.nodeId(), voters);

        if (role == Role.LEADER && !voters.contains(cfg.nodeId())) {
            // A leader that has removed itself has to stop leading, or the cluster has a leader that
            // is not a member and cannot count its own vote toward anything.
            LOG.info("node {} removed itself from the cluster; stepping down", cfg.nodeId());
            becomeFollower(term, NO_NODE);
        } else if (role == Role.LEADER) {
            // The quorum may have shrunk, which can make an already-stored entry committable.
            maybeCommit();
        }
        return confState();
    }

    /** The current membership, for a snapshot's metadata. */
    public ConfState confState() {
        return ConfState.newBuilder().addAllVoters(voters).build();
    }

    /**
     * Adopts the membership recorded in a snapshot.
     *
     * <p>A node restoring from a snapshot cannot recover this from the log, because the entries that
     * would have carried the changes are exactly what the snapshot replaced.
     */
    private void adoptConfState(ConfState state) {
        if (state.getVotersCount() == 0) {
            return;
        }
        voters.clear();
        voters.addAll(state.getVotersList());
        peers.keySet().retainAll(voters);
        LOG.info("node {} adopted voters {} from a snapshot", cfg.nodeId(), voters);
    }

    /**
     * Asks for an index that can be read at without violating linearizability.
     *
     * <p>This is ReadIndex, paper section 6.4. Reading a leader's local state without it is not
     * linearizable: a leader that has been partitioned away still believes it is the leader for up to
     * one election timeout, and will happily serve a value a newer leader has already replaced.
     *
     * <p>Two conditions have to hold, and skipping either is the usual bug:
     *
     * <ol>
     *   <li>The leader must have committed an entry in its current term, so it knows the full
     *       committed prefix. That is what the no-op appended on election is for. Until then the read
     *       is held, not answered.
     *   <li>The leader must confirm it is <em>still</em> the leader by completing a heartbeat round
     *       with a quorum after recording the index. Recording the commit index proves nothing on its
     *       own; the quorum round is what rules out a leader that has already been deposed.
     * </ol>
     *
     * <p>A follower forwards the request and serves the read itself once its own state machine reaches
     * the index it gets back. Nothing is appended to the log either way.
     *
     * <p>The answer arrives as a {@link ReadState} in a later {@link Ready}. Requests are dropped
     * without notice if leadership changes while they are in flight, so callers need a timeout.
     *
     * @param requestId caller's identifier, echoed back in the {@link ReadState}
     * @throws NotLeaderException if no leader is known, so there is nobody to ask
     */
    public void requestRead(long requestId) {
        if (role == Role.LEADER) {
            beginRead(cfg.nodeId(), requestId);
            return;
        }
        if (leaderId == NO_NODE) {
            throw new NotLeaderException(NO_NODE);
        }
        send(new RaftMessage.ReadIndexRequest(cfg.nodeId(), leaderId, term, requestId));
    }

    /**
     * Starts an election immediately instead of waiting for the timeout.
     *
     * <p>Used to bootstrap a cluster and to make tests deterministic. Elections still obey every
     * safety rule, so this cannot force an outcome, only the attempt.
     */
    public void campaign() {
        if (!promotable()) {
            LOG.debug("node {} cannot campaign: not a voter", cfg.nodeId());
            return;
        }
        if (cfg.preVote()) {
            becomePreCandidate();
        } else {
            becomeCandidate();
        }
        solicitVotes();
    }

    /**
     * Collects the work the core wants done. See {@link Ready} for the required ordering.
     *
     * <p>Messages are moved out, so a batch is returned once. Losing a batch loses only messages,
     * which Raft already tolerates.
     */
    public Ready ready() {
        Ready rd =
                new Ready(
                        hardStateDirty ? currentHardState() : null,
                        log.unstableEntries(),
                        log.nextCommittedEntries(),
                        List.copyOf(outbound),
                        List.copyOf(readStates),
                        log.pendingSnapshot());
        outbound.clear();
        readStates.clear();
        return rd;
    }

    /**
     * Acknowledges that a {@link Ready} batch was persisted and applied.
     *
     * <p>Must be called with the batch that was actually completed. Everything it reports is
     * index-based, so calling {@link #step} or {@link #tick} in between is safe.
     */
    public void advance(Ready rd) {
        Objects.requireNonNull(rd, "rd");
        long persisted = Entries.lastIndex(rd.entriesToPersist());
        if (persisted > 0) {
            log.stableTo(persisted);
        }
        long applied = Entries.lastIndex(rd.committedEntries());
        if (applied > 0) {
            log.appliedTo(applied);
        }
        if (rd.hasHardState()) {
            hardStateDirty = false;
        }
        if (rd.hasSnapshotToInstall()) {
            // The driver has installed it, so storage is authoritative again.
            log.snapshotInstalled();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Observation
    // ---------------------------------------------------------------------------------------------

    public long nodeId() {
        return cfg.nodeId();
    }

    public Role role() {
        return role;
    }

    public long term() {
        return term;
    }

    public long leaderId() {
        return leaderId;
    }

    public long commitIndex() {
        return log.committed();
    }

    public long appliedIndex() {
        return log.applied();
    }

    public long lastIndex() {
        return log.lastIndex();
    }

    public Set<Long> voters() {
        return java.util.Collections.unmodifiableSet(new TreeSet<>(voters));
    }

    public Status status() {
        return new Status(
                cfg.nodeId(),
                role,
                term,
                leaderId,
                log.committed(),
                log.applied(),
                log.lastIndex(),
                voters);
    }

    /** Decodes a configuration change from an entry's payload. */
    public static ConfChange decodeConfChange(Entry entry) {
        try {
            return ConfChange.parseFrom(entry.getData());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "entry at index " + entry.getIndex() + " is not a configuration change", e);
        }
    }

    /** Term at {@code index}, for invariant checks that compare logs across nodes. */
    public long termAt(long index) {
        return log.term(index);
    }

    // ---------------------------------------------------------------------------------------------
    // Term handling
    // ---------------------------------------------------------------------------------------------

    /**
     * Whether a message at a higher term must not raise this node's term.
     *
     * <p>Two cases, both of them pre-vote:
     *
     * <ul>
     *   <li>A pre-vote <em>request</em> asks about a term the candidate has not entered. Answering it
     *       must not drag the responder into that term, since that is the disruption pre-vote exists
     *       to prevent.
     *   <li>A <em>granted</em> pre-vote response echoes the future term the candidate asked about.
     *       Adopting it here would be adopting a term on the strength of one vote; the term is
     *       incremented only once a quorum has granted, in {@link #becomeCandidate()}.
     * </ul>
     *
     * <p>A <em>rejected</em> pre-vote response is the opposite case and deliberately not listed: its
     * term is the responder's real term, so it is genuine evidence this node is behind and should
     * step down to it.
     */
    private static boolean ignoresTermBump(RaftMessage m) {
        return switch (m) {
            case RaftMessage.Vote v -> v.preVote();
            case RaftMessage.VoteReply v -> v.preVote() && v.granted();
            default -> false;
        };
    }

    private void handleStaleTerm(RaftMessage m) {
        switch (m) {
            case RaftMessage.Vote v when v.preVote() ->
                    // Tell the candidate our term so it stops polling at a term it cannot win.
                    send(new RaftMessage.VoteReply(cfg.nodeId(), v.from(), term, true, false));
            case RaftMessage.Append a ->
                    // A leader from an older term. Replying with our term makes it step down now
                    // rather than after its own election timeout.
                    send(
                            new RaftMessage.AppendReply(
                                    cfg.nodeId(), a.from(), term, false, 0, 0, 0));
            case RaftMessage.Heartbeat h ->
                    send(new RaftMessage.HeartbeatReply(cfg.nodeId(), h.from(), term, h.readSeq()));
            case RaftMessage.InstallSnapshot s ->
                    send(
                            new RaftMessage.InstallSnapshotReply(
                                    cfg.nodeId(), s.from(), term, false, 0));
            default -> LOG.debug(
                    "node {} ignoring {} from term {} (current term {})",
                    cfg.nodeId(),
                    m.getClass().getSimpleName(),
                    m.term(),
                    term);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Role transitions
    // ---------------------------------------------------------------------------------------------

    private void becomeFollower(long newTerm, long leader) {
        if (newTerm < term) {
            throw new IllegalStateException("term would move backwards: " + term + " -> " + newTerm);
        }
        if (newTerm > term) {
            term = newTerm;
            vote = NO_NODE;
            hardStateDirty = true;
        }
        role = Role.FOLLOWER;
        leaderId = leader;
        electionElapsed = 0;
        electionTimeout = nextElectionTimeout();
        peers.clear();
        ballots.clear();
        abandonReads();
        pendingConfIndex = 0;
    }

    private void becomePreCandidate() {
        // Deliberately no term change: that is the difference between a pre-vote and a vote.
        role = Role.PRE_CANDIDATE;
        leaderId = NO_NODE;
        electionElapsed = 0;
        electionTimeout = nextElectionTimeout();
        peers.clear();
        ballots.clear();
        LOG.debug("node {} starting pre-vote for term {}", cfg.nodeId(), term + 1);
    }

    private void becomeCandidate() {
        term++;
        vote = cfg.nodeId();
        hardStateDirty = true;
        role = Role.CANDIDATE;
        leaderId = NO_NODE;
        electionElapsed = 0;
        electionTimeout = nextElectionTimeout();
        peers.clear();
        ballots.clear();
        LOG.debug("node {} campaigning in term {}", cfg.nodeId(), term);
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = cfg.nodeId();
        electionElapsed = 0;
        heartbeatElapsed = 0;
        ballots.clear();
        abandonReads();

        peers.clear();
        long nextIndex = log.lastIndex() + 1;
        for (long v : voters) {
            if (v != cfg.nodeId()) {
                peers.put(v, new Progress(nextIndex));
            }
        }

        // Any configuration entry from a previous leader is either applied or will be; this leader
        // starts with none of its own outstanding.
        pendingConfIndex = 0;

        // Commit an entry in this term before doing anything else. The commit rule (5.4.2) will not
        // let a new leader advance the commit index over entries from previous terms until one of
        // its own is committed, and ReadIndex has nothing to anchor to until then either.
        log.append(List.of(Entries.noop(nextIndex, term)));

        LOG.info(
                "node {} is leader for term {} (lastIndex={}, voters={})",
                cfg.nodeId(),
                term,
                log.lastIndex(),
                voters);

        if (peers.isEmpty()) {
            // Single-node cluster: the no-op is committed as soon as it is appended.
            maybeCommit();
        } else {
            broadcastAppend();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Elections
    // ---------------------------------------------------------------------------------------------

    private void solicitVotes() {
        boolean preVote = role == Role.PRE_CANDIDATE;
        long campaignTerm = preVote ? term + 1 : term;

        // A node always votes for itself, so record that first: in a single-node cluster it is
        // already a majority and no message needs to go anywhere.
        if (recordBallot(cfg.nodeId(), true)) {
            return;
        }
        for (long v : voters) {
            if (v == cfg.nodeId()) {
                continue;
            }
            send(
                    new RaftMessage.Vote(
                            cfg.nodeId(),
                            v,
                            campaignTerm,
                            preVote,
                            log.lastIndex(),
                            log.lastTerm()));
        }
    }

    private void handleVote(RaftMessage.Vote v) {
        // Paper 5.2 and 5.4.1, plus the pre-vote extension. Granting requires all three of:
        //   - we have not already promised this term to someone else
        //   - or this is a pre-vote about a future term, which promises nothing
        //   - and the candidate's log is at least as up to date as ours
        boolean canVote =
                vote == v.from()
                        || (vote == NO_NODE && leaderId == NO_NODE)
                        || (v.preVote() && v.term() > term);

        if (canVote && log.isUpToDate(v.lastLogIndex(), v.lastLogTerm())) {
            // Granting echoes the requested term so the candidate can match the reply to its round;
            // for a pre-vote that term is one above ours and we still do not adopt it.
            send(new RaftMessage.VoteReply(cfg.nodeId(), v.from(), v.term(), v.preVote(), true));
            if (!v.preVote()) {
                electionElapsed = 0;
                vote = v.from();
                hardStateDirty = true;
            }
        } else {
            send(new RaftMessage.VoteReply(cfg.nodeId(), v.from(), term, v.preVote(), false));
        }
    }

    private void handleVoteReply(RaftMessage.VoteReply reply) {
        boolean expectingPreVotes = role == Role.PRE_CANDIDATE;
        if (reply.preVote() != expectingPreVotes) {
            return;
        }
        if (role != Role.PRE_CANDIDATE && role != Role.CANDIDATE) {
            return;
        }
        recordBallot(reply.from(), reply.granted());
    }

    /**
     * Records one vote and reacts if the round is decided.
     *
     * @return true when this vote ended the round, either by winning or by giving up
     */
    private boolean recordBallot(long voter, boolean granted) {
        if (!voters.contains(voter)) {
            return false;
        }
        ballots.putIfAbsent(voter, granted);

        int for_ = 0;
        int against = 0;
        for (boolean b : ballots.values()) {
            if (b) {
                for_++;
            } else {
                against++;
            }
        }

        if (for_ >= quorum()) {
            if (role == Role.PRE_CANDIDATE) {
                becomeCandidate();
                solicitVotes();
            } else {
                becomeLeader();
            }
            return true;
        }
        if (against >= quorum()) {
            // The round is lost. Waiting for the timeout would just delay the next attempt.
            LOG.debug("node {} lost its election round in term {}", cfg.nodeId(), term);
            becomeFollower(term, NO_NODE);
            return true;
        }
        return false;
    }

    // ---------------------------------------------------------------------------------------------
    // Replication, follower side
    // ---------------------------------------------------------------------------------------------

    private void handleAppend(RaftMessage.Append a) {
        if (role != Role.FOLLOWER) {
            // Same term and a leader exists, so this node's campaign is over.
            becomeFollower(a.term(), a.from());
        }
        leaderId = a.from();
        electionElapsed = 0;

        long commitBefore = log.committed();
        RaftLog.AppendOutcome outcome =
                log.maybeAppend(a.prevLogIndex(), a.prevLogTerm(), a.leaderCommit(), a.entries());
        if (log.committed() != commitBefore) {
            hardStateDirty = true;
        }

        if (outcome.accepted()) {
            if (!a.entries().isEmpty()) {
                hardStateDirty = true;
            }
            send(
                    new RaftMessage.AppendReply(
                            cfg.nodeId(), a.from(), term, true, outcome.lastNewIndex(), 0, 0));
        } else {
            LOG.debug(
                    "node {} rejecting append at prevIndex={} prevTerm={}, hint index={} term={}",
                    cfg.nodeId(),
                    a.prevLogIndex(),
                    a.prevLogTerm(),
                    outcome.conflictIndex(),
                    outcome.conflictTerm());
            send(
                    new RaftMessage.AppendReply(
                            cfg.nodeId(),
                            a.from(),
                            term,
                            false,
                            0,
                            outcome.conflictIndex(),
                            outcome.conflictTerm()));
        }
    }

    private void handleHeartbeat(RaftMessage.Heartbeat h) {
        if (role != Role.FOLLOWER) {
            becomeFollower(h.term(), h.from());
        }
        leaderId = h.from();
        electionElapsed = 0;
        // A heartbeat may advertise a commit index past the end of our log when we are behind, so
        // clamp: committing an entry we do not have would be committing nothing.
        if (log.commitTo(Math.min(h.leaderCommit(), log.lastIndex()))) {
            hardStateDirty = true;
        }
        send(new RaftMessage.HeartbeatReply(cfg.nodeId(), h.from(), term, h.readSeq()));
    }

    // ---------------------------------------------------------------------------------------------
    // Replication, leader side
    // ---------------------------------------------------------------------------------------------

    private void handleAppendReply(RaftMessage.AppendReply reply) {
        Progress p = peers.get(reply.from());
        if (p == null) {
            return;
        }
        p.recentActive = true;

        if (reply.accepted()) {
            boolean advanced = p.maybeUpdate(reply.matchIndex());
            if (p.state == Progress.State.PROBE) {
                p.becomeReplicate();
            } else {
                p.probeSent = false;
            }
            if (advanced) {
                maybeCommit();
            }
            if (p.next <= log.lastIndex()) {
                sendAppend(reply.from());
            }
            return;
        }

        // Rejected. The follower told us where it diverges; retry from there.
        //
        // The paper's "decrement nextIndex and retry" is correct but takes one round trip per index.
        // Using the follower's first index of the conflicting term skips a whole term at a time.
        // A smarter variant also consults the leader's own log for that term; this one does not,
        // because the extra saved round trip is not worth the extra state to reason about.
        long retryFrom = reply.conflictIndex() > 0 ? reply.conflictIndex() : 1;
        retryFrom = Math.min(retryFrom, log.lastIndex() + 1);
        p.becomeProbe(retryFrom);
        LOG.debug("node {} rewinding node {} to next={}", cfg.nodeId(), reply.from(), p.next);
        sendAppend(reply.from());
    }

    private void handleHeartbeatReply(RaftMessage.HeartbeatReply reply) {
        Progress p = peers.get(reply.from());
        if (p == null) {
            return;
        }
        p.recentActive = true;
        confirmReadRound(reply.readSeq());
        // A heartbeat response proves the follower is reachable again, so let the flow resume.
        // Without this, a probe whose response was lost, or one sent to a node that then crashed,
        // leaves the follower paused forever: nothing else ever clears the flag, and the follower
        // silently stops receiving entries even after it comes back.
        p.probeSent = false;
        // Followers do not learn about new entries from heartbeats, which carry none.
        if (p.match < log.lastIndex()) {
            sendAppend(reply.from());
        }
    }

    private void broadcastAppend() {
        for (long peer : peers.keySet()) {
            sendAppend(peer);
        }
    }

    private void broadcastHeartbeat(long readSeq) {
        for (Map.Entry<Long, Progress> e : peers.entrySet()) {
            send(
                    new RaftMessage.Heartbeat(
                            cfg.nodeId(),
                            e.getKey(),
                            term,
                            // Never advertise a commit index beyond what this follower is known to
                            // hold; it would be asking it to commit an entry it does not have.
                            Math.min(log.committed(), e.getValue().match),
                            readSeq));
        }
    }

    private void sendAppend(long peer) {
        Progress p = peers.get(peer);
        if (p == null || p.paused()) {
            return;
        }
        if (p.next < log.firstIndex()) {
            // This follower needs entries that compaction has removed. Sending anything else would
            // only be rejected, and the rejection would rewind progress the snapshot is about to fix.
            sendSnapshot(peer, p);
            return;
        }
        long prevIndex = p.next - 1;
        long prevTerm;
        try {
            prevTerm = log.term(prevIndex);
        } catch (RaftStorage.CompactedException e) {
            sendSnapshot(peer, p);
            return;
        }

        long hi = Math.min(log.lastIndex() + 1, p.next + cfg.maxEntriesPerAppend());
        List<Entry> entries = log.slice(p.next, hi, cfg.maxBytesPerAppend());
        send(
                new RaftMessage.Append(
                        cfg.nodeId(), peer, term, prevIndex, prevTerm, entries, log.committed()));

        if (p.state == Progress.State.PROBE) {
            p.probeSent = true;
        } else if (!entries.isEmpty()) {
            // Optimistic pipelining: assume acceptance and keep sending. A rejection rewinds next.
            p.next = Entries.lastIndex(entries) + 1;
        }
    }

    /**
     * Starts catching a follower up from a snapshot rather than from entries.
     *
     * <p>Only the metadata goes on the wire from the core's point of view. Moving the payload is the
     * driver's job: a snapshot is arbitrarily large, and the core has no I/O. What the core decides is
     * when a snapshot is needed and which boundary it establishes.
     */
    private void sendSnapshot(long peer, Progress p) {
        SnapshotMetadata meta = snapshotMetadata();
        if (meta.getLastIndex() == 0) {
            // Nothing has been compacted, so the entries this follower wants should still exist. If
            // they do not, the log and the snapshot boundary disagree and continuing would be guessing.
            throw new IllegalStateException(
                    "node "
                            + peer
                            + " needs index "
                            + p.next
                            + " but there is no snapshot and the log starts at "
                            + log.firstIndex());
        }
        LOG.info(
                "node {} sending a snapshot at index {} to node {}",
                cfg.nodeId(),
                meta.getLastIndex(),
                peer);
        p.becomeSnapshot(meta.getLastIndex());
        send(new RaftMessage.InstallSnapshot(cfg.nodeId(), peer, term, meta));
    }

    private void handleInstallSnapshot(RaftMessage.InstallSnapshot message) {
        if (role != Role.FOLLOWER) {
            becomeFollower(message.term(), message.from());
        }
        leaderId = message.from();
        electionElapsed = 0;

        SnapshotMetadata meta = message.meta();
        if (meta.getLastIndex() <= log.committed()) {
            // Already covered by what this node has committed. Reporting the higher index keeps the
            // leader from sending the same snapshot again.
            send(
                    new RaftMessage.InstallSnapshotReply(
                            cfg.nodeId(), message.from(), term, true, log.committed()));
            return;
        }
        log.restore(meta);
        adoptConfState(meta.getConf());
        hardStateDirty = true;
        send(
                new RaftMessage.InstallSnapshotReply(
                        cfg.nodeId(), message.from(), term, true, meta.getLastIndex()));
    }

    private void handleInstallSnapshotReply(RaftMessage.InstallSnapshotReply reply) {
        Progress p = peers.get(reply.from());
        if (p == null) {
            return;
        }
        p.recentActive = true;
        if (reply.success()) {
            p.maybeUpdate(reply.matchIndex());
            p.snapshotFinished(true);
            maybeCommit();
            if (p.next <= log.lastIndex()) {
                sendAppend(reply.from());
            }
        } else {
            LOG.info("node {} refused a snapshot; will retry", reply.from());
            p.snapshotFinished(false);
        }
    }

    /** The snapshot boundary the log has been compacted to, or a zeroed message. */
    public SnapshotMetadata snapshotMetadata() {
        return storage.snapshotMetadata();
    }

    /** Advances the commit index if a majority has stored an entry from the current term. */
    private void maybeCommit() {
        long[] matches = new long[voters.size()];
        int i = 0;
        for (long v : voters) {
            matches[i++] = (v == cfg.nodeId()) ? log.lastIndex() : matchOf(v);
        }
        Arrays.sort(matches);
        // Largest index stored on a majority: with n voters and quorum q, that is the (n-q)th
        // smallest, zero-based.
        long quorumIndex = matches[matches.length - quorum()];

        if (quorumIndex <= log.committed()) {
            return;
        }
        if (log.term(quorumIndex) != term) {
            // Paper 5.4.2. An entry from an earlier term can be present on a majority and still be
            // uncommitted; committing it on a count alone is the figure 8 scenario, where a later
            // leader can still overwrite it.
            LOG.debug(
                    "node {} not committing index {} from term {} in term {}",
                    cfg.nodeId(),
                    quorumIndex,
                    log.term(quorumIndex),
                    term);
            return;
        }
        if (log.commitTo(quorumIndex)) {
            hardStateDirty = true;
            // Let followers learn the new commit index now rather than on the next heartbeat.
            broadcastAppend();
            releaseDeferredReads();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Linearizable reads
    // ---------------------------------------------------------------------------------------------

    /** True once this leader has committed an entry of its own term. */
    private boolean committedInCurrentTerm() {
        return log.committed() > 0 && log.term(log.committed()) == term;
    }

    private void beginRead(long origin, long requestId) {
        if (!committedInCurrentTerm()) {
            // No safe index exists yet: this leader does not know its own committed prefix until one
            // of its entries commits. Hold the request rather than guessing.
            deferredReads.add(new PendingRead(origin, requestId, 0, 0));
            return;
        }
        if (peers.isEmpty()) {
            // A single-node cluster is its own quorum, so leadership is already confirmed.
            deliverRead(new PendingRead(origin, requestId, log.committed(), 0));
            return;
        }
        long round = ++readRound;
        pendingReads.add(new PendingRead(origin, requestId, log.committed(), round));
        readRoundAcks.put(round, 1);
        broadcastHeartbeat(round);
    }

    /** Releases reads that were waiting for this leader to commit something in its term. */
    private void releaseDeferredReads() {
        if (deferredReads.isEmpty() || !committedInCurrentTerm()) {
            return;
        }
        List<PendingRead> waiting = List.copyOf(deferredReads);
        deferredReads.clear();
        for (PendingRead read : waiting) {
            beginRead(read.origin(), read.requestId());
        }
    }

    /**
     * Counts a heartbeat response toward its round and answers whatever it confirms.
     *
     * <p>Rounds are confirmed in order: a response for round R also confirms every earlier round,
     * because leadership at R implies leadership at every point before it.
     */
    private void confirmReadRound(long round) {
        if (round == 0 || !readRoundAcks.containsKey(round)) {
            // Round 0 is a plain heartbeat, and an unknown round is a response from a round this node
            // has already resolved or never started. Counting either would be counting a response
            // from the wrong round, which is exactly what the token exists to prevent.
            return;
        }
        int acks = readRoundAcks.merge(round, 1, Integer::sum);
        if (acks < quorum()) {
            return;
        }
        readRoundAcks.keySet().removeIf(r -> r <= round);
        while (!pendingReads.isEmpty() && pendingReads.peek().round() <= round) {
            deliverRead(pendingReads.poll());
        }
    }

    private void deliverRead(PendingRead read) {
        if (read.origin() == cfg.nodeId()) {
            readStates.add(new ReadState(read.requestId(), read.readIndex()));
        } else {
            send(
                    new RaftMessage.ReadIndexReply(
                            cfg.nodeId(), read.origin(), term, read.requestId(), read.readIndex()));
        }
    }

    /** Abandons reads in flight. Whoever was waiting sees a timeout and retries elsewhere. */
    private void abandonReads() {
        pendingReads.clear();
        deferredReads.clear();
        readRoundAcks.clear();
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    private long matchOf(long voter) {
        Progress p = peers.get(voter);
        return p == null ? 0 : p.match;
    }

    private int quorum() {
        return voters.size() / 2 + 1;
    }

    /** True when this node is a voter and may therefore campaign. */
    private boolean promotable() {
        return voters.contains(cfg.nodeId());
    }

    private boolean quorumRecentlyActive() {
        int active = 1; // this node
        for (Progress p : peers.values()) {
            if (p.recentActive) {
                active++;
            }
            p.recentActive = false;
        }
        return active >= quorum();
    }

    /**
     * A randomized timeout in {@code [electionTimeoutTicks, 2 * electionTimeoutTicks)}.
     *
     * <p>Fixed timeouts make every follower campaign on the same tick, which is how a cluster ends
     * up in a run of split votes (paper 5.2).
     */
    private int nextElectionTimeout() {
        return cfg.electionTimeoutTicks() + random.nextInt(cfg.electionTimeoutTicks());
    }

    private HardState currentHardState() {
        return HardState.newBuilder()
                .setTerm(term)
                .setVote(vote)
                .setCommit(log.committed())
                .build();
    }

    private void send(RaftMessage m) {
        outbound.add(m);
    }

    @Override
    public String toString() {
        return "RaftNode["
                + cfg.nodeId()
                + " "
                + role
                + " term="
                + term
                + " leader="
                + leaderId
                + " commit="
                + log.committed()
                + " last="
                + log.lastIndex()
                + "]";
    }
}
