package io.keel.node;

import io.keel.proto.raft.AppendRequest;
import io.keel.proto.raft.AppendResponse;
import io.keel.proto.raft.HeartbeatRequest;
import io.keel.proto.raft.HeartbeatResponse;
import io.keel.proto.raft.RaftEnvelope;
import io.keel.proto.raft.ReadIndexRequest;
import io.keel.proto.raft.ReadIndexResponse;
import io.keel.proto.raft.VoteRequest;
import io.keel.proto.raft.VoteResponse;
import io.keel.raft.RaftMessage;

/**
 * Converts between the core's sealed message set and the wire schema.
 *
 * <p>The duplication is deliberate. The core's types are sealed so a dispatch that forgets a message
 * fails to compile, which a protobuf {@code oneof} cannot offer, and this class is where that
 * guarantee is paid for. Both directions switch exhaustively, so adding a message to either side
 * breaks the build here rather than silently dropping traffic at runtime.
 *
 * <p>The wire form carries no {@code to} field: the recipient is whichever node received the call.
 */
final class RaftCodec {

    private RaftCodec() {}

    static RaftEnvelope toWire(RaftMessage message) {
        RaftEnvelope.Builder envelope = RaftEnvelope.newBuilder();
        switch (message) {
            case RaftMessage.Vote v ->
                    envelope.setVote(
                            VoteRequest.newBuilder()
                                    .setPreVote(v.preVote())
                                    .setTerm(v.term())
                                    .setCandidateId(v.from())
                                    .setLastLogIndex(v.lastLogIndex())
                                    .setLastLogTerm(v.lastLogTerm()));
            case RaftMessage.VoteReply v ->
                    envelope.setVoteResponse(
                            VoteResponse.newBuilder()
                                    .setTerm(v.term())
                                    .setGranted(v.granted())
                                    .setPreVote(v.preVote())
                                    .setVoterId(v.from()));
            case RaftMessage.Append a ->
                    envelope.setAppend(
                            AppendRequest.newBuilder()
                                    .setTerm(a.term())
                                    .setLeaderId(a.from())
                                    .setPrevLogIndex(a.prevLogIndex())
                                    .setPrevLogTerm(a.prevLogTerm())
                                    .addAllEntries(a.entries())
                                    .setLeaderCommit(a.leaderCommit()));
            case RaftMessage.AppendReply a ->
                    envelope.setAppendResponse(
                            AppendResponse.newBuilder()
                                    .setTerm(a.term())
                                    .setAccepted(a.accepted())
                                    .setFollowerId(a.from())
                                    .setMatchIndex(a.matchIndex())
                                    .setConflictIndex(a.conflictIndex())
                                    .setConflictTerm(a.conflictTerm()));
            case RaftMessage.Heartbeat h ->
                    envelope.setHeartbeat(
                            HeartbeatRequest.newBuilder()
                                    .setTerm(h.term())
                                    .setLeaderId(h.from())
                                    .setLeaderCommit(h.leaderCommit())
                                    .setReadSeq(h.readSeq()));
            case RaftMessage.HeartbeatReply h ->
                    envelope.setHeartbeatResponse(
                            HeartbeatResponse.newBuilder()
                                    .setTerm(h.term())
                                    .setFollowerId(h.from())
                                    .setReadSeq(h.readSeq()));
            case RaftMessage.ReadIndexRequest r ->
                    envelope.setReadIndex(
                            ReadIndexRequest.newBuilder()
                                    .setTerm(r.term())
                                    .setFollowerId(r.from())
                                    .setRequestId(r.requestId()));
            case RaftMessage.ReadIndexReply r ->
                    envelope.setReadIndexResponse(
                            ReadIndexResponse.newBuilder()
                                    .setTerm(r.term())
                                    .setLeaderId(r.from())
                                    .setRequestId(r.requestId())
                                    .setReadIndex(r.readIndex()));
        }
        return envelope.build();
    }

    /**
     * @param self the receiving node, which becomes the message's recipient
     * @throws IllegalArgumentException if the envelope carries nothing this build understands, which
     *     means a peer is running a newer version
     */
    static RaftMessage fromWire(RaftEnvelope envelope, long self) {
        return switch (envelope.getMessageCase()) {
            case VOTE -> {
                VoteRequest v = envelope.getVote();
                yield new RaftMessage.Vote(
                        v.getCandidateId(),
                        self,
                        v.getTerm(),
                        v.getPreVote(),
                        v.getLastLogIndex(),
                        v.getLastLogTerm());
            }
            case VOTE_RESPONSE -> {
                VoteResponse v = envelope.getVoteResponse();
                yield new RaftMessage.VoteReply(
                        v.getVoterId(), self, v.getTerm(), v.getPreVote(), v.getGranted());
            }
            case APPEND -> {
                AppendRequest a = envelope.getAppend();
                yield new RaftMessage.Append(
                        a.getLeaderId(),
                        self,
                        a.getTerm(),
                        a.getPrevLogIndex(),
                        a.getPrevLogTerm(),
                        a.getEntriesList(),
                        a.getLeaderCommit());
            }
            case APPEND_RESPONSE -> {
                AppendResponse a = envelope.getAppendResponse();
                yield new RaftMessage.AppendReply(
                        a.getFollowerId(),
                        self,
                        a.getTerm(),
                        a.getAccepted(),
                        a.getMatchIndex(),
                        a.getConflictIndex(),
                        a.getConflictTerm());
            }
            case HEARTBEAT -> {
                HeartbeatRequest h = envelope.getHeartbeat();
                yield new RaftMessage.Heartbeat(
                        h.getLeaderId(), self, h.getTerm(), h.getLeaderCommit(), h.getReadSeq());
            }
            case HEARTBEAT_RESPONSE -> {
                HeartbeatResponse h = envelope.getHeartbeatResponse();
                yield new RaftMessage.HeartbeatReply(
                        h.getFollowerId(), self, h.getTerm(), h.getReadSeq());
            }
            case READ_INDEX -> {
                ReadIndexRequest r = envelope.getReadIndex();
                yield new RaftMessage.ReadIndexRequest(
                        r.getFollowerId(), self, r.getTerm(), r.getRequestId());
            }
            case READ_INDEX_RESPONSE -> {
                ReadIndexResponse r = envelope.getReadIndexResponse();
                yield new RaftMessage.ReadIndexReply(
                        r.getLeaderId(), self, r.getTerm(), r.getRequestId(), r.getReadIndex());
            }
            case MESSAGE_NOT_SET ->
                    throw new IllegalArgumentException(
                            "empty raft envelope; a peer may be running a newer protocol");
        };
    }
}
