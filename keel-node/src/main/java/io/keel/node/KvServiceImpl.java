package io.keel.node;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.keel.kv.Commands;
import io.keel.proto.kv.CommandResult;
import io.keel.proto.kv.Session;
import io.keel.proto.service.CasRequest;
import io.keel.proto.service.CasResponse;
import io.keel.proto.service.DeleteRequest;
import io.keel.proto.service.DeleteResponse;
import io.keel.proto.service.ErrorCode;
import io.keel.proto.service.GetRequest;
import io.keel.proto.service.GetResponse;
import io.keel.proto.service.KvServiceGrpc;
import io.keel.proto.service.PutRequest;
import io.keel.proto.service.PutResponse;
import io.keel.proto.service.RegisterClientRequest;
import io.keel.proto.service.RegisterClientResponse;
import io.keel.proto.service.ResponseHeader;
import io.keel.proto.service.StatusRequest;
import io.keel.proto.service.StatusResponse;
import io.keel.raft.NotLeaderException;
import io.keel.raft.Status;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * The client-facing service.
 *
 * <p>Every failure becomes a header on a normal response rather than a gRPC status. A client's next
 * move depends on which failure it was: retry here, retry elsewhere, or stop. Collapsing all three
 * into UNAVAILABLE would throw that away, and a leader hint is worth far more to a client than a
 * status code.
 */
final class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final KeelNode node;

    KvServiceImpl(KeelNode node) {
        this.node = node;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> observer) {
        try {
            KeelNode.ReadAnswer answer =
                    await(node.read(request.getKey(), request.getLinearizable()));
            GetResponse.Builder response =
                    GetResponse.newBuilder().setHeader(ok()).setReadIndex(answer.readIndex());
            answer.value().ifPresent(value -> response.setFound(true).setValue(value));
            respond(observer, response.build());
        } catch (Failure failure) {
            respond(observer, GetResponse.newBuilder().setHeader(failure.header()).build());
        }
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> observer) {
        submit(
                Commands.put(session(request.getSession()), request.getKey(), request.getValue()),
                observer,
                result -> PutResponse.newBuilder().setHeader(ok()).build(),
                header -> PutResponse.newBuilder().setHeader(header).build());
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> observer) {
        submit(
                Commands.delete(session(request.getSession()), request.getKey()),
                observer,
                result -> DeleteResponse.newBuilder().setHeader(ok()).setFound(result.getFound()).build(),
                header -> DeleteResponse.newBuilder().setHeader(header).build());
    }

    @Override
    public void compareAndSwap(CasRequest request, StreamObserver<CasResponse> observer) {
        ByteString command =
                request.getExpectAbsent()
                        ? Commands.compareAndSwapIfAbsent(
                                session(request.getSession()), request.getKey(), request.getValue())
                        : Commands.compareAndSwap(
                                session(request.getSession()),
                                request.getKey(),
                                request.getExpected(),
                                request.getValue());
        submit(
                command,
                observer,
                result ->
                        CasResponse.newBuilder()
                                .setHeader(ok())
                                .setApplied(result.getApplied())
                                .setFound(result.getFound())
                                .setValue(result.getValue())
                                .build(),
                header -> CasResponse.newBuilder().setHeader(header).build());
    }

    @Override
    public void registerClient(
            RegisterClientRequest request, StreamObserver<RegisterClientResponse> observer) {
        submit(
                Commands.registerClient(),
                observer,
                result ->
                        RegisterClientResponse.newBuilder()
                                .setHeader(ok())
                                .setClientId(result.getClientId())
                                .build(),
                header -> RegisterClientResponse.newBuilder().setHeader(header).build());
    }

    @Override
    public void status(StatusRequest request, StreamObserver<StatusResponse> observer) {
        Status status = node.status();
        respond(
                observer,
                StatusResponse.newBuilder()
                        .setHeader(ok())
                        .setNodeId(status.nodeId())
                        .setRole(status.role().name())
                        .setTerm(status.term())
                        .setLeaderId(status.leaderId())
                        .setCommitIndex(status.commitIndex())
                        .setAppliedIndex(node.appliedIndex())
                        .setLastIndex(status.lastIndex())
                        .addAllVoters(status.voters())
                        .setKeys(node.keyCount())
                        .build());
    }

    private <T> void submit(
            ByteString command,
            StreamObserver<T> observer,
            Function<CommandResult, T> onSuccess,
            Function<ResponseHeader, T> onFailure) {
        try {
            CommandResult result = await(node.submit(command));
            if (!result.getApplied() && !result.getMessage().isEmpty()) {
                // A refusal the state machine decided on, such as an expired session. Deterministic and
                // final, so there is nothing to retry.
                respond(
                        observer,
                        onFailure.apply(
                                header(ErrorCode.ERROR_CODE_SESSION_EXPIRED, 0, result.getMessage())));
                return;
            }
            respond(observer, onSuccess.apply(result));
        } catch (Failure failure) {
            respond(observer, onFailure.apply(failure.header()));
        }
    }

    private <T> T await(CompletableFuture<T> future) throws Failure {
        try {
            return future.get(node.options().requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new Failure(header(ErrorCode.ERROR_CODE_TIMEOUT, 0, "interrupted"));
        } catch (TimeoutException e) {
            future.cancel(false);
            throw new Failure(
                    header(
                            ErrorCode.ERROR_CODE_TIMEOUT,
                            0,
                            "no answer within " + node.options().requestTimeout()));
        } catch (ExecutionException e) {
            throw new Failure(classify(e.getCause()));
        }
    }

    private static ResponseHeader classify(Throwable cause) {
        if (cause instanceof NotLeaderException notLeader) {
            return notLeader.leaderHint() == 0
                    ? header(ErrorCode.ERROR_CODE_NO_LEADER, 0, cause.getMessage())
                    : header(ErrorCode.ERROR_CODE_NOT_LEADER, notLeader.leaderHint(), cause.getMessage());
        }
        if (cause instanceof KeelNode.OverwrittenException) {
            // The write demonstrably did not commit, so retrying is safe and correct.
            return header(ErrorCode.ERROR_CODE_TIMEOUT, 0, cause.getMessage());
        }
        return header(ErrorCode.ERROR_CODE_INVALID, 0, String.valueOf(cause));
    }

    private static Session session(Session session) {
        return session == null ? Commands.NO_SESSION : session;
    }

    private static ResponseHeader ok() {
        return ResponseHeader.newBuilder().setCode(ErrorCode.ERROR_CODE_OK).build();
    }

    private static ResponseHeader header(ErrorCode code, long leaderHint, String message) {
        return ResponseHeader.newBuilder()
                .setCode(code)
                .setLeaderHint(leaderHint)
                .setMessage(message == null ? "" : message)
                .build();
    }

    private static <T> void respond(StreamObserver<T> observer, T value) {
        observer.onNext(value);
        observer.onCompleted();
    }

    /** Internal control flow for a request that cannot be answered. */
    private static final class Failure extends Exception {
        private final ResponseHeader header;

        Failure(ResponseHeader header) {
            super(header.getMessage(), null, false, false);
            this.header = header;
        }

        ResponseHeader header() {
            return header;
        }
    }
}
