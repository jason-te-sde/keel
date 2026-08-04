package io.keel.node;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.MetadataUtils;
import io.keel.proto.kv.Session;
import io.keel.proto.service.AddMemberRequest;
import io.keel.proto.service.CasRequest;
import io.keel.proto.service.CasResponse;
import io.keel.proto.service.DeleteRequest;
import io.keel.proto.service.ErrorCode;
import io.keel.proto.service.GetRequest;
import io.keel.proto.service.GetResponse;
import io.keel.proto.service.KvServiceGrpc;
import io.keel.proto.service.MemberChangeResponse;
import io.keel.proto.service.PutRequest;
import io.keel.proto.service.RegisterClientRequest;
import io.keel.proto.service.RemoveMemberRequest;
import io.keel.proto.service.RegisterClientResponse;
import io.keel.proto.service.ResponseHeader;
import io.keel.proto.service.StatusRequest;
import io.keel.proto.service.StatusResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client that finds the leader and keeps its writes exactly-once.
 *
 * <p>Two things it does that a naive client does not. It follows the leader hint on a rejection
 * instead of rediscovering the cluster, and it opens a session and numbers its requests, so a retry
 * after a timeout is deduplicated by the state machine rather than applied twice. Without the session
 * a retried compare-and-swap is a different outcome, and the client cannot tell the difference.
 *
 * <p>Not thread safe: one client issues one request at a time, which is also what the session table's
 * single-slot-per-client design assumes.
 */
public final class KeelClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KeelClient.class);
    private static final int MAX_ATTEMPTS = 12;

    private final Map<Long, ManagedChannel> channels = new LinkedHashMap<>();
    private final Map<Long, KvServiceGrpc.KvServiceBlockingStub> stubs = new LinkedHashMap<>();
    private final List<Long> nodeIds;
    private final long deadlineMillis;

    private long preferred;
    private long clientId;
    private long sequence;

    /** Connects to every node in {@code cluster}, mapping node id to {@code host:port}. */
    public KeelClient(Map<Long, String> cluster) {
        this(cluster, SecurityOptions.none(), 5_000);
    }

    public KeelClient(Map<Long, String> cluster, long deadlineMillis) {
        this(cluster, SecurityOptions.none(), deadlineMillis);
    }

    /**
     * Connects with TLS and tokens.
     *
     * <p>{@link SecurityOptions#insecure()} is ignored here: it governs what a server is willing to
     * bind, not what a client is willing to dial.
     */
    public KeelClient(Map<Long, String> cluster, SecurityOptions security, long deadlineMillis) {
        this.deadlineMillis = deadlineMillis;
        // Both tokens travel on every call. The server checks whichever one the method requires, and
        // deciding that here would mean the client encoding the server's authorisation rules.
        Metadata headers = new Metadata();
        if (security.clientToken() != null) {
            headers.put(AuthInterceptor.TOKEN, security.clientToken());
        }
        if (security.adminToken() != null) {
            headers.put(AuthInterceptor.ADMIN_TOKEN, security.adminToken());
        }

        cluster.forEach(
                (id, address) -> {
                    ManagedChannel channel = channelTo(address, security);
                    channels.put(id, channel);
                    KvServiceGrpc.KvServiceBlockingStub stub = KvServiceGrpc.newBlockingStub(channel);
                    if (headers.keys().size() > 0) {
                        stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
                    }
                    stubs.put(id, stub);
                });
        this.nodeIds = new ArrayList<>(stubs.keySet());
        this.preferred = nodeIds.isEmpty() ? 0 : nodeIds.get(0);
    }

    /** Opens a session so retries are deduplicated. Optional, but writes are safer with one. */
    public long openSession() {
        RegisterClientResponse response =
                call(
                        stub -> stub.registerClient(RegisterClientRequest.getDefaultInstance()),
                        RegisterClientResponse::getHeader);
        clientId = response.getClientId();
        sequence = 0;
        LOG.debug("opened session {}", clientId);
        return clientId;
    }

    private static ManagedChannel channelTo(String address, SecurityOptions security) {
        if (!security.tlsEnabled()) {
            return NettyChannelBuilder.forTarget(address).usePlaintext().build();
        }
        try {
            SslContext ssl =
                    GrpcSslContexts.forClient()
                            .keyManager(security.certificate().toFile(), security.privateKey().toFile())
                            .trustManager(security.trustedCa().toFile())
                            .build();
            return NettyChannelBuilder.forTarget(address).sslContext(ssl).build();
        } catch (SSLException e) {
            throw new IllegalStateException("failed to build a TLS channel to " + address, e);
        }
    }

    public void put(String key, String value) {
        put(bytes(key), bytes(value));
    }

    public void put(ByteString key, ByteString value) {
        Session session = nextSession();
        call(
                stub ->
                        stub.put(
                                PutRequest.newBuilder()
                                        .setKey(key)
                                        .setValue(value)
                                        .setSession(session)
                                        .build()),
                response -> response.getHeader());
    }

    /** A linearizable read: the value is at least as new as any write that has already returned. */
    public Optional<String> get(String key) {
        return get(bytes(key), true).map(ByteString::toStringUtf8);
    }

    public Optional<ByteString> get(ByteString key, boolean linearizable) {
        GetResponse response =
                call(
                        stub ->
                                stub.get(
                                        GetRequest.newBuilder()
                                                .setKey(key)
                                                .setLinearizable(linearizable)
                                                .build()),
                        GetResponse::getHeader);
        return response.getFound() ? Optional.of(response.getValue()) : Optional.empty();
    }

    public boolean delete(String key) {
        Session session = nextSession();
        return call(
                        stub ->
                                stub.delete(
                                        DeleteRequest.newBuilder()
                                                .setKey(bytes(key))
                                                .setSession(session)
                                                .build()),
                        response -> response.getHeader())
                .getFound();
    }

    /** Writes only if the key currently holds {@code expected}, or is absent when it is null. */
    public boolean compareAndSwap(String key, String expected, String value) {
        Session session = nextSession();
        CasRequest.Builder request =
                CasRequest.newBuilder().setKey(bytes(key)).setValue(bytes(value)).setSession(session);
        if (expected == null) {
            request.setExpectAbsent(true);
        } else {
            request.setExpected(bytes(expected));
        }
        CasResponse response = call(stub -> stub.compareAndSwap(request.build()), CasResponse::getHeader);
        return response.getApplied();
    }

    /**
     * Adds a voter and returns the membership afterwards.
     *
     * <p>The address travels in the configuration entry, so every node learns it from the log rather
     * than needing its own configuration updated.
     */
    public List<Long> addMember(long nodeId, String address) {
        MemberChangeResponse response =
                call(
                        stub ->
                                stub.addMember(
                                        AddMemberRequest.newBuilder()
                                                .setNodeId(nodeId)
                                                .setAddress(address)
                                                .build()),
                        MemberChangeResponse::getHeader);
        return response.getVotersList();
    }

    public List<Long> removeMember(long nodeId) {
        MemberChangeResponse response =
                call(
                        stub ->
                                stub.removeMember(
                                        RemoveMemberRequest.newBuilder().setNodeId(nodeId).build()),
                        MemberChangeResponse::getHeader);
        return response.getVotersList();
    }

    public StatusResponse status(long nodeId) {
        KvServiceGrpc.KvServiceBlockingStub stub = stubs.get(nodeId);
        if (stub == null) {
            throw new IllegalArgumentException("no such node: " + nodeId);
        }
        return stub.withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS)
                .status(StatusRequest.getDefaultInstance());
    }

    /**
     * Sends a request, following leader hints and retrying transient failures.
     *
     * <p>A timeout is retried with the same sequence number on purpose: the state machine will either
     * apply it once or return the answer it already produced. That is the entire reason for sessions.
     */
    private <T> T call(
            Function<KvServiceGrpc.KvServiceBlockingStub, T> request,
            Function<T, ResponseHeader> headerOf) {
        RuntimeException last = null;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            long target = preferred == 0 ? nodeIds.get(attempt % nodeIds.size()) : preferred;
            KvServiceGrpc.KvServiceBlockingStub stub = stubs.get(target);
            if (stub == null) {
                preferred = 0;
                continue;
            }
            try {
                T response =
                        request.apply(stub.withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS));
                ResponseHeader header = headerOf.apply(response);
                switch (header.getCode()) {
                    case ERROR_CODE_OK -> {
                        preferred = target;
                        return response;
                    }
                    case ERROR_CODE_NOT_LEADER -> {
                        // Go straight to the node it named rather than starting discovery over.
                        preferred = header.getLeaderHint();
                        last = new IllegalStateException(header.getMessage());
                    }
                    case ERROR_CODE_NO_LEADER, ERROR_CODE_TIMEOUT -> {
                        preferred = 0;
                        last = new IllegalStateException(header.getMessage());
                    }
                    default ->
                            throw new IllegalStateException(
                                    header.getCode() + ": " + header.getMessage());
                }
            } catch (io.grpc.StatusRuntimeException e) {
                if (e.getStatus().getCode() == io.grpc.Status.Code.UNAUTHENTICATED
                        || e.getStatus().getCode() == io.grpc.Status.Code.PERMISSION_DENIED) {
                    // A bad credential is not a transient failure. Trying every other node with the
                    // same token would turn one clear error into a dozen confusing ones.
                    throw new IllegalStateException("authentication failed: " + e.getStatus().getDescription(), e);
                }
                preferred = 0;
                last = e;
            } catch (RuntimeException e) {
                // Unreachable node: try another one.
                preferred = 0;
                last = e;
            }
            sleepBriefly(attempt);
        }
        throw new IllegalStateException("gave up after " + MAX_ATTEMPTS + " attempts", last);
    }

    private Session nextSession() {
        if (clientId == 0) {
            return Session.getDefaultInstance();
        }
        return Session.newBuilder().setClientId(clientId).setSequence(++sequence).build();
    }

    private static void sleepBriefly(int attempt) {
        try {
            // Enough for an election to finish, without turning a retry loop into a busy wait.
            Thread.sleep(Math.min(50L * (attempt + 1), 400L));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while retrying", e);
        }
    }

    private static ByteString bytes(String s) {
        return ByteString.copyFromUtf8(s);
    }

    /** True when the response header says the call succeeded. */
    public static boolean isOk(ResponseHeader header) {
        return header.getCode() == ErrorCode.ERROR_CODE_OK;
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
        stubs.clear();
    }
}
