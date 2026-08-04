package io.keel.node;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Set;

/**
 * Token checks on the client API, with membership changes held to a higher bar.
 *
 * <p>Split into two credentials on purpose. {@code RemoveMember} can eject a node from the cluster,
 * and {@code AddMember} can bring an unknown process into a quorum. A token that lets a client write
 * a value has no business doing either, so a separate admin token gates them.
 *
 * <p>Tokens are compared with {@link MessageDigest#isEqual}, which does not return early on the first
 * differing byte. A token check that leaks its answer through timing is a token check that can be
 * guessed one byte at a time.
 *
 * <p>This guards the client-facing service only. The peer protocol is protected by mutual TLS
 * instead, which is a stronger boundary: an unauthorised process fails the handshake and never gets
 * as far as sending a Raft message.
 */
final class AuthInterceptor implements ServerInterceptor {

    static final Metadata.Key<String> TOKEN =
            Metadata.Key.of("x-keel-token", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> ADMIN_TOKEN =
            Metadata.Key.of("x-keel-admin-token", Metadata.ASCII_STRING_MARSHALLER);

    /** Methods that change the cluster rather than its contents. */
    private static final Set<String> ADMIN_METHODS =
            Set.of("keel.service.v1.KvService/AddMember", "keel.service.v1.KvService/RemoveMember");

    private final String clientToken;
    private final String adminToken;

    AuthInterceptor(String clientToken, String adminToken) {
        this.clientToken = clientToken;
        this.adminToken = adminToken;
    }

    @Override
    public <Q, S> ServerCall.Listener<Q> interceptCall(
            ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
        if (clientToken == null && adminToken == null) {
            return next.startCall(call, headers);
        }

        boolean admin = ADMIN_METHODS.contains(call.getMethodDescriptor().getFullMethodName());
        String required = admin ? adminOrFallback() : clientToken;
        Metadata.Key<String> header = admin && adminToken != null ? ADMIN_TOKEN : TOKEN;

        if (required != null && !matches(required, headers.get(header))) {
            call.close(
                    Status.UNAUTHENTICATED.withDescription(
                            admin
                                    ? "membership changes require a valid " + header.name() + " header"
                                    : "a valid " + header.name() + " header is required"),
                    new Metadata());
            return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
    }

    /**
     * The admin token, or the client token when no admin token is configured.
     *
     * <p>Falling back is the lesser of two bad options: refusing all membership changes would make a
     * cluster unmanageable, and allowing them unauthenticated would be worse. Configuring an admin
     * token is what actually separates the two privileges, which is why {@code SecurityOptions} says
     * so.
     */
    private String adminOrFallback() {
        return adminToken != null ? adminToken : clientToken;
    }

    private static boolean matches(String expected, String presented) {
        if (presented == null) {
            return false;
        }
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8), presented.getBytes(StandardCharsets.UTF_8));
    }
}
