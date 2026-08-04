package io.keel.node;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Transport security and access control for one node.
 *
 * <p>The default is deliberately awkward to deploy by accident. A node refuses to bind a
 * non-loopback address unless it has both TLS and a client token, or unless {@link #insecure()} was
 * asked for explicitly. Running on a laptop stays one command; exposing an unauthenticated store to
 * a network becomes something you have to mean.
 *
 * <p>Mutual TLS is the membership boundary. A process without a certificate signed by
 * {@code trustedCa} cannot speak the peer protocol at all, which is a stronger statement than any
 * token check: it fails during the handshake, before a single Raft message is parsed.
 *
 * <p>Two tokens rather than one, because reading a key and reconfiguring the cluster are not the
 * same privilege. {@code RemoveMember} can eject a node; a credential that lets a client write a
 * value should not also let it do that.
 *
 * @param certificate PEM certificate chain this node presents, or null for no TLS
 * @param privateKey PEM private key for {@code certificate}
 * @param trustedCa PEM CA that peer and client certificates are verified against
 * @param clientToken required in the {@code x-keel-token} header on client calls; null disables the
 *     check
 * @param adminToken required in {@code x-keel-admin-token} for membership changes. When null,
 *     membership changes fall back to {@code clientToken}, which is worth avoiding.
 * @param insecure permits binding a non-loopback address with no TLS and no token
 */
public record SecurityOptions(
        Path certificate,
        Path privateKey,
        Path trustedCa,
        String clientToken,
        String adminToken,
        boolean insecure) {

    public SecurityOptions {
        boolean hasCert = certificate != null;
        boolean hasKey = privateKey != null;
        if (hasCert != hasKey) {
            throw new IllegalArgumentException(
                    "a TLS certificate and private key have to be configured together");
        }
        if (hasCert && trustedCa == null) {
            // Server-only TLS would encrypt the peer protocol while letting anyone who can reach the
            // port take part in it. Encryption without authentication is not the useful half.
            throw new IllegalArgumentException(
                    "TLS requires a trusted CA: peers authenticate each other with certificates");
        }
        for (Path path : new Path[] {certificate, privateKey, trustedCa}) {
            if (path != null && !Files.isReadable(path)) {
                throw new IllegalArgumentException("cannot read " + path);
            }
        }
    }

    /** No TLS, no tokens. Only usable on a loopback address. */
    public static SecurityOptions none() {
        return new SecurityOptions(null, null, null, null, null, false);
    }

    /** No TLS, no tokens, and permission to bind a real address anyway. */
    public static SecurityOptions insecureAnywhere() {
        return new SecurityOptions(null, null, null, null, null, true);
    }

    /** Mutual TLS with no token checks, for a cluster where certificates are the only credential. */
    public static SecurityOptions mutualTls(Path certificate, Path privateKey, Path trustedCa) {
        return new SecurityOptions(certificate, privateKey, trustedCa, null, null, false);
    }

    public SecurityOptions withTokens(String clientToken, String adminToken) {
        return new SecurityOptions(
                certificate, privateKey, trustedCa, clientToken, adminToken, insecure);
    }

    public boolean tlsEnabled() {
        return certificate != null;
    }

    public boolean authEnabled() {
        return clientToken != null;
    }

    /**
     * Checks whether this configuration may be used on {@code host}.
     *
     * @throws IllegalStateException with an explanation, rather than a boolean, because the caller is
     *     always about to refuse to start and the operator needs to know which part was missing
     */
    public void checkUsableOn(String host) {
        if (insecure || isLoopback(host)) {
            return;
        }
        if (!tlsEnabled() || !authEnabled()) {
            throw new IllegalStateException(
                    "refusing to listen on "
                            + host
                            + " without "
                            + (!tlsEnabled() ? "TLS" : "a client token")
                            + ". Configure --tls-cert, --tls-key, --tls-ca and --client-token, or pass"
                            + " --insecure to accept an unauthenticated store on this address.");
        }
    }

    static boolean isLoopback(String host) {
        return "127.0.0.1".equals(host)
                || "localhost".equals(host)
                || "::1".equals(host)
                || "[::1]".equals(host)
                || "0:0:0:0:0:0:0:1".equals(host);
    }

    @Override
    public String toString() {
        return "SecurityOptions[tls="
                + tlsEnabled()
                + " clientToken="
                + (clientToken == null ? "none" : "set")
                + " adminToken="
                + (adminToken == null ? "none" : "set")
                + (insecure ? " insecure" : "")
                + "]";
    }
}
