# Test certificates

Generated once with OpenSSL and committed on purpose, so the TLS tests need no key generation
at build time and no extra dependency to do it.

**These are worthless as secrets.** The private keys are in a public repository. They exist to
prove the code paths work, and nothing else.

| File | What it is |
| --- | --- |
| `ca.pem` | the cluster CA that node and client certificates are signed by |
| `node.pem`, `node.key` | a node certificate, with SANs for `localhost`, `127.0.0.1` and `::1` |
| `client.pem`, `client.key` | a client certificate, `clientAuth` only |
| `other-ca.pem` | a second, unrelated CA |
| `stranger.pem`, `stranger.key` | a node certificate signed by that other CA |

The last two are the interesting ones. A certificate signed by `other-ca` is technically valid
and must still be rejected, which is what makes the cluster CA the membership boundary rather
than decoration.

The signing keys for both CAs were discarded after generation, so nothing else can be signed
with them. Validity is 100 years, because a test suite that starts failing on a date is worse
than one with an implausible expiry.
