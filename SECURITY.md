# Security policy

## Reporting a vulnerability

Report privately through GitHub's advisory form:
[**Report a vulnerability**](https://github.com/jason-te-sde/keel/security/advisories/new).

Please do not open a public issue for anything exploitable. I will acknowledge within a week
and keep you updated while it is being fixed.

Useful things to include: the version or commit, whether TLS and authentication were enabled,
and the smallest reproduction you have. If a simulation seed reproduces it, that is the ideal
report.

## Supported versions

Pre-1.0, only the newest minor version gets fixes. There is no long-term support branch.

| Version | Supported |
| --- | --- |
| 0.3.x | yes |
| < 0.3 | no |

## What this project does and does not defend against

Being explicit, because a consensus implementation invites assumptions.

**Defended:** a cluster runs over mutual TLS, so the cluster CA is the membership boundary — a
process without a certificate signed by it cannot speak the peer protocol. Client access needs
a token, and membership changes need a separate admin token, so read and write access does not
imply the ability to reconfigure the cluster.

**Not defended:**

- **A compromised node.** Raft assumes crash faults, not Byzantine ones. A node that lies
  about its log can break the cluster's guarantees, and no amount of transport security helps.
- **Anything with `--insecure`.** That flag disables TLS and is intended for a laptop. A node
  refuses to bind a non-loopback address without either TLS or that flag, but if you pass it,
  the traffic is plaintext and unauthenticated.
- **Confidentiality at rest.** The log and snapshots are unencrypted files. Protect the data
  directory with filesystem permissions.
- **Denial of service from an authenticated client.** Request size is bounded and a leader with
  no quorum stops accepting writes, but there is no rate limiting or quota.
- **Certificate revocation.** Rotating a compromised certificate means restarting the nodes
  with a new CA.

## Dependencies

Dependabot watches Maven dependencies and GitHub Actions. Protobuf, gRPC, and RocksDB have all
had CVEs; if you are packaging this, watch their advisories as well as this repository's.
