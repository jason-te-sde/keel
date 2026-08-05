# Changelog

Notable changes, newest first. Versions follow [semantic versioning](https://semver.org),
and until 1.0 the wire format and the on-disk format may both change between minor
versions.

## v0.3.1

A patch release, and the only one so far that anyone running v0.1.0 through v0.3.0 needs to take
seriously: every tag before it can lose a committed entry.

### Fixed (#53)

Widening the nightly soak from 500 seeds to 10,000 turned up four separate ways a committed entry
could end up on fewer than a majority of nodes. Three were in the consensus core and one was in the
simulator that drives it, which is why the fourth went unnoticed while the first three were being
fixed.

- **`InstallSnapshot` discarded the receiver's whole log.** When the entry at the snapshot's boundary
  already matches, Log Matching says the prefix below it matches too and there is nothing to install.
  Discarding threw away entries above the boundary that the node had acknowledged and a leader had
  counted toward a quorum. Paper figure 13, step 6
- **A heartbeat's commit index was clamped to the receiver's last index.** A heartbeat carries no
  proof of what the log below it holds, so this committed whichever entry happened to sit there,
  which after a leader change is not the entry the leader meant
- **A leader counted its own undurable entries toward a quorum.** It now counts only as far as the
  driver has confirmed persisted, which makes the durable boundary moving a reason to recheck what is
  committed
- **Snapshot metadata took its boundary index and its term from different entries,** so across a term
  change it advertised a term its own boundary did not have. Receivers failed the match check and
  discarded acknowledged entries. Both log stores now reject metadata that misdescribes its own
  boundary, so this fails where it is built rather than several hundred ticks later

The three seeds reachable from the simulator are pinned in `RegressionSeedTest`, and each fix has a
unit test that isolates it. 10,000 seeds now pass: 12,000,000 invariant checks, 572,141 snapshots.

### Build and CI

- The nightly soak reports the seed and the cluster state for **any** exception, not just invariant
  violations. Two of the four seeds above were unreplayable before this
- Dependency groups for coupled artifacts, after a log4j bump spent a week red because it needed an
  slf4j bump the enforcer correctly refused without (#55)
- `MetricsEndpointTest.followerMetrics` no longer assumes a cluster holds still between a leader
  lookup and a scrape. It was the only failing check on four unrelated dependency pull requests
- The protobuf plugin no longer races with itself over a temporary directory, which broke local
  builds on macOS about every other run while CI stayed green (#57)
- `scripts/preflight.sh` runs the full verify on every JDK in the CI matrix, since `-Werror` plus
  lint categories that differ between releases had twice turned a job red only on JDK 25
- Every dependency current, including three major bumps (JUnit 6, RocksDB 10, protobuf 4.35)
- `keel-proto` attaches a javadoc jar. Maven Central requires one, and because the published site is
  an aggregate that honoured the same skip, every proto type had been missing from it (#64)

## v0.3.0

The release that makes this usable by someone other than its author. v0.2.0 was a correct store
that could not be published, could not be deployed safely, and could not be observed once running.

### Security (#23)

- **Mutual TLS** between nodes and to clients. The cluster CA is the membership boundary: a process
  without a certificate signed by it fails the handshake and never sends a Raft message
- TLS without a trusted CA is refused as a configuration error, because encryption without
  authentication is not the useful half
- A **client token**, and a **separate admin token** for membership changes. `RemoveMember` can eject
  a node, and a credential that writes a value has no business doing that. Compared in constant time
- **Secure by default**: a node refuses to bind a non-loopback address without both TLS and a token,
  unless `--insecure` is passed. The error names the missing piece
- Request size bounded below gRPC's default, so an oversized value is refused with a reason

### Observability (#24)

- Prometheus metrics, hand-written because the format is a few lines of text and a client library
  would be the largest dependency in the project
- `/healthz` and `/readyz` answering **different** questions. Readiness is false with no known leader
  or a state machine more than 1000 entries behind; conflating them is how a rolling restart takes a
  cluster down
- Off unless a port is configured, since a fixed default collides when three nodes share a host

### Packaging and operations (#25)

- A properties config file, with flags overriding it, so an image can ship what a deployment shares
- A container image running unprivileged, sizing the heap from the container limit, health-checking
  `/readyz`. CI builds it, brings up the compose stack, writes and reads, then kills the leader and
  reads the value back
- `docs/operations.md`: tick tuning against real round trips, disk sizing, what to alert on, backup
  and restore, upgrades, and a symptom-to-cause table
- Logging configuration. Log4j2 had none, so a node failing to elect looked identical to an idle one

### Distribution (#22, #26)

- Group id is now `io.github.jason-te-sde`; `io.keel` could never be published, since that namespace
  requires controlling `keel.io`. Java packages stay `io.keel`
- A `release` profile attaching sources and javadoc, separate from a `publish` profile that signs and
  uploads, so building locally needs no GPG key
- Pushing a tag builds, verifies, and attaches a checksummed runnable jar
- Aggregate javadoc published to GitHub Pages
- `SECURITY.md` with a private reporting route and an explicit list of what is **not** defended,
  `CODE_OF_CONDUCT.md`, `CODEOWNERS`, and Dependabot for Maven and Actions
- `RELEASING.md`, which leads with the admission that the first two tags were cut while every POM
  said `0.1.0-SNAPSHOT`

### Snapshots (#27)

- Streamed in both directions, one chunk in memory, so a state machine may exceed the heap
- An abandoned transfer leaves nothing behind, because a half-written snapshot is indistinguishable
  from a good one and the log gets compacted on the strength of it

### Bugs found while building this

- **A single-node cluster could not commit anything.** `maybeCommit` only ran on a reply, and a lone
  voter is its own majority that nobody replies to. Found by writing a backup test, which was the
  first thing to run one node and write to it
- **Log4j2 had no configuration**, so a node failing to elect a leader looked exactly like an idle one
- **The container health check used a bash feature** that the image's dash shell does not have, and
  the first attempt to fix it edited a string that was not in the file
- **An orphaned Javadoc comment** failed `-Werror` on JDK 25 only, which has a lint JDK 21 does not

### Measured on an Apple M-series laptop, JDK 21

| | |
| --- | --- |
| Tests | 246 |
| Line / branch coverage | 83.9% / 78.4% |
| Simulation | 146,038 ticks/s; 200 seeds, 240,000 invariant checks, 10,462 snapshots in 1.6s |
| Log append, fsync per batch of 64 | 19,708 entries/s |
| Log append, fsync per entry | 328 entries/s |

### Still absent, on purpose

Joint consensus, leader leases, leader transfer, learners, certificate rotation without a restart,
rate limiting, encryption at rest, multi-raft.

## v0.2.0

Closes the two gaps v0.1.0 shipped with. The Raft implementation is now feature complete for
a single group: it compacts, and its membership can change while it runs.

### Snapshots and log compaction (#3)

- The state machine is serialized, the log prefix discarded, and a follower that has fallen
  past the leader's oldest retained entry caught up from a snapshot instead of from entries it
  can never receive
- Streamed over its own RPC whose response means *installed*, not received: a leader must not
  learn a follower holds a snapshot before the follower holds it
- Snapshot files are checksummed and promoted by rename, so a half-written one can never be
  mistaken for a good one after the log has already been compacted on its strength
- Startup refuses to run if the log claims a boundary that no snapshot covers, rather than
  hiding data loss

### Single-node membership changes (#4)

- Add or remove one voter on a running cluster, through the core, the simulator, gRPC, and
  `keelctl member add|remove`
- A change takes effect when the entry is applied, not appended; a second change is refused
  while the first is unapplied; a leader removed from the configuration steps down; and
  membership travels in snapshots
- A joining node starts as a non-voter, which required separating the address book from the
  voter set in the node layer. They are not the same thing.
- Addresses travel inside the configuration entries, so membership comes from the log alone

### Testing

- The simulator now compacts during every run, on a deliberately tiny threshold, so every seed
  crosses the snapshot paths
- The soak run asserts snapshots actually happened; a sweep where nothing compacted would leave
  all of it untested
- `MembershipTest` covers the four rules through the simulator, so every safety invariant is
  checked after every step of those runs

### Bugs found while building this

All of one shape, "the log no longer starts at index 1":

- Log Matching in the invariant checker compared durable logs by list position, so two nodes
  that compacted at different points had index 5 compared against index 1
- A follower whose snapshot covered `prevLogIndex` treated already-committed entries as
  conflicts and refused to continue
- A compaction marker discarded entries written above its own boundary during replay, losing
  the log tail on every restart after a compaction
- The simulator paired snapshot payloads with the wrong metadata, because they were keyed by
  recipient rather than by recipient and boundary

Adding two messages to the sealed `RaftMessage` interface also broke the codec's compilation
immediately, which is what that design was bought for.

### Measured on an Apple M-series laptop, JDK 21

| | |
| --- | --- |
| Tests | 215 |
| Line / branch coverage | 83.9% / 78.0% |
| Simulation | 135,246 ticks/s; 200 seeds, 240,000 invariant checks, 10,462 snapshots in 1.8s |
| Log append, no fsync | 226,253 entries/s |
| Log append, fsync per batch of 64 | 19,360 entries/s |
| Log append, fsync per entry | 336 entries/s |

### Still absent, on purpose

Joint consensus, leader leases, leader transfer, learners, TLS, multi-raft.
`docs/design/0004-scope.md` gives the reasoning for each.

## v0.1.0

First release. A three-node cluster elects a leader, replicates writes, serves
linearizable reads, survives `kill -9` on the leader, and recovers from its own data
directory on restart.

### Consensus (`keel-raft`)

- Leader election with pre-vote (paper 9.6) and check-quorum (6.2)
- The election restriction (5.4.1) and the commit rule that refuses to count entries from
  earlier terms (5.4.2, figure 8)
- Log replication with per-follower progress tracking, probe and replicate states, and
  conflict-index hints so a lagging follower converges in a round trip per term rather
  than per index
- Linearizable reads via ReadIndex (6.4) on leaders and followers, with a round token on
  heartbeats so a response cannot be counted toward a round it does not belong to
- Backpressure: a leader that has lost its quorum refuses writes rather than buffering
  them until the heap runs out
- A pure state machine: no threads, no clock, no I/O, no locks

### Durability (`keel-storage`)

- Segmented, append-only log with CRC32C per record and rotation by size
- Entries and hard state share one log so both become durable in a single fsync
- A conflicting suffix is superseded during replay rather than truncated in place, which
  makes it impossible for a crash to resurrect an older term and let a node vote twice in
  one term
- Recovery truncates an unfinished record at the tail of the newest segment and refuses to
  start on damage a crash cannot explain

### State machine (`keel-kv`)

- `get`, `put`, `delete`, and compare-and-swap over byte keys
- Client sessions that make a retried write exactly-once, snapshotted with the rest of the
  state
- Deterministic client ids (the registration's log index) and deterministic session
  eviction (ordered by last-active log index)
- Two backends, in-memory and RocksDB, sharing one contract test

### Testing (`keel-testkit`)

- Deterministic cluster simulator: virtual clock, per-link latency, drops, duplicates,
  arbitrary partitions, and crashes that discard unsynced writes
- Election Safety, Log Matching, State Machine Safety, and term and commit monotonicity
  asserted after every step
- A linearizability checker with per-key decomposition and memoization, plus a test that
  it rejects the history a deliberately broken read path produces
- A seed sweep and a nightly workflow that runs 2,000 seeds

### Running it (`keel-node`)

- gRPC transport, one-way, one envelope message type
- One thread owns the core; applying runs on its own thread
- Client with leader-hint following and session-based retries
- `keeld` and `keelctl`, and a script that brings up three local nodes

### Known gaps at the time

- No snapshots or log compaction (#3, closed in v0.2.0)
- No membership changes (#4, closed in v0.2.0)
- No TLS, no authentication, no leader transfer, no metrics endpoint

### Measured on an Apple M-series laptop, JDK 21

| | |
| --- | --- |
| Tests | 190 |
| Line / branch coverage | 86.4% / 82.1% |
| Simulation | 83,684 ticks/s; 200 seeds and 240,000 invariant checks in 2.9s |
| Log append, no fsync | 256,660 entries/s |
| Log append, fsync per batch of 64 | 21,241 entries/s |
| Log append, fsync per entry | 334 entries/s |
