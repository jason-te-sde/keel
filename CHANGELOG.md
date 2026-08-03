# Changelog

Notable changes, newest first. Versions follow [semantic versioning](https://semver.org),
and until 1.0 the wire format and the on-disk format may both change between minor
versions.

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
