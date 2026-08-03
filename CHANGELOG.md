# Changelog

Notable changes, newest first. Versions follow [semantic versioning](https://semver.org),
and until 1.0 the wire format and the on-disk format may both change between minor
versions.

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

### Known gaps

- No snapshots or log compaction (#3). The log grows without bound and a badly lagging
  follower cannot be caught up.
- No membership changes (#4). The cluster is fixed at startup.
- No TLS, no authentication, no leader transfer, no metrics endpoint.

`docs/design/0004-scope.md` explains the reasoning for each.

### Measured on an Apple M-series laptop, JDK 21

| | |
| --- | --- |
| Tests | 190 |
| Line / branch coverage | 86.4% / 82.1% |
| Simulation | 83,684 ticks/s; 200 seeds and 240,000 invariant checks in 2.9s |
| Log append, no fsync | 256,660 entries/s |
| Log append, fsync per batch of 64 | 21,241 entries/s |
| Log append, fsync per entry | 334 entries/s |
