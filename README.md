# keel

A replicated key-value store built on a from-scratch Raft implementation in
Java, with a deterministic fault-injection simulator and a linearizability
checker as first-class parts of the project rather than an afterthought.

The goal is not another Raft library. It is a store whose correctness claims can
be tested: every safety property in the Raft paper is asserted after every step
of a seeded simulation, and client histories recorded under crashes and network
partitions are checked for linearizability.

## Status

Early. The roadmap below is tracked as issues, and each item lands as its own
pull request.

- [ ] Write-ahead log with segment rotation, checksums, and crash recovery
- [ ] Raft core: pre-vote elections, log replication, commit rules
- [ ] Snapshots, log compaction, and snapshot streaming to lagging followers
- [ ] Single-node membership changes
- [ ] Linearizable reads via ReadIndex
- [ ] Key-value state machine with exactly-once client sessions
- [ ] Deterministic simulator with partitions, crashes, and message faults
- [ ] Linearizability checker
- [ ] gRPC transport, server, and CLI client

## Design notes

The consensus core is a pure state machine: no threads, no clocks, no I/O. It
takes messages and logical ticks in, and returns the messages to send, the
entries to persist, and the entries to apply. Every interesting failure mode is
therefore reproducible from a single integer seed, and the core needs no locks
because exactly one thread ever touches it.

`docs/architecture.md` covers the layering, and `docs/design/` holds one
document per significant decision.

## Layout

```
keel-proto      wire and on-disk schemas (.proto is the source of truth)
keel-raft       consensus core: no I/O, no concurrency
keel-storage    segmented write-ahead log and snapshot store
keel-kv         key-value state machine and client sessions
keel-node       wiring, gRPC transport, server, client, CLI
keel-testkit    deterministic simulator, invariant checks, linearizability checker
```

## Building

Requires JDK 21 or newer and Maven 3.9 or newer. There are no other build
prerequisites: `protoc` is fetched by the build, and the only runtime
dependencies are Protobuf, gRPC, RocksDB, and SLF4J.

```
mvn verify              # compile with -Werror, run every test
mvn verify -Dcoverage   # the same, plus JaCoCo reports
```

## License

MIT
