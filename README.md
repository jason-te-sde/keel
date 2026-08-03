# keel

A linearizable distributed key-value store in Java, built on a Raft implementation
written from scratch, with a deterministic fault-injection simulator and a
linearizability checker as part of the project rather than as an afterthought.

The goal was never another Raft library. It was a store whose correctness claims can
be tested: every safety property in the Raft paper is asserted after every step of a
seeded simulation, and the histories clients actually observed are checked for
linearizability under crashes and network partitions.

```
$ keelctl --cluster=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003 status
node 1  FOLLOWER  term=1  leader=2  commit=1  applied=1  keys=0
node 2  LEADER    term=1  leader=2  commit=1  applied=1  keys=0
node 3  FOLLOWER  term=1  leader=2  commit=1  applied=1  keys=0

$ keelctl ... put greeting hello
ok
$ kill -9 <leader pid>
$ keelctl ... get greeting
hello
```

## What works

- **Consensus**: leader election with pre-vote, check-quorum, log replication with
  conflict-index backtracking, and the commit rule that refuses to count entries from
  earlier terms
- **Durability**: a segmented, checksummed, append-only write-ahead log that recovers
  from a crash at any byte and refuses to start on damage a crash cannot explain
- **Linearizable reads** via ReadIndex, on leaders and on followers, with no log writes
  on the read path
- **Exactly-once writes**: client sessions that deduplicate a retry, snapshotted as part
  of the state machine
- **Two state machine backends**: on the heap, or RocksDB
- **gRPC transport**, a server, a client that follows leader hints, and a CLI

## What does not work yet

Stated plainly because a store without these is not a finished store:

- **No snapshots or log compaction** ([#3](https://github.com/jason-te-sde/keel/issues/3)).
  The log grows without bound, restarts replay it from the beginning, and a follower
  cannot be caught up from a snapshot. Nothing silently degrades: the core throws rather
  than pretending it can send entries it has discarded.
- **No membership changes** ([#4](https://github.com/jason-te-sde/keel/issues/4)).
  The cluster is fixed at startup, so replacing a dead machine means restarting the
  cluster.
- No TLS or authentication, and no leader transfer.

## Numbers

Every figure below came from a command in this repository, printed next to it. Hardware:
Apple M-series laptop, APFS, JDK 21. They are here to be reproduced, not admired.

| | |
| --- | --- |
| Tests | 190 |
| Line coverage (hand-written code) | 86.4% |
| Branch coverage | 82.1% |
| Simulation throughput | 83,684 ticks/s |
| Simulation soak | 200 seeds, 240,000 invariant checks, 2.9s, zero violations |
| Log append, no fsync | 256,660 entries/s (62.7 MiB/s) |
| Log append, fsync per batch of 64 | 21,241 entries/s (5.2 MiB/s) |
| Log append, fsync per entry | 334 entries/s |
| Hand-written Java | 7,099 lines main, 3,931 lines test |
| Runtime dependencies | Protobuf, gRPC, RocksDB, SLF4J |

That last row of the log benchmark is the one worth sitting with. Durable writes cost
about 3ms each on this hardware, so batching a whole `Ready` into one fsync is worth
roughly 60x. It is the reason the core hands the driver a batch instead of a stream of
instructions.

```
mvn verify -Dcoverage                                    # tests and coverage
mvn test -Dkeel.sim.seeds=200 -Dtest=SoakTest \
    -Dsurefire.failIfNoSpecifiedTests=false              # simulation soak
mvn test -Dkeel.bench=true -Dtest=SegmentedLogThroughputTest \
    -Dsurefire.failIfNoSpecifiedTests=false              # log throughput
```

## Quick start

Requires JDK 21 or newer and Maven 3.9 or newer. Nothing else: `protoc` and the gRPC
code generator are fetched by the build.

```
mvn package -DskipTests
./scripts/local-cluster.sh
```

That starts three nodes on ports 9001 to 9003 with data under `run/`. In another shell:

```
CLUSTER=1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003
JAR=keel-node/target/keel.jar

java -cp $JAR io.keel.node.Keelctl --cluster=$CLUSTER status
java -cp $JAR io.keel.node.Keelctl --cluster=$CLUSTER put greeting hello
java -cp $JAR io.keel.node.Keelctl --cluster=$CLUSTER get greeting
java -cp $JAR io.keel.node.Keelctl --cluster=$CLUSTER cas greeting hello goodbye
```

Kill the leader with `kill -9` and read the key again. The value is still there and a
new leader takes over within an election timeout.

## How it is built

```
keel-proto      wire and on-disk schemas; .proto is the source of truth
keel-raft       consensus core: no threads, no clock, no I/O, no locks
keel-storage    segmented write-ahead log
keel-kv         key-value state machine and client sessions
keel-testkit    deterministic simulator, invariant checks, linearizability checker
keel-node       gRPC transport, the thread that owns the core, client, CLI
```

The decision everything else follows from: **the consensus core is a pure state
machine**. Time arrives as `tick()`, messages as `step()`, and everything it wants done
leaves as a `Ready` batch. It owns no threads, reads no clock, and cannot reach a disk
or a socket even by accident, because it is handed a read-only view of storage.

Two things fall out of that. A cluster's behaviour becomes a pure function of one seed,
so a bug found at seed 8123 is still there at seed 8123 tomorrow. And the core needs no
synchronization, because exactly one thread ever calls into it.

`docs/architecture.md` covers the layering and the path a write and a read take.
`docs/design/` holds one document per decision a reader might otherwise take for a
mistake. `docs/testing.md` describes what the suite proves and what it does not.

## Testing

Four layers, each covering what the cheaper one below it cannot:

| Layer | What it covers |
| --- | --- |
| Unit | one method or one state transition |
| Deterministic network | multi-node message exchange with no clocks or threads |
| Simulation | seeded fault injection with invariant checks after every step |
| Integration | three nodes on real sockets and real files |

The simulator asserts Election Safety, Log Matching, State Machine Safety, and term and
commit monotonicity **after every step**, not at the end of a run. A cluster that elects
two leaders in one term and then recovers looks perfectly healthy by the time a run
finishes.

The linearizability checker answers a different question. Invariants confirm replicas
agree with each other; a store can do that flawlessly and still hand a client a value no
sequential execution allows. `SimLinearizabilityTest` runs the simulator with the read
path deliberately broken in exactly the way a naive implementation breaks it and asserts
the checker rejects the resulting history. Without that test, a clean verdict on the
correct path would say nothing.

Three bugs worth mentioning, because they are why the strategy is shaped this way:

1. The pre-vote term check was inverted, so a *granted* pre-vote made the candidate step
   down and multi-node clusters could never elect anyone. Found by the first election
   test, before the branch had a commit.
2. Segments were replayed in base-index order, but a superseding append can create a
   lower-numbered segment later, so replay could resurrect overwritten entries. Found by
   a differential test against the in-memory store; no hand-written test came near it.
3. Membership was held in a set whose iteration order the JDK randomizes per JVM, so
   message ordering differed between JVM invocations. The in-process determinism check
   could not see it because it replays both runs in one JVM. The two-JDK CI matrix found
   it.

## Reading the code

If you have ten minutes and want the interesting parts:

- `keel-raft/src/main/java/io/keel/raft/RaftNode.java` — the state machine, with paper
  section references where an argument is being relied on
- `keel-raft/src/main/java/io/keel/raft/Ready.java` — the ordering contract that makes
  persist-before-send structural rather than a rule to remember
- `keel-testkit/src/main/java/io/keel/testkit/Invariants.java` — the safety properties as
  executable checks
- `keel-testkit/src/main/java/io/keel/testkit/linz/Linearizability.java` — the checker

## License

MIT
