# Testing

## The four layers

New code lands in the cheapest layer that can catch its bugs.

| Layer | Where | Covers |
| --- | --- | --- |
| Unit | each module's `src/test/java` | one method, one state transition |
| Deterministic network | `keel-raft`, `TestCluster` | multi-node exchange, no clocks, no threads |
| Simulation | `keel-testkit` | seeded fault injection, invariants after every step |
| Integration | `keel-node` | three nodes, real sockets, real files |

190 tests, 86.4% line and 82.1% branch coverage on hand-written code. Coverage is a
smoke alarm, not a goal: the untested remainder is mostly `toString` and unreachable
defensive branches, and a number near 100% usually means someone wrote tests for
`toString`.

## Rules that are actually enforced

- **No test sleeps waiting for progress.** The deterministic layers have no clock at all;
  the integration tests poll a condition with a deadline. A loaded CI runner makes a test
  slower, never flakier.
- **Anything randomized takes a seed and prints it on failure.** A red test that cannot be
  replayed is close to worthless.
- **Assertions carry a message naming the state**, not just the expected value.
- **Chaos tests assert the run was hostile**: messages dropped, proposals made, invariants
  checked once per tick. A safety suite that passes because the cluster sat idle is the
  classic false negative, and it is the failure mode this project is most exposed to.

## What the simulator asserts

After **every step**, not at the end of a run. A cluster that elects two leaders in one
term and then recovers looks perfectly healthy by the time a run finishes.

| Property | Paper | Meaning |
| --- | --- | --- |
| Election Safety | 5.2 | at most one leader per term |
| Log Matching | 5.3 | equal `(index, term)` implies identical prefixes |
| State Machine Safety | 5.4.3 | no two replicas apply different commands at one index, and no replica rewrites what it applied |
| Term monotonicity | — | a node's term never decreases, including across a crash |
| Commit monotonicity | — | a node's commit index never decreases |

The last two are not in the paper's list. A violation means a persisted value was lost,
and they fail much closer to the cause than the properties above would.

The checker is stateful, because several of these are claims about history rather than
about an instant. It remembers what each node applied so it can notice a committed prefix
being rewritten after a restart.

`InvariantsTest` tests the checker itself against fabricated states, including two cases
it must *not* flag: two leaders in different terms, and a restarted node replaying its log
from the start. A checker that fired on those would be useless under exactly the
conditions it exists for.

## What the linearizability checker adds

Invariants confirm the replicas agree with each other. That is not the property a user
cares about. A store can keep every replica byte-identical and still hand a client a
value that no sequential execution allows, which is exactly what a read served by a
deposed leader looks like.

The checker takes a recorded history — each operation with an invocation time, a
completion time or none, an input, and an output — and searches for an order the
sequential model accepts. Two things make it affordable:

- **Per-key decomposition.** A key-value store is a composition of independent registers
  and linearizability is compositional, so one intractable search becomes many small ones.
  `decompositionAgreesWithTheFullSearch` checks that claim rather than assuming it.
- **Memoization on (state, remaining).** Without it the search revisits the same position
  along every permutation that reaches it.

Operations whose outcome the client never learned are tried both ways: applied, and never
applied. There is also a test that this generosity does not extend to accepting any
response at all.

## Reproducing a failure

Simulation failures print the seed and tick:

```
Election Safety violated at tick 412 (seed 8123): term 7 has two leaders: nodes 2 and 5
```

That seed reproduces the run exactly, including message latencies and every fault
decision. Add it to the seed list in the relevant test so it is replayed on every run
afterwards.

```
# one seed, verbose
mvn test -Dtest=SimTest -Dsurefire.failIfNoSpecifiedTests=false

# wide sweep
mvn install -DskipTests
mvn test -pl keel-testkit -Dkeel.sim.seeds=2000 -Dtest=SoakTest \
    -Dsurefire.failIfNoSpecifiedTests=false
```

The nightly workflow runs 2,000 seeds. Locally, 200 seeds is 240,000 invariant checks in
about three seconds.

## What this suite does not cover

Stated because a testing document that only lists strengths is marketing.

- **No snapshot or compaction testing**, because neither exists yet. When they land the
  simulator needs a compaction fault, and the interesting case is a snapshot arriving at a
  follower that is mid-append.
- **No membership change testing**, same reason.
- **The simulator does not serialize anything.** It hands core messages straight from one
  node to another, so a codec bug is only caught by the integration tests. That is a
  deliberate split: serialization bugs are shallow and consensus bugs are not.
- **The simulated disk models sync boundaries, not the disk.** Unsynced writes vanish on a
  crash, which is the property the core depends on, but there is no torn-sector or
  reordered-write injection. The real log's recovery is tested separately by corrupting
  bytes in files by hand.
- **Crash injection is process-level, not thread-level.** A node dies between ticks. A real
  process can die between two instructions inside a `Ready` drain, and the simulator cannot
  express that yet.
- **No clock skew.** The core reads no clock, so there is nothing for skew to affect. That
  changes the moment leader leases are added, and it is the main reason they are not.
- **Performance is measured, not tested.** The throughput numbers come from a benchmark
  that is disabled by default; nothing fails a build if it regresses.
