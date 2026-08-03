# 1. The consensus core is a pure state machine

## Context

Every interesting bug in a replicated system lives in a schedule: a leader crashing
between persisting an entry and acknowledging it, a partition healing halfway through
an election, a duplicated message arriving after the term it belonged to is over.

The usual way to test one is to start a cluster and wait. That explores one arbitrary
schedule per run, and when something does break it cannot be reproduced.

## Decision

`RaftNode` owns no threads, reads no clock, performs no I/O, and holds no locks. Logical
time arrives as `tick()`, messages as `step()`, and everything it wants done leaves as a
`Ready` batch. Randomness is an injected `Random`. Membership iterates in sorted order.

The driver owns the disk, the network, and the clock.

## Consequences

A cluster becomes a pure function of one seed. The simulator runs five nodes for twelve
hundred ticks under partitions, crashes, drops, and duplicates in about fifteen
milliseconds, identically every time, and a failure reported as "seed 8123" reproduces.

The core needs no synchronization, because exactly one thread ever calls in. That is not
a comment in the code, it is a property the node layer enforces by making everything
post a task to one executor.

It also forces the durability boundary into the open. The core cannot sync a disk, so
the ordering requirement has to be stated somewhere a driver will see it, which is what
`Ready` is.

## Costs

The driver is more work than a core that just does the I/O itself. `Ready`/`advance` is
a contract a caller can get wrong, and getting it wrong is silent until a crash at an
unlucky moment. Both the simulator and the node layer implement that contract, and both
had to be reviewed for it.

The core also carries an in-memory tail of entries the driver has not persisted yet,
which is extra index arithmetic that a core owning its own writes would not need.

## Alternatives rejected

**A core that owns its threads and disk.** Simpler to call and much harder to test. This
is the shape of the Raft implementation I worked on before, and its test suite could
only start clusters and hope; the two stale-read bugs in its read path had no test that
could have found them.

**A core with an injectable clock but its own threads.** Removes the clock as a source of
nondeterminism and leaves the thread scheduler, which is the larger one.

## Evidence it was the right call

Three bugs found by tests only this design makes possible: an inverted pre-vote term
check, a segment replay ordering bug found by differential testing, and per-JVM
nondeterminism in membership iteration found by running the same seeds on two JDKs.
