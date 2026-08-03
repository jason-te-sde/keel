# Architecture

## Layering

```
                       keelctl (CLI)          KeelClient
                            |                     |
                            +----- gRPC ----------+
                                     |
   +---------------------------------|------------------------------+
   |  keel-node                      v                             |
   |                          KvServiceImpl                         |
   |                                 |                             |
   |          +----------------------+---------------------+        |
   |          |                                            |       |
   |    raft thread  <-- RaftTransport <-- gRPC       apply thread  |
   |          |                                            |       |
   +----------|--------------------------------------------|-------+
              v                                            v
        keel-raft (RaftNode)                        keel-kv (StateMachine)
              |                                            |
              v                                     heap or RocksDB
        keel-storage (SegmentedLog)
```

Dependencies point inward. `keel-raft` depends on nothing but the schema module and a
logging facade; it does not know that `keel-storage` or gRPC exist. The core defines the
`RaftStorage` port and `keel-storage` implements it, so the disk is a detail the core
cannot reach.

## The core is a pure state machine

`RaftNode` owns no threads, reads no clock, performs no I/O, and holds no locks. Its
entire surface is:

- `tick()` — one unit of logical time has passed
- `step(RaftMessage)` — a message arrived
- `propose(byte[])`, `requestRead(long)`, `campaign()` — things a driver asks for
- `ready()` / `advance(Ready)` — collect the work, then confirm it was done

Randomness is an injected `Random`. Membership iterates in sorted order. Nothing in the
core consults the environment.

Two consequences, and they are the reason for the design:

1. **A cluster is a pure function of one seed.** The simulator can run five nodes for
   twelve hundred ticks with partitions, crashes, drops, and duplicates, and get exactly
   the same run every time. A failure reported as "seed 8123" reproduces.
2. **No synchronization is needed.** Exactly one thread ever calls in, which the node
   layer enforces structurally rather than by convention.

## Where the durability boundary is

`Ready` is a batch, and the order it documents is not advisory:

1. write the hard state, if it changed
2. write the entries
3. **sync**
4. send the messages
5. apply the committed entries
6. `advance()`

Step 3 before step 4 is the whole game. An acknowledgement is a promise that an entry
will survive a crash, and a leader counts acknowledgements toward a quorum. Sending
first turns that promise into a guess, and a majority restarting at the wrong moment
then loses an entry that was reported committed.

Applying after sending is a latency choice rather than a safety one: followers learn the
new commit index a round trip sooner.

The core cannot get this wrong on the driver's behalf, so the API is shaped to make it
hard to get wrong: one batch, one documented order, and a read-only storage view inside
the core.

## Threads in a running node

| Thread | Owns | Never does |
| --- | --- | --- |
| `keel-raft-N` | `RaftNode`, the `LogStore` | Block on the network |
| `keel-apply-N` | applying to the state machine | Touch `RaftNode` |
| `keel-send-N` (4) | outbound gRPC calls | Touch `RaftNode` |
| `keel-tick-N` | posting ticks to the raft thread | Anything else |
| gRPC threads | decoding requests | Touch `RaftNode` directly |

Everything that needs the core posts a task to the raft thread. Disk writes happen on
that thread, synchronously, so a slow disk appears as latency rather than as a queue
that grows until the heap runs out.

The apply loop is separate because a state machine blocked on a disk must not stall
replication. That also means the state machine's applied index trails the core's commit
index, which is why a linearizable read waits on the *state machine's* progress and not
the core's.

The state machine is guarded by a lock, because reads arrive on gRPC threads while the
apply loop is writing. Routing reads through the apply thread would remove the lock at
the cost of latency and a queue; the lock is the trade, and the code says so.

## The path of a write

1. `KvServiceImpl.put` builds a `Command` and calls `KeelNode.submit`.
2. On the raft thread, `RaftNode.propose` appends the entry and returns its index. The
   write is registered as pending under that index.
3. `drainReady()` writes the entry, syncs, and sends `AppendEntries` to the followers.
4. Followers persist and acknowledge. The leader advances its commit index once a quorum
   has stored an entry *from its own term* (paper 5.4.2).
5. Committed entries go onto the apply queue. The apply thread applies each one and
   completes the pending write with the state machine's result.
6. If a different command turns up at that index, the write never committed and the
   pending future fails with `OverwrittenException`. The client sees a timeout and can
   safely retry, and with a session that retry is exactly-once.

## The path of a linearizable read

1. `KeelNode.read` registers a pending read and calls `RaftNode.requestRead` on the raft
   thread.
2. If this node is not the leader, the request is forwarded to the leader. If no leader
   is known, the read fails immediately: there is nobody to ask.
3. The leader records `readIndex = commitIndex` and starts a heartbeat round carrying a
   round token. It answers only once a quorum has responded to *that* round. This is the
   step that rules out a leader that has already been replaced, and skipping it is the
   usual stale-read bug.
4. If the leader has not yet committed an entry in its current term, the read is held
   rather than answered, because the leader does not yet know its own committed prefix.
   The no-op appended on election is what resolves this.
5. The answer is a `ReadState`, which carries an index and not a value. The read waits
   for the state machine to reach that index, then reads local state.

A follower serving its own read is still linearizable, because the index came from a
leader that confirmed itself. That is what keeps reads off the leader.

## Snapshots

The state machine is serialized, the log prefix discarded, and a follower that has
fallen past the leader's oldest retained entry caught up from the snapshot instead.

The core's part is narrow: it decides *when* a follower needs a snapshot and *which
boundary* it establishes. It never moves bytes, because it has no I/O. So
`RaftMessage.InstallSnapshot` carries metadata only, and the transport streams the
payload on its own RPC whose **response means installed, not received**. A leader must
not learn a follower holds a snapshot before the follower holds it.

Three orderings carry the safety here, and each can lose committed data if reversed:

1. The state machine snapshot is written and fsynced **before** the log is compacted.
   Compaction is the point of no return.
2. A received snapshot is durable and checksum-verified **before** the core is told.
3. A snapshot in a `Ready` batch is installed **before** that batch's entries are
   appended, because those entries sit above its boundary.

Startup refuses to run if the log claims a boundary that no snapshot covers. The
entries below it are gone and so is their effect; continuing would hide data loss.

## Membership changes

One voter at a time. With a single change in flight the old and new majorities always
overlap, so no joint configuration is needed.

Four rules:

- A change takes effect when the entry is **applied**, not when it is appended.
  Applying early would mean counting a vote from a node that is not a member yet.
- A leader will not append a second change while the first is unapplied. Two in flight
  can produce two disjoint majorities, and therefore two leaders in one term.
- A leader removed from the configuration steps down. It cannot lead a cluster it is
  not in, because it cannot count its own vote toward anything.
- Membership travels in snapshots, because a node restoring from one cannot recover the
  configuration from the log: the entries that carried it are what the snapshot
  replaced.

A joining node starts as a **non-voter**: it accepts entries, does not campaign, and is
not counted in any quorum until the entry adding it is applied. That is why the node
layer keeps the address book and the voter set separate — a joining node knows where
everyone is and is a member of nothing. Addresses travel inside the configuration
entries, so membership comes from the log and nowhere else.

## What is deliberately not here

- **No joint consensus**, ever. Single-node changes make it unnecessary; see
  `docs/design/0004-scope.md`.
- **No leader leases.** They would let a leader skip the ReadIndex quorum round for a
  bounded window, at the cost of a clock assumption the core does not currently make.
- **No learners**, no leader transfer, no TLS, no multi-raft.
