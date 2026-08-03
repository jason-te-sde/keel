# 4. Scope: what is deliberately missing

A reader who cannot tell an omission from an oversight has to assume the worst. This is
the list, with reasoning. Two entries here were gaps in v0.1.0 and are now implemented;
their reasoning is kept because the decisions still constrain what comes next.

## Snapshots and log compaction

**Implemented.** The state machine can be serialized, the log prefix discarded, and a follower
caught up from a snapshot streamed over its own RPC.

The decision worth recording is the split: the core decides when a snapshot is needed and which
boundary it establishes, and never touches the payload. That keeps the core free of I/O, which is
what keeps the simulator deterministic, and it puts the three orderings that matter — snapshot
before compaction, verify before accept, install before append — in the driver where they can be
stated once and tested.

Rejected: letting the core carry payload bytes in its messages. It would have made the transport
trivial and the core untestable in the simulator, which is the wrong trade for this project.

## Membership changes

**Implemented**, single-node only.

With one change in flight the old and new majorities always overlap, so no joint configuration is
needed. Joint consensus handles arbitrary reconfiguration and roughly doubles the membership state
that every safety argument must account for; for a cluster that grows from three nodes to five
once, that is a bad trade. This remains a permanent decision rather than a stepping stone.

Learners — non-voting replicas that catch up before promotion — are absent. They make adding a
badly lagging node cheaper and change no safety argument, so they are a reasonable next addition.

## Leader leases

**Deliberately absent.** They would let a leader skip the ReadIndex quorum round for a
bounded window. The cost is a clock assumption, and the core currently reads no clock at
all, which is exactly what makes a run reproducible from a seed. See
`0003-read-path.md`.

## Leader transfer

**Absent.** Useful for planned maintenance, no safety content. It is a small addition once
membership changes exist, since both need the same "step down cleanly" path.

## Joint consensus

**Never planned.** See membership changes above.

## TLS and authentication

**Absent.** gRPC does both; wiring it in is configuration rather than design, and adding it
now would mean certificate handling in every test.

## Multi-raft and sharding

**Out of scope entirely.** One Raft group, one key space. Sharding is a different project
that would use this one as a component.

## Observability beyond a status call

**Thin.** There is `keelctl status` and structured logging. No metrics endpoint, no
tracing. The status call carries what a person debugging an election needs: role, term,
leader, commit index, applied index.

## A single-module build

**Rejected.** Six modules is more build surface than one, and the dependency direction is
the point: `keel-raft` cannot accidentally import a socket, because gRPC is not on its
compile path. The enforcer runs `reactorModuleConvergence` and a clean-clone CI job
because a multi-module build's characteristic failure is a module that is declared and
never committed. I have shipped that failure before; the guard is cheap.
