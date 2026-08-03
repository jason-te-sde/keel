# 4. Scope: what is deliberately missing

A reader who cannot tell an omission from an oversight has to assume the worst. This is
the list, with reasoning.

## Snapshots and log compaction (issue #3)

**Missing.** The log grows without bound, restart replays it from the beginning, and a
follower that falls behind the leader's oldest retained entry cannot be caught up.

Nothing degrades quietly: `sendAppend` throws if a peer needs a compacted entry, and
because nothing compacts, that is unreachable today. The failure mode is disk and restart
time, both visible.

This is the largest gap and the next thing to build. When it lands it also needs a
compaction fault in the simulator, and the interesting case is a snapshot arriving at a
follower that is mid-append.

## Membership changes (issue #4)

**Missing.** `RaftConfig` fixes the voters at startup, so replacing a dead machine means
restarting the cluster.

The plan is single-node changes only, and that is a decision rather than a shortcut. With
one change at a time, the old and new majorities always overlap, so no joint configuration
is needed. Joint consensus handles arbitrary reconfiguration and roughly doubles the
membership state that every safety argument has to account for; for a store that grows
from three nodes to five once, that is a bad trade.

The rules that will need enforcing, since they are where implementations lose safety:
a configuration entry takes effect when it is *applied*, not when it is appended; a leader
must not append a new one while an earlier one is uncommitted; a leader removed from the
configuration steps down once that entry applies; a node not in the configuration does not
campaign.

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
