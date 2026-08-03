# 3. Reads go through ReadIndex

## Context

Reading a leader's local state is not linearizable. A leader that has been partitioned
away still believes it leads for up to one election timeout, and will return a value a
newer leader has already replaced.

Reading a follower's local state is worse: it can be arbitrarily behind.

## Decision

ReadIndex, paper section 6.4, for both leaders and followers. Two conditions:

1. The leader must have committed an entry in its **current term**, so it knows its own
   committed prefix. The no-op appended on election is what satisfies this. A read
   arriving earlier is held, not answered.
2. The leader must confirm it is **still** the leader by completing a heartbeat round with
   a quorum, taken *after* recording `readIndex = commitIndex`.

Heartbeats carry a round token, and a response only counts toward the round it belongs
to. The answer is an index, not a value: the caller waits for the state machine to reach
it and then reads local state.

A follower forwards the request, gets an index back, waits for its own state machine, and
serves the read itself.

## Consequences

Reads never append to the log, and there is a test asserting the log length does not move
across twenty-five reads.

Followers can serve linearizable reads, which keeps reads off the leader while remaining
correct, because the index came from a leader that confirmed itself.

A read costs one heartbeat round trip. That is what linearizability costs without a clock
assumption.

## Why both conditions, spelled out

I have written the version that gets this wrong. In an earlier Raft project, the
follower-read path fetched the leader's commit index over RPC and returned it directly
with no leadership confirmation, and the leader-side helper that *did* try to confirm
leadership counted an `appendEntries` that had already stepped the leader down as a
success. Both paths serve stale reads under a partition. Neither had a test that could
have noticed, because noticing requires partitioning the leader and asserting that
*nothing* was served.

So the negative tests here were written first: `partitionedLeaderAnswersNothing`,
`readBeforeTheTermIsEstablishedWaits`, `staleRoundDoesNotConfirmANewerRead`, and
`readsAreDroppedOnStepDown`.

## Costs

A read is a round trip even when the data is local. Reads in flight are dropped on a
leadership change rather than answered late, so callers need a timeout.

## Alternatives rejected

**Leader leases.** A leader that knows no other leader can exist for the next `t`
milliseconds can skip the quorum round. It trades a clock assumption for latency, and
this core reads no clock at all, which is what makes the simulator deterministic. Adding
leases means adding clock skew as something the tests have to model. Worth doing, not
worth doing first.

**Read from the leader with no confirmation.** The bug above.

**Serve reads only from the leader, no follower reads.** Simpler and correct, but throws
away the read scaling that ReadIndex makes safe. `--stale` already exists for callers who
want speed and know what they are giving up.
