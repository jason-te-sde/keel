# 2. The log is append-only, and the hard state lives in it

## Context

Raft needs two things durable: log entries, and the `(term, vote)` pair. A vote lost in
a crash lets a node vote twice in one term, which is how two leaders happen.

Followers also have to overwrite a conflicting suffix when a new leader's log diverges
from theirs.

## Decision

One log holds both entries and hard state, as framed records:
`[length int32][crc32c int32][payload]`.

Nothing is ever rewritten in place. Overwriting a conflicting suffix appends the
replacement entries and lets them supersede the earlier records during replay.

Segment files are named `<sequence>-<baseIndex>.log`, and recovery orders by sequence.

## Consequences

Entries and hard state become durable in one fsync. Sharing the log is not just an
optimisation: if a recovered commit index could refer to entries that are not on disk,
recovery cannot distinguish that from having lost committed data.

A crash can only ever cost a suffix of a file, which is the only failure recovery has to
understand.

Most importantly, it makes a specific bug unrepresentable. If truncating a suffix could
remove an already-written vote record, a crash immediately after would resurrect an older
term, and the node could vote a second time in a term it had already voted in. There is a
test named for exactly that: `supersedingAppendKeepsHardState`.

## Costs

Space. A log that is overwritten repeatedly holds dead records until a snapshot lets whole
segments go — and snapshots do not exist yet, so today nothing reclaims them. Conflicting
suffixes only happen on leader changes, so this is small in practice, but it is a real
open edge.

The base index in a segment's name is only a hint after this decision, since a superseding
append can put lower indexes into a segment created later. Anything needing an
authoritative answer reads the in-memory location index.

## What recovery forgives, and what it does not

A partial record at the tail of the newest segment is expected: a process can die between
the write and the sync, and that suffix was never acknowledged to anyone. It is truncated
away and the discarded byte count is reported.

Anything else fails the open. A record that fails its checksum with valid records after it
means the bytes were corrupted after they were written. Skipping it would silently discard
entries a quorum may have committed, and the cluster would carry on as though nothing had
happened.

## Alternatives rejected

**Hard state in its own file.** Needs its own atomicity, and because the commit index
changes on almost every write it would mean two fsyncs per batch.

**Truncate the file, then append.** One fsync, and a window where a crash leaves the old
entries gone and the new ones absent.

**Re-append the hard state after truncating.** Correct only if the truncation and the
re-append are atomic, which they cannot be. A crash between them loses the vote, which is
the bug this whole decision exists to avoid.

## Evidence

A differential test against the in-memory store found that segments were being replayed
in base-index order, which a superseding append can make differ from write order. Replay
could resurrect overwritten entries. No hand-written test came near it.
