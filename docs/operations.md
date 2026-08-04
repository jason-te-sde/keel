# Operations

What someone running this actually needs to know. Written for the case where it is three in the
morning and something is wrong.

## Before anything else

**Pre-1.0, the on-disk format is not stable.** An upgrade across a minor version may require a
fresh data directory. There is no migration tooling and there will not be one until 1.0. If that
is unacceptable, do not run this yet.

**A node refuses to listen on a non-loopback address without TLS and a client token.** That is
deliberate. `--insecure` overrides it and means what it says: unencrypted, unauthenticated, and
anyone who can reach the port can eject nodes from the cluster.

## Starting a cluster

Three nodes tolerate one failure. Five tolerate two. Even sizes buy nothing: four nodes need a
quorum of three, so they tolerate one failure and cost an extra machine.

```bash
keeld --config=/etc/keel/keel.properties --id=1
```

```properties
# /etc/keel/keel.properties — everything a deployment shares
cluster            = 1=10.0.0.1:9001,2=10.0.0.2:9001,3=10.0.0.3:9001
data.dir           = /var/lib/keel
metrics.port       = 9101
tick.ms            = 50
snapshot.threshold = 8192

tls.cert     = /etc/keel/tls/node.pem
tls.key      = /etc/keel/tls/node.key
tls.ca       = /etc/keel/tls/ca.pem
client.token = ...
admin.token  = ...
```

The file holds what every node shares; `--id` and anything else per-node arrives as a flag. Flags
override the file, so a container image can ship the file unchanged.

Adding a node to a running cluster is two steps, in this order: start the process with the
existing cluster in its address book, then

```bash
keelctl --cluster=... member add 4=10.0.0.4:9001
```

The address travels inside the configuration entry, so the other nodes learn it from the log. You
do not have to update their config files.

## Tuning the tick

The core counts logical ticks and never reads a clock. `tick.ms` is the only place real time
enters, and everything else follows from it:

| | Ticks | At `tick.ms = 50` |
| --- | --- | --- |
| Heartbeat interval | 1 | 50ms |
| Election timeout | 10 to 20, randomized | 500ms to 1s |

The rule: **the election timeout must comfortably exceed the round trip between your slowest pair
of nodes**, including a disk sync on the receiving side. Inside one datacentre, 50ms is
conservative and fine. Across regions, raise it — a timeout below the round trip produces a
cluster that elects a new leader every few seconds and never commits anything, and the symptom
(`keel_raft_term` climbing steadily) looks nothing like the cause.

Lowering it makes failover faster and false failovers more likely. There is no setting that gives
both.

## Sizing the disk

Two things live in the data directory:

- **`wal/`** — segment files. Bounded by `snapshot.threshold` times the average entry size, plus
  one segment, plus dead records left by leader changes. At the default 8192 entries and 1 KiB
  values, that is roughly 10–30 MiB.
- **`snapshots/`** — three snapshots are kept, each the size of the whole key space.

So budget **three to four times your key space, plus the log**. The log is not the thing that
grows; the snapshots are.

Lowering `snapshot.threshold` reclaims log space sooner and snapshots more often, which costs CPU
and disk writes. Raising it does the reverse and lengthens restart replay. Watch
`keel_raft_log_entries_since_snapshot`: if it climbs past the threshold and stays there, something
is stopping compaction and the disk will fill.

Writes are `fsync`ed before they are acknowledged. On this project's benchmark that is about 3ms
per durable batch, so **disk latency is the write latency**. Put the data directory on an SSD, and
never on a network filesystem.

## What to watch

| Alert on | Why |
| --- | --- |
| `keel_raft_leader_id == 0` for more than a few seconds | no leader, so nothing commits and no linearizable read is answered |
| `keel_raft_term` increasing steadily | repeated elections; usually an election timeout below the real round trip, or a flapping link |
| `keel_raft_apply_lag_entries` growing | the state machine cannot keep up with the log |
| `keel_raft_log_entries_since_snapshot` above `snapshot.threshold` | compaction has stopped; the disk will fill |
| `rate(keel_peer_send_failures_total[5m]) > 0` | a link between nodes is failing |
| `rate(keel_snapshot_send_failures_total[5m]) > 0` | a lagging follower cannot be repaired |
| `keel_writes_overwritten_total` increasing | writes are reaching a leader that then loses its term |

Wire load balancers to **`/readyz`**, not `/healthz`. A follower replaying a long log is perfectly
healthy and will serve stale reads if asked; `/readyz` returns 503 when no leader is known or when
this node is more than 1000 entries behind. Use `/healthz` for liveness, where a 503 should get the
process restarted.

## Backup and restore

A backup is the data directory. Both the log and the snapshots are needed: the snapshot alone is
missing everything after its boundary, and the log alone has had its prefix compacted away.

```bash
# Back up one node. Stopping it first is the only way to get a consistent copy, because a running
# node is writing to the log continuously.
systemctl stop keel
tar czf keel-backup-$(date +%F).tar.gz -C /var/lib/keel .
systemctl start keel
```

Take it from a **follower**, so the cluster keeps serving while it is down. Losing one node of
three costs no availability.

Restoring, in increasing order of severity:

**One node lost.** Do not restore anything. Wipe its data directory and start it with the same
node id; the leader will catch it up from entries or from a snapshot. This is the normal case and
a backup is not involved.

**All nodes lost.** Restore the same backup to every node and start them. They will elect a leader
and agree, because they all hold the same log. Restoring *different* backups to different nodes is
the one thing not to do: the newest one wins the election and the divergence is silently discarded.

**A restore is not a rollback.** Restoring an older backup to a running cluster does not roll the
cluster back; the surviving nodes will overwrite the restored node with the current log.

## Upgrades

Read the changelog first, for the pre-1.0 warning above.

If the format has not changed, a rolling restart works: one node at a time, waiting for
`/readyz` to pass before moving to the next.

Each restart causes an election, because there is no leader transfer yet
([#31](https://github.com/jason-te-sde/keel/issues/31) if it exists by the time you read this).
Expect a sub-second gap in writes per node restarted. Restart followers first and the leader last,
so you pay for exactly one election instead of one per node.

If the format has changed: stop everything, take a backup, wipe every data directory, start the new
version, and reload your data. There is no in-place path.

## Failure modes and what they look like

| Symptom | Likely cause |
| --- | --- |
| No leader, terms climbing | election timeout below the real round trip; raise `tick.ms` |
| Writes time out, reads work with `--stale` | leader cannot reach a quorum; check `keel_peer_send_failures_total` |
| A node never becomes ready | it is catching up; watch `keel_raft_apply_lag_entries` fall. If it does not, look for snapshot send failures |
| Node exits on start with "the log was compacted to index N but no snapshot covering it exists" | the snapshots directory was deleted or partially restored. Wipe the data directory and let the cluster refill the node |
| `UNAUTHENTICATED` from every call | wrong or missing `--token`; a membership change needs `--admin-token` |
| A new node runs but never joins | its certificate is not signed by the cluster CA, or `member add` was never run |

## What this does not do yet

- No leader transfer, so a rolling restart costs one election per restarted leader
- No TLS certificate rotation without a restart
- No rate limiting or per-client quotas
- No encryption at rest; protect the data directory with filesystem permissions
- One Raft group, so capacity is bounded by a single node's disk and memory. No sharding.
