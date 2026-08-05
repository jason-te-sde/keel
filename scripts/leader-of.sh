#!/bin/sh
# Prints the node id of the cluster's current leader, or exits non-zero if there is not exactly one.
#
# Usage: leader-of.sh 1 2 3
#
# Reads keel_raft_role from each node's /metrics rather than parsing `keelctl status`. The CLI table
# is laid out for people to read, so its column positions are not a contract and a change to the
# header alignment would silently break anything that greps it. The metric name and its labels are
# part of the documented interface.
#
# The metrics port is derived as 9100 + id, which is the mapping docker-compose.yml publishes. A
# cluster started some other way should pass its own ports.
set -eu

found=''
for id in "$@"; do
    port=$((9100 + id))
    body=$(curl -sf --max-time 2 "http://127.0.0.1:${port}/metrics" 2>/dev/null) || continue
    role=$(printf '%s\n' "$body" | sed -n 's/^keel_raft_role{role="leader"} //p')
    [ "${role:-0}" = '1' ] || continue

    if [ -n "$found" ]; then
        # Two nodes claiming leadership across separate scrapes means the scrapes straddled a term
        # change, not that the cluster has split brain. Refusing to pick one makes the caller retry,
        # which is what it wanted anyway.
        echo "nodes $found and $id both report being leader; scrape straddled a term change" >&2
        exit 1
    fi
    found=$id
done

[ -n "$found" ] || {
    echo "no node reports being leader" >&2
    exit 1
}
echo "$found"
