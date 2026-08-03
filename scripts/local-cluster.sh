#!/usr/bin/env bash
# Brings up a three-node cluster on localhost, for trying things by hand.
#
# Data and logs go under ./run, which is gitignored. Ctrl-C stops every node.
set -euo pipefail

cd "$(dirname "$0")/.."
JAR="keel-node/target/keel.jar"

if [ ! -f "$JAR" ]; then
  echo "building $JAR"
  mvn -q -B -ntp package -DskipTests
fi

CLUSTER="1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
mkdir -p run/logs

pids=()
stop() {
  echo
  echo "stopping cluster"
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap stop EXIT INT TERM

for id in 1 2 3; do
  java -cp "$JAR" io.keel.node.Keeld \
    --id="$id" --cluster="$CLUSTER" --data-dir="run/node-$id" \
    > "run/logs/node-$id.log" 2>&1 &
  pids+=($!)
  echo "started node $id (pid ${pids[-1]}), log run/logs/node-$id.log"
done

cat <<USAGE

cluster is starting. try:

  java -cp $JAR io.keel.node.Keelctl --cluster=$CLUSTER status
  java -cp $JAR io.keel.node.Keelctl --cluster=$CLUSTER put greeting hello
  java -cp $JAR io.keel.node.Keelctl --cluster=$CLUSTER get greeting

press Ctrl-C to stop.
USAGE

wait
