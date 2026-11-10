#!/usr/bin/env bash
# OPS-8 with real binaries: an earlier release's lendisk makes a node,
# this tree's lendisk upgrades its state (backing it up first), and the
# earlier binary then still runs on the upgraded database: status, a
# new bucket, and serve. CI runs it against the previous release tag
# (the one-minor downgrade); TestRealHistoryUpgradesAndKeepsTheEarlierSchemaUsable
# checks the same rule on the schema at every migration.
# Usage: downgrade.sh PREVIOUS_REF   (a tag such as v0.1.0, or any commit)
set -euo pipefail
PREV=$1
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
pid=
cleanup() {
	[ -n "$pid" ] && kill "$pid" 2>/dev/null || true
	rm -rf "$WORK"
}
trap cleanup EXIT

mkdir "$WORK/src"
git -C "$ROOT" archive "$PREV" | tar -x -C "$WORK/src"
(cd "$WORK/src" && CGO_ENABLED=0 go build -o "$WORK/prev" ./cmd/lendisk)
(cd "$ROOT" && CGO_ENABLED=0 go build -o "$WORK/head" ./cmd/lendisk)
mkdir -m 0700 "$WORK/c" "$WORK/d"
cfg=(--config "$WORK/c/lendisk.toml" --data-dir "$WORK/d" --endpoint-loopback 127.0.0.1:0)

# serve BIN: starts the daemon, waits for its S3 endpoint, stops it.
serve() {
	"$1" serve "${cfg[@]}" >"$WORK/serve.log" 2>&1 &
	pid=$!
	for _ in $(seq 60); do
		if grep -q 'S3 endpoint listening' "$WORK/serve.log"; then
			kill -TERM "$pid"
			wait "$pid" || { echo "$1 serve exited badly:"; cat "$WORK/serve.log"; exit 1; }
			pid=
			return
		fi
		kill -0 "$pid" 2>/dev/null || { echo "$1 serve exited:"; cat "$WORK/serve.log"; exit 1; }
		sleep 1
	done
	echo "$1 serve did not start:"; cat "$WORK/serve.log"; exit 1
}

schema() { sed -n 's/.*applied state migration.*version=\([0-9]*\).*/\1/p' "$1" | tail -1; }

echo "== $PREV makes a node"
phrase="$(printf 'abandon %.0s' $(seq 23))art"
echo "$phrase" | "$WORK/prev" init --restore --repository-size 1G --copies 1 "${cfg[@]}" >/dev/null 2>"$WORK/init.log"
from=$(schema "$WORK/init.log")
"$WORK/prev" bucket add before --profile restic "${cfg[@]}" >/dev/null
serve "$WORK/prev"

echo "== this tree upgrades it"
"$WORK/head" status "${cfg[@]}" >/dev/null 2>"$WORK/upgrade.log"
to=$(schema "$WORK/upgrade.log")
echo "schema ${from:-?} -> ${to:-unchanged}"
if [ -n "$to" ] && ! ls "$WORK/d"/lendisk.db.schema-*.bak >/dev/null 2>&1; then
	echo "no backup of the state database before migrating"; exit 1
fi
"$WORK/head" bucket add after --profile kopia "${cfg[@]}" >/dev/null
serve "$WORK/head"

echo "== $PREV runs on the upgraded state"
"$WORK/prev" status "${cfg[@]}" >/dev/null
"$WORK/prev" bucket list "${cfg[@]}" | grep -q after
"$WORK/prev" bucket add again --profile restic "${cfg[@]}" >/dev/null
serve "$WORK/prev"
echo "downgrade to $PREV works"
