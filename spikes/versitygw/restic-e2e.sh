#!/usr/bin/env bash
# restic init, backup, check and restore against the spike, through the
# TCP->unix forward. Prints the process's sockets with ss first.
set -euo pipefail
cd "$(dirname "$0")"

RESTIC=${RESTIC:-restic}
PORT=${PORT:-19000}
work=$(mktemp -d)
trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; rm -rf "$work"' EXIT

go build -o "$work/spike" .
"$work/spike" -store "$work/store" -sock "$work/run/gw.sock" -forward "127.0.0.1:$PORT" \
	>"$work/spike.log" 2>&1 &
pid=$!
for _ in $(seq 50); do [ -S "$work/run/gw.sock" ] && break; sleep 0.1; done

echo "== sockets held by pid $pid (ss -lntupx)"
ss -lntupxp | grep "pid=$pid," || true
echo "   (the TCP listener is the spike's forward, not versitygw)"

export AWS_ACCESS_KEY_ID=borrower1
export AWS_SECRET_ACCESS_KEY=borrower1-secret-key-0123456789
export RESTIC_PASSWORD=spike
export RESTIC_REPOSITORY="s3:http://127.0.0.1:$PORT/laptop"

mkdir -p "$work/src"
head -c 20000000 /dev/urandom >"$work/src/big.bin"
seq 1 100000 >"$work/src/numbers.txt"

echo "== restic init";    "$RESTIC" init
echo "== restic backup";  "$RESTIC" backup "$work/src"
echo "== restic backup (second snapshot)"; echo more >>"$work/src/numbers.txt"; "$RESTIC" backup "$work/src"
echo "== restic snapshots"; "$RESTIC" snapshots
echo "== restic check --read-data"; "$RESTIC" check --read-data
echo "== restic restore"; "$RESTIC" restore latest --target "$work/restore"
cmp "$work/src/big.bin" "$work/restore$work/src/big.bin" && echo "restore matches"

echo "== P-5: the store opened directly with nothing in between"
"$RESTIC" -r "$work/store/laptop" snapshots

echo "== decorator log (account per call, first lines)"
grep decorator "$work/spike.log" | awk '{$1="";$2=""; print}' | sort | uniq -c | sort -rn | head -20
