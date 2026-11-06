#!/usr/bin/env bash
# Two-process demo of the spike. Usage: ./demo.sh [local|real]
#   local  a DERP+STUN relay on 127.0.0.1 (default; no internet needed)
#   real   tailcat's default DERP map (tailcat.dev), region picked once at init
set -u
mode=${1:-local}
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$work"' EXIT
cd "$work"
(cd "$here" && go build -o "$work/tcspike" .) || exit 1
B=./tcspike

if [ "$mode" = local ]; then
	$B derp -region-out region.json 2>derp.log &
	for _ in $(seq 50); do [ -s region.json ] && break; sleep 0.1; done
	init_flags=(-region-file region.json)
else
	init_flags=()
fi

step() { printf '\n=== %s\n' "$*"; }

step "init server and client"
$B init -state srv "${init_flags[@]}"
# The client's region file is only for its own key; it reaches the
# server through the region embedded in the server's address.
if [ "$mode" = local ]; then $B init -state cli "${init_flags[@]}"; else
	mkdir -p cli && $B init -state cli -region-file srv/region.json; fi
CLI=$($B pubkey -state cli)

step "nil hook: the guard refuses to start"
$B serve -state srv -nil-hook; echo "exit status $?"

step "address across two restarts (pre-shared key disabled, saved region)"
start_server() { # $1 = extra flags; leaves pid in $spid, address in $addr
	: >srv.out
	$B serve -state srv $1 >srv.out 2>srv.log &
	spid=$!
	for _ in $(seq 300); do grep -q '^address ' srv.out && break; sleep 0.1; done
	addr=$(sed -n 's/^address //p' srv.out)
}
start_server ""; a1=$addr; kill $spid; wait $spid 2>/dev/null
start_server ""; a2=$addr; kill $spid; wait $spid 2>/dev/null
echo "run 1: $a1"
echo "run 2: $a2"
[ "$a1" = "$a2" ] && echo "identical" || echo "DIFFERENT"
step "decoded address"
$B decode "$a1"

step "deny-all hook: the client is refused, the server sees no byte"
start_server -deny-all
$B dial -state cli -addr "$addr" -timeout 8s -msg "should never arrive"; echo "dial exit status $?"
kill $spid; wait $spid 2>/dev/null
cat srv.log

step "file hook, client key not listed: refused"
start_server ""
$B dial -state cli -addr "$addr" -timeout 8s -msg "should never arrive"; echo "dial exit status $?"
kill $spid; wait $spid 2>/dev/null
cat srv.log

step "file hook, client key listed: accepted, server reports the peer key"
echo "$CLI" >srv/allowed
echo "client key $CLI"
start_server ""
$B dial -state cli -addr "$addr" -msg "hello over tailcat"; echo "dial exit status $?"
sleep 1
kill $spid; wait $spid 2>/dev/null
cat srv.log
