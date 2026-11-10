#!/usr/bin/env bash
# Runs the container image the way examples/default-nas/compose.yaml
# does (D2, OPS-7): an unprivileged user, every capability dropped,
# no-new-privileges, a read-only root with a tmpfs /tmp, host
# networking, the config and data directories as the only volumes.
# It initialises a node from a fixed phrase, serves, and checks that the
# daemon answers `status`, that the process holds no capability and is
# not root, and that `bucket add` works against the running daemon.
# The container is removed on exit; the image is left alone.
# Usage: image-smoke.sh IMAGE [NAME]
set -euo pipefail
IMAGE=$1
NAME=${2:-lendisk-image-smoke}
WORK=$(mktemp -d)
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT
mkdir -m 0700 "$WORK/config" "$WORK/data"

# A port nothing else on the host uses, for the loopback endpoint.
PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')

run=(--user "$(id -u):$(id -g)" --cap-drop ALL --security-opt no-new-privileges:true
	--read-only --tmpfs /tmp --network host
	-e LENDISK_ENDPOINT_LOOPBACK="127.0.0.1:$PORT"
	-v "$WORK/config:/etc/lendisk" -v "$WORK/data:/var/lib/lendisk")

# The BIP-39 test phrase: init --restore reads it from stdin, so no
# question is asked.
phrase="$(printf 'abandon %.0s' $(seq 23))art"
echo "$phrase" | docker run --rm -i "${run[@]}" "$IMAGE" init --restore --repository-size 1G --copies 1 >/dev/null

docker run -d --name "$NAME" "${run[@]}" "$IMAGE" >/dev/null
for i in $(seq 60); do
	if docker exec "$NAME" lendisk status --json >"$WORK/status.json" 2>"$WORK/status.err"; then
		break
	fi
	if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != true ]; then
		echo "serve exited:"; docker logs "$NAME"; exit 1
	fi
	[ "$i" = 60 ] && { echo "status never answered:"; cat "$WORK/status.err"; docker logs "$NAME"; exit 1; }
	sleep 1
done
docker exec "$NAME" lendisk status --quiet

# The daemon's process, seen from the host: no capability, not root,
# no way to gain privileges.
pid=$(docker inspect -f '{{.State.Pid}}' "$NAME")
grep -E '^(Uid|CapPrm|CapEff|CapBnd|CapAmb|NoNewPrivs):' "/proc/$pid/status"
awk '/^Uid:/ { if ($2 == 0 || $3 == 0) bad = 1 }
	/^Cap(Prm|Eff|Bnd|Amb):/ { if ($2 != "0000000000000000") bad = 1 }
	/^NoNewPrivs:/ { if ($2 != 1) bad = 1 }
	END { exit bad }' "/proc/$pid/status" || { echo "the daemon is root or holds a capability"; exit 1; }
[ "$(docker inspect -f '{{.HostConfig.ReadonlyRootfs}}' "$NAME")" = true ]

docker exec "$NAME" lendisk bucket add smoke --profile restic | grep -q "RESTIC_REPOSITORY=s3:http://127.0.0.1:$PORT/smoke"
echo "image smoke test passed: $IMAGE"
