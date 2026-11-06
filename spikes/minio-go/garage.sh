#!/usr/bin/env bash
# Starts a single-node Garage container for the spike and prints
#   GARAGE_ENDPOINT=127.0.0.1:PORT GARAGE_ACCESS=... GARAGE_SECRET=...
# on the last line. Stop it with: docker rm -f "$NAME".
# Usage: garage.sh NAME WORKDIR
set -euo pipefail
NAME=${1:-lendisk-spike-garage}
WORK=${2:-$(mktemp -d)}
IMAGE=dxflrs/garage:v2.5.0

mkdir -p "$WORK"
cat >"$WORK/garage.toml" <<EOF
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
[admin]
api_bind_addr = "[::]:3903"
admin_token = "spike-admin-token"
EOF

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p 127.0.0.1::3900 \
  -v "$WORK/garage.toml:/etc/garage.toml:ro" "$IMAGE" >/dev/null

g() { docker exec "$NAME" /garage "$@" 2>>"$WORK/garage-cli.log"; }
for _ in $(seq 50); do
  g status >/dev/null 2>&1 && break
  sleep 0.2
done
NODE=$(g node id -q | cut -d@ -f1)
g layout assign -z dc1 -c 1G "$NODE" >/dev/null
g layout apply --version 1 >/dev/null
KEY=$(g key create spike)
ACCESS=$(echo "$KEY" | awk '/Key ID:/ {print $3}')
SECRET=$(echo "$KEY" | awk '/Secret key:/ {print $3}')
g bucket create spike >/dev/null
g bucket allow --read --write --owner spike --key spike >/dev/null
PORT=$(docker port "$NAME" 3900/tcp | head -1 | sed 's/.*://')
echo "GARAGE_ENDPOINT=127.0.0.1:$PORT GARAGE_ACCESS=$ACCESS GARAGE_SECRET=$SECRET"
