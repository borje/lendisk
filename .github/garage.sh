#!/usr/bin/env bash
# Starts a single-node Garage container as an overflow target's provider
# (DESIGN §3.11, tier 2) for TestScenarioOverflowAtAnExternalProvider,
# and prints the variables the test reads, one per line, for
# $GITHUB_ENV or `export`:
#   LENDISK_TEST_OVERFLOW_ENDPOINT=http://127.0.0.1:PORT
#   LENDISK_TEST_OVERFLOW_REGION=garage
#   LENDISK_TEST_OVERFLOW_BUCKET=offsite
#   LENDISK_TEST_OVERFLOW_ACCESS_KEY=…
#   LENDISK_TEST_OVERFLOW_SECRET_KEY=…
# Stop it with: docker rm -f "$NAME".
# Usage: garage.sh [NAME [WORKDIR]]
set -euo pipefail
NAME=${1:-lendisk-ci-garage}
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
admin_token = "ci-admin-token"
EOF

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p 127.0.0.1::3900 \
  -v "$WORK/garage.toml:/etc/garage.toml:ro" "$IMAGE" >/dev/null

g() { docker exec "$NAME" /garage "$@" 2>>"$WORK/garage-cli.log"; }
for _ in $(seq 100); do
  g status >/dev/null 2>&1 && break
  sleep 0.2
done
NODE=$(g node id -q | cut -d@ -f1)
g layout assign -z dc1 -c 1G "$NODE" >/dev/null
g layout apply --version 1 >/dev/null
KEY=$(g key create lendisk-ci)
ACCESS=$(echo "$KEY" | awk '/Key ID:/ {print $3}')
SECRET=$(echo "$KEY" | awk '/Secret key:/ {print $3}')
g bucket create offsite >/dev/null
# In GitHub Actions the generated keys are masked in every later log
# line first (#96). stdout goes to $GITHUB_ENV, so the ::add-mask::
# commands go to stderr, which the runner reads for commands too.
if [ "${GITHUB_ACTIONS:-}" = true ]; then
  echo "::add-mask::$ACCESS" >&2
  echo "::add-mask::$SECRET" >&2
fi
g bucket allow --read --write --owner offsite --key lendisk-ci >/dev/null
PORT=$(docker port "$NAME" 3900/tcp | head -1 | sed 's/.*://')
echo "LENDISK_TEST_OVERFLOW_ENDPOINT=http://127.0.0.1:$PORT"
echo "LENDISK_TEST_OVERFLOW_REGION=garage"
echo "LENDISK_TEST_OVERFLOW_BUCKET=offsite"
echo "LENDISK_TEST_OVERFLOW_ACCESS_KEY=$ACCESS"
echo "LENDISK_TEST_OVERFLOW_SECRET_KEY=$SECRET"
