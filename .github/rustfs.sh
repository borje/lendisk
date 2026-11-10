#!/usr/bin/env bash
# Starts a single-node RustFS container with a versioned bucket as an
# overflow target's provider (DESIGN §3.11, tier 1) for the overflow
# tests that need versioning (#39), and prints the variables they read,
# one per line, for $GITHUB_ENV or `export`:
#   LENDISK_TEST_OVERFLOW_VERSIONED_ENDPOINT=http://127.0.0.1:PORT
#   LENDISK_TEST_OVERFLOW_VERSIONED_REGION=us-east-1
#   LENDISK_TEST_OVERFLOW_VERSIONED_BUCKET=offsite
#   LENDISK_TEST_OVERFLOW_VERSIONED_ACCESS_KEY=…
#   LENDISK_TEST_OVERFLOW_VERSIONED_SECRET_KEY=…
# The bucket is created with versioning enabled through the AWS CLI
# (`aws` if installed, else the amazon/aws-cli image).
# Stop it with: docker rm -f "$NAME".
# Usage: rustfs.sh [NAME]
set -euo pipefail
NAME=${1:-lendisk-ci-rustfs}
IMAGE=rustfs/rustfs:1.0.3
AWSCLI=amazon/aws-cli:2.31.13
ACCESS=lendisk-ci-$(head -c 6 /dev/urandom | od -An -tx1 | tr -d ' \n')
SECRET=$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')

# In GitHub Actions the generated keys are masked in every later log
# line first (#96). stdout goes to $GITHUB_ENV, so the ::add-mask::
# commands go to stderr, which the runner reads for commands too.
if [ "${GITHUB_ACTIONS:-}" = true ]; then
  echo "::add-mask::$ACCESS" >&2
  echo "::add-mask::$SECRET" >&2
fi
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p 127.0.0.1::9000 \
  -e RUSTFS_ACCESS_KEY="$ACCESS" -e RUSTFS_SECRET_KEY="$SECRET" \
  "$IMAGE" >/dev/null
PORT=$(docker port "$NAME" 9000/tcp | head -1 | sed 's/.*://')
EP=http://127.0.0.1:$PORT

s3() {
  if command -v aws >/dev/null 2>&1; then
    AWS_ACCESS_KEY_ID=$ACCESS AWS_SECRET_ACCESS_KEY=$SECRET AWS_DEFAULT_REGION=us-east-1 \
      aws --endpoint-url "$EP" "$@"
  else
    docker run --rm --network host -e AWS_ACCESS_KEY_ID="$ACCESS" -e AWS_SECRET_ACCESS_KEY="$SECRET" \
      -e AWS_DEFAULT_REGION=us-east-1 "$AWSCLI" --endpoint-url "$EP" "$@"
  fi
}
for _ in $(seq 150); do
  s3 s3api list-buckets >/dev/null 2>&1 && break
  sleep 0.2
done
s3 s3api create-bucket --bucket offsite >/dev/null
s3 s3api put-bucket-versioning --bucket offsite --versioning-configuration Status=Enabled >/dev/null
echo "LENDISK_TEST_OVERFLOW_VERSIONED_ENDPOINT=$EP"
echo "LENDISK_TEST_OVERFLOW_VERSIONED_REGION=us-east-1"
echo "LENDISK_TEST_OVERFLOW_VERSIONED_BUCKET=offsite"
echo "LENDISK_TEST_OVERFLOW_VERSIONED_ACCESS_KEY=$ACCESS"
echo "LENDISK_TEST_OVERFLOW_VERSIONED_SECRET_KEY=$SECRET"
