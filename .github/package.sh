#!/usr/bin/env bash
# Packs one platform's release package (D2, COMPAT-2): a tarball holding
# the static binary, the systemd unit and its environment file, and the
# README, all under lendisk-VERSION-linux-ARCH/. Writes
# OUTDIR/lendisk-VERSION-linux-ARCH.tar.gz and its .sha256 beside it.
# Usage: package.sh VERSION ARCH BINARY OUTDIR   (ARCH: amd64, arm64, armv7)
set -euo pipefail
VERSION=$1 ARCH=$2 BINARY=$3 OUTDIR=$4
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NAME=lendisk-$VERSION-linux-$ARCH

STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT
mkdir -m 0755 "$STAGE/$NAME"
install -m 0755 "$BINARY" "$STAGE/$NAME/lendisk"
install -m 0644 "$ROOT/examples/systemd/lendisk.service" "$ROOT/examples/systemd/lendisk.env.example" \
	"$ROOT/README.md" "$STAGE/$NAME/"

mkdir -p "$OUTDIR"
OUT=$(cd "$OUTDIR" && pwd)/$NAME.tar.gz
# Reproducible: fixed owner, order and times.
tar --sort=name --owner=0 --group=0 --numeric-owner --mtime=@0 -C "$STAGE" -cf - "$NAME" | gzip -9n >"$OUT"
(cd "$(dirname "$OUT")" && sha256sum "$NAME.tar.gz" >"$NAME.tar.gz.sha256")
echo "$OUT"
