#!/usr/bin/env bash
# Installs a pinned release of a backup tool the end-to-end tests drive
# (COMPAT-1) into /usr/local/bin, checked against the SHA-256 of the
# linux amd64 download: the digest GitHub lists for the release asset,
# which matches restic's SHA256SUMS and Kopia's checksums.txt.
# Downloads are kept in ~/tools-cache, which CI caches, so a GitHub
# release outage matters only when a version changes.
# Usage: install-tool.sh restic|kopia VERSION SHA256
set -euo pipefail
TOOL=$1 VERSION=$2 SHA256=$3
CACHE=~/tools-cache
mkdir -p "$CACHE"
case $TOOL in
restic)
	file=restic_${VERSION}_linux_amd64.bz2
	url=https://github.com/restic/restic/releases/download/v${VERSION}/$file
	;;
kopia)
	file=kopia-${VERSION}-linux-x64.tar.gz
	url=https://github.com/kopia/kopia/releases/download/v${VERSION}/$file
	;;
*)
	echo "unknown tool $TOOL" >&2
	exit 2
	;;
esac
[ -f "$CACHE/$file" ] || curl -fsSL --retry 5 --retry-all-errors --retry-delay 10 -o "$CACHE/$file" "$url"
echo "$SHA256  $CACHE/$file" | sha256sum -c -
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
case $TOOL in
restic)
	bunzip2 -c "$CACHE/$file" >"$tmp/restic"
	sudo install -m 0755 "$tmp/restic" /usr/local/bin/restic
	restic version
	;;
kopia)
	tar xzf "$CACHE/$file" -C "$tmp"
	sudo install -m 0755 "$tmp/kopia-${VERSION}-linux-x64/kopia" /usr/local/bin/kopia
	kopia --version
	;;
esac
