# The lendisk container image (D2, COMPAT-2, OPS-7): the static binary on
# distroless static, which adds CA certificates (overflow targets, relay
# maps, webhooks) and time zones (replication.schedule is local time),
# and nothing that runs: no shell, no package manager.
#
# It runs as the unprivileged user nonroot (65532) and needs no
# capability, so run it with every capability dropped and a read-only
# root, as examples/default-nas/compose.yaml does:
#
#   docker run --read-only --tmpfs /tmp --cap-drop ALL \
#     --security-opt no-new-privileges --network host \
#     -v ./config:/etc/lendisk -v ./data:/var/lib/lendisk lendisk serve
#
# The binary is not built here: CI's build job cross-compiles it, and the
# context holds dist/linux-<arch>/lendisk (amd64, arm64, armv7), so the
# image builds for every platform without emulation. To build one
# locally:
#
#   CGO_ENABLED=0 go build -trimpath -o dist/linux-amd64/lendisk ./cmd/lendisk
#   docker build -t lendisk .
FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329

ARG TARGETARCH
ARG TARGETVARIANT
COPY --chmod=0755 dist/linux-${TARGETARCH}${TARGETVARIANT}/lendisk /usr/local/bin/lendisk

USER 65532:65532
# The config and data directories, the only volumes (D2), created owned
# by nonroot so a named volume starts writable; lendisk makes them 0700.
WORKDIR /var/lib/lendisk
WORKDIR /etc/lendisk
ENV LENDISK_CONFIG=/etc/lendisk/lendisk.toml \
    LENDISK_DATA_DIR=/var/lib/lendisk
VOLUME ["/etc/lendisk", "/var/lib/lendisk"]

ENTRYPOINT ["/usr/local/bin/lendisk"]
CMD ["serve"]
