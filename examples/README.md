# lendisk deployment examples

One static binary, one `[store]` section for both roles, third-party
credentials from the environment and never from a file the daemon
writes (SEC-3), `network_mode: host` so NAT traversal is not behind
Docker's own NAT (D2, G-1). A test (`internal/config/examples_test.go`)
loads every v1 example against the shipped settings: no unknown key,
and every setting shown somewhere, set or commented out.

The image is built from the `Dockerfile` at the repository root
(distroless, user 65532, no shell); nothing is published yet, so
`ghcr.io/lendisk/lendisk` stands for an image you build with
`docker build -t ghcr.io/lendisk/lendisk:0.10 .` after the build step
the Dockerfile describes.

| Folder | Who | What it shows |
|---|---|---|
| `default-nas/` | Anna, Synology NAS behind home NAT | The default install: plain files on the NAS, restic, copies spread over three friends, a Backblaze B2 bucket as overflow |
| `lender-only-pi/` | Carla, Raspberry Pi with a USB disk | The lender-only profile (PEER-9): a node with no buckets yet, symmetric friendships with two people, 60-day window |
| `garage-on-lan/` | Björn, homelab behind CGNAT | **Post-v1 (EP-16).** A store the user already runs: Garage as a second compose service, named as the `s3` backend. Kept for v1.1; in v1 Björn runs the default install |
| `provider-store-vps/` | Dag, small VPS with a public IP | **Post-v1 (EP-16).** A provider as the store, Hetzner Object Storage behind the `s3` backend, Kopia. Kept for v1.1; in v1 Dag's store is the VPS disk |
| `relay-vps/` | Dag, small VPS with a public IP | `lendisk relay` for the group (NET-5), so their nodes need no relay run by a third party; the `[relay]` settings |
| `systemd/` | Any of the above without Docker | The unit file and the root-owned environment file that SEC-3 assumes; the release package holds both |

## What is in the config file and what is not

The TOML file holds settings the user chooses: role, listeners, the
store, relay policy, replication limits and schedule, the lending cap,
alert hooks (`[alerts]`: a hook program, which must exist and be
executable before `serve` starts, and a webhook). State the daemon
changes is not in it. Friendships,
buckets and overflow targets are created with `lendisk invite`,
`lendisk peer add`, `lendisk bucket add` and `lendisk overflow add`
and live in `lendisk.db` (PEER-1, EP-6, S3-1), and the node manifest
carries them for recovery (ID-4). Each side's number for a friendship
is set with `lendisk peer quota <peer> <bytes>`; the smaller of the two
numbers is the quota in force (LEND-8).

Credentials are never values in the file. The file names the
environment variables, and the defaults are:

| Variable | Used for |
|---|---|
| `LENDISK_OVERFLOW_<NAME>_ACCESS_KEY`, `LENDISK_OVERFLOW_<NAME>_SECRET_KEY` | each overflow target, `<NAME>` upper-cased (S3-1); `overflow add --env-prefix` overrides the prefix |
| `LENDISK_STORE_ACCESS_KEY`, `LENDISK_STORE_SECRET_KEY` | the `s3` backend store credential; post-v1 (EP-16) |

The daemon refuses to start a store or overflow target whose variables are
unset, redacts them from logs and `status`, and never writes them to
disk. With compose they arrive through `env_file`; the file on the host
should be root-owned, mode 0600. With systemd they arrive through
`EnvironmentFile=`.

## Volumes

| Mount | Contents | Mode |
|---|---|---|
| `/etc/lendisk` | `lendisk.toml`, the identity key (plain, mode 0600, ID-3), the per-bucket keys the node issues | directory 0700, files 0600, owned by the service user |
| `/var/lib/lendisk` | `store/` (plain files), `lendisk.db`, `run/` (the internal socket `gw.sock`) | owned by the service user; the filesystem must support xattrs (EP-12): ext4, xfs, btrfs, zfs, overlay2 |

On the post-v1 `s3` backend the data directory would hold only
`lendisk.db` and `run/`; the objects live in the named service.

## Listeners you will see

Every TCP listener is lendisk's own bridge, which proxies to embedded
versitygw on the internal unix socket; versitygw opens no TCP port.

- `127.0.0.1:9000` TCP: the S3 endpoint for your backup tools (EP-1).
  Because of `network_mode: host` this is the host's loopback, which is
  where restic or Kopia on the same machine look.
- `127.0.0.1:9101` TCP: Prometheus metrics at `/metrics` (OPS-4), where
  `[metrics] listen` sets it; off by default. Refused in the
  lender-only profile, where nothing listens on loopback (PEER-9).
- The tailcat tunnel: one UDP port for WireGuard, chosen by tailcat,
  carrying the lender-facing S3 and control API on port 9000 inside it.
- `/var/lib/lendisk/run/gw.sock`: internal, lendisk's bridge to
  versitygw.

Nothing else. `ss -lntup` on the host should show no lendisk socket on
a non-loopback address except the tailcat UDP port and any
`[[endpoint.listen]]` address you configured (SEC-8).

## First run

```sh
mkdir -p config data && chmod 0700 config
docker compose run --rm lendisk init            # prints the 24-word phrase once
docker compose up -d
docker compose exec lendisk lendisk status
```

`init` asks for the expected repository size and `copies`, and
suggests how much to lend in total: `copies` × size plus 20% for
tombstones (LEND-7). The friendships between the four example
households are Anna–Björn 200 GB, Anna–Carla 250 GB, Anna–Dag 150 GB,
Björn–Dag 200 GB and Carla–Dag 200 GB, each lent both ways. Write the
phrase down; nothing in these folders can recover it.
