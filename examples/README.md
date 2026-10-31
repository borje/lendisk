# lendisk deployment examples

These files follow DESIGN.md v0.10.1 and REQUIREMENTS.md v0.9.1
(2026-10-30). The daemon does not exist yet, so the TOML keys and the
image name are the proposed surface, kept consistent with the design
text: one static binary, one `[store]` section for both roles,
third-party credentials from the environment and never from a file the
daemon writes (SEC-3), `network_mode: host` so NAT traversal is not
behind Docker's own NAT (D2, G-1).

| Folder | Who | What it shows |
|---|---|---|
| `default-nas/` | Anna, Synology NAS behind home NAT | The default install: plain files on the NAS, restic, two friends plus a Backblaze B2 replica |
| `lender-only-pi/` | Carla, Raspberry Pi with a USB disk | The lender-only profile (PEER-9): backs up nothing, lends to two friends, 60-day window |
| `garage-on-lan/` | Björn, homelab behind CGNAT | A store the user already runs: Garage as a second compose service, named as the `s3` backend (EP-16) |
| `provider-store-vps/` | Dag, small VPS with a public IP | A provider as the store, Hetzner Object Storage behind the `s3` backend, Kopia, the WAN-speed warning accepted |
| `systemd/` | Any of the above without Docker | The unit file and the root-owned environment file that SEC-3 assumes |

## What is in the config file and what is not

The TOML file holds static settings: role, listeners, the store, relay
policy, replication limits and schedule, external replicas, alert hooks.
Peers and buckets are not in it. They are created with `lendisk invite`,
`lendisk peer add` and `lendisk bucket add` and live in `lendisk.db`
(PEER-1, EP-6), and the node manifest carries them for recovery (ID-4).

Credentials are never values in the file. The file names the
environment variables, and the defaults are:

| Variable | Used for |
|---|---|
| `LENDISK_STORE_ACCESS_KEY`, `LENDISK_STORE_SECRET_KEY` | the `s3` backend store credential (EP-16) |
| `LENDISK_REPLICA_<NAME>_ACCESS_KEY`, `LENDISK_REPLICA_<NAME>_SECRET_KEY` | each external replica, `<NAME>` upper-cased (S3-1) |

The daemon refuses to start a store or replica whose variables are
unset, redacts them from logs and `status`, and never writes them to
disk. With compose they arrive through `env_file`; the file on the host
should be root-owned, mode 0600. With systemd they arrive through
`EnvironmentFile=`.

## Volumes

| Mount | Contents | Mode |
|---|---|---|
| `/etc/lendisk` | `lendisk.toml`, the encrypted identity key, the per-bucket keys the node issues | directory 0700, files 0600, owned by the service user |
| `/var/lib/lendisk` | `store/` (plain files on `posix`), `lendisk.db`, `run/` (internal socket) | owned by the service user; on `posix` the filesystem must support xattrs (EP-12): ext4, xfs, btrfs, zfs, overlay2 |

On the `s3` backend the data directory holds only `lendisk.db` and
`run/`; the objects live in the named service.

## Listeners you will see

- `127.0.0.1:9000` TCP: the S3 endpoint for your backup tools (EP-1).
  Because of `network_mode: host` this is the host's loopback, which is
  where restic or Kopia on the same machine look.
- `127.0.0.1:9100` TCP: Prometheus metrics (OPS-4).
- The tailcat tunnel: one UDP port for WireGuard, chosen by tailcat,
  carrying the lender-facing S3 and control API on port 9000 inside it.
- `/var/lib/lendisk/run/tunnel.sock`: internal, lendisk to versitygw.

Nothing else. `ss -lntp` on the host should show no lendisk listener on
a non-loopback address (SEC-8).

## First run

```sh
mkdir -p config data && chmod 0700 config
docker compose run --rm lendisk init            # prints the 24-word phrase once
docker compose up -d
docker compose exec lendisk lendisk status
```

`init` asks for the expected repository size and how many lenders you
plan to have, and suggests how much to lend back (LEND-7). Write the
phrase down; nothing in these folders can recover it.
