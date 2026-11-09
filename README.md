# lendisk

Friends lend each other disk space for offsite backups.

> **Status:** early development, not yet released. Friends, replication to
> friends and recovery from the 24-word phrase work; some of the v1 list
> below is still being built (marked *not yet*). "lendisk" is a working
> title.

## What it is

Each participant runs one lendisk node, typically on a home NAS or server.
Your backup tool (restic, or anything that speaks S3) talks to the node as
if it were an ordinary S3 endpoint. The node keeps a complete local copy
and spreads offsite copies across the friends you lend space to, over an
overlay network that works behind NAT.

- **Friends only.** Peers are added by invite. There is no marketplace and no discovery of strangers.
- **Untrusted holders.** The backup tool encrypts everything before it leaves your machine. A friend's node can only do what replication needs at yours.
- **Pay with disk.** Each friendship has one quota, lent in both directions.
- **Fast local backups.** A backup finishes at local disk speed. Offsite status is tracked separately.
- **Deletes are kept.** What you delete stays at friends for a retention window (30 days by default), and nothing a friend's node sends can purge it early. Undelete is *not yet* built.
- **No lock-in.** Your data stays readable with free tools and no lendisk code.
- **Recovery from paper.** A 24-word phrase and one friend's address rebuild everything.

## Documentation

- [REQUIREMENTS.md](REQUIREMENTS.md): what the product does
- [DESIGN.md](DESIGN.md): how it is built
- [architecture.html](architecture.html): overview diagrams
- [examples/](examples/): deployment examples (compose, systemd)

## Install

No releases yet. Build from source with Go (pure Go, no cgo):

```sh
go build -o lendisk ./cmd/lendisk
```

## Quick start

On your node:

```sh
lendisk init                                  # write the 24 words down
lendisk bucket add laptop --profile restic    # prints the lines below
lendisk serve &                               # S3 on 127.0.0.1:9000
export AWS_ACCESS_KEY_ID=LDK…
export AWS_SECRET_ACCESS_KEY=…
export RESTIC_REPOSITORY=s3:http://127.0.0.1:9000/laptop
restic init && restic backup ~
```

Add a friend. Each of you runs a node; friends reach each other through
public relays, so both need internet access but no open ports.

```sh
# on the friend's node: lend you 100G, print a single-use invite
lendisk invite --quota 100G
# on yours: redeem it; it asks what you lend back (Enter for the same).
# The friendship's quota, each way, is the smaller of the two.
lendisk peer add <invite>
lendisk peer list
lendisk status                                # per friend: queue, held, last answer
```

Backups replicate to friends in the background. Each bucket's objects are
held by `--copies` friends (default 2); its metadata by every friend.
`status` names each bucket's latest snapshot and whether it is offsite at
its copies (for Kopia, the time by which every finished snapshot is; for a
generic bucket, the last write that is), and what is still short. A backup
script can block until the backup is offsite:

```sh
restic backup ~ && lendisk status --wait laptop --timeout 6h
# 0 offsite; 3 timed out; 4 cannot be as things stand (status says why)
```

Profiles: `restic`, `kopia` and `generic` (any S3 tool, such as rclone or
Duplicacy). A generic bucket is not encrypted, so friends can read it;
`bucket add` wants `--i-understand-no-encryption` for one. With rclone, pass
`--disable Copy,Move --no-update-modtime`; `--backup-dir` and `--suffix` do
not work against lendisk. Kopia must be 0.9 or later, with the repository
in format 2 or later (the default since Kopia 0.9). A repository still in
format 1 (made by Kopia 0.8 or earlier) is accepted, but `status` and a
`kopia_format1` alert say to run `kopia repository set-parameters --upgrade`.

## Restoring

If the machine is lost, rebuild the node on a new one from the 24 words
and the address of any one friend (the `tc…` string `lendisk status`
shows on the friend's node):

```sh
lendisk recover <friend's tc… address>    # asks for the 24 words
lendisk serve &
lendisk bucket show laptop                # the bucket's NEW credential
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…
restic restore latest --target /restore
```

`recover` fetches the node's encrypted manifest from the friends, restores
the friendships and buckets, and asks each friend what it holds. Nothing is
downloaded up front: objects are fetched from friends as the backup tool
reads them, and checked against their hashes. Stop the old node first if it
still runs.

Recovered buckets get new S3 credentials, because the manifest never holds
them. If friends disagree about an object, restic's content-named files are
checked against their names and otherwise the version most friends hold
wins; a tie is listed in `status` and settled with
`lendisk bucket resolve <bucket> <key> <friend>`.

## Configuration

One TOML file, `/etc/lendisk/lendisk.toml` by default (`--config` or
`LENDISK_CONFIG` to change it). Its directory is the config directory,
which also holds the identity key. Every setting is also a flag and an
environment variable, named after its key: `log.level` is
`--log-level` and `LENDISK_LOG_LEVEL`. A flag beats the environment,
which beats the file, which beats the default. `lendisk --help` lists
them all. lendisk never writes this file; `init` creates a minimal one
if none exists.

| Key | Default | |
|---|---|---|
| `role` | `full` | `full`, or `lender` for a node that only lends (no buckets, no LAN endpoint) |
| `data_dir` | `/var/lib/lendisk` | see below |
| `store.backend` | `posix` | the only store in v1: plain files |
| `store.path` | `<data_dir>/store` | the store directory; its filesystem needs xattrs |
| `store.free_space_floor` | `5%` | refuse writes below this much free space (`5%`, `20G`) |
| `store.reconcile_every` | `24h` | how often, besides at start, the store is compared with what was captured, so objects written or removed outside the S3 endpoint are replicated |
| `store.reconcile_delete_limit` | `10` | percent: a pass finding more of a bucket's objects missing deletes none until `lendisk bucket reconcile <bucket> --allow-deletes N` (100 never asks) |
| `endpoint.loopback` | `127.0.0.1:9000` | the S3 endpoint for tools on this machine; loopback only |
| `endpoint.listen` | none | S3 endpoints for other machines on your LAN, opt-in and repeatable: `address` or `interface` + `port`, `tls` = `generate` (default), `files` (with `cert`, `key`) or `off`. `lendisk endpoint cert` prints the generated certificate's fingerprint |
| `tunnel.region` | nearest | the relay region in this node's address; changing it changes the address |
| `replication.copies` | `2` | how many friends hold each object of a bucket created without `--copies`; with 1, losing one friend loses data, and `bucket add` and `status` say so |
| `replication.reroute_after` | `1h` | move unsent copies from a friend failing this long to another with room |
| `replication.replace_after` | `168h` | a friend failing this long no longer counts as room for copies |
| `alerts.command` | none | absolute path of a program run for each alert, the alert as JSON on stdin |
| `alerts.webhook` | none | http(s) URL each alert's JSON is POSTed to |
| `alerts.unreachable_after` | `24h` | alert when a friend has not answered for this long |
| `log.level` | `info` | `debug`, `info`, `warn`, `error` |
| `log.format` | `text` | `text` (logfmt) or `json` |

Files:

| Path | Mode | Contents |
|---|---|---|
| `<config>/` | 0700 | the config directory |
| `<config>/lendisk.toml` | 0600 | your settings |
| `<config>/identity.key` | 0600 | the node's X25519 private key, base64. Recreated from the recovery phrase by `lendisk init --restore` |
| `<config>/endpoint.crt`, `endpoint.key` | 0600 | the generated TLS certificate of the LAN endpoint (`tls = "generate"`) |
| `<config>/profiles/` | | optional custom backup-tool profiles (`*.toml`) |
| `<config>/locks/` | 0700 | the lock `peer add` takes while redeeming |
| `<data>/lendisk.db` | 0600 | SQLite state: everything lendisk creates or learns, including each bucket's credential and each friendship. Migrated forward automatically; a copy `lendisk.db.schema-<n>.bak` is kept before each migration |
| `<data>/store/` | 0700 | one directory per bucket, holding the backup tool's files as plain files (metadata in xattrs), plus what you lend friends, under `lent-…` buckets. With lendisk stopped, `restic -r <data>/store/<bucket>` opens the repository. Don't change files here while lendisk runs |
| `<data>/store/.lendisk/store-marker` | 0600 | made by the daemon's first reconcile; while it is missing (an unmounted or replaced store) nothing is reconciled, so an empty directory never deletes your backup at friends. If the store is right, `touch` it back |
| `<data>/store/.lendisk/manifests/` | 0700 | each friend's encrypted node manifest, which it fetches to recover |
| `<data>/store/lent-…/.lendisk/tomb/` | 0700 | what a friend deleted, kept until the retention window ends; a direct reader of the bucket ignores it |
| `<data>/run/gw.sock` | 0600 | the internal socket between lendisk and its embedded S3 gateway |

The recovery phrase is never stored anywhere. The daemon (`lendisk
serve`) logs to stdout; other commands log to stderr.

## Contributing

## License
