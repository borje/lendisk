# lendisk

Friends lend each other disk space for offsite backups.

> **Status:** early development. A node serves S3 to backup tools on the
> same machine; friends, replication and recovery do not exist yet.
> "lendisk" is a working title.

## What it will be

Each participant runs one lendisk node, typically on a home NAS or server.
Your backup tool (restic, Kopia, or anything that speaks S3) talks to the
node as if it were an ordinary S3 endpoint. The node keeps a complete local
copy and spreads offsite copies across the friends you lend space to, over
an overlay network that works behind NAT.

- **Friends only.** Peers are added by invite. There is no marketplace and no discovery of strangers.
- **Untrusted holders.** The backup tool encrypts everything before it leaves your machine.
- **Pay with disk.** Each friendship has one quota, lent in both directions.
- **Fast local backups.** A backup finishes at local disk speed. Offsite status is tracked separately.
- **Deletes can be undone.** Deleted data stays recoverable at friends for a retention window.
- **No lock-in.** Your data stays readable with free tools and no lendisk code.
- **Recovery from paper.** A 24-word phrase and one friend's address rebuild everything.

```sh
lendisk init
lendisk invite --quota 100G           # send the string to a friend
lendisk peer add <invite>             # the friend redeems it
lendisk bucket add laptop --profile restic
restic backup ~                       # against the printed endpoint
lendisk status --wait laptop          # exits 0 once the backup is offsite
```

## Documentation

- [REQUIREMENTS.md](REQUIREMENTS.md): what the product does
- [DESIGN.md](DESIGN.md): how it is built
- [architecture.html](architecture.html): overview diagrams
- [examples/](examples/): deployment examples

## Install

## Quick start

What works today: one node, local backups.

```sh
lendisk init                                  # write the 24 words down
lendisk bucket add laptop --profile restic    # prints the lines below
lendisk serve &                               # S3 on 127.0.0.1:9000
export AWS_ACCESS_KEY_ID=LDK…
export AWS_SECRET_ACCESS_KEY=…
export RESTIC_REPOSITORY=s3:http://127.0.0.1:9000/laptop
restic init && restic backup ~
```

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
| `role` | `full` | `full`, or `lender` for a node that only lends |
| `data_dir` | `/var/lib/lendisk` | see below |
| `store.backend` | `posix` | the only store in v1: plain files |
| `store.path` | `<data_dir>/store` | the store directory; its filesystem needs xattrs |
| `store.free_space_floor` | `5%` | refuse writes below this much free space (`5%`, `20G`) |
| `endpoint.loopback` | `127.0.0.1:9000` | the S3 endpoint for tools on this machine; loopback only |
| `log.level` | `info` | `debug`, `info`, `warn`, `error` |
| `log.format` | `text` | `text` (logfmt) or `json` |

Files:

| Path | Mode | Contents |
|---|---|---|
| `<config>/` | 0700 | the config directory |
| `<config>/lendisk.toml` | 0600 | your settings |
| `<config>/identity.key` | 0600 | the node's X25519 private key, base64. Recreated from the recovery phrase by `lendisk init --restore` |
| `<data>/lendisk.db` | 0600 | SQLite state: everything lendisk creates or learns, including each bucket's credential. Migrated forward automatically; a copy `lendisk.db.schema-<n>.bak` is kept before each migration |
| `<data>/store/` | 0700 | one directory per bucket, holding the backup tool's files as plain files (metadata in xattrs). With lendisk stopped, `restic -r <data>/store/<bucket>` opens the repository. Don't change files here while lendisk runs |
| `<data>/run/gw.sock` | 0600 | the internal socket between lendisk and its embedded S3 gateway |

The recovery phrase is never stored anywhere. The daemon (`lendisk
serve`) logs to stdout; other commands log to stderr.

## Restoring

## Contributing

## License
