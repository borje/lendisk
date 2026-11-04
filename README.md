# lendisk

Friends lend each other disk space for offsite backups.

> **Status:** specification only. No code exists yet. "lendisk" is a working title.

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

## Configuration

## Restoring

## Contributing

## License
