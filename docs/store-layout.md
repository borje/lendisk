# Store layout

What lendisk keeps on disk, where, and what each file is for (COMPAT-4).
With this page and the [restore guide](restore-without-lendisk.md) you
can read your backups, and your friends' replicas, with no lendisk code
at all.

Paths below use the defaults: `<config>` is `/etc/lendisk` (the directory
of `lendisk.toml`), `<data>` is `/var/lib/lendisk` (`data_dir`), and
`<store>` is `<data>/store` (`store.path`).

Don't change files in the store while lendisk runs: the daemon notices
files written or removed outside its S3 endpoint and replicates the change
(deletes included, up to a limit). Reading is always safe.

## Overview

```
<config>/
  lendisk.toml              your settings (lendisk never writes it)
  identity.key              the node's private key (0600)
  endpoint.crt, endpoint.key  the LAN endpoint's generated TLS certificate
  profiles/                 custom backup-tool profiles, optional
  locks/                    the lock `peer add` holds while redeeming an invite

<data>/
  lendisk.db                the state database, SQLite (0600)
  lendisk.db.schema-<n>.bak a copy kept before each schema migration
  run/gw.sock               the socket between lendisk and its S3 gateway
  store/                    the store: everything below is versitygw's posix layout
    laptop/                 an own bucket (the name you gave it)
      config, keys/…, data/<2 hex>/<id>, index/…, snapshots/…   restic's files
      .sgwtmp/              versitygw: temporary and multipart files
    vault/                  an own Kopia bucket: one file per blob ID, flat
    lent-<id16>-<sealed>/   a friend's bucket you hold (a replica)
      <key>…                the friend's files, at their key paths
      .lendisk/tomb/<key>/<unix nanoseconds>   what the friend deleted
      .sgwtmp/              versitygw: temporary and multipart files
    .lendisk/
      manifests/<node ID>   each friend's encrypted node manifest
      store-marker          proof that this is the right store directory
    .vgwlocks/<hash>/<nn>   versitygw: object publish locks
```

## The store

The store is a plain directory served by [versitygw](https://github.com/versity/versitygw)'s
`posix` backend, embedded in lendisk (version 1.10.0). Every directory at
the top of the store is an S3 bucket, and every file below a bucket's
directory is an object whose key is its path inside that directory.

### Own buckets

Each bucket you made with `lendisk bucket add` is `<store>/<name>/`,
holding exactly the keys your backup tool wrote:

- **restic** writes `config`, `keys/<id>`, `snapshots/<id>`, `index/<id>`,
  `data/<first 2 hex digits>/<id>` and `locks/<id>`. This is restic's own
  repository layout, so with lendisk stopped
  `restic -r <store>/laptop --no-lock snapshots` opens it directly.
- **Kopia** writes one object per blob, named by its blob ID with no
  directories (`kopia.repository`, `p8bf4fa…`, `xn0_…`). That is Kopia's S3
  layout, not its filesystem layout: read it through versitygw (the
  [restore guide](restore-without-lendisk.md#your-own-copy)), not as a
  filesystem repository.
- **generic** buckets hold whatever the tool wrote, in plain text.

### Friends' buckets (replicas)

A friend's bucket is `<store>/lent-<id16>-<sealed>/`:

- `<id16>` is the first 16 characters of the friend's node ID;
- `<sealed>` is the bucket's **sealed name**, `s` and 26 characters derived
  from the friend's key and the bucket's real name. Your node never learns
  the real name (SEC-16). The friend's recovery sheet lists each of their
  buckets with the name it has at every friend.

Its files are the friend's keys at their key paths, the same layout as an
own bucket: a restic replica is a restic repository directory (but see
[how many replicas](restore-without-lendisk.md#how-many-replicas)), a
Kopia replica is flat blob IDs.

Each replica holds **all of the bucket's metadata** (restic's `config`,
`keys`, `index` and `snapshots`; Kopia's `kopia.*`, `q…` and `x…` blobs)
and **the data objects placed there** (restic's `data/`, Kopia's `p…`).
With `copies` = c and the bucket spread over N places, each data object is
at c of them, so the replicas of any N − c + 1 places together hold every
object.

#### Tombs: `lent-…/.lendisk/tomb/`

When a friend deletes or rewrites an object, your node keeps the old
version for the retention window (30 days by default) so a compromised
friend cannot destroy their own offsite copy (RET-1). The file is moved,
not copied, to `.lendisk/tomb/<key>/<unix nanoseconds>` inside the bucket's
directory; it is removed when the window ends. A direct reader of the
bucket ignores `.lendisk/`: it is not part of the repository.

### lendisk's housekeeping: `<store>/.lendisk/`

- `manifests/<node ID>`: each friend's **node manifest**, encrypted with
  that friend's key: their bucket list, friends' addresses and settings,
  which `lendisk recover` fetches to rebuild their node (ID-4). One file per
  friend, 0600, replaced whole on each update. Kept until a former friend's
  retention window ends.
- `store-marker`: written by the daemon's first store reconcile. While it
  is missing (an unmounted disk, another directory in the store's place)
  the daemon reconciles nothing, so an empty directory never deletes your
  backup at friends. If the store is right, `touch` it back.

The name `.lendisk` is not a valid bucket name, so no S3 client sees it.

### versitygw's housekeeping

- `<bucket>/.sgwtmp/`: temporary files of writes in progress, and under
  `multipart/` the parts of unfinished multipart uploads. Not objects.
- `<store>/.vgwlocks/<bucket hash>/<shard>`: empty lock files versitygw
  takes to publish an object atomically. Not a bucket.
- **Extended attributes** (xattrs, `user.*`): S3 metadata lives in xattrs,
  so the store's filesystem must support them.
  - On a bucket directory: `user.acl` (its owner: a bucket's access key, or
    for a friend's bucket the friend's node ID) and `user.ownership`.
  - On an object file: `user.etag`, `user.checksums`, `user.content-type`,
    `user.mp-metadata` on an object assembled from a multipart upload, and
    `user.metadata`, a JSON map of the `x-amz-meta-*` headers. A friend's
    object carries `lendisk-sha256`, the SHA-256 it was stored with, which
    your node checks against; an own object carries `lendisk-capture`, a
    token telling which write it was (its hash is in `lendisk.db`).

Copying a bucket's directory with `cp -R` or `rsync -a` (without `-X`)
leaves the xattrs behind. The backup tool does not need them.

## The state database: `<data>/lendisk.db`

SQLite, 0600. Everything lendisk creates or learns that is not the
configuration file. It is not needed to read any backup: a restore without
lendisk uses the store alone. Tables:

| Table | What it holds |
|---|---|
| `schema_meta` | the schema version, migrated forward at start |
| `node` | facts about this node: its ID, when it was set up, its relay region, the last printed recovery sheet, the store marker |
| `buckets` | your buckets: name, profile, `copies`, and each one's S3 credential |
| `friendship` | your friends: node ID, address, the quota each way, retention |
| `former_friend` | friendships ended and still in their retention window |
| `invite`, `pending_redeem` | invites you issued and invites being redeemed |
| `queue` | every write and delete of your buckets, in order, with its hash and size |
| `placement` | where each queued object is to go and which friends acknowledged it, with the hash they verified: the index of your offsite copies |
| `place`, `place_use`, `place_freed` | each friend's last answer and how much of your data it holds |
| `transfer`, `transfer_part` | large uploads to friends in progress, to resume at the next part |
| `below_copies` | buckets with objects at fewer than `copies` friends |
| `manifest_push` | which friend has your latest manifest |
| `relist`, `listed`, `recovered_key` | after `lendisk recover`: what each friend said it holds, and the verdict on each key |
| `lent_bucket` | friends' buckets you hold: friend, sealed name (stored as `lent-<id16>-<sealed>`) |
| `lent_usage`, `lent_upload`, `lent_part` | what each friend uses of your disk, and their uploads in progress |
| `tombstone` | the tombs in friends' buckets and when each is purged |
| `replica_check`, `verify_request` | verification of your replicas at friends (`lendisk verify`) |
| `bitrot_sweep`, `bitrot` | the bit-rot sampler's progress and what it found |
| `reconcile` | the store reconciler's guard against mass deletion |
| `undelete_request`, `undelete_key` | `lendisk undelete` runs |
| `seed` | `lendisk export <bucket> --for <friend>`: a friend seeded from a disk, whose rows of that bucket wait until its list confirms the import |
| `alert` | raised alerts and their delivery |

## The export layout

`lendisk export --lent <stored name> <directory>` writes a friend's replica
held on your node as plain files, for the friend to restore without lendisk
(LEND-6):

- **restic** (and generic buckets): one file per key at its key path, so
  the directory is a restic repository directory (`config`, `keys/`,
  `snapshots/`, `index/`, `data/<2 hex>/`).
- **Kopia**, with `--kopia-layout`: Kopia's filesystem repository layout.
  A blob ID of 20 characters or fewer is the file `<id>.f`; a longer one is
  `<first character>/<next 3>/<rest>.f` (`p8bf4fa…` is `p/8bf/4fa….f`), and
  `.shards` records that sharding: `{"default":[1,3],"maxNonShardedLength":20}`.

Tombs, `.sgwtmp/` and the xattrs are left out. Files are 0600 and
directories 0700. The directory must be new or empty. Exports from several
friends copied into one directory are one repository: the same key always
has the same bytes, so the union is a plain copy. The
[restore guide](restore-without-lendisk.md) has the commands.

The export reads the store directory directly and needs no daemon; run it
with lendisk stopped or running.

## What a friend can see of your backups

A friend who holds your replica can read their own disk. The backup tool
encrypts and authenticates everything before it leaves your machine, so
they cannot read your files, learn their names, or change them without
the backup tool noticing (SEC-1). They can observe, and lendisk accepts
leaking (SEC-2):

- the **sizes and number** of your objects;
- the **timing** of your backups;
- your **IP address**, unless the connection goes through a relay;
- **how many buckets** you keep with them;
- **which backup tool** wrote each bucket: restic's and Kopia's key layouts
  are recognisable;
- your **total repository size**, rounded up to a whole GiB, which friends
  tell each other for capacity planning (LEND-7).

They do **not** learn your buckets' names: they see only sealed names
(SEC-16). A **generic** bucket is not encrypted by its tool: friends holding
it can read it.

## How much disk

Let S be your repository size (after the backup tool's deduplication and
compression, with the history your `forget` policy keeps) and c the
bucket's `copies` (default 2).

- **Lend c × S, plus 20%**: 2.4 × S at two copies. The 20% is the margin
  for tombs: pruning rewrites packs, and the old ones stay at friends for
  the retention window.
- **Your disk holds about 3.4 × S** at two copies: your local copy (S) plus
  what you lend.
- **You are fully protected while S is at most your friends' backups
  combined, divided by c.** Past that, `lendisk status` says so, and an
  overflow bucket, or another friend, covers the rest.
- Each friend also holds all of your metadata, about 1% of S.

DESIGN.md §4 has the derivation and worked examples.
