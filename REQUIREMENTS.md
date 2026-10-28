# lendisk — requirements

Status: draft v0.7, 2026-10-28. Split out of SPEC v0.6. The design
decisions that were §3 of that document, and the implementation detail
that was mixed into its §4, now live in DESIGN.md.

## 0. How to read this document

This document is the contract: what the product does for its users,
stated so that it stays true whichever components, protocols or
languages are chosen to build it. DESIGN.md records those choices and
may change freely. A change to this document is a change to the
product, made deliberately and with a new version number.

The test for every sentence here: would it have to change if a
different library, transport, storage engine, wire protocol or language
were chosen? If yes, it does not belong here. Product names appear only
where supporting that product is itself a requirement (restic, Kopia,
S3-compatible storage providers, Linux).

IDs are stable; they are never renumbered or reused. "Shall" is mandatory
for v1, "should" is desired, "may" is optional. Each requirement has an
acceptance criterion (AC) where one is meaningful. *Rationale* lines
explain why a requirement exists; they are not themselves requirements.

Command names (`lendisk init`, `lendisk status`) describe the proposed
CLI surface. The behaviour, the meaning of flags and the exit codes are
the requirement; the binary name is a working title (DESIGN.md §1).

## 1. Purpose

lendisk lets a small group of people who know each other lend each other disk
space for offsite backups. Each participant runs one node. A backup tool
(restic, Kopia, or anything that speaks S3) talks to the node as if it were
an ordinary S3 endpoint; restic may also use its REST protocol. The node
keeps a local copy of the repository and replicates it to the peers who
have lent you space, over an overlay network that works behind NAT.

The person holding your copy never needs to be trusted with your data: the
backup tool encrypts and authenticates everything before it leaves your
machine. lendisk only has to solve the remaining problems: availability,
replication, abuse of the host, and getting your data back after you lose
everything.

Original requirements this document grew from:

1. Point an unmodified restic client at the service.
2. Offsite backup without trusting whoever holds the remote copy.
3. Configure how much space you are willing to lend to other peers.
4. Peers sync over a NAT-traversing overlay, like Tailscale.

## 2. Glossary

| Term | Meaning |
|---|---|
| **Node** | One lendisk daemon plus its local store, with one identity. A household NAS is one node. |
| **Identity** | A cryptographic keypair. The public key is the node ID. Derived from the recovery phrase. |
| **Peer** | Another node you have exchanged invites with. |
| **Borrower** | The role of a node storing its buckets on a peer. |
| **Lender** (host) | The role of a node storing a peer's buckets. Most nodes play both roles. |
| **Local store** | The node's own copy of its buckets, kept as plain files. One per node, never shared. |
| **Bucket** | One backup repository as the backup tool sees it: a namespace of keys in the local store. Formerly "repo". |
| **Tool profile** | Per-bucket setting (`restic`, `kopia`, `generic`) that selects validation rules and what "snapshot" means. |
| **Replica** | A copy of a bucket held by one lender or in one external S3 bucket. |
| **Replication set** | The replicas a bucket is replicated to. |
| **Offsite-complete** | A point in a bucket's write sequence up to which every object is acknowledged by at least `min_replicas` replicas. Tool profiles map this to "snapshot X is offsite". |
| **Object** | One key in a bucket with its bytes. For restic, a file such as `data/ab/abcd…`. |
| **Tombstone** | A deleted object the lender keeps for the retention window. |
| **Recovery phrase** | 24 words from which the identity is derived. |
| **Node manifest** | An encrypted record of a node's peers and buckets, stored on every lender for recovery. |
| **Lending store** | The lender's copy of borrowers' buckets, each reachable only by the borrower it belongs to, over the peer connection. |
| **Peer protocol** | Everything that crosses between two nodes: the replica operations (NET-7), challenges, manifest, quota and version exchange. |

## 3. Principles

These are requirements too. Each one shapes most of §4, so it is stated
once here and referenced from there.

### P-1. Friends only, by invitation

Peers are added only by exchanging an invite. There is no discovery of
strangers, no marketplace, no payments and no proof-of-storage
cryptography beyond what §4.6 specifies.

*Rationale:* an open network of strangers needs Sybil resistance,
economic incentives, abuse handling and legal cover for hosts. That is a
different product. Social accountability ("your server has been down
for a week") solves availability for a circle of 2–10 friends.

### P-2. Trust is pairwise, directional and never transitive

There is no group, no network and no shared membership list. Each node
holds a set of pairwise relationships, each one created by one invite and
each one pointing one way (lender → borrower). Nothing a node does is
visible to, or depends on, a node it has no relationship with.

Concretely, with four people A, B, C and D:

- A can lend to B and C and never hear of D. If B also lends to D, A's
  data and D's data sit apart on B, each reachable only with its owner's
  identity (PEER-8). A and D never connect, are never listed to each
  other, and cannot tell that the other exists.
- A lender never forwards. B does not replicate A's data onward to C or D,
  even if B trusts them. If A wants a second copy, A invites C directly.
  This rules out "re-replicate automatically when a peer disappears"; the
  replacement is an alert and a manual invite (OPS-2).
- A can lend to B without B lending to A. Mutual lending is two invites;
  `--mutual` creates both in one handshake but they stay two
  relationships that can be revoked separately.
- A's node manifest (ID-4) is encrypted to A's key and stored on A's
  lenders. It lists A's peers; B can see that a manifest exists and its
  size, not its contents.
- The only transitive exposure is the relay. If A reaches B through a
  relay D runs, D sees A's and B's IP addresses and traffic volume, and
  nothing else: the connection is encrypted end to end between A and B,
  and the backup tool's own encryption sits inside that. A controls
  which relays its node will use (NET-5a) and can refuse any relay it
  does not trust with that metadata.

For a fully mutual group of n friends this costs n(n−1)/2 `--mutual`
invites: ten for five people. That is accepted: a "group invite" would be
transitive trust by another name.

*Rationale:* a shared group key or a signed membership list held by an
admin makes one person's compromise everyone's problem and puts a "who
is in the group" decision in one person's hands.

### P-3. The lender is never trusted with the data

The backup tool encrypts and authenticates client-side. The product
relies on that and never weakens it: it never needs the backup tool's
password, never inspects object contents, and never asks the user to
trust a lender with anything but availability (SEC-1, SEC-2). Buckets
for tools that do not encrypt are outside the guarantee and are marked
as such (EP-8).

### P-4. "Backup finished" and "backup is offsite" are two events

The product never blurs them. A backup completes against the local copy
at local disk speed; the offsite state is tracked and reported
separately (§4.4). A lender being offline has no effect on a backup
(REPL-9).

### P-5. Zero lock-in on both sides

With no daemon running anywhere, the borrower's local copy and each
lender's replica are each a repository the backup tool opens directly
from disk (LEND-6, EP-15, scenario 7). A lender can hand you a disk. An
external S3 replica is likewise a repository the tool opens with no
lendisk installed (S3-2).

### P-6. Recovery needs the phrase, one lender's address and the backup tool's password

Nothing else needs to be kept or remembered. Everything else about a
node is rebuilt from what its lenders hold (ID-4, ID-5, NET-7a).

### P-7. Offsite data outlives the borrower's machine by the retention window

A lender never deletes an object immediately. Deletes become tombstones
and are purged after a retention window the lender controls. There is
no operation that purges early, so an attacker who owns the borrower's
machine can delete the local copy and queue deletes, but cannot destroy
offsite data within the window (RET-1, RET-2, SEC-4).

### P-8. No enforced reciprocity

A lender sets a quota per borrower at invite time. There is no ratio, no
credit and no accounting beyond showing each side what they give and get
(LEND-7). Friends sort this out themselves.

### P-9. v1 audience: self-hosting Linux users, CLI only

- A single binary for Linux, plus a container image for NAS appliances
  (COMPAT-2).
- CLI plus one configuration file (OPS-6). No GUI and no web UI in v1.
- macOS, Windows and FreeBSD are post-v1.

*Rationale:* the first users already run restic and a homelab. A GUI would
double the surface for no change in the core problem.

## 4. Requirements

### 4.1 Identity and recovery

- **ID-1** A node shall have exactly one cryptographic identity. The node
  ID is the public key, displayed in a human-readable encoding with a
  checksum. AC: two nodes initialised from the same phrase have the same
  ID.
- **ID-2** `lendisk init` shall generate a 24-word recovery phrase,
  derive the identity from it, show it once and require the user to
  confirm they wrote it down.
- **ID-3** The daemon shall store the private key encrypted at rest with a
  key kept in the config directory (mode 0600). The phrase itself is never
  stored.
- **ID-4** Every node shall maintain a node manifest containing its peer
  list (IDs and the hints needed to reach each peer), bucket list,
  replication sets and settings. The manifest is encrypted with a key
  derived from the identity and pushed to every lender whenever it
  changes.
- **ID-5** `lendisk recover` shall rebuild a node from (a) the recovery
  phrase and (b) the address of any one lender: a single string that
  carries the lender's key and whatever is needed to reach it. It shall
  fetch the manifest, re-establish all peers and list all buckets. AC: on
  a fresh VM with nothing but the phrase, one lender address and the
  restic password, `restic restore` of the latest snapshot succeeds.
- **ID-6** Recovery shall support read-through restore: `restic restore`
  against the recovered node fetches objects from a lender on demand, with
  no requirement to download the entire repository first. Fetched objects
  are cached locally.
- **ID-7** `lendisk recovery-sheet` shall print a page meant for paper:
  the phrase, the node ID, the addresses of all lenders, and the restore
  commands. It shall remind the user that the restic password is a
  separate secret. The sheet shall be regenerated (and the user alerted)
  whenever a lender's address changes.

### 4.2 Peers and invites

- **PEER-1** A lender shall create an invite with `lendisk invite --quota
  500G [--retention 30d]`. The invite is a single string containing the
  lender's address (its key and the hints needed to reach it), the
  offered quota and a single-use secret valid for a configurable time
  (default 7 days). The lender shall accept a connection from a node it
  does not know only while that node presents an unexpired invite.
- **PEER-2** A borrower shall redeem an invite with `lendisk peer add
  <invite>`. The two nodes complete a mutual handshake; afterwards each
  stores the other as a peer. AC: a replayed invite is rejected.
- **PEER-3** The peer relationship shall be directional. A single invite
  makes one side lender and the other borrower; mutual lending needs an
  invite in each direction. The CLI should offer `--mutual` to generate
  the reverse invite in the same handshake.
- **PEER-4** `lendisk peer list` shall show for each peer: role(s), quota
  given and used, quota received and used, reachability (direct/relayed/
  offline), last seen, last verified, protocol version.
- **PEER-5** Either side shall be able to end a relationship with
  `lendisk peer remove`. Removal by the lender gives the borrower a notice
  and starts the retention window; data is purged when it expires. Removal
  by the borrower deletes the replica after the window.
- **PEER-6** A lender shall be able to lower a borrower's quota. If the new
  quota is below current use, the lender's daemon stops accepting new
  objects and notifies both sides; nothing already stored is deleted.
- **PEER-7** A node shall support at least 20 peers and 20 buckets.
- **PEER-8** Each borrower's data on a lender shall be isolated from every
  other borrower's and from the lender's own, and addressable only with
  that borrower's identity. AC: peer A cannot list, read, delete or create
  objects in peer B's buckets, nor list any bucket on the lender beyond
  its own.
- **PEER-9** The binary shall support a lender-only profile (`lendisk init
  --lender-only`, or `role = "lender"` in config) that disables the
  tool-facing endpoints and the borrower-side machinery; the lender-facing
  service still runs. It is the install for the friend who contributes a
  disk and backs up nothing. AC: in this profile nothing listens on
  loopback and `lendisk status` shows only lent space.
- **PEER-10** A node shall never disclose its peer list, or the existence
  of any peer, to another peer. There is no peer discovery, no gossip and
  no "friends of friends". AC: with A→B and B→D relationships, A's node
  has no record of D and D's has none of A, in logs, status output, the
  persisted state or on the wire.
- **PEER-11** A lender shall never replicate a borrower's objects to any
  third node. Replication is initiated only by the owner of the data.
- **PEER-12** Each relationship shall be revocable independently of any
  other, including the two halves of a `--mutual` pair.

### 4.3 Endpoint and local store

- **EP-1** The node shall expose an S3-compatible endpoint to backup
  tools, reachable from the local machine only. AC: restic's and Kopia's
  S3 backends, `rclone` and the AWS CLI all work with path-style
  addressing; `restic check --read-data` and `kopia repository
  validate-provider` pass.
- **EP-2** Every successful object write and every successful delete
  through the endpoint shall be captured for replication. Parts of an
  unfinished multipart upload, aborted uploads and failed requests shall
  not. AC: capture adds less than 5% wall-clock overhead to a 10 GB
  `restic backup` and a 10 GB `kopia snapshot` compared with the same
  store without capture.
- **EP-3** Capture shall be crash-safe and exactly-once, and shall not
  buffer or re-read request bodies in the write path. AC: kill the
  daemon with `SIGKILL` at random points during a 10 GB backup, restart,
  and every object present on disk is replicated and none is replicated
  twice.
- **EP-4** The node shall also expose restic's REST protocol (v1 and v2
  listings), reachable from the local machine only, with basic-auth
  credentials generated at init, over the same local store. AC: restic's
  backend test suite passes against it; an object written via REST is
  visible via S3 and vice versa.
- **EP-5** Neither endpoint shall ever listen on a non-loopback
  interface.
- **EP-6** Buckets shall be created with `lendisk bucket add <name>
  --profile restic|kopia|generic --replicas peerA,peerB [--min-replicas
  N] [--mode full|evict]`. The command creates the bucket in the store,
  issues credentials scoped to it, and prints the configuration lines
  for the chosen tool. `min_replicas` defaults to 1. When a bucket has
  exactly one replica, `bucket add`, `status` and the daily summary shall
  say so and recommend a second (a friend or an external S3 bucket,
  §4.11); this nudge is suppressed once a second replica exists or the
  user sets `min_replicas = 1` explicitly.
- **EP-7** Tool profiles shall define: how to recognise snapshot or
  manifest keys (restic: `snapshots/*`; Kopia: `xn*`/`xs*` manifest
  blobs and `kopia.repository`), which keys are metadata and never
  evicted (restic: everything but `data/`; Kopia: everything but
  `p*`/`q*`/`s*` pack blobs), and whether the profile is inside the
  encryption guarantee (restic and kopia: yes; generic: no). Profiles
  are data, not code; adding one shall not require a release.
- **EP-8** `generic` buckets shall require `--i-understand-no-encryption`
  at creation and be marked in `status` as outside the trust guarantee.
- **EP-9** Two storage modes shall exist per bucket. In `full` (default)
  the local copy is complete and restores never need a peer. In `evict`,
  pack objects shall be removed from the local store only after
  `min_replicas` replicas have acknowledged them, and never while a tool
  lock is present (restic `locks/*`; Kopia has no server-side lock, so
  eviction waits for the bucket to be idle for a configurable interval).
  Metadata objects are never evicted.
- **EP-10** Reads of an evicted or missing object shall be served from a
  replica on demand through the endpoint, and the fetched object stored
  locally.
- **EP-11** Immutability policy: a write to a key that a replica has
  already acknowledged, with content whose hash differs, shall raise an
  alert, shall not be replicated, and shall be reported in `status` with
  the key and both hashes. The local write itself is allowed (the local
  store is the user's). Identical re-uploads are silently accepted.
- **EP-12** `lendisk init` shall verify that the store directory's
  filesystem supports what the store needs and has at least the
  configured free-space floor, and refuse otherwise with a message naming
  the filesystem and the missing property.
- **EP-13** The daemon should sustain at least 200 MB/s of writes from a
  backup tool on local NVMe, measured end to end.
- **EP-14** The daemon shall reconcile the store directory against its
  index periodically (default daily, and at startup): a walk of every
  bucket is compared with the index, files present but not indexed are
  enqueued as writes, and indexed keys with no file are enqueued as
  deletes. This covers anything written to the directory outside the
  daemon. Reconciled changes are counted in `status` so out-of-band
  traffic is visible.
- **EP-15** Writing to the store directory directly, with the daemon
  running, is unsupported and documented as such; EP-14 limits the
  damage. Reading it directly is always safe in `full` mode and is the
  lendisk-free restore path (scenario 7). Restores through the S3 or
  REST endpoint work in both modes with the daemon running; that is the
  normal path and needs no special mode.

### 4.4 Replication

- **REPL-1** Every object written to a bucket shall be queued for
  replication to each replica in the replication set, in the order it
  was written. Per-replica queues are strictly FIFO. Because restic and
  Kopia both write packs before indexes before the snapshot or manifest,
  FIFO alone keeps every replica consistent at every watermark. AC: a
  lender never holds a restic snapshot or Kopia manifest whose
  referenced packs and index it does not also hold.
- **REPL-2** Deletes shall be replicated as delete operations in the same
  queue, after any writes that preceded them.
- **REPL-3** The replication queue shall be persisted and survive
  restarts, crashes and days-long peer outages. In `full` mode the queue
  is unbounded; in `evict` mode the local disk holds every
  unacknowledged pack.
- **REPL-4** A snapshot shall be marked offsite-complete when it and every
  object queued before it are acknowledged by at least `min_replicas`
  replicas. `lendisk status` shall list each bucket's latest snapshot, its
  offsite-complete state, and the replication lag per replica.
- **REPL-5** `lendisk status --wait <bucket>` (exit code 0 when offsite-
  complete, non-zero on timeout) shall exist so that backup scripts can
  block on replication.
- **REPL-6** Transfers shall resume at the object level after an
  interruption and should resume at a byte offset for objects larger
  than 16 MiB.
- **REPL-7** The node shall support per-peer and global upload/download
  rate limits and a time-of-day schedule (e.g. "replicate 01:00–07:00").
- **REPL-8** Initial seeding shall be possible without the network:
  `lendisk export <bucket> <path>` writes the replica to a removable disk and
  `lendisk import` on the lender ingests it. Queued operations continue
  from where the export ended.
- **REPL-9** Replication shall never block the endpoint. A lender being
  offline shall have no effect on a backup to the local copy.
- **REPL-10** Every object sent to a replica shall carry a cryptographic
  hash of its content, computed by the borrower from the local copy as
  it sends. The replica verifies the received bytes against it before
  acknowledging and rejects a mismatch (LEND-4); the borrower records the
  hash on acknowledgement. The index of acknowledged hashes is what
  verification (VER-1, VER-2) and the immutability policy (EP-11) check
  against. The hash shall be computed the same way for friends and for
  external buckets.
- **REPL-11** Offsite-complete shall be tracked as a per-bucket,
  per-replica watermark in the write sequence. Tool profiles (EP-7) map
  watermarks to snapshot or manifest keys so `status` can say "snapshot
  abc123 is offsite at B and C"; for `generic` buckets `status` shows the
  timestamp of the last acknowledged write.

### 4.5 Lending and quotas

- **LEND-1** Quota shall be set per borrower in bytes, counting live
  objects plus tombstones. There should be an optional global cap for
  total lent space.
- **LEND-2** When a borrower reaches quota, or the lender's filesystem is
  below a configurable free-space floor (default 5%), the lender shall
  refuse new objects with an error the borrower can distinguish from
  every other failure. The borrower pauses that replica's queue, raises
  an alert and retries hourly. Nothing is deleted.
- **LEND-3** A borrower shall be able to see remaining quota at each lender
  (`lendisk peer list`).
- **LEND-4** A lender shall verify every received object against the hash
  the borrower sends with it (REPL-10), reject a mismatch, and reject a
  write to an existing key whose stored hash differs. It shall never need
  the backup tool's password and shall never inspect object contents
  beyond the hash.
- **LEND-5** A lender may rate-limit each borrower independently.
- **LEND-6** The lender's replica shall be a plain filesystem: each key
  stored at its path under a directory that belongs to that borrower and
  bucket, plus the tombstoned versions and an index of hashes. For a
  restic bucket this is a restic repository readable with
  `restic -r <path>`; for a Kopia bucket `lendisk export --kopia-layout`
  shall write the sharded layout Kopia's filesystem backend expects. The
  lender-only profile (PEER-9) uses the same single binary and the same
  store.
- **LEND-7** Capacity planning shall be built in. `lendisk init` shall
  ask for the expected repository size and intended number of lenders
  `k`, and suggest how much to lend in return. `lendisk status` shall
  show, in one place, local copy size, space consumed at each lender,
  space lent and used by each borrower, and the ratio consumed/lent. The
  user guide shall state the rule of thumb.

### 4.6 Deletion, retention and verification

- **RET-1** A lender shall never remove object data on a delete request.
  The object becomes a tombstone, which the lender removes after the
  retention window. Default 30 days; the lender sets a minimum, the
  borrower may request a longer window.
- **RET-2** There shall be no operation a borrower can invoke that purges
  a tombstone before the window expires: not a versioned delete, not a
  bulk or bucket-level operation, not an administrative call. AC: a
  borrower holding every credential its own node holds cannot cause
  offsite data loss within the window.
- **RET-3** `lendisk undelete <bucket> [--since <time>]` shall restore
  tombstoned objects to the live set at each replica, with the same
  command and behaviour for friends and external buckets, so that an
  attacker's `forget --prune` can be reversed.
- **VER-1** The borrower's daemon shall challenge each lender daily per
  bucket with a proof the lender cannot answer without holding the bytes:
  a random object, a random byte range and a fresh nonce; the lender's
  answer is checked against the local copy. A failed challenge raises an
  alert and marks the replica unverified.
- **VER-2** In `evict` mode, where no local copy exists, verification shall
  instead download a random sample of objects (default 0.1% of bytes per
  week, configurable) and check their hashes against the index recorded
  at acknowledgement (REPL-10).
- **VER-3** `lendisk verify <bucket> --peer <id> [--full]` shall run the
  challenge on demand, and `--full` shall check every object by hash.
- **VER-4** `lendisk status` shall show, per replica, when it was last
  successfully verified.
- **VER-5** The daemon shall check its own local store for bit-rot: a
  daily random sample (default 0.1% of bytes per week, configurable) is
  hashed and compared with the index; a lender does the same for each
  replica it holds. A mismatch raises an alert, and on the borrower
  side the object is re-fetched from a replica (EP-10).

### 4.7 Network

- **NET-1** All peer traffic shall be encrypted and mutually authenticated
  by node identity. A node shall only accept connections from known peers
  and from pending invites.
- **NET-2** Two nodes behind different NATs, including symmetric NAT on one
  side, shall be able to replicate. Direct connection is attempted first;
  relay is the fallback. AC: integration test with two nodes behind
  simulated NAT, with and without a relay reachable.
- **NET-3** A node with a static public address or port forward shall be
  dialled directly. Users may add explicit addresses to a peer record.
- **NET-4** Relay use shall be visible in status output, and the node
  should re-attempt a direct path periodically while relayed.
- **NET-5** Relays shall exist for peers that cannot connect directly.
  The project shall operate relays and publish their list; a node shall
  accept any other relay list instead; `lendisk relay` shall let a user
  host a relay on a VPS. The product shall not depend on relays run by
  any third party: it may use them as a last-resort bootstrap but shall
  work with them removed. AC: scenario 1 passes with the project relays
  as the only relays, and again with a single self-hosted relay.
- **NET-5a** A node shall use only relays from its own configured set,
  never a relay named in a peer's address hint unless the user allows
  it (`relays.accept_peer_hints = true`, default false). A relay sees IP
  addresses and traffic volume; which relays may see that is the user's
  decision, not a peer's.
- **NET-6** The peer protocol shall carry a version. A node shall
  interoperate with peers one minor version behind or ahead, and shall
  refuse with a clear error otherwise.
- **NET-7** A lender shall serve each borrower's replica over the peer
  connection with exactly the operations replication needs and no
  others: store an object (large objects in resumable parts), read an
  object or a byte range of it, check an object's existence and size,
  list objects and tombstoned versions, list and create the borrower's
  own buckets (creation counted against quota), delete (which only
  tombstones) and undelete. Every other operation shall be refused
  before it reaches storage. Beside these, the peer protocol shall
  provide: challenge (VER-1), manifest (ID-4, fetchable with nothing but
  the identity, before any bucket exists), quota (LEND-3) and version
  (NET-6). AC: the peer protocol specification fits on a few pages; the
  hostile-peer test (SEC-7) shows every operation outside this list
  refused.
- **NET-7a** Authentication between peers shall rest on node identity
  alone. No per-pair secret shall be stored on either side or exchanged
  between them. AC: `lendisk recover` (ID-5) reaches a lender's replica
  with nothing beyond the phrase and the lender's address; no per-pair
  credential appears in any file on either side.
- **NET-7b** `lendisk peer forward <peer> <addr>` should expose a friend's
  replica on a local address through the peer connection, so a backup
  tool on the borrower's machine can read it directly, with no lendisk
  replication code in the path (scenario 10).
- **NET-8** The overlay network shall sit behind an internal interface so
  that it can be replaced without touching replication, storage or the
  CLI.

### 4.8 Operations and observability

- **OPS-1** `lendisk status [--json]` shall report: identity, peers and
  reachability, each bucket's latest snapshot and offsite-complete state,
  replication lag and queue size, quota use in both directions, last
  verification, and pending alerts.
- **OPS-2** The daemon shall raise alerts for: lender unreachable longer
  than 24h (configurable), replication lag exceeding a threshold (default
  48h), a failed verification, quota above 90%, quota exhausted, retention
  purge due within 7 days after a peer removal, and a peer on an
  unsupported version.
- **OPS-3** Alerts shall be delivered through a configurable command hook
  (receives JSON on stdin) and a webhook URL. Built-in email or push is out
  of scope; the hook covers ntfy, healthchecks.io, mail, etc.
- **OPS-4** The daemon should expose Prometheus-format metrics on loopback.
- **OPS-5** Logs go to stdout/journald in structured form, with a
  configurable level.
- **OPS-6** Configuration shall be one file; every setting shall have a
  CLI flag and env var equivalent. The data directory layout shall be
  documented.
- **OPS-7** The daemon shall run as an unprivileged user and should stay
  under 256 MiB RSS when idle with a 1 TiB repository and 10 peers.
- **OPS-8** Upgrades shall migrate state automatically and shall support
  downgrade by one minor version.

### 4.9 Security

- **SEC-1** The lender shall be unable to read backup contents, learn file
  names, or alter data undetected. (Provided by the backup tool; lendisk
  shall not weaken it, e.g. by requiring the restic password anywhere.)
- **SEC-2** Metadata a lender can observe, and that the product accepts
  leaking: object sizes and counts, timing of backups, borrower IP address
  unless relayed. This shall be documented for users.
- **SEC-3** Secrets on disk shall be limited to: the identity key, the
  REST endpoint credentials, and per-bucket endpoint credentials, all
  mode 0600 in the config directory. No embedded component shall keep a
  credential store of its own.
- **SEC-4** A compromised borrower machine shall not be able to cause
  offsite data loss within the retention window (RET-2) nor to read
  another borrower's data on a shared lender (PEER-8).
- **SEC-5** A malicious borrower shall not be able to exceed quota, fill
  the lender's disk below the floor (LEND-2), or write outside their
  directory (key names are validated).
- **SEC-6** Invite secrets shall be single-use and time-limited (PEER-1).
- **SEC-7** Compromise of one peer shall affect only the relationships
  that peer is part of. With A→B, A→C and B→D: a compromised B can lose
  or withhold A's replica at B (detected by VER-1) and sees A's traffic
  metadata, and nothing else. It cannot reach A's replica at C, cannot
  impersonate A to C, and cannot cause C or D to learn anything about A.
  AC: a test with a hostile B node exercising every operation of the
  peer protocol, and every operation the underlying store could expose,
  against A and C; everything outside the NET-7 list is refused.
- **SEC-8** The daemon shall expose no listener beyond those this
  document names: the tool-facing endpoints on loopback (EP-1, EP-4,
  OPS-4) and the lender-facing service on the peer connection (NET-7).
  Administrative, web or other auxiliary listeners of any embedded
  component shall be disabled. AC: `ss -lntp` shows no lendisk listener
  on a non-loopback address.
- **SEC-9** The lender-facing surface shall be deny-by-default: the
  decorating layer admits the NET-7 list and nothing else, and every
  operation of the underlying store shall be classified as admitted,
  refused or unreachable. AC: the classification is checked into the
  repository, a test asserts it is complete, and it is re-reviewed on
  every upgrade of the store.

### 4.10 Compatibility

- **COMPAT-1** Supported backup tools in v1: restic and Kopia, each at
  the latest release and the two before it, tested in CI through both
  the S3 endpoint and (restic) the REST endpoint. Duplicacy and rclone
  should work via `generic` and are smoke-tested.
- **COMPAT-2** Supported platforms for v1: Linux amd64 and arm64, as a
  single static binary with a systemd unit, and as a container image.
  ARMv7 should work.
- **COMPAT-3** An existing repository (restic directory or rest-server
  data, Kopia filesystem repository) shall be importable with
  `lendisk bucket import <name> <path>`, which uploads it into the store
  and marks nothing as acknowledged.
- **COMPAT-4** The store directory layout shall be documented: one file
  per key at its key path under the bucket's directory, and the name and
  purpose of every housekeeping directory, so that users and other tools
  can read it.

### 4.11 External S3 replicas

An external S3-compatible bucket is a replication target alongside
friends and counts towards `min_replicas`. The borrower shall use the
same replica abstraction, resume behaviour and undelete for both kinds.
The requirements in this section that are specific to **external**
buckets are credentials (S3-1, S3-5, S3-8), provider-side retention
(S3-4) and cost (S3-7); for friends, quota, retention and verification
follow §4.5 and §4.6 because the lender enforces them.

- **S3-1** `lendisk replica add s3 <name> --endpoint … --bucket …
  --prefix …` shall register a bucket as a replication target; it then
  appears in `--replicas` lists and in `status` like a peer.
  Credentials are stored in the config directory (mode 0600) and in the
  node manifest (ID-4), so recovery through a friend also recovers
  bucket access.
- **S3-2** The replica layout in the bucket shall be the backup tool's
  standard layout under the prefix. AC: `restic -r
  s3:<endpoint>/<bucket>/<prefix> snapshots` works with no lendisk
  installed.
- **S3-3** Uploads shall carry a content checksum the provider verifies,
  so the store rejects corrupted transfers. Large objects shall be
  uploaded in resumable parts.
- **S3-4** Retention at an external bucket shall be enforced by the
  provider, using object versioning and a lifecycle rule that expires
  non-current versions after the retention window. `replica add` shall
  check that versioning is enabled and warn if not; it shall offer to
  create the lifecycle rule when the credential allows it. `undelete`
  shall restore objects deleted within the window (RET-3). For providers
  without versioning, the daemon shall fall back to lendisk-side
  tombstones: deletes are recorded locally and issued to the bucket only
  after the retention window. This protects against accidental deletion
  but not against a compromised borrower, and `status` shall label such
  replicas "retention: client-side".
- **S3-5** The documentation shall tell the user to create a credential
  with put/get/list but no delete permission, and `replica add` shall
  test for and report delete permission, recommending it be removed.
  With such a credential, restic's `prune` through lendisk produces
  tombstones that only the lifecycle rule ever purges.
- **S3-6** Verification shall compare the provider's stored checksums
  against local, and download a random sample as in VER-2. A bucket
  cannot answer a challenge (VER-1), and this document accepts that this
  trusts the provider's checksum.
- **S3-7** There is no lender-side quota. The user shall set `max_bytes`
  on the replica; the daemon raises an alert at 90% and pauses the queue
  at 100%, as in LEND-2. `status` shall show an estimated monthly cost if
  the user provides a price per GB.
- **S3-8** `lendisk recovery-sheet` (ID-7) shall include the bucket
  endpoint and prefix, and state that the access key is needed; it shall
  not print the secret key unless `--with-secrets` is given.
- **S3-9** An existing restic repository already in a bucket should be
  adoptable as a replica (`replica add s3 … --adopt`): the daemon lists
  it, treats its objects as acknowledged, and reconciles against the
  local copy.

## 5. Out of scope for v1

- Open network, discovery of strangers, payments or credits.
- Erasure coding across peers.
- GUI or web UI.
- macOS, Windows, FreeBSD builds.
- sftp as an endpoint.
- Object storage behind a friend's lender.
- Backup tools without an S3 backend (Borg, rdiff-backup).
- Tool profiles beyond restic and Kopia in CI (others run as
  `generic`).
- Multiple identities per node, or one identity across several nodes.
- Built-in email or push notifications.
- Deduplication across borrowers on a lender (repositories are encrypted
  per repository; nothing to dedupe).

## 6. Release gates

These are acceptance tests of the product as a whole. Gates on the
current design (component behaviour, API assumptions) are in DESIGN.md
§5 and may come and go with the design; these do not.

- **G-1 Field connectivity.** Before v1 ships, run a field test with at
  least 10 households on consumer NAT, including CGNAT and at least one
  symmetric NAT. Gate: ≥80% of peer pairs reach a direct path, 100% can
  replicate via relay, and a 100 GB initial seed completes unattended.
- **G-2 Long-lived operation.** Two nodes replicate for 72 hours, then
  sit idle for 7 days, then replicate again, with no restart on either
  side.
- **G-3 Small hardware.** On a Raspberry Pi 4 and a Synology-class NAS,
  measure idle and active RSS (OPS-7), end-to-end write throughput
  (EP-13), listing time at 500k objects in one bucket, and behaviour on
  a full disk (LEND-2).

## 7. Acceptance scenarios

1. **First backup.** Two nodes, A behind NAT at home, B behind NAT at a
   friend's. B invites A with 500 GB. A adds bucket `laptop --profile
   restic` with replica B. `restic backup` on A finishes at local disk
   speed. Within the bandwidth available, `lendisk status` shows the
   snapshot offsite-complete.
2. **Friend offline.** B is powered off for three days. `restic backup` on
   A keeps working. Status shows lag; at 24h an alert fires. When B
   returns, the queue drains and the snapshot becomes offsite-complete.
3. **Total loss.** A's house burns down. On a new machine: `lendisk
   recover` with the phrase and B's address from the paper sheet;
   `restic -r rest:... restore latest` succeeds via read-through without
   downloading the whole repository.
4. **Ransomware.** An attacker on A runs `restic forget --keep-last 0
   --prune` and wipes A's disk. Within 30 days, A recovers as in scenario 3
   and runs `lendisk undelete laptop`; all snapshots are back.
5. **Bad lender.** B silently deletes half of A's packs. Within a day, the
   daily challenge fails and A is alerted that the replica is unverified.
6. **Quota.** A exceeds 500 GB at B. The local backup still succeeds;
   status shows replication paused for quota; B raises the quota and
   replication resumes without intervention on A.
7. **Zero lock-in.** With both daemons stopped: on B, `restic -r <the
   directory holding A's laptop replica> snapshots` works on the plain
   files; on A, `restic -r <A's local store>/laptop snapshots` works the
   same way. No service is running on either side.
8. **Partial trust.** A lends to B and C; B lends to D; A and D have no
   relationship. D cannot connect to A (rejected before any application
   data is exchanged), cannot list or read A's data on B, and does not
   appear anywhere in A's state. A's `peer list` shows B and C only.
   Revoking A→B leaves A→C untouched and B's replica of A's data is
   purged after the retention window.
9. **Kopia.** A creates bucket `photos --profile kopia`, runs `kopia
   repository create s3 …` against the S3 endpoint and takes a snapshot.
   Status shows the manifest offsite-complete at B. Kopia maintenance
   deletes old packs; B tombstones them. A restores a file on a fresh
   machine via read-through after `lendisk recover`.
10. **Restore straight from a friend.** On A, `lendisk peer forward B
    127.0.0.1:9100` then `restic -r s3:http://127.0.0.1:9100/laptop
    snapshots` lists A's snapshots held at B. B's daemon refuses `restic
    prune`'s attempt to remove tombstoned data (RET-2) and refuses every
    operation outside the NET-7 list (SEC-9).

## 8. Changes

- **v0.7 (2026-10-28)** Split from SPEC v0.6. Former design decisions
  D1, D1a, D2, D7, D8, D9 and the consequences of D3, D4 and D6 became
  principles P-1 to P-9. Implementation detail removed from every
  requirement in §4 and moved to DESIGN.md §3; requirement IDs and
  meaning unchanged. Former Q-C, Q-E and the measurement part of Q-F
  became release gates G-1 to G-3; the remaining questions moved to
  DESIGN.md §5. Capacity planning (former §8) moved to DESIGN.md §4,
  since its numbers follow from the mirroring decision. Acceptance
  scenarios 8 and 9 put in numeric order.
