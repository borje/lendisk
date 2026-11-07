# lendisk — requirements

Status: draft v0.11.3, 2026-11-08. Split out of SPEC v0.6 as v0.7; v0.8
admits a storage engine with its own on-disk format (P-5); v0.8.1 makes
peer admission fail closed (SEC-10 to SEC-15); v0.9 admits an
S3-compatible service the user names as the store, holding the node's
own and its friends' buckets alike (EP-16); v0.9.1 reads third-party
credentials from the environment (SEC-3); v0.9.2 lets backup tools on
other machines on the local network use the node (EP-17); v0.9.3 hides
bucket names from lenders (SEC-16); v0.10 replaces per-bucket replica
lists with copies placed across a pool of friends, makes every
friendship one symmetric quota, and turns external S3 buckets into
overflow for what friends cannot hold; v0.10.1 corrects
contradictions in v0.10 found by a consistency review; v0.11 takes the
product down to what v1 needs: quotas are numbers people set, with no
negotiation, the local copy is always complete, an S3 service as the
node's own store is post-v1, lock files stay local, and the identity
key is a plain file; v0.11.1 (2026-11-06) makes the `copies = 1`
warning unconditional, in `bucket add` and `status` only, and not an
alert (EP-6); v0.11.2 gives a node a dial key per friend, derived
from the same phrase, to connect to its friends with (ID-1, SEC-10,
SEC-12, SEC-14); v0.11.3 lets a profile name the few keys its tool
rewrites in place, which replicate as rewrites while lenders keep the
replaced version (EP-7, EP-11, LEND-4, RET-1). The design
decisions that were §3 of that document, and the implementation detail
that was mixed into its §4, live in DESIGN.md.

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
an ordinary S3 endpoint. The node keeps a local copy of the repository
and spreads offsite copies of it over the friends you lend space to and
who lend space to you, over an overlay network that works behind NAT.
Your own disk is what you pay with: you lend roughly as much as your
copies take up at your friends.

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
| **Storage engine** | The S3 store that holds the node's local store and lending store: an unmodified, freely available open-source program started on the node's own directories, or, post-v1, an S3-compatible service the user names (EP-16). Either is readable without lendisk (P-5). The program is chosen in DESIGN.md. |
| **Local store** | The node's own copy of its buckets, held in the storage engine. One per node, never shared. |
| **Bucket** | One backup repository as the backup tool sees it: a namespace of keys in the local store. Formerly "repo". |
| **Tool profile** | Per-bucket setting (`restic`, `kopia`, `generic`) that classifies the tool's keys (EP-7) and says what "snapshot" means. |
| **Friendship** | The relationship between two peers who lend to each other. It has one quota, the same in both directions (P-8). |
| **Symmetric quota** | What each side of a friendship lends the other: the smaller of the two numbers the sides have set (LEND-8). |
| **Pool** | Everywhere a node can place its copies: its friends, plus any overflow targets. |
| **Overflow target** | An external S3-compatible bucket that holds only the copies a node's friends cannot hold (§4.11). Never a complete repository by design. |
| **Copies** | Per-bucket setting `copies` (default 2): how many distinct places in the pool must hold each object. A **copy** is one such instance, at one friend or one overflow target. |
| **Placement** | Which friends or overflow targets hold each object of a bucket. Metadata objects are placed everywhere; data objects on `copies` places (REPL-12). |
| **Replica** | What one friend or overflow target holds of a bucket: all its metadata and the data objects placed there. Since v0.10 a replica is not, by itself, a complete repository. |
| **Offsite-complete** | A point in a bucket's write sequence up to which every object is acknowledged by `copies` distinct places in the pool. Tool profiles map this to "snapshot X is offsite". |
| **Protectable size** | The largest repository a node's pool can hold at `copies` copies, given its friends' quotas and repository sizes (LEND-7). |
| **Object** | One key in a bucket with its bytes. For restic, a key such as `data/ab/abcd…`. |
| **Tombstone** | A deleted object the lender keeps for the retention window. |
| **Recovery phrase** | 24 words from which the identity is derived. |
| **Node manifest** | An encrypted record of a node's friends, overflow targets and buckets, stored on every lender for recovery. |
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
holds a set of pairwise friendships, each one created by one invite.
A friendship has two directions (A's data at B, B's data at A), and
each node is the only one that decides where its own data goes. Nothing
a node does is visible to, or depends on, a node it has no friendship
with.

Concretely, with four people A, B, C and D:

- A can be friends with B and C and never hear of D. If B is also
  friends with D, A's data and D's data sit apart on B, each reachable only with its owner's
  identity (PEER-8). A and D never connect, are never listed to each
  other, and cannot tell that the other exists.
- A lender never forwards. B does not replicate A's data onward to C or D,
  even if B trusts them. When B disappears, A's own node places the
  copies B held onto A's other friends (REPL-13); B is never asked to.
  A new invite is needed only when A's pool cannot hold its copies:
  fewer places than `copies`, or too little room even after the
  friends have raised their numbers (LEND-8) and overflow (OPS-2).
- Every friendship is mutual, with one quota for both directions (P-8).
  One invite creates it and one removal ends it. A node that backs up
  nothing yet still has symmetric friendships; it just leaves its half
  unused (PEER-9).
- A's node manifest (ID-4) is encrypted to A's key and stored on A's
  lenders. It lists A's friends; B can see that a manifest exists and
  its size, not its contents.
- The only transitive exposure is the relay. If A reaches B through a
  relay D runs, D sees A's and B's IP addresses and traffic volume, and
  nothing else: the connection is encrypted end to end between A and B,
  and the backup tool's own encryption sits inside that. A controls
  which relays its node will use (NET-5a) and can refuse any relay it
  does not trust with that metadata.

For a group of n friends who all lend to each other this costs
n(n−1)/2 invites: ten for five people. That is accepted: a "group invite" would be
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
at the speed of the node's own store, never waiting on a lender; the
offsite state is tracked and reported separately (§4.4). A lender
being offline has no effect on a backup (REPL-9).

### P-5. Zero lock-in on both sides

With no lendisk running anywhere, the borrower's local copy is readable
by the backup tool through the storage engine alone: an unmodified,
freely available open-source program, started on the same directories,
with the steps in the user guide, or, where the store is an S3 service
the user named (EP-16), any S3 client with the store's credential
(LEND-6, EP-15, scenario 7). Each lender's replica is readable the
same way, and holds every metadata object of the bucket and the data
objects placed there (REPL-12).

The offsite copies together are a complete repository: with `copies`
= c and the bucket spread over N friends and overflow targets, the
replicas of any N − c + 1 of them (any one, when N ≤ c) hold every
object. Copying their
keys into one place (a plain union: the same key always has the same
bytes, EP-11) gives a repository the backup tool opens with no lendisk
installed. Each replica can be written out as plain files in the
backup tool's own on-disk layout (`lendisk export`, REPL-8, LEND-6),
so enough lenders can each hand you a disk and the union of those
disks is your repository. An overflow target's objects are in the
tool's layout too (S3-2).

*Rationale:* v0.7 required plain files readable with nothing running.
v0.8 relaxed this to admit a storage engine with its own on-disk
format, in exchange for quotas, indexed listings and integrity checks
the product would otherwise build itself (DESIGN.md D14). The promise
kept is that no lendisk code, and nothing proprietary, stands between
the user and their bytes. v0.9 admits an S3 service the user names as
the store, so that someone who already runs or pays for one keeps
their own and their friends' data in it; access is then standard S3
with the user's own credential, as S3-2 already promises for an
external replica. v0.10 gave up "one lender's replica is a complete
repository" so that a bucket's copies can be spread over a pool by
capacity instead of mirrored to named friends (REPL-12): a restore
without lendisk now needs N − c + 1 replicas instead of one. The
promise kept is the same: plain keys in the tool's own layout, readable
with free software and nothing of lendisk's.

### P-6. Recovery needs the phrase, one lender's address and the backup tool's password

Nothing else needs to be kept or remembered. Everything else about a
node is rebuilt from what its lenders hold (ID-4, ID-5, NET-7a). The
one lender's manifest names every other place in the pool; restoring
every object needs enough of them reachable (P-5).

### P-7. Offsite data outlives the borrower's machine by the retention window

A lender never deletes an object immediately. Deletes become tombstones
and are purged after a retention window the lender controls. There is
no operation that purges early, so an attacker who owns the borrower's
machine can delete the local copy and queue deletes, but cannot destroy
offsite data within the window (RET-1, RET-2, SEC-4).

### P-8. Reciprocity is pairwise, and paid in disk

Every friendship has one quota, the smaller of the two numbers its
sides have set, and each side lends the other exactly that much
(PEER-1, LEND-1, LEND-8). Each lender enforces it. Your disk is what
you pay with: with a repository of size S kept at c copies, you need
c × S at your friends and lend about c × S in return.

The balance is per friendship and nowhere else. There is no ratio across
friends, no credit, no ledger and nothing passed from one friendship to
another: "E lends to A so that A lends to F" would be transitive trust
by another name (P-2). Symmetry is on the agreed quota, not on what is
used: a friend who backs up less than the quota allows leaves the rest
of their half unused, and `status` shows it so the two can lower the
number (LEND-7).

The consequence for unequal sizes is accepted. A friend can usefully
hold at most one copy of your repository, and a friend whose own backup
is small has no use for much of your disk, so a fair quota between you
is small; a larger one costs that friend disk it gets nothing for. A
node is
fully protected at c copies only while its repository is at most about
1/c of its friends' repositories combined (LEND-7). Beyond that it
needs more or bigger friends, or an overflow target that costs money
instead of disk (§4.11).

*Rationale:* v0.9 left reciprocity to the friends. Making each
friendship one number that both sides lend keeps the accounting inside
the pair, where P-2 wants it, and makes "you pay with your own disk"
automatic: what you lend in total equals what you get.

### P-9. v1 audience: self-hosting Linux users, CLI only

- A single service for Linux, with every binary it needs shipped
  together, plus a container image for NAS appliances (COMPAT-2).
- CLI plus one configuration file (OPS-6). No GUI and no web UI in v1.
- macOS, Windows and FreeBSD are post-v1 as platforms for the node. A
  backup tool on any operating system may use a node over the local
  network (EP-17).

*Rationale:* the first users already run restic and a homelab. A GUI would
double the surface for no change in the core problem.

## 4. Requirements

### 4.1 Identity and recovery

- **ID-1** A node shall have exactly one cryptographic identity. The node
  ID is the public key, displayed in a human-readable encoding with a
  checksum. The node serves its friends with the identity key and
  connects to each friend with a dial key per friend, derived from the
  same phrase and that friend's ID. A dial key is not a second
  identity: the friend learns it when they become friends (PEER-2),
  and it stands for the node's identity with that friend alone. AC: two
  nodes initialised from the same phrase have the same ID and the same
  dial key for each friend.
- **ID-2** `lendisk init` shall generate a 24-word recovery phrase,
  derive the identity from it, show it once and require the user to
  confirm they wrote it down.
- **ID-3** The daemon shall store the private key as a file in the
  config directory, mode 0600, readable by the service user alone. The
  phrase itself is never stored. (Until v0.11 the key was encrypted
  with a second key kept beside it, which protected nothing.)
- **ID-4** Every node shall maintain a node manifest containing its peer
  list (IDs and the hints needed to reach each peer), its overflow
  targets, its bucket list with each bucket's `copies`, and settings.
  The placement of objects (REPL-12) is not in the manifest; it is
  rebuilt by listing the replicas. The manifest is encrypted with a key
  derived from the identity and pushed to every lender whenever it
  changes.
- **ID-5** `lendisk recover` shall rebuild a node from (a) the recovery
  phrase and (b) the address of any one lender: a single string that
  carries the lender's key and whatever is needed to reach it. It shall
  fetch the manifest, re-establish all peers and overflow targets, list
  all buckets and rebuild the placement from every replica's list of
  what it holds (NET-7). AC: on
  a fresh VM with nothing but the phrase, one lender address and the
  restic password, with the bucket spread over three friends at two
  copies and one of them offline, `restic restore` of the latest
  snapshot succeeds.
- **ID-6** Recovery shall support read-through restore: `restic restore`
  against the recovered node fetches each object on demand from any
  reachable place that holds it, with no requirement to download the
  entire repository first. Fetched objects are cached locally.
- **ID-7** `lendisk recovery-sheet` shall print a page meant for paper:
  the phrase, the node ID, the addresses of all lenders and overflow
  targets, the restore commands, for each bucket the name it has at its
  lenders (SEC-16), and how many of the replicas a restore without
  lendisk needs (N − c + 1, P-5). It shall remind the user that the restic password is a
  separate secret. The sheet shall be regenerated (and the user alerted)
  whenever a lender's address changes.

### 4.2 Peers and invites

- **PEER-1** A node shall create an invite with `lendisk invite --quota
  100G [--retention 30d]`. The invite is a single string containing the
  inviting node's address (its key and the hints needed to reach it),
  the number the inviting node lends (LEND-8) and a single-use secret valid for a
  configurable time (default 7 days). The node shall accept a connection
  from a node it does not know only while that node presents an
  unexpired invite.
- **PEER-2** The invited node shall redeem an invite with `lendisk peer
  add <invite>`, which shows the inviter's number and asks the user for
  their own, the same by default. The two nodes complete a mutual
  handshake; afterwards each stores the other as a friend, lender and
  borrower at once, with the smaller of the two numbers as the quota
  (LEND-8). AC: a replayed invite is rejected.
- **PEER-3** Every relationship shall be a friendship: mutual, with one
  quota that each side lends the other (P-8, LEND-8). There are no one-way
  relationships. Before v0.10 a relationship was directional and
  `--mutual` created two; both are gone.
- **PEER-4** `lendisk peer list` shall show for each friend: the quota,
  each side's number where they differ, how much of the quota each side uses, reachability (direct/relayed/offline),
  last seen, last verified, protocol version, and the friend's
  repository size as reported for LEND-7.
- **PEER-5** Either side shall be able to end a friendship with
  `lendisk peer remove`. Each side then holds the other's replica for
  the retention window and purges it when the window expires; each side
  is notified, and each side's node re-places the copies the other held
  (REPL-13).
- **PEER-6** Either side shall be able to change its own number for a
  friendship at any time (LEND-8). Lowering it below the other side's
  lowers the quota at once in both directions; raising it takes effect
  when the other side's number allows. If the new quota is below current use on either
  side, that side's node stops accepting new objects from the other and
  notifies both; nothing already stored is deleted, and the borrower's
  node re-places what no longer fits (REPL-13).
- **PEER-7** A node shall support at least 20 peers and 20 buckets.
- **PEER-8** Each borrower's data on a lender shall be isolated from every
  other borrower's and from the lender's own, and addressable only with
  that borrower's identity. AC: peer A cannot list, read, delete or create
  objects in peer B's buckets, nor list any bucket on the lender beyond
  its own.
- **PEER-9** The binary shall support a lender-only profile (`lendisk init
  --lender-only`, or `role = "lender"` in config) that disables the
  tool-facing endpoints and the borrower-side machinery; the lender-facing
  service still runs. It is the install for a node with no buckets: the
  friend who contributes a disk and backs up nothing yet. Its
  friendships are symmetric like any other (PEER-3); the space its
  friends lend it back stays unused until it adds a bucket, which
  needs no change to any friendship. AC: in this profile nothing
  listens on loopback and `lendisk status` shows lent space and the
  unused quota received.
- **PEER-10** A node shall never disclose its peer list, or the existence
  of any peer, to another peer. There is no peer discovery, no gossip and
  no "friends of friends". AC: with friendships A–B and B–D, A's node
  has no record of D and D's has none of A, in logs, status output, the
  persisted state or on the wire.
- **PEER-11** A lender shall never replicate a borrower's objects to any
  third node. Replication is initiated only by the owner of the data.
- **PEER-12** Each friendship shall be revocable independently of any
  other. A friendship is revoked as a whole; its two directions are not
  revoked separately (PEER-3).

### 4.3 Endpoint and local store

- **EP-1** The node shall expose an S3-compatible endpoint to backup
  tools, reachable from the local machine, and from other machines only
  where the user enables it (EP-17). AC: restic's and Kopia's
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
- **EP-4** Removed in v0.8. It required a restic REST endpoint beside
  the S3 one; restic's S3 backend covers every use and the second
  endpoint added a credential, a listener and a test matrix for nothing.
  The ID is not reused.
- **EP-5** The endpoint shall listen on loopback only, unless the user
  configures a network address for it (EP-17). No default, upgrade or
  migration shall add a non-loopback listener the user did not
  configure. AC: with no EP-17 address configured, `ss -lntp` shows the
  endpoint on loopback only, on a fresh install and after an upgrade.
- **EP-6** Buckets shall be created with `lendisk bucket add <name>
  --profile restic|kopia|generic [--copies N]`. The
  command creates the bucket in the store, issues credentials scoped to
  it, and prints the configuration lines for the chosen tool. The user
  does not name the friends: copies are placed across the whole pool
  (REPL-12). `copies` defaults to 2 and can be changed later; a change
  places or removes copies to match (REPL-13). With `copies = 1`,
  `bucket add` and `status` shall say that one lost friend loses data
  and recommend 2, however `copies` was set; it is not an alert. When
  the pool has fewer than
  `copies` places, `bucket add` shall say so (OPS-2).
- **EP-7** Tool profiles shall classify every key a tool writes into
  one of four classes, and say whether the tool is inside the
  encryption guarantee:
  - **snapshot keys**, whose arrival means a backup is complete and
    which `status` names (restic: `snapshots/*`; Kopia: the index blobs
    `n*` and `xn*`, since Kopia keeps its manifests inside pack blobs
    and a snapshot exists once the index that references it does);
  - **metadata keys**, placed at every friend (REPL-12) (restic:
    `config`, `keys/*`, `index/*`, `snapshots/*`; Kopia:
    `kopia.repository`, `kopia.blobcfg`, `kopia.maintenance`, the
    index and epoch blobs `n*` and `x*`, and the session blobs `s*`);
  - **data keys**, placed at `copies` places (restic: `data/*`; Kopia:
    the pack blobs `p*` and `q*`);
  - **local-only keys**, stored and served but never replicated,
    because a replica has no use for them and a tool that reads a
    replica makes its own (restic: `locks/*`; Kopia: `_log*`).

  A profile may also name **rewritable keys**: the few keys its tool
  rewrites in place in normal use (restic: `config`, rewritten by
  `restic migrate`; Kopia: `kopia.repository`, `kopia.blobcfg` and
  `kopia.maintenance`, the last after every maintenance run). A
  rewritable key keeps its class. A rewrite of one is replicated like a
  new write and raises no alert (EP-11), and each lender keeps the
  version it replaces as a tombstone (LEND-4, RET-1). A key no rule matches is data. The `generic` profile has no snapshot,
  metadata or local-only keys: everything is data. restic and kopia are
  inside the encryption guarantee; generic is not (EP-8). Profiles are
  data, not code; adding one shall not require a release.
- **EP-8** `generic` buckets shall require `--i-understand-no-encryption`
  at creation and be marked in `status` as outside the trust guarantee.
- **EP-9** The local copy shall be complete: a restore never needs a
  peer, and the local disk holds every object until the backup tool
  deletes it. An `evict` mode, in which data objects leave the local
  store once `copies` places have acknowledged them and come back on
  demand (EP-10), is specified for a later version and is not in v1;
  what it would need (eviction only while no tool lock is present,
  metadata never evicted, verification by sampling) is kept in
  DESIGN.md as a post-v1 note. The ID is kept for it.
- **EP-10** Reads of a missing object (after recovery, ID-6, or after
  a bit-rot re-fetch, VER-5) shall be served on demand through the
  endpoint from any reachable place that holds it, and the fetched
  object stored locally.
- **EP-11** Immutability policy: a write to a key that a replica has
  already acknowledged, with content whose hash differs, unless the key
  is rewritable in its bucket's profile (EP-7), shall raise an
  alert, shall not be replicated, and shall be reported in `status` with
  the key and both hashes. The local write itself is allowed (the local
  store is the user's). Identical re-uploads are silently accepted.
- **EP-12** `lendisk init` shall verify that the store directory's
  filesystem supports what the store needs and has at least the
  configured free-space floor, and refuse otherwise with a message naming
  the filesystem and the missing property. Post-v1, for a store that is an S3
  service (EP-16), `init` shall instead verify that the service is
  reachable with the credential, determine which of the properties the
  daemon relies on it provides, record the result and show it in
  `status`.
- **EP-13** The daemon should sustain at least 200 MB/s of writes from a
  backup tool on local NVMe, measured end to end.
- **EP-14** The daemon shall reconcile the store's listing against its
  index periodically (default daily, and at startup): a listing of
  every bucket is compared with the index, objects present but not
  indexed are enqueued as writes, and indexed keys with no object are
  enqueued as deletes. This covers anything that reached the store
  outside the daemon's capture path. Reconciled changes are counted in
  `status` so such traffic is visible.
- **EP-15** Touching the storage engine's directories directly, with or
  without the daemon running, is unsupported and documented as such;
  EP-14 limits the damage. The lendisk-free restore path is the storage
  engine alone, post-v1 any S3 client against a named service (EP-16), or an
  export (P-5, LEND-6, scenario 7). Restores through the S3 endpoint
  with the daemon running are the normal path and need no special mode.
- **EP-16** *Post-v1.* The store may be an S3-compatible service the user names
  (`lendisk init --store s3 --endpoint … --bucket …`, or `[store]` in
  config) instead of a directory on the node. One store holds the
  node's own buckets and every bucket it lends (LEND-6): what the user
  runs or pays for is where their friends' data goes too. `init` and
  `store set` shall warn when the service is not on the local network,
  since backups then run at the speed of that link, not of local disk
  (P-4, scenario 1). Where the service lacks a property the lender's
  guarantees rest on (versioning, object lock, checksum verification),
  the daemon shall provide it itself and `status` shall label the
  weaker mode, as S3-4 does for overflow targets. The borrower never
  holds the lender's store credential (PEER-8, SEC-3). AC: with the
  store on an S3 server on the LAN, scenarios 1, 7 and 10 pass, with
  scenario 7's lendisk-free read done by any S3 client against that
  server.
- **EP-17** The user may configure the endpoint to listen on one or more
  network addresses in addition to loopback (`endpoint.listen`, with
  flag and env var equivalents, OPS-6), so that backup tools on other
  machines on the local network use the node. Each must be a specific
  address or interface; a wildcard address is refused. On a
  non-loopback address:
  - the endpoint shall be able to serve TLS, with a certificate the
    user supplies or one the node generates, in which case it tells
    the user what the backup tool needs in order to trust it. TLS is
    the default; the user may turn it off per address. With TLS on, a
    plaintext request is refused;
  - with TLS off, configuring the address shall warn that request
    metadata and object contents cross the network unencrypted, and
    that only restic and Kopia buckets are protected by the backup
    tool's own encryption (EP-8); `status` shall label the listener
    "plaintext";
  - every request shall carry a valid per-bucket credential (EP-6), as
    on loopback; there is no anonymous access;
  - `bucket add` shall print the configuration lines for the network
    address as well as for loopback, and `status` shall list every
    non-loopback listener.

  The setting is refused in the lender-only profile (PEER-9). A backup
  from another machine finishes at the speed of that machine's link to
  the node rather than at local disk speed; it is still a backup to the
  local copy, and offsite state is tracked as for any other (P-4).
  AC: scenario 11 passes with TLS on and with TLS off; with TLS on a
  plaintext request is refused; on either, a request without a valid
  bucket credential is refused.

  *Rationale:* the typical node is a household NAS, and the machines
  that need backing up are the laptops around it. A tunnel to the
  node's loopback works but has to be up for every scheduled backup.
  Enabling a network address exposes the local copy to any machine on
  that network holding a bucket credential; it does not weaken the
  offsite guarantees, which rest on the lenders (P-7, RET-2). TLS is
  not mandatory because restic and Kopia already encrypt and
  authenticate what they store, the request signature never carries
  the secret key, and on a home network a certificate every client
  has to be told to trust is often more friction than the threat
  warrants; the user decides, and `status` keeps the choice visible.

### 4.4 Replication

- **REPL-1** Every object written to a bucket shall be queued for
  replication to the places its placement chose (REPL-12), in the order
  it was written. Per-place queues are strictly FIFO. Because no single
  place holds every data object, FIFO alone no longer keeps the copies
  consistent; in addition, a metadata object (as the tool profile
  defines it, EP-7: restic indexes and snapshots, Kopia index and
  manifest blobs) shall be sent to any place only after every data
  object written before it has been acknowledged by every place it is
  placed at: `copies` places, or fewer while the pool lacks room for
  them (REPL-12), in which case `status` shows those objects as below
  `copies`. Restic and Kopia both write packs before indexes before the
  snapshot or manifest, so this keeps the pool consistent at every
  point. AC: with N places and `copies` = c, and every object at c
  places, the replicas of any N − c + 1 places together never hold a
  restic snapshot or Kopia manifest whose referenced packs and index
  they do not also hold; and with too little room for c, every place
  that has room still receives its data and metadata.
- **REPL-2** Deletes shall be replicated as delete operations to every
  place holding the object, in that place's queue after any writes that
  preceded them.
- **REPL-3** The replication queue shall be persisted and survive
  restarts, crashes and days-long peer outages. The queue is unbounded;
  the local copy holds every object (EP-9).
- **REPL-4** A snapshot shall be marked offsite-complete when it and every
  object queued before it are acknowledged by `copies` distinct places.
  `lendisk status` shall list each bucket's latest snapshot, its
  offsite-complete state, the replication lag per place, and how many
  bytes are held at fewer than `copies` places.
- **REPL-5** `lendisk status --wait <bucket>` (exit code 0 when offsite-
  complete, non-zero on timeout) shall exist so that backup scripts can
  block on replication.
- **REPL-6** Transfers shall resume at the object level after an
  interruption and should resume at a byte offset for objects larger
  than 16 MiB.
- **REPL-7** The node shall support per-peer and global upload/download
  rate limits and a time-of-day schedule (e.g. "replicate 01:00–07:00").
- **REPL-8** Initial seeding shall be possible without the network:
  `lendisk export <bucket> --for <peer> <path>` writes to a removable
  disk the objects placed at that friend (the bucket's metadata and its
  share of data objects), and `lendisk import` on the friend ingests
  it. Queued operations continue from where the export ended.
- **REPL-9** Replication shall never block the endpoint. A lender being
  offline shall have no effect on a backup to the local copy.
- **REPL-10** Every object sent to a replica shall carry a cryptographic
  hash of its content, computed by the borrower from the object's
  bytes: at the latest as it sends them, at the earliest as the backup
  tool wrote them. The replica verifies the received bytes against it
  before acknowledging and rejects a mismatch (LEND-4), so a copy that
  changed between hashing and sending is refused rather than
  replicated; the borrower records the hash on acknowledgement. The index of acknowledged hashes is what
  verification (VER-1, VER-2) and the immutability policy (EP-11) check
  against. The hash shall be computed the same way for friends and for
  external buckets.
- **REPL-11** Offsite-complete shall be tracked as a per-bucket
  watermark in the write sequence: the latest point up to which every
  object has `copies` acknowledgements. Per-place lag is tracked beside
  it for `status` and alerts. Tool profiles (EP-7) map the watermark to
  snapshot or manifest keys so `status` can say "snapshot abc123 is
  offsite at 2 copies"; for `generic` buckets `status` shows the
  timestamp of the last fully placed write.
- **REPL-12** Placement. Every metadata object of a bucket shall be
  placed at every friend in the pool, including friends added later,
  and at every overflow target that holds data of the bucket, before
  or together with its first data object there. Each data object shall
  be placed at `copies` distinct places, chosen by the node from its
  friends by free quota, preferring friends that are reachable. A
  friend with room counts as room even while unreachable, its copies
  waiting for it or going to another friend, until it has been
  unreachable past the REPL-13 threshold; from then on it is no longer
  room and gets no new copies. Overflow targets (§4.11) shall be
  chosen only for copies that no friend, reachable or not, has room
  for. Every friend is treated as an independent place; failure
  domains (two friends in one house) are out of scope for v1 (§5). The
  placement shall be recorded by the node and shall be recoverable from
  the replicas' lists (ID-5, NET-7). AC:
  with friends B, C and D at quotas 100, 55 and 55 GB, a 100 GB restic
  bucket at `copies = 2` is fully placed, with no data object twice at
  one friend, every metadata object at all three, and no friend above
  its quota.
- **REPL-13** Re-placement. The node shall move copies, never letting an
  object fall below the copies it has while doing so, when:
  - a friend is removed (PEER-5), lowers the quota below use (PEER-6),
    or fails verification (VER-1): its copies are placed elsewhere
    straight away;
  - a friend has been unreachable longer than a threshold (default 7
    days, configurable): its copies are placed elsewhere, and the
    friend's replica is trimmed to what it still holds by placement
    when it returns;
  - friends gain room (a quota raised, a friend added): copies held at
    an overflow target move to friends and are then deleted at the
    overflow target;
  - `copies` changes (EP-6).

  The bytes come from the local copy. Moves obey the rate limits and schedule
  (REPL-7). Re-placement is the owner's node acting on its own data; no
  lender ever sends a borrower's object to another node (PEER-11).
  AC: scenario 13.

### 4.5 Lending and quotas

- **LEND-1** Quota shall be set per friendship in bytes, and each side
  shall enforce it on what the other stores with it, counting live
  objects plus tombstones. There shall be a global cap for total lent
  space (`lending.max_total`), which the sum of a node's numbers (LEND-8)
  may not exceed.
- **LEND-2** When a borrower reaches quota, or the lender's filesystem is
  below a configurable free-space floor (default 5%), the lender shall
  refuse new objects with an error the borrower can distinguish from
  every other failure. The borrower stops placing new copies there,
  places them elsewhere in the pool where it can (REPL-12), raises an
  alert and retries hourly. Nothing is deleted.
- **LEND-3** A borrower shall be able to see remaining quota at each lender
  (`lendisk peer list`).
- **LEND-4** A lender shall verify every received object against the hash
  the borrower sends with it (REPL-10), reject a mismatch, and reject a
  write to an existing key whose stored hash differs unless the borrower
  sends it as a rewrite (EP-7); a rewrite is stored and the version it
  replaces becomes a tombstone (RET-1). It shall never need
  the backup tool's password and shall never inspect object contents
  beyond the hash.
- **LEND-5** A lender may rate-limit each borrower independently.
- **LEND-6** The lender's replica shall be readable without lendisk:
  each borrower's buckets are ordinary buckets in the lender's storage
  engine, beside the lender's own (EP-16), which the backup tool reads
  with the engine alone or, for a named S3 service, with any S3 client
  (P-5), plus the tombstones and an index of hashes. A replica holds
  all of the bucket's metadata and the data objects placed there
  (REPL-12); the union of enough replicas is the repository (P-5).
  `lendisk export` on the lender shall write any borrower's bucket,
  named by the name it has in the store since the lender does not know
  the real one (SEC-16, COMPAT-4), as
  plain files in the tool's layout: the restic repository layout, or
  for a Kopia bucket (`--kopia-layout`) the sharded layout Kopia's
  filesystem backend expects, so that exports from several lenders
  copied into one directory form a repository `restic -r <path>` or
  Kopia opens. The lender-only profile (PEER-9) uses the same binaries
  and the same store.
- **LEND-7** Capacity planning shall be built in.
  - `lendisk init` shall ask for the expected repository size S and
    `copies` c, and say how much to lend in total: c × S, plus the
    retention margin (tombstones held for the window, RET-1).
  - Friends shall tell each other their repository size (the total of
    their own buckets, as a number) through the quota exchange (NET-7).
  - `lendisk status` shall show, in one place: local copy size; space
    used at each friend and at each overflow target; space lent and
    used by each friend; and the **protectable size**. With D the
    data part of a repository of size S and m its metadata (held at
    every place, about 1% for restic and Kopia), it is the largest S
    for which Σ over friends of min(D, quota − m), plus each overflow
    target's room less m, is at least c × D (one friend can hold at
    most one copy of an object, hence the min). Beside it, the most the
    circle could protect with fair quotas: the same sum with each quota
    replaced by the friend's repository size, since a friend has no use
    for more of your disk than its own repository.
  - For each friendship where one side has used less than half of the
    quota for 30 days, `status` shall say so, so the friends can lower
    the number.
  - The user guide shall state the rule of thumb: you are fully
    protected at c copies while your repository is at most your
    friends' repositories combined divided by c, and your disk holds
    about (1 + c) × S.
- **LEND-8** Quota setting. Each side of a friendship holds its own
  number for it: what it is willing to lend the other. The two numbers
  are exchanged in the quota call (NET-7), and the quota of the
  friendship, enforced by both sides (LEND-1), is the smaller. Either
  side changes its number at any time with `lendisk peer quota <peer>
  <bytes>`; the sum of a node's numbers may not exceed
  `lending.max_total`. Lowering takes effect at once on both sides
  (PEER-6); raising takes effect when the other side's number allows.
  Nothing is proposed, nothing waits for acceptance and the daemon
  never changes a number on its own. When the protectable size falls
  below the repository size, `status` and the alert (OPS-2) name, per
  friendship, the number that would cover the deficit at `copies`, and
  the two people agree on it. AC: three nodes A, B and C, each pair
  friends at 50 GB; A's repository grows past what B and C protect;
  `status` on A names B and C and a number for each; after A and B
  both set 80 GB for their friendship, its quota is 80 GB on both
  sides and A's copies resume at B with no further action.

### 4.6 Deletion, retention and verification

- **RET-1** A lender shall never remove object data on a delete request
  or a rewrite (LEND-4). The deleted object, or the version a rewrite
  replaces, becomes a tombstone, which the lender removes after the
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
  a random object among those placed there, a random byte range and a
  fresh nonce; the lender's answer is checked against the local copy. A
  failed challenge raises an alert, marks the replica unverified and
  starts re-placement of its copies (REPL-13).
- **VER-2** For a place that cannot answer a challenge (an overflow
  target, S3-6), verification shall instead download a random sample of
  objects (default 0.1% of bytes per week, configurable) and check their
  hashes against the index recorded at acknowledgement (REPL-10).
- **VER-3** `lendisk verify <bucket> --peer <id> [--full]` shall run the
  challenge on demand, and `--full` shall check by hash every object
  placed at that peer.
- **VER-4** `lendisk status` shall show, per replica, when it was last
  successfully verified.
- **VER-5** The daemon shall check its own local store for bit-rot at a
  rate of at least 0.1% of bytes per week (configurable), by hashing a
  sample and comparing with the index, by the storage engine's own
  integrity check, or both; a lender does the same for each replica it
  holds. A mismatch raises an alert, and on the borrower side the
  object is re-fetched from a place that holds it (EP-10).

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
  list objects, list and create the borrower's own buckets (creation
  counted against quota), and delete (which only tombstones). Every
  other operation shall be refused before it reaches storage. Beside
  these, the peer protocol shall provide: challenge (VER-1), manifest
  (ID-4, fetchable with nothing but the identity, before any bucket
  exists), quota (LEND-3, the repository size of LEND-7 and each
  side's number of LEND-8), version (NET-6), undelete (RET-3), and
  list: the lender's own index of a borrower's bucket, each key with
  its size, its hash (REPL-10) and, if tombstoned, when, which is what
  recovery (ID-5) and seeding (REPL-8) read, so that neither needs a
  request per object. AC: the peer protocol specification fits on a
  few pages; the hostile-peer test (SEC-7) shows every operation
  outside this list refused.
- **NET-7a** Authentication between peers shall rest on node identity
  alone. No per-pair secret shall be stored on either side or exchanged
  between them. AC: `lendisk recover` (ID-5) reaches a lender's replica
  with nothing beyond the phrase and the lender's address; no per-pair
  credential appears in any file on either side.
- **NET-7b** `lendisk peer forward <peer> <addr>` should expose a friend's
  replica on a local address through the peer connection, so a tool on
  the borrower's machine can read it directly, with no lendisk
  replication code in the path (scenario 10). It shall print, for each
  of the borrower's buckets held by that peer, the bucket's name at the
  lender (SEC-16), the credential the tool needs for that friend
  (NET-7a: computed on demand, shown, never written), and the commands
  that copy that replica's keys into
  a local directory with a standard S3 client. Run against N − c + 1
  friends, these give a directory the backup tool opens as a
  repository (P-5).
- **NET-8** The overlay network shall sit behind an internal interface so
  that it can be replaced without touching replication, storage or the
  CLI.

### 4.8 Operations and observability

- **OPS-1** `lendisk status [--json]` shall report: identity, peers and
  reachability, each bucket's latest snapshot and offsite-complete state,
  bytes held at fewer than `copies` places, replication lag and queue
  size, quota use in both directions, space used at overflow targets,
  the protectable size (LEND-7), last verification, re-placement in
  progress, and pending alerts.
- **OPS-2** The daemon shall raise alerts for: lender unreachable longer
  than 24h (configurable), replication lag exceeding a threshold (default
  48h), a failed verification, quota above 90%, quota exhausted, retention
  purge due within 7 days after a peer removal, a peer on an
  unsupported version, the repository above 90% of the protectable
  size (naming, per friendship, the number that would cover it,
  LEND-8), an overflow target starting to receive copies, objects below
  `copies` places for longer than the REPL-13 threshold, and fewer
  places in the pool than `copies`. These last two are the only cases
  re-placement cannot fix: the pool needs a new friendship, a higher
  quota or overflow (P-2).
- **OPS-3** Alerts shall be delivered through a configurable command hook
  (receives JSON on stdin) and a webhook URL. Built-in email or push is out
  of scope; the hook covers ntfy, healthchecks.io, mail, etc.
- **OPS-4** The daemon should expose Prometheus-format metrics on loopback.
- **OPS-5** Logs go to stdout/journald in structured form, with a
  configurable level.
- **OPS-6** Configuration shall be one file; every setting shall have a
  CLI flag and env var equivalent. The file holds what the user sets;
  what the daemon creates or learns (friendships, buckets, overflow
  targets, placement) is state, kept apart from it and never written
  into it. The data directory layout shall be documented.
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
  unless relayed, how many buckets the borrower keeps at that lender,
  which backup tool wrote each one (its key layout is recognisable), and
  the borrower's total repository size (LEND-7). The
  names of the borrower's buckets are not on this list (SEC-16). This
  shall be documented for users.
- **SEC-3** Secrets on disk shall be limited to the identity key, the
  per-bucket endpoint credentials the node itself issues, the secrets
  of unexpired invites it has issued (PEER-1) and the
  private key of the endpoint's TLS certificate where one is used
  (EP-17), mode 0600
  under the node's own directories. Third-party credentials, meaning
  the credential of a named S3 service used as the store (EP-16, post-v1) and
  the credentials of overflow targets (S3-1), shall be read from
  environment variables when the daemon starts, supplied by the service
  manager or container runtime, and the daemon shall never write them
  to disk; the configuration file names the variables, not the values.
  No per-bucket or per-peer credential shall be held anywhere else; the
  store credential grants access to the node's own store and nothing
  else, and no peer ever receives it. The encrypted node manifest
  (ID-4) may carry overflow target credentials for recovery.
- **SEC-4** A compromised borrower machine shall not be able to cause
  offsite data loss within the retention window (RET-2) nor to read
  another borrower's data on a shared lender (PEER-8).
- **SEC-5** A malicious borrower shall not be able to exceed quota, fill
  the lender's disk below the floor (LEND-2), or write outside their
  directory (key names are validated).
- **SEC-6** Invite secrets shall be single-use and time-limited (PEER-1).
- **SEC-7** Compromise of one peer shall affect only the relationships
  that peer is part of. With friendships A–B, A–C and B–D: a compromised
  B can lose or withhold A's replica at B (detected by VER-1, and the
  copies re-placed by REPL-13) and sees A's traffic metadata, and
  nothing else. It cannot reach A's replica at C, cannot
  impersonate A to C, and cannot cause C or D to learn anything about A.
  AC: a test with a hostile B node exercising every operation of the
  peer protocol, and every operation the underlying store could expose,
  against A and C; everything outside the NET-7 list is refused.
- **SEC-8** The daemon shall expose no listener beyond those this
  document names: the tool-facing endpoint on loopback and on any
  network address the user configures for it (EP-1, EP-5, EP-17),
  metrics on loopback (OPS-4) and the lender-facing service on the peer
  connection (NET-7).
  Administrative, web or other auxiliary listeners of any embedded or
  bundled component shall be disabled, and a listener a bundled
  component cannot run without shall bind loopback or a unix socket
  only. AC: `ss -lntup` shows no lendisk or storage-engine socket on a
  non-loopback address other than the endpoint addresses configured
  under EP-17 and the one datagram socket of the peer connection
  (NET-1).
- **SEC-9** The lender-facing surface shall be deny-by-default: the
  decorating layer admits the NET-7 list and nothing else, and every
  operation of the underlying store shall be classified as admitted,
  refused or unreachable. AC: the classification is checked into the
  repository, a test asserts it is complete, and it is re-reviewed on
  every upgrade of the store.
- **SEC-10** Peer admission shall fail closed. A node with no peers and
  no pending invites shall accept no connection on the peer connection,
  and neither shall a node that cannot read its own relationship state.
  No state of the node, including a fresh install, the moment after the
  last peer is removed, a restart and an upgrade, shall admit an unknown
  identity. A friend is admitted by its identity key or by its dial key
  for this node (ID-1: a dial key per friend), with that friend's
  rights alone either way; removing the
  friendship refuses both. AC: SEC-7's hostile-peer test runs from an unknown identity
  against a node with zero peers, with one peer, and with zero peers
  after the last one is removed; every connection is refused before any
  application data is exchanged.
- **SEC-11** Authorisation shall not rest on connection admission alone.
  Every request the lender-facing service handles shall be checked
  against the relationship held for the identity the request arrived
  from, at the point where the request is served. A request from an
  identity with no relationship is refused even if the connection was
  admitted. Proving an identity grants nothing by itself. AC: in a test
  build with connection-level admission disabled, SEC-7's hostile-peer
  test from an unknown identity still has every request refused.
- **SEC-12** Every request shall be bound to the authenticated identity
  of the connection it arrives on. Any identity a request claims (a
  credential, an access key, a borrower ID, a bucket name) shall match
  that identity, and a mismatch shall be refused. The peer connection
  shall report, for each incoming connection, the identity that
  authenticated it; a friend's dial key authenticates that friend's
  identity (ID-1). A transport that cannot is not a way to meet NET-1.
  AC: B, on its own connection to the lender, sends a request carrying
  A's valid credential; it is refused.
- **SEC-13** An invite admits only its own redemption. A connection
  accepted because it presents a pending invite (PEER-1) shall be able
  to complete the handshake of PEER-2 and nothing else until that
  handshake completes: no NET-7 operation, no manifest, challenge or
  quota request. AC: from a connection holding an unredeemed invite,
  every NET-7 operation is refused.
- **SEC-14** A node's address shall carry no secret. Knowing an address
  (ID-5, ID-7, PEER-1) shall let a node attempt a connection and nothing
  more; access is granted by identity and relationship (NET-7a), never
  by possession of the address. A node's address shall not change when
  the node restarts or upgrades. The address names the identity key; a
  dial key (ID-1) is learnt in the redemption handshake (PEER-2), bound
  to the identity that proves it, and never from an address. The
  single-use secret in an invite (SEC-6) is the only secret an invite
  carries. AC: a node given only
  A's address, with no relationship and no invite, is refused as in
  SEC-10; A's address is identical before and after a restart.
- **SEC-15** Changes to relationships shall apply to open connections.
  When a relationship is removed (PEER-5, PEER-12) or its quota lowered
  (PEER-6), or an invite is redeemed or expires, the node shall apply
  the change to connections that are already open as well as new ones,
  within 60 seconds and without a restart. AC: during a running
  transfer from B, A removes B; within 60 seconds every request on B's
  open connection gets the answer PEER-5 prescribes for a removed
  borrower, and none is served under the old relationship.
- **SEC-16** A lender shall not learn the names of the borrower's
  buckets. The name under which a borrower's bucket is sent to and
  stored at a lender shall be derived from the borrower's identity and
  the bucket's name so that it reveals nothing about that name, shall
  be the same at every lender and stable for the life of the bucket,
  and shall be recomputable from the recovery phrase and the bucket
  name alone, so that recovery (ID-5) and reading a replica with
  lendisk stopped (P-5) still work. AC: after scenario 1, a search of
  B's store, `lendisk.db`, logs and `status` output for the string
  `laptop` finds nothing.

*Rationale for SEC-10 to SEC-15:* the lender-facing service is the only
surface reachable from outside the machine, and the connection layer
in front of it is a replaceable component (NET-8) whose defaults and
guarantees can differ from one implementation to the next. Admission
must not depend on any of them: an empty allow-list that means "allow
everyone", an address that doubles as a password, or a revocation that
waits for a reconnect would each turn a component choice into a hole.

### 4.10 Compatibility

- **COMPAT-1** Supported backup tools in v1: restic and Kopia, each at
  the latest release and the two before it, tested in CI through the S3
  endpoint. Duplicacy and rclone should work via `generic` and are
  smoke-tested.
- **COMPAT-2** Supported platforms for v1: Linux amd64 and arm64, as
  static binaries in one package with one systemd unit, and as a
  container image. ARMv7 should work.
- **COMPAT-3** An existing repository (restic directory or rest-server
  data, Kopia filesystem repository) shall be importable with
  `lendisk bucket import <name> <path>`, which uploads it into the store
  and marks nothing as acknowledged.
- **COMPAT-4** The store layout shall be documented: which directories
  the storage engine owns, or which buckets and prefixes in a named S3
  service (EP-16), how the node's own buckets and each
  borrower's buckets are named inside it (SEC-16), where tombstoned
  objects are kept (RET-1), and the name and purpose of
  every housekeeping directory and file, so that users can read the
  store with the engine alone (P-5). The plain-file export layout
  (LEND-6) shall be documented too: one file per key at its key path
  under the bucket's directory.

### 4.11 External overflow targets

An external S3-compatible bucket can be added to the pool as an
**overflow target**: a place that holds the copies a node's friends
cannot hold, and only those (REPL-12). It is the second choice, always.
A friend's node enforces retention the borrower cannot bypass (RET-2);
an external bucket does so only where the provider has versioning and
the credential lacks delete permission (S3-4, S3-5), and costs money
instead of disk. So copies go to friends whenever friends have room,
and copies at an overflow target move back to friends as soon as they
do (REPL-13). An overflow target counts as one place towards `copies`.
It is never configured as a complete extra copy of a bucket.

The borrower shall use the same replica abstraction, resume behaviour
and undelete for friends and overflow targets. The requirements in this
section that are specific to overflow targets are credentials (S3-1,
S3-5, S3-8), provider-side retention (S3-4) and cost (S3-7); for
friends, quota, retention and verification follow §4.5 and §4.6 because
the lender enforces them.

- **S3-1** `lendisk overflow add s3 <name> --endpoint … --bucket …
  --prefix …` shall add a bucket to the pool as an overflow target; it
  then appears in `status` beside the friends, with what it holds. The
  credential is read from the environment (SEC-3) and carried in the
  encrypted node manifest (ID-4), so recovery through a friend also
  recovers bucket access: `recover` shows it once for the user to place
  in the environment and does not write it.
- **S3-2** The objects in the bucket shall be in the backup tool's
  standard layout under the prefix, one key per object as the tool
  wrote it, so that the bucket is one of the replicas a lendisk-free
  restore can combine (P-5). Once it holds data objects of a bucket it
  also holds all of that bucket's metadata, sent before or together
  with the first of them (REPL-12). AC: copying the keys
  under the prefix and those of the friends' replicas into one
  directory gives a repository `restic -r <directory> snapshots` opens
  with no lendisk installed.
- **S3-3** Uploads shall carry a content checksum the provider verifies,
  so the store rejects corrupted transfers. Large objects shall be
  uploaded in resumable parts.
- **S3-4** Retention at an external bucket shall be enforced by the
  provider, using object versioning and a lifecycle rule that expires
  non-current versions after the retention window. `overflow add` shall
  check that versioning is enabled and warn if not; it shall offer to
  create the lifecycle rule when the credential allows it. `undelete`
  shall restore objects deleted within the window (RET-3), with the
  credential S3-5 recommends: by making the last version before the
  delete current again, not by removing the delete marker. For providers
  without versioning, the daemon shall fall back to lendisk-side
  tombstones: deletes are recorded locally and issued to the bucket only
  after the retention window. This protects against accidental deletion
  but not against a compromised borrower, and `status` shall label such
  targets "retention: client-side". Copies moved back to friends
  (REPL-13) are deleted at the target the same way, so they stay
  recoverable for the window.
- **S3-5** The documentation shall tell the user to create a credential
  that can put, get (including earlier versions), list and mark objects
  deleted, but not remove object versions, and `overflow add` shall test for and report
  version-removal permission, recommending it be removed. With such a
  credential, restic's `prune` through lendisk and the drain-back of
  REPL-13 produce tombstones that only the lifecycle rule ever purges.
  Where a provider cannot grant the one without the other, `status`
  shall say which the user chose.
- **S3-6** Verification shall compare the provider's stored checksums
  against local, and download a random sample as in VER-2. A bucket
  cannot answer a challenge (VER-1), and this document accepts that this
  trusts the provider's checksum.
- **S3-7** There is no lender-side quota. The user shall set `max_bytes`
  on the overflow target, which is the room it adds to the pool
  (LEND-7); the daemon raises an alert at 90% and stops placing copies
  there at 100%, as in LEND-2. `status` shall show what the target holds
  now and an estimated monthly cost if the user provides a price per GB.
- **S3-8** `lendisk recovery-sheet` (ID-7) shall include the bucket
  endpoint and prefix, and state that the access key is needed; it shall
  not print the secret key unless `--with-secrets` is given.
- **S3-9** Removed in v0.10. It adopted an existing complete repository
  in a bucket as a replica; an overflow target holds only what friends
  cannot, so there is nothing for a complete repository to be adopted
  as. The ID is not reused.

## 5. Out of scope for v1

- Open network, discovery of strangers, payments or credits.
- Balances or credit carried from one friendship to another (P-8).
- One-way lending; every friendship is symmetric (PEER-3).
- Erasure coding across peers. Placement (REPL-12) spreads whole
  objects; no object is split.
- Failure domains: two friends in one house or at one provider count
  as two places (REPL-12).
- Automatic quota changes. Quotas are numbers people set (LEND-8).
- Specified but deferred to a later version: an S3 service as the
  node's own store (EP-16) and a local copy that evicts acknowledged
  data (EP-9).
- An external bucket as a complete extra copy; external buckets are
  overflow only (§4.11).
- GUI or web UI.
- macOS, Windows, FreeBSD builds.
- sftp as an endpoint.
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

1. **First backup.** Four nodes A, B, C and D, each behind NAT, each
   with about 100 GB to back up. Each pair is friends at 80 GB (six
   invites). A adds bucket `laptop --profile restic`, at the default
   `copies = 2`. `restic backup` on A, whose store is the default
   directory, finishes at local disk speed. Within the bandwidth
   available, `lendisk status` shows the snapshot offsite-complete,
   every data object at two of B, C and D, the metadata at all three,
   and a protectable size of about 120 GB (80 GB from each of three
   friends, less the metadata each holds).
2. **Friend offline.** B is powered off for three days. `restic backup` on
   A keeps working, and new data objects are placed at C and D. Status
   shows lag for B; at 24h an alert fires. When B returns, its queue
   drains (metadata and the objects still placed there). Had B stayed
   away past the REPL-13 threshold, A would have re-placed B's copies
   at C and D without an invite; at 80 GB each they hold only 80 GB of
   A's 100 GB at two copies, so A's `status` would first name the
   number to agree with C and D (LEND-8), as in scenario 13.
3. **Total loss.** A's house burns down. On a new machine: `lendisk
   recover` with the phrase and B's address from the paper sheet; the
   manifest from B names C and D, and the placement is rebuilt by
   listing them. With D offline, `restic -r s3:... restore latest`
   succeeds via read-through from B and C without downloading the
   whole repository.
4. **Ransomware.** An attacker on A runs `restic forget --keep-last 0
   --prune` and wipes A's disk. Within 30 days, A recovers as in scenario 3
   and runs `lendisk undelete laptop`; all snapshots are back.
5. **Bad lender.** B silently deletes half of A's packs. Within a day, the
   daily challenge fails, A is alerted that the replica is unverified,
   and A's node re-places B's copies at C and D (REPL-13).
6. **Quota.** A's repository grows past what its pool holds at two
   copies. The local backup still succeeds; status shows the protectable
   size below the repository size and copies paused for quota, and
   names, per friendship, the number that would cover it (LEND-8). A
   and B set that number for their friendship, as do A and C; both
   quotas grow in both directions and replication resumes.
7. **Zero lock-in.** With lendisk stopped everywhere: on A, the storage
   engine alone is started on A's store directories following the user
   guide, and `restic -r
   s3:…` against it lists A's snapshots from A's own copy. Then the
   same is done on B and on C, with B's and C's credentials, and the
   keys of A's laptop replica, found on each under the name A's
   recovery sheet gives for it (SEC-16), are copied into one directory
   with a standard S3 client; `restic -r <directory> check
   --read-data` passes, with D never consulted. Alternatively `lendisk
   export` has written B's and C's replicas to directories, and
   `restic -r` on the two copied into one works on the plain files
   with nothing running at all. No lendisk code runs anywhere.
8. **Partial trust.** A is friends with B and C; B is friends with D; A
   and D have no friendship. D cannot connect to A (rejected before any
   application data is exchanged), cannot list or read A's data on B,
   and does not appear anywhere in A's state. A's `peer list` shows B
   and C only. Removing the friendship A–B leaves A–C untouched; B's
   replica of A's data and A's replica of B's are purged after the
   retention window. Every object of A's is then at C alone, and A is
   alerted that the pool has fewer places than `copies`.
9. **Kopia.** A creates bucket `photos --profile kopia`, runs `kopia
   repository create s3 …` against the S3 endpoint and takes a snapshot.
   Status shows the manifest offsite-complete at two copies. Kopia
   maintenance deletes old packs; each place holding one tombstones it.
   A restores a file on a fresh machine via read-through after
   `lendisk recover`.
10. **Restore straight from friends.** On A, `lendisk peer forward B
    127.0.0.1:9100` and `lendisk peer forward C 127.0.0.1:9101` print
    the name of `laptop` at each, the credential for each, and the
    commands that copy each
    replica's keys into a local directory; after both, `restic -r
    <directory> snapshots` lists A's snapshots, with no lendisk
    replication code in the path. B's daemon refuses `restic prune`'s
    attempt, through the forward, to remove tombstoned data (RET-2) and
    refuses every operation outside the NET-7 list (SEC-9).
11. **Backup from another machine.** A runs on a NAS. The user sets
    `endpoint.listen` to the NAS's LAN address and adds bucket `laptop
    --profile restic`; `bucket add` prints the lines for that address,
    including how restic trusts the certificate. On a laptop on the
    same LAN, `restic backup` against the NAS's address succeeds, and
    `lendisk status` on A shows the snapshot offsite-complete at two
    copies. `restic restore latest` from the laptop
    succeeds. A plaintext request to that address is refused. With TLS
    turned off for the address, the same backup and restore succeed
    over plain HTTP and `status` labels the listener "plaintext".
12. **Overflow.** E has 380 GB to back up (about 4 GB of it metadata)
    and four friends A, B, C and D with about 100 GB each, every
    friendship at 100 GB. `status` on E shows a protectable size just
    under 200 GB at two copies, and says that fair quotas cannot raise
    it (each friend's repository is 100 GB). E adds a B2 bucket with
    `lendisk overflow add s3 … --max-bytes 450G`; the protectable size
    rises above 380 GB, an alert says the overflow target is receiving
    copies, and the snapshot becomes offsite-complete with every data
    object at two places, one of them a friend. Later F, who backs up
    400 GB, becomes E's friend at 400 GB. E's
    node moves the copies at B2 to F, deletes them at B2 (tombstoned
    for the window by the provider's versioning, S3-4), and `status`
    shows the overflow target empty.
13. **Friend leaves.** In scenario 1's circle, D leaves: the
    friendships with D are
    removed. A's pool is B and C at 80 GB, a protectable size of 80 GB,
    below A's 100 GB; `status` on A says 100 GB with each would cover
    it. A and B set 100 GB, as do A and C (LEND-8); A re-places the
    copies D held at B and C,
    never dropping an object below the copies it had, and `status`
    shows the latest snapshot offsite-complete at two copies again. No
    invite was needed.

## 8. Changes

- **v0.11.3 (2026-11-08)** Kopia rewrites `kopia.maintenance` after
  every maintenance run, and `kopia.repository` and `kopia.blobcfg` on
  a password change or `set-parameters`; restic rewrites `config` in
  `restic migrate` (#74). Under v0.11.2 each such rewrite raised the
  immutability alert and never reached friends. EP-7 adds rewritable
  keys per profile; EP-11 exempts them; LEND-4 accepts a rewrite the
  borrower marks as one, and RET-1 keeps the replaced version as a
  tombstone for the retention window, as S3 Object Lock with versioning
  keeps a replaced version until its retain-until date. RET-2 is
  unchanged: no borrower operation loses data within the window.
- **v0.11.2 (2026-11-07)** A node connects to each friend with a dial
  key per friend, derived from the same phrase as its identity key,
  which it keeps for serving (#69). With one key for both, a relay
  shared by two friends delivered packets meant for a node's server to
  its own client, and a friendship could stall until a restart; with
  one dial key for all friends, the same happened between the node's
  connections to two friends. ID-1, SEC-10,
  SEC-12 and SEC-14 reworded to match; the node ID, the address and the
  S3 credential (NET-7a) are unchanged.
- **v0.11 (2026-11-04)** Simplifications from a review of v0.10.1
  against what v1 needs. Changed meaning:
  - LEND-8 rewritten: each side sets its own number for a friendship,
    the quota is the smaller, and nothing is proposed, accepted or
    changed by the daemon; `lending.max_per_peer` is gone,
    `lending.max_total` stays. PEER-1, PEER-2, PEER-3, PEER-4, PEER-6,
    LEND-1, OPS-2, P-2, P-8, the glossary and scenarios 2, 6 and 13
    reworded to match; scenario 6 and 13 each gain the step where the
    two people set the number.
  - EP-9: one storage mode in v1. `evict` is specified and deferred;
    EP-6 loses `--mode`, EP-10, REPL-3 and REPL-13 lose their evict
    clauses, VER-2 becomes the sampling method for a place that cannot
    answer a challenge (overflow targets, S3-6).
  - EP-16 (an S3 service as the node's own store) is post-v1; EP-12,
    EP-15, SEC-3, the glossary and scenario 7 marked to match.
  - EP-7 rewritten as four key classes; restic `locks/*` and Kopia
    `_log*` are local-only and never replicated; Kopia's prefixes
    corrected (`s*` are session blobs, not packs).
  - REPL-12 loses failure domains; §5 lists them as out of scope.
  - ID-3: the private key is a plain file at mode 0600; the encryption
    under a key kept beside it is gone.
  - NET-7: the borrower no longer lists tombstoned versions or deletes
    by version over the replica surface; undelete is a peer-protocol
    call, and a new `list` call returns the lender's index (key, size,
    hash, tombstoned) so recovery and seeding need no request per
    object. ID-5 and REPL-12 reworded to match.
  - NET-7b prints the credential the tool needs; scenario 10 matches.
  - LEND-6: export on the lender names the bucket by its stored name.
  - OPS-6: the configuration file holds settings only; state is apart.
  - SEC-3 lists unexpired invite secrets; SEC-8's AC checks datagram
    sockets too and names the peer connection's as allowed; COMPAT-4
    documents where tombstones live.
  - Scenario 1 uses 80 GB quotas (120 GB protectable), the figure
    DESIGN.md §4 already used; 70 GB left no room for tombstones.
  - §5 gains failure domains, automatic quota changes and the two
    deferred features.
- **v0.10.1 (2026-11-01)** Contradictions in v0.10 corrected after a
  consistency review. Changed meaning: REPL-1's barrier waits for the
  places an object is actually placed at, so a pool short of room still
  receives what fits, and its AC is conditioned to match; REPL-12
  counts an unreachable friend with room before overflow until the
  REPL-13 threshold, backfills metadata to new places, and its AC
  leaves room for the metadata every place holds; LEND-7's formula
  subtracts that metadata; LEND-1 and LEND-8 never let a quota exceed
  the global cap, and LEND-8's AC uses three nodes; S3-4 undeletes by
  making the last version current again, so S3-5's credential (which
  may now read earlier versions) suffices; S3-2 says when metadata
  arrives. Reworded: P-2 and OPS-2 agree on when a new friendship is
  needed; P-5 covers N ≤ c; P-8's quota sentence; scenarios 1, 2 and
  12 corrected.
- **v0.10 (2026-11-01)** Pooled placement, symmetric quotas, overflow.
  - **Placement instead of replica lists.** A bucket now has `copies`
    (default 2) instead of `--replicas` and `min_replicas`. Data
    objects are spread over the pool by free quota, and metadata goes
    everywhere (REPL-12, new). The owner's node re-places copies when
    a friend leaves, stays away, fails verification or shrinks its
    quota, and when friends gain room (REPL-13, new).
  - **Changed meaning:**
    - EP-6, EP-9, EP-10, REPL-1 (metadata waits for `copies`
      acknowledgements of earlier data; new AC), REPL-2, REPL-4,
      REPL-8, REPL-11 (one watermark per bucket).
    - ID-4, ID-5 (new AC), ID-6, ID-7.
    - VER-1 (a failure starts re-placement), VER-3, VER-5.
    - LEND-2, LEND-6, OPS-1, OPS-2.
    - Glossary: "replication set" removed; friendship, symmetric quota,
      pool, overflow target, copies, placement and protectable size
      added.
  - **P-5 weakened:** a single lender's replica is no longer a complete
    repository. A lendisk-free restore combines any N − c + 1 replicas.
    NET-7b, S3-2 and scenarios 7 and 10 reworded to match. P-6 notes it.
  - **Reciprocity:** P-8 is replaced by "reciprocity is pairwise, and
    paid in disk".
    - Every relationship is a friendship with one quota lent in both
      directions, enforced by each side (PEER-1, PEER-2, PEER-3,
      PEER-6, PEER-12, LEND-1).
    - `--mutual` and one-way relationships are removed. P-2, PEER-4,
      PEER-5, PEER-10 and SEC-7 are reworded to match.
    - PEER-9 is kept as a node with no buckets.
    - LEND-7 is rewritten around the protectable size.
    - LEND-8 added: quota negotiation within ceilings each user sets.
    - SEC-2 adds the repository size a friend learns. NET-7's quota
      call carries it.
  - **External buckets become overflow targets.** §4.11 is retitled.
    S3-1 (`overflow add`), S3-2, S3-4 and S3-7 are reworded; S3-9 is
    removed. S3-5 now allows delete markers but not version removal,
    because drain-back must delete; it had asked for no delete
    permission at all, which also stopped `prune` from tombstoning.
  - **Renamed only:** "external replica" becomes "overflow target" in
    EP-16 and SEC-3; no change of meaning.
  - **Elsewhere:** §5 gains four out-of-scope lines. Scenarios 1, 2, 3,
    5, 6, 8, 9 and 11 are reworded; 12 (overflow) and 13 (friend
    leaves) are added. §1 reworded.
- **v0.9.3 (2026-10-31)** Bucket names hidden from lenders. SEC-16
  added: the name a lender sees for a borrower's bucket is derived from
  the borrower's identity and reveals nothing about the real name, and
  can be recomputed from the phrase. SEC-2 now lists what the lender
  still learns about buckets: how many, and which tool wrote each.
  ID-7 puts each bucket's name at its lenders on the recovery sheet;
  NET-7b prints it. COMPAT-4 and scenarios 7 and 10 reworded to match.
  No other requirement changed meaning.
- **v0.9.2 (2026-10-31)** Backup tools on other machines. EP-17 added:
  the user may give the endpoint network addresses besides loopback,
  with per-bucket credentials and TLS by default (plaintext if the
  user turns it off, labelled in `status`), so laptops can back up to
  a NAS node. EP-5 changes meaning from "never" to "loopback
  unless the user configures EP-17"; EP-1, SEC-8 and its AC, and P-9
  reworded to match. SEC-3 admits the endpoint's TLS private key.
  Scenario 11 added. No other requirement changed meaning.
- **v0.9.1 (2026-10-30)** Third-party credentials come from the
  environment. SEC-3 now limits secrets on disk to the identity key and
  the node's own per-bucket credentials; the store credential (EP-16)
  and external replica credentials (S3-1) are read from environment
  variables at start and never written by the daemon. S3-1 reworded to
  match: the manifest still carries replica credentials for recovery,
  and `recover` shows them once instead of writing them. No other
  requirement changed meaning.
- **v0.9 (2026-10-30)** A named S3 service admitted as the store.
  EP-16 added: the store may be an S3-compatible service the user
  names, and one store holds the node's own buckets and the buckets it
  lends. Glossary "storage engine", P-5, EP-12, EP-15, LEND-6, COMPAT-4
  and scenarios 1 and 7 extended to match. SEC-3 admits that service's
  credential and now lists external replica credentials (S3-1), which
  it had omitted. REPL-10 reworded: the hash may be taken when the
  backup tool writes the object rather than only as it is sent, since
  the replica verifies the received bytes either way. No other
  requirement changed meaning.
- **v0.8.1 (2026-10-30)** Peer admission requirements added: fail closed
  with no peers (SEC-10), authorisation per request and not only per
  connection (SEC-11), requests bound to the connection's identity
  (SEC-12), an invite admits only its redemption (SEC-13), addresses
  carry no secret and survive restarts (SEC-14), and relationship
  changes apply to open connections (SEC-15). No existing requirement
  changed meaning.
- **v0.8 (2026-10-28)** Storage engine admitted. P-5 no longer requires
  plain files readable with nothing running; it requires readability
  through the storage engine alone plus a plain-file export. LEND-6,
  EP-14, EP-15, COMPAT-4 and scenario 7 reworded to match. SEC-3 allows
  the node-local credentials the daemon needs to drive its storage
  engine; SEC-8 allows a bundled component's unavoidable listener on
  loopback or a unix socket. VER-5 accepts the storage engine's own
  integrity check as a way to meet the rate. P-9 and COMPAT-2 say "one
  service, binaries shipped together" instead of "single binary".
  Glossary gains "storage engine". EP-4 (restic REST endpoint) removed;
  EP-5, EP-15, SEC-3, SEC-8, COMPAT-1 and scenario 3 lose their
  references to it. No requirement ID changed meaning otherwise.
- **v0.7 (2026-10-28)** Split from SPEC v0.6. Former design decisions
  D1, D1a, D2, D7, D8, D9 and the consequences of D3, D4 and D6 became
  principles P-1 to P-9. Implementation detail removed from every
  requirement in §4 and moved to DESIGN.md §3; requirement IDs and
  meaning unchanged. Former Q-C, Q-E and the measurement part of Q-F
  became release gates G-1 to G-3; the remaining questions moved to
  DESIGN.md §5. Capacity planning (former §8) moved to DESIGN.md §4,
  since its numbers follow from the mirroring decision. Acceptance
  scenarios 8 and 9 put in numeric order.
