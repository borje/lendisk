# lendisk — requirement specification

Status: draft v0.5, 2026-10-27. Decisions in §3 are recommendations and can be
overturned; everything in §4 follows from them. v0.4 generalised the
product from restic-only to any S3-speaking backup tool; v0.5 replaces
the Garage sidecar and capturing proxy with versitygw embedded in the
daemon (D4, D13). The trust model is unchanged throughout.

*Name:* "lendisk" is a working title. It says what the product does but is
awkward to pronounce and names only the lender's side. Candidate
replacement: **belay** (a climbing partner holds your rope: pairwise,
directional, friends only, and the whole point is the fall you hope never
happens; `belay invite`, `belay recover`, `belayd`). Runners-up: `kith`
(friends you know), `lodge` (lodge your backups with a friend), `escrow`
(a holder who cannot open the box). Rejected for collisions: `attic`
(borg's predecessor), `safekeep` (rdiff-backup wrapper, packaged in
Fedora), `stow`, `burrow`, `cellar`. Decide before the first public
release; the command name appears throughout §4.

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

Original requirements this spec grew from:

1. Point an unmodified restic client at the service.
2. Offsite backup without trusting whoever holds the remote copy.
3. Configure how much space you are willing to lend to other peers.
4. Peers sync over a NAT-traversing overlay, like Tailscale.

## 2. Glossary

| Term | Meaning |
|---|---|
| **Node** | One lendisk daemon plus its local store, with one identity. A household NAS is one node. |
| **Identity** | An Ed25519 keypair. The public key is the node ID. Derived from the recovery phrase. |
| **Peer** | Another node you have exchanged invites with. |
| **Borrower** | The role of a node storing its buckets on a peer. |
| **Lender** (host) | The role of a node storing a peer's buckets. Most nodes play both roles. |
| **Local store** | The S3 server embedded in the daemon (versitygw's POSIX backend) that holds the borrower's own copy as plain files. One per node, never shared. |
| **Bucket** | One backup repository as the backup tool sees it: a namespace of keys in the local store. Formerly "repo". |
| **Tool profile** | Per-bucket setting (`restic`, `kopia`, `generic`) that selects validation rules and what "snapshot" means. |
| **Replica** | A copy of a bucket held by one lender or in one external S3 bucket. |
| **Replication set** | The replicas a bucket is replicated to. |
| **Offsite-complete** | A point in a bucket's write sequence up to which every object is acknowledged by at least `min_replicas` replicas. Tool profiles map this to "snapshot X is offsite". |
| **Object** | One key in a bucket with its bytes. For restic, a file such as `data/ab/abcd…`. |
| **Tombstone** | A deleted object the lender keeps for the retention window. |
| **Recovery phrase** | 24 BIP-39 words from which the identity is derived. |
| **Node manifest** | An encrypted record of a node's peers and repos, stored on every lender for recovery. |

## 3. Design decisions

Each decision lists the alternatives considered and why the chosen one won.
These are the roots of the design tree; §4 is the leaves.

### D1. Trust model: friends only, by invitation

Peers are added only by exchanging an invite. There is no discovery of
strangers, no marketplace, no payments and no proof-of-storage cryptography
beyond what §4.6 specifies.

*Rejected:* an open network of strangers (Storj/Sia style). It needs Sybil
resistance, economic incentives, abuse handling and legal cover for hosts.
That is a different product. Social accountability ("your server has been
down for a week") solves availability for a circle of 2–10 friends.

### D1a. Trust is pairwise, directional and never transitive

There is no group, no network and no shared membership list. Each node
holds a set of pairwise relationships, each one created by one invite and
each one pointing one way (lender → borrower). Nothing a node does is
visible to, or depends on, a node it has no relationship with.

Concretely, with four people A, B, C and D:

- A can lend to B and C and never hear of D. If B also lends to D, A's
  data and D's data sit in separate directories on B, each reachable only
  with its owner's key (PEER-8). A and D never connect, are never listed
  to each other, and cannot tell that the other exists.
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
  nothing else (WireGuard end-to-end, restic end-to-end inside that). A
  controls which relays its node will use (NET-5) and can refuse any
  relay it does not trust for metadata.

For a fully mutual group of n friends this costs n(n−1)/2 `--mutual`
invites: ten for five people. That is accepted: a "group invite" would be
transitive trust by another name.

*Rejected:* a shared group key or a signed membership list held by an
admin. Both make one person's compromise everyone's problem and put a
"who is in the group" decision in one person's hands.

### D2. v1 audience: self-hosting Linux users, CLI only

- Single static binary for Linux amd64 and arm64, plus a Docker image for
  Synology, TrueNAS and Unraid. A systemd unit file ships with it.
- CLI plus one TOML config file. No GUI and no web UI in v1.
- macOS, Windows and FreeBSD are post-v1.

*Rationale:* the first users already run restic and a homelab. A GUI would
double the surface for no change in the core problem.

### D3. Write model: local staging with asynchronous replication

restic writes to the local daemon at disk speed. The daemon keeps the full
repository locally and replicates every object to lenders in the background.

*Rejected:* synchronous pass-through to a peer. Backups would run at WAN
upload speed, fail whenever the peer is offline, and give the user no local
copy. Local staging gives 3-2-1 (one local copy, N offsite) for free and
makes most restores fast.

*Consequence:* "the backup finished" and "the backup is offsite" are two
different events, and the product must never blur them (§4.4).

Two storage modes, per repo:

- `full` (default): the local copy is complete; restores never need a peer.
- `evict`: data packs are removed locally once `min_replicas` lenders have
  acknowledged them. Metadata (`config`, `keys`, `index`, `snapshots`,
  `locks`) is always kept locally. Reads of evicted packs are fetched from
  a lender on demand.

### D4. Endpoint: S3 served in-process, restic REST as a shim

The backup tool talks S3 to the lendisk daemon on `127.0.0.1:9000`. The
S3 protocol is served by **versitygw embedded in the daemon** (D13) over
a lendisk-owned decorator of its POSIX backend; the decorator captures
every write and delete into the replication queue at the
`backend.Backend` interface. restic users may instead use
`rest:http://user:pass@127.0.0.1:8000/<bucket>`: the daemon translates
the five REST operations into calls on the same decorated backend, so
there is one local copy and one capture point.

Why S3 rather than restic's REST protocol as the primary endpoint:

- Every backup tool that matters speaks S3 (restic, Kopia, Duplicacy,
  Duplicati, Velero, rclone). restic's REST protocol is restic-only.
- The S3 object model is a strict superset of the REST one; nothing in
  replication, retention or recovery depends on which front end was used.
- With versitygw embedded, the daemon does not implement S3 and does
  not proxy it: SigV4, multipart, listings and `aws-chunked` are
  versitygw's; lendisk only wraps the storage calls underneath.

What changes versus a restic-only design, and the replacement for each:

- *Name-equals-hash validation* (restic names every object by SHA-256;
  Kopia names pack blobs `p<random>`) is replaced by an **immutability
  policy**: once a key is acknowledged by a replica, a write to the same
  key with different content is a violation that raises an alert and is
  not replicated. The daemon hashes each object when the replication
  worker reads it from the store to send it, which it must do anyway,
  and sends the hash with the object; the lender verifies the bytes
  against it (LEND-4).
- *"Snapshot X is offsite-complete"* becomes a per-bucket high-water mark
  in the write sequence. Tool profiles (`restic`, `kopia`) recognise
  snapshot or manifest keys and present the watermark as "snapshot X";
  `generic` shows a timestamp.
- *Write ordering* (REPL-1) generalises unchanged: restic and Kopia both
  write packs, then indexes, then the snapshot or manifest, so FIFO
  replication keeps the replica consistent at every watermark.
- *Multipart uploads* are enqueued on `CompleteMultipartUpload` only;
  parts and aborted uploads never reach the queue.
- *Zero lock-in* holds on both sides. The local store keeps objects as
  plain files at their key paths, so with the daemon stopped
  `restic -r /var/lib/lendisk/store/<bucket>` opens the borrower's own
  copy, and Kopia's filesystem backend opens a Kopia bucket. The lender's
  replica is the same keys-as-paths layout.
- *The trust promise* is per tool profile. restic and Kopia always
  encrypt client-side, so the "lender never sees plaintext" guarantee
  holds. A `generic` bucket (rclone, `aws s3 cp`) is outside the
  guarantee; `bucket add --profile generic` says so and requires
  `--i-understand-no-encryption`.

*Rejected:* sftp (needs an SSH server); writing an S3 implementation
(`gofakes3` is a test double and the protocol surface is large);
embedding rest-server's filesystem handler as the store (a second store
beside the S3 one, so the restic path would diverge from the Kopia
path); a separate S3 store behind a capturing proxy (v0.4's design: a
second process, a compose requirement, SigV4 pass-through risk, and a
crash window between the store's acknowledgement and the queue entry).

### D5. Overlay network: Tailscale's data plane via `tailcat`

The node ID is a WireGuard public key. Connections are mutually
authenticated by key, hole-punched where possible, and fall back to a DERP
relay.

Chosen: **`github.com/tailscale/tailcat`** (BSD-3, released August 2026).
It is Tailscale's open-source data plane (WireGuard, magicsock NAT
traversal including UPnP/NAT-PMP/PCP port mapping, DERP relays) with no
control plane, no account and no admin panel. Each node runs a tailcat
`Server` with a saved key; its address string (`tc…`) encodes the public key
and relay bootstrap info and is exactly what an invite needs to carry. Each
node also dials peers as a `Client` with its own stable key, and every
server restricts `AllowedClients` to known peers and pending invites.

Why this over the alternatives:

- It is the most field-proven NAT traversal available, and it is the thing
  the original requirement asked for ("like Tailscale").
- It exposes a `net.Listener` and `Dial` over a userspace TCP/IP stack, so
  the peer protocol is plain HTTP inside an authenticated WireGuard
  tunnel. No custom RPC framing, no second transport security layer.
- The relay server (`cmd/derper`) is mature and has been self-hosted by
  Headscale users for years.

Risks, and how the spec handles them:

- tailcat is two months old and states no API stability guarantee. The
  underlying components are mature; the wrapper is thin. Pin the version
  and keep it behind an internal `Transport` interface (NET-8).
- Tailscale positions it for short-lived connections and its hosted
  relays are throttled, carry no SLA and "may be revoked". The product
  therefore never depends on them: the project runs its own `derper`
  fleet, any user can run one, and a peer with a public address never
  needs a relay (NET-5).

*Alternatives considered*, roughly in order of how close they came:

- **iroh** (Rust, 1.0, derived from Tailscale's magicsock and DERP): the
  credible second choice, with a comparable traversal design and a stable
  wire protocol. n0's public relays are explicitly for hobby use only and
  n0 is a startup; self-hosting `iroh-relay` is easy. iroh gives QUIC
  streams rather than TCP, so the peer protocol would need its own
  framing or HTTP/3. Pick this if D10 is reversed to Rust.
- **Syncthing's stack** (Go, MPL-2.0): device IDs are certificate hashes,
  trust is pairwise exactly as in D1a, and the project has a volunteer
  relay pool (`strelaysrv`, ~hundreds of relays) and global discovery
  servers that any Go program can import (`lib/relay`, `lib/discover`).
  Its hole punching is QUIC-based and weaker than Tailscale's, the relay
  protocol is TCP-only, and the public pool is run for Syncthing users,
  not for third-party products. Worth keeping as the second `Transport`
  implementation if a Go alternative to tailcat is ever needed. See also
  D11 on using Syncthing itself.
- **go-libp2p**: measured ~70–80% hole-punch success, and its public
  relays are rate-limited for signalling only.
- **WebRTC (pion) + TURN (coturn)**: standard ICE traversal and the most
  widely deployed relay protocol in existence. Overkill in API surface
  for a daemon-to-daemon link, and ICE's traversal is weaker than
  magicsock's (no port-mapping protocols, no persistent endpoint
  learning).
- **Tailscale `tsnet` + Headscale**: mature, but binds every peer to one
  tailnet, and whoever runs Headscale decides membership and ACLs for
  everyone. That is exactly the transitive trust D1a rules out.
- **Nebula** (Slack, MIT): needs a certificate authority to sign every
  node and lighthouses with public IPs. The CA holder is a group admin,
  same objection as Headscale.
- **ZeroTier**: BSL licence and root servers run by ZeroTier Inc.
- **Tor onion services**: works behind any NAT with no relay to run, but
  a few MB/s at best. Could serve as a last-resort transport for
  metadata and small restores; not for bulk replication.
- **A rented VPS as a WireGuard hub**: everyone dials out to one box.
  Simplest possible setup for a group of three, and the VPS sees only
  encrypted traffic. It is "always relayed", costs money, and whoever
  rents the box is a single point of failure. Users who want this can
  get it from NET-3 (static address) plus a WireGuard tunnel of their
  own; the product does not need to build it.
- **Own WireGuard + STUN + relay stack**: months of undifferentiated work.

*Relay policy:* direct connection first; relay is a degraded mode, shown
as such in status output. Nodes ship with the project's DERP map, can be
pointed at any `derper`, and may optionally include Tailscale's hosted
relays as a last-resort bootstrap.

### D6. Replication: whole-repo mirrors, configurable factor

Each repo is mirrored in full to each lender in its replication set.
`min_replicas` (default 1, recommended 2) defines when a snapshot counts as
offsite-complete.

*Rejected:* erasure coding across peers. It reduces storage cost but
makes a restore depend on k-of-n peers being online, and friends' NASes
are not reliable enough for that. Mirrors also keep the zero-lock-in
guarantee: a lender can hand you a disk.

### D7. Reciprocity: none enforced

A lender sets a quota per borrower at invite time. There is no ratio, no
credit and no accounting beyond showing each side what they give and get.
Friends sort this out themselves.

### D8. Recovery: a recovery phrase and the restic password are enough

The identity is derived from a 24-word phrase. The node manifest is stored,
encrypted to the identity, on every lender. A new machine with the phrase
and the node ID of any one lender rebuilds the node and can restore directly
from that lender without first downloading the whole repo.

### D9. Host-side ransomware protection: soft deletes with retention

A lender never deletes an object immediately. Deletes become tombstones
and are purged after a retention window the lender controls (default
30 days). There is no API to purge early. Tombstoned data counts against
the borrower's quota. An attacker who owns the borrower's machine can
therefore delete the local copy and queue deletes, but cannot destroy
offsite data for 30 days.

### D10. Language and licence: Go, because of D5

The language follows the transport, not the other way round. Nothing else
in the product forces a choice: the daemon stores opaque objects by key
and never parses a backup repository, so neither restic's Go code nor
Rust's `rustic_core` is needed. The embedded S3 layer (D13) is Go, which
confirms the choice rather than forcing it.

- **Go** is required by tailcat (D5). It also matches the restic ecosystem
  (restic, rest-server) and cross-compiles to static Linux amd64/arm64/
  armv7 binaries trivially.
- **Rust** would be the choice with iroh as transport. It would give a
  slightly smaller and safer daemon at the cost of a custom stream
  protocol over QUIC. It is a reasonable product; it is not a better one
  for this problem.

If field testing fails the connectivity gate in Q-C, the fallback is iroh,
and that means a Rust rewrite of a thin daemon (REST handler, SQLite queue,
replication loop). The spec is written so that nothing but §4.7 changes.

Licence: Apache-2.0.

### D11. Build on existing software

Everything that already exists and fits is used as a library, not
reimplemented. The daemon is glue around these:

| Concern | Component | Licence | Notes |
|---|---|---|---|
| Backup format, client, encryption | **restic**, **Kopia**, unmodified | BSD-2 / Apache | Never imported; only their S3 (or REST) usage and key naming conventions are relied on, via tool profiles. |
| S3 protocol and local store | **`github.com/versity/versitygw`**: `embedgw` + `backend/posix` | Apache-2.0 | D13. Embedded in-process; lendisk wraps the backend to capture writes and deletes and serve read-through. Pinned at a tested version. |
| restic REST shim | protocol per restic's `doc/REST_backend.rst` | — | Five operations translated to calls on the wrapped backend. ~300 lines. |
| S3 client (external replicas) | `minio-go` | Apache | |
| Overlay network and NAT traversal | **`github.com/tailscale/tailcat`** | BSD-3 | D5. |
| Relay | **`tailscale.com/cmd/derper`** | BSD-3 | `lendisk relay` is a thin wrapper. |
| State and queues | **SQLite** via `modernc.org/sqlite` | MIT / BSD | Pure Go, no cgo, so static builds stay simple. |
| Recovery phrase | `github.com/tyler-smith/go-bip39` | MIT | |
| CLI, config | `cobra`, `BurntSushi/toml` | Apache / MIT | |
| Metrics | `prometheus/client_golang` | Apache | |

What is written from scratch, because nothing exists: the capturing
backend decorator (D4, D13), the replication queue and its ordering
guarantee (REPL-1), the lender's filesystem store with tombstones and
retention (§4.6), the challenge protocol (VER-1), the node manifest and
recovery flow (§4.1), invites (PEER-1), tool profiles, an
`auth.IAMService` over lendisk's SQLite, and the CLI.

*Considered and rejected as the foundation:* **Syncthing** itself. The
obvious "existing software" answer is: run an S3 or REST server locally,
make its data directory a Syncthing send-only folder, and have each
friend hold it as a receive-only folder with file versioning turned on.
That is a working prototype in an afternoon and is worth doing as a
baseline. It falls short as the product because Syncthing does not
preserve write order (a snapshot file can arrive before the packs it
references, so the replica is inconsistent until sync completes), has
no per-peer quota, treats versioning as a user-side convenience rather
than a guarantee the borrower cannot bypass, gives no "offsite-complete"
signal, and cannot serve read-through restores. Those are the product's
core guarantees, so wrapping Syncthing would mean fighting it.

*Also rejected:* **rest-server's `repo` package** as the local store. It
is a fine embeddable restic REST handler, but it is a second,
filesystem-based store beside the S3 one, and keeping both means the
restic path and the Kopia path diverge. The REST shim over the S3 store
replaces it. **rclone `serve restic`** and **rclone `serve s3`**: same
objection, plus they are processes without capture hooks.

### D13. Local store: versitygw's POSIX backend, embedded

The daemon embeds **versitygw** (Apache-2.0, Go) through its `embedgw`
package: `embedgw.RunVersityGW(ctx, backend, cfg)` serves the S3 API
over any `backend.Backend`. lendisk passes a decorator around
`backend/posix` that records every `PutObject`,
`CompleteMultipartUpload`, `DeleteObject` and `DeleteObjects` into the
replication queue and serves `GetObject`/`HeadObject` misses by
read-through from a replica. Objects are plain files at
`<store>/<bucket>/<key>`, written temp-then-rename; metadata (ETags)
lives in xattrs.

What this buys over a separate store process:

- One binary, one process. `docker compose` becomes optional packaging
  rather than a requirement.
- Capture is synchronous and in the same process as the backend call.
  A pending queue row is written before the backend call and completed
  after it (EP-3), which closes the crash window that a proxy design
  can only patch by reconciliation.
- No SigV4 pass-through: versitygw terminates authentication itself.
- The borrower's local copy is a plain directory. With the daemon
  stopped, `restic -r <store>/<bucket>` works. The lender's replica is
  the same layout, so there is one storage format in the product.
- The restic REST shim calls the same decorated backend; both front
  ends share one capture point.
- Credentials: lendisk implements versitygw's `auth.IAMService` over its
  own SQLite, one key pair per bucket, so no plaintext `IAMDir` and no
  admin API.

The trust model (D1a) is untouched: the store is a directory, the S3
listener binds loopback only (`Ports = ["127.0.0.1:9000"]` and a unix
socket), and everything that crosses between households is the lendisk
peer protocol over tailcat (NET-7).

Known costs and their handling:

- *Maturity and API churn.* `embedgw` is recent (added across
  v1.8–v1.10) and `backend.Backend` may change between minors. The
  version is pinned and vendored; the decorator is the only file that
  touches the interface; CI runs restic and Kopia end to end on every
  upgrade.
- *xattrs are required.* ext4, xfs, btrfs, zfs and Docker overlay2
  volumes support them; SMB and some NAS exports do not. `init` tests
  the store directory and refuses one without xattr support (EP-12).
- *Multipart completion copies parts into the final file.* On btrfs and
  xfs `copy_file_range` is a reflink; on ext4 it is a real copy for
  objects above the client's multipart threshold (16 MiB for minio-go).
  Accepted.
- *Listings are `readdir` plus sort.* A gate (Q-F) measures listing
  time at 500k objects; if it fails, the decorator serves `ListObjects`
  from lendisk's own index, which it maintains anyway.
- *No scrub.* The daemon runs a daily local sample hash check against
  its index (VER-5).
- *One `RunVersityGW` per process.* One store per node; buckets are
  directories under it. Fine.

*Alternatives considered:*

- **Garage** (Rust, AGPL-3.0) as a single-node sidecar behind a
  capturing proxy, which was v0.4's design. Mature, built for small
  hardware, indexed listings, block scrub. Rejected once embedding was
  confirmed possible: it is always a second process, its on-disk format
  (hashed blocks plus LMDB) is opaque to restic, it needs the proxy and
  its SigV4 pass-through, and it cannot be embedded under AGPL. Garage
  remains a supported **external** S3 replica target (D12), where its
  lack of versioning means lendisk-side tombstones apply (S3-4).
- **MinIO**: AGPL, heavier, and its community edition has been shedding
  features since 2025.
- **SeaweedFS**, **RustFS**: capable but larger or younger than needed,
  and not embeddable.
- **Any of these as a cluster spanning friends**: rejected for the trust
  reasons in D11; a cluster is one trust domain.

### D12. S3-compatible storage as a second kind of lender

A replication target is anything that implements the replica interface
(`Put`, `Get` with range, `Head`, `List`, `Delete`, `Undelete`, `Verify`).
Friends implement it over tailcat (NET-7). An S3-compatible bucket
implements it over the S3 API. Both appear in a repo's replication set
and count towards `min_replicas`.

Why: it removes the "I only have one friend" problem, it costs little
(the `minio-go` client and one interface implementation), and the
bucket holds a plain restic layout, so **restic can restore from it
directly** (`restic -r s3:…`) with no lendisk installed. That is a
second recovery path and the strongest zero-lock-in guarantee in the
product.

Retention and ransomware protection are delegated to the bucket:
versioning plus a lifecycle rule provide tombstones, and the borrower
is told to use a credential **without delete permission**, which makes
the copy immune to a compromised borrower machine, a stronger promise
than a friend's node can make (RET-2 relies on the friend's daemon
being honest and intact).

*Not used for:* the backing store of a friend's lender (the lender's
replica is a local filesystem; a lender who wants object storage behind
it can use a FUSE mount, but it is unsupported), nor as the borrower's
local store, which is the embedded POSIX backend (D13).

Tested in CI against MinIO; documented for Backblaze B2, Wasabi,
Hetzner Object Storage, Cloudflare R2 and Garage.

## 4. Requirements

IDs are stable. "Shall" is mandatory for v1; "should" is desired; "may" is
optional. Each requirement has an acceptance criterion (AC) where one is
meaningful.

### 4.1 Identity and recovery

- **ID-1** A node shall have exactly one Ed25519 identity. The node ID is
  the public key, displayed in a human-readable encoding (base32, with a
  checksum). AC: two nodes initialised from the same phrase have the same ID.
- **ID-2** `lendisk init` shall generate a 24-word BIP-39 recovery phrase,
  derive the identity from it, show it once and require the user to confirm
  they wrote it down.
- **ID-3** The daemon shall store the private key encrypted at rest with a
  key kept in the config directory (mode 0600). The phrase itself is never
  stored.
- **ID-4** Every node shall maintain a node manifest containing its peer
  list (IDs, relay hints, last known addresses), bucket list, replication
  sets and settings. The manifest is encrypted with a key derived from the
  identity and pushed to every lender whenever it changes.
- **ID-5** `lendisk recover` shall rebuild a node from (a) the recovery
  phrase and (b) the address of any one lender (its `tc…` string, which
  carries the lender's key and relay hints). It shall fetch the manifest,
  re-establish all peers and list all repos. AC: on a fresh VM with nothing
  but the phrase, one lender address and the restic password,
  `restic restore` of the latest snapshot succeeds.
- **ID-6** Recovery shall support read-through restore: `restic restore`
  against the recovered node fetches objects from a lender on demand, with
  no requirement to download the entire repo first. Fetched objects are
  cached locally.
- **ID-7** `lendisk recovery-sheet` shall print a page meant for paper:
  the phrase, the node ID, the addresses of all lenders, and the restore
  commands. It shall remind the user that the restic password is a
  separate secret. The sheet shall be regenerated (and the user alerted)
  whenever a lender's address changes.

### 4.2 Peers and invites

- **PEER-1** A lender shall create an invite with `lendisk invite --quota
  500G [--retention 30d]`. The invite is a single string containing the
  lender's tailcat address (key, relay and direct-address hints), the
  offered quota and a single-use secret valid for a configurable time
  (default 7 days). The lender's `AllowedClients` admits the pending
  invite's secret-derived key until it is redeemed or expires.
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
- **PEER-8** Each peer's data on a lender shall be isolated in its own
  directory and addressable only with that peer's identity. AC: peer A
  cannot list, read, or delete peer B's objects.
- **PEER-9** The binary shall support a lender-only profile (`lendisk init
  --lender-only`, or `role = "lender"` in config) that disables the restic
  endpoint and the borrower-side machinery. It is the install for the
  friend who contributes a disk and backs up nothing. AC: in this profile
  nothing listens on loopback and `lendisk status` shows only lent space.
- **PEER-10** A node shall never disclose its peer list, or the existence
  of any peer, to another peer. There is no peer discovery, no gossip and
  no "friends of friends". AC: with A→B and B→D relationships, A's node
  has no record of D and D's has none of A, in logs, status output, the
  SQLite state or on the wire.
- **PEER-11** A lender shall never replicate a borrower's objects to any
  third node. Replication is initiated only by the owner of the data.
- **PEER-12** Each relationship shall be revocable independently of any
  other, including the two halves of a `--mutual` pair.

### 4.3 Endpoint and local store

- **EP-1** The daemon shall serve S3 on loopback (`127.0.0.1:9000`) and
  a unix socket through embedded versitygw (D13) over the decorated
  POSIX backend. AC: restic's and Kopia's S3 backends, `rclone` and the
  AWS CLI all work with path-style addressing; `restic check
  --read-data` and `kopia repository validate-provider` pass.
- **EP-2** The decorator shall enqueue a write on a successful
  `PutObject` or `CompleteMultipartUpload` and a delete on a successful
  `DeleteObject` or each successful entry of `DeleteObjects`. Parts,
  aborted uploads and failed calls never reach the queue. AC: the
  decorator adds less than 5% wall-clock overhead to a 10 GB
  `restic backup` and a 10 GB `kopia snapshot` against undecorated
  versitygw.
- **EP-3** Capture shall be crash-safe: the decorator writes a pending
  queue row before calling the backend and marks it complete after the
  backend returns success; on failure it deletes the row. At startup,
  pending rows are resolved by checking the filesystem. The decorator
  shall not buffer or hash request bodies; hashing happens when the
  replication worker reads the object to send it (REPL-10). AC: kill the
  daemon with `SIGKILL` at random points during a 10 GB backup, restart,
  and every object present on disk is replicated and none is replicated
  twice.
- **EP-4** The daemon shall also expose restic's REST protocol (v1 and
  v2 listings) on `127.0.0.1:8000` and a unix socket, with basic-auth
  credentials generated at init, implemented as calls on the same
  decorated backend. AC: restic's backend test suite passes against the
  shim; an object written via REST is visible via S3 and vice versa.
- **EP-5** Neither endpoint shall ever listen on a non-loopback
  interface.
- **EP-6** Buckets shall be created with `lendisk bucket add <name>
  --profile restic|kopia|generic --replicas peerA,peerB [--min-replicas
  N] [--mode full|evict]`. The command creates the bucket in the store,
  issues an S3 key pair scoped to it, and prints the configuration
  lines for the chosen tool. `min_replicas` defaults to 1. When a bucket
  has exactly one replica, `bucket add`, `status` and the daily summary
  shall say so and recommend a second (a friend or an external S3
  bucket, §4.11); this nudge is suppressed once a second replica exists
  or the user sets `min_replicas = 1` explicitly.
- **EP-7** Tool profiles shall define: how to recognise snapshot or
  manifest keys (restic: `snapshots/*`; Kopia: `xn*`/`xs*` manifest
  blobs and `kopia.repository`), which keys are metadata and never
  evicted (restic: everything but `data/`; Kopia: everything but
  `p*`/`q*`/`s*` pack blobs), and whether the profile is inside the
  encryption guarantee (restic and kopia: yes; generic: no). Profiles
  are data, not code; adding one shall not require a release.
- **EP-8** `generic` buckets shall require `--i-understand-no-encryption`
  at creation and be marked in `status` as outside the trust guarantee.
- **EP-9** In `evict` mode, pack objects shall be removed from the local
  store only after `min_replicas` replicas have acknowledged them, and
  never while a tool lock is present (restic `locks/*`; Kopia has no
  server-side lock, so eviction waits for the bucket to be idle for a
  configurable interval). Metadata objects are never evicted.
- **EP-10** Reads of an evicted or missing object shall be served from a
  replica on demand: the decorator's `GetObject`/`HeadObject` catch a
  not-found from the POSIX backend, fetch from a replica, write the
  object into the store, and return it.
- **EP-11** Immutability policy: a write to a key that a replica has
  already acknowledged, with content whose hash differs, shall raise an
  alert, shall not be replicated, and shall be reported in `status` with
  the key and both hashes. The local write itself is allowed (the local
  store is the user's). Identical re-uploads are silently accepted.
- **EP-12** `lendisk init` shall verify that the store directory's
  filesystem supports xattrs and has at least the configured free-space
  floor, and refuse otherwise with a message naming the filesystem.
  The daemon shall pin the versitygw version it was built with and
  record it in `status`.
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
- **REPL-3** The replication queue shall be persisted (SQLite) and survive
  restarts, crashes and days-long peer outages. In `full` mode the queue is
  unbounded; in `evict` mode the local disk holds every unacknowledged pack.
- **REPL-4** A snapshot shall be marked offsite-complete when it and every
  object queued before it are acknowledged by at least `min_replicas`
  lenders. `lendisk status` shall list each repo's latest snapshot, its
  offsite-complete state, and the replication lag per lender.
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
- **REPL-10** When sending an object, the replication worker shall read
  it from the local store, compute its SHA-256 while streaming, send the
  hash in a header, and record the hash in its index on acknowledgement.
  The lender verifies the received bytes against the header and rejects
  a mismatch (LEND-4). The index of acknowledged hashes is what VER-1
  challenges and EP-11 violations are checked against.
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
  refuse new objects with a distinct "no space" error. The borrower's
  daemon pauses that queue, raises an alert and retries hourly. Nothing is
  deleted.
- **LEND-3** A borrower shall be able to see remaining quota at each lender
  (`lendisk peer list`).
- **LEND-4** A lender shall verify every received object against the
  SHA-256 the borrower sends with it (REPL-10), reject a mismatch, and
  reject a write to an existing key whose stored hash differs. It shall
  never need the backup tool's password and shall never inspect object
  contents beyond the hash.
- **LEND-5** A lender may rate-limit each borrower independently.
- **LEND-6** The lender's replica shall be a plain filesystem: each key
  stored at its path under `lent/<borrower-id>/<bucket>/`, plus a
  sidecar `tombstones/` directory and a SQLite index of hashes. For a
  restic bucket this is a restic repository readable with
  `restic -r <path>`; for a Kopia bucket `lendisk export --kopia-layout`
  shall write the sharded layout Kopia's filesystem backend expects.
  Lenders do not run the local store (D13); the lender-only profile is
  a single binary.
- **LEND-7** Capacity planning shall be built in. `lendisk init` shall
  ask for the expected repository size and intended number of lenders
  `k`, and suggest lending `1.2 × k × R`. `lendisk status` shall show, in
  one place, local copy size, space consumed at each lender, space lent
  and used by each borrower, and the ratio consumed/lent. The user guide
  shall state the rule of thumb in §8.

### 4.6 Deletion, retention and verification

- **RET-1** A lender shall never remove object data on a delete request.
  The object moves to tombstones and is purged after the retention window.
  Default 30 days; the lender sets a minimum, the borrower may request a
  longer window.
- **RET-2** There shall be no protocol operation that purges a tombstone
  before the window expires. AC: a borrower with full daemon credentials
  cannot cause offsite data loss within the window.
- **RET-3** `lendisk undelete <bucket> [--since <time>]` shall ask each lender
  to restore tombstoned objects to the live set, so that an attacker's
  `forget --prune` can be reversed within the window.
- **VER-1** The borrower's daemon shall challenge each lender daily per
  repo: it picks a random object and a random byte range, sends a nonce,
  and compares the lender's HMAC(nonce, range) with the local copy. A
  failed challenge raises an alert and marks the replica unverified.
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
- **NET-5** Relays shall be DERP servers. The project shall publish a DERP
  map of relays it operates; nodes shall accept any DERP map URL or an
  explicit relay list instead; `lendisk relay` shall wrap `derper` so a
  user can host one on a VPS. Tailscale's hosted relays may be included
  as a last-resort bootstrap but the product shall work with them removed.
  AC: scenario 1 passes with the project relays as the only relays, and
  again with a single self-hosted relay.
- **NET-5a** A node shall use only relays from its own configured set,
  never a relay named in a peer's address hint unless the user allows
  it (`relays.accept_peer_hints = true`, default false). A relay sees IP
  addresses and traffic volume; which relays may see that is the user's
  decision, not a peer's.
- **NET-6** The peer protocol shall carry a version. A node shall
  interoperate with peers one minor version behind or ahead, and shall
  refuse with a clear error otherwise.
- **NET-7** The peer protocol shall be HTTP over the tunnel: `HEAD`,
  `GET` with Range, `PUT` with `X-Lendisk-SHA256`, `DELETE` and a paged
  `LIST` on `/<bucket>/<key>`, plus lendisk endpoints: `undelete`,
  `challenge`, `manifest`, `quota`, `version`. The borrower's identity
  is the tunnel peer key; there is no second authentication layer. AC: a
  lender's replica can be listed with plain `curl` from an allowed peer,
  and the peer-protocol spec fits on two pages.
- **NET-8** The transport shall sit behind an internal interface
  (`Listen`, `Dial(peerKey)`, `AllowedPeers`, `PathInfo`) so that tailcat
  can be replaced without touching replication, storage or the CLI.

### 4.8 Operations and observability

- **OPS-1** `lendisk status [--json]` shall report: identity, peers and
  reachability, each repo's latest snapshot and offsite-complete state,
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
- **OPS-4** The daemon should expose Prometheus metrics on loopback.
- **OPS-5** Logs go to stdout/journald in structured form, with a
  configurable level.
- **OPS-6** Configuration shall be one TOML file; every setting shall have
  a CLI flag and env var equivalent. State lives in SQLite. The data
  directory layout shall be documented.
- **OPS-7** The daemon shall run as an unprivileged user and should stay
  under 256 MiB RSS when idle with a 1 TiB repo and 10 peers.
- **OPS-8** Upgrades shall migrate state automatically and shall support
  downgrade by one minor version.

### 4.9 Security

- **SEC-1** The lender shall be unable to read backup contents, learn file
  names, or alter data undetected. (Provided by restic; lendisk shall not
  weaken it, e.g. by requiring the restic password anywhere.)
- **SEC-2** Metadata a lender can observe, and that the product accepts
  leaking: object sizes and counts, timing of backups, borrower IP address
  unless relayed. This shall be documented for users.
- **SEC-3** Secrets on disk shall be limited to: the identity key, the
  REST shim credentials, and per-bucket S3 keys in lendisk's SQLite,
  all mode 0600 in the config directory. versitygw's `IAMDir` shall not
  be used.
- **SEC-8** The embedded S3 server shall bind loopback and a unix
  socket only, with versitygw's admin, web UI and website listeners
  disabled. AC: `ss -lntp` shows no lendisk listener on a non-loopback
  address.
- **SEC-4** A compromised borrower machine shall not be able to cause
  offsite data loss within the retention window (RET-2) nor to read
  another borrower's data on a shared lender (PEER-8).
- **SEC-5** A malicious borrower shall not be able to exceed quota, fill
  the lender's disk below the floor (LEND-2), or write outside their
  directory (path names are validated against the restic naming rules).
- **SEC-6** Invite secrets shall be single-use and time-limited (PEER-1).
- **SEC-7** Compromise of one peer shall affect only the relationships
  that peer is part of. With A→B, A→C and B→D: a compromised B can lose
  or withhold A's replica at B (detected by VER-1) and sees A's traffic
  metadata, and nothing else. It cannot reach A's replica at C, cannot
  impersonate A to C, and cannot cause C or D to learn anything about A.
  AC: a test with a hostile B node exercising every peer-protocol
  endpoint against A and C.

### 4.10 Compatibility

- **COMPAT-1** Supported backup tools in v1: restic and Kopia, each at
  the latest release and the two before it, tested in CI through both
  the S3 endpoint and (restic) the REST shim. Duplicacy and rclone should
  work via `generic` and are smoke-tested.
- **COMPAT-2** Supported platforms for v1: Linux amd64 and arm64, as a
  single static binary with a systemd unit, and as a Docker image
  (`network_mode: host` recommended so tailcat is not behind a second
  NAT). ARMv7 should work. versitygw also builds on macOS, BSD and
  Windows, which keeps post-v1 ports open.
- **COMPAT-3** An existing repository (restic directory or rest-server
  data, Kopia filesystem repository) shall be importable with
  `lendisk bucket import <name> <path>`, which uploads it into the store
  and marks nothing as acknowledged.
- **COMPAT-4** The store directory layout shall be documented as
  `<store>/<bucket>/<key>` with versitygw's `.sgwtmp/` and `.vgwlocks/`
  housekeeping directories, so that users and other tools can read it.

### 4.11 S3 replicas

- **S3-1** `lendisk replica add s3 <name> --endpoint … --bucket …
  --prefix …` shall register a bucket as a replication target; it then
  appears in `--replicas` lists and in `status` like a peer.
  Credentials are stored in the config directory (mode 0600) and in the
  node manifest (ID-4), so recovery through a friend also recovers
  bucket access.
- **S3-2** The replica layout in the bucket shall be the standard restic
  layout under the prefix. AC: `restic -r s3:<endpoint>/<bucket>/<prefix>
  snapshots` works with no lendisk installed.
- **S3-3** Uploads shall carry a SHA-256 checksum (`x-amz-checksum-sha256`
  where supported, otherwise Content-MD5) so the store rejects corrupted
  transfers. Objects above 64 MiB use multipart upload with resume.
- **S3-4** Retention shall use bucket versioning and a lifecycle rule
  that expires non-current versions after the retention window. `replica
  add` shall check that versioning is enabled and warn if not; it shall
  offer to create the lifecycle rule when the credential allows it.
  `undelete` shall remove delete markers created within the window.
  For providers without versioning (e.g. Garage), the daemon shall fall
  back to lendisk-side tombstones: deletes are recorded locally and
  issued to the bucket only after the retention window. This protects
  against accidental deletion but not against a compromised borrower,
  and `status` shall label such replicas "retention: client-side".
- **S3-5** The documentation shall tell the user to create a credential
  with put/get/list but no delete permission, and `replica add` shall
  test for and report delete permission, recommending it be removed.
  With such a credential, restic's `prune` through lendisk produces
  tombstones that only the lifecycle rule ever purges.
- **S3-6** Verification shall compare stored checksums (via
  `GetObjectAttributes` or HEAD) against local, and download a random
  sample as in VER-2. A bucket cannot answer an HMAC challenge, and the
  spec accepts that this trusts the provider's checksum.
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
- Deduplication across borrowers on a lender (restic repos are encrypted
  per repo; nothing to dedupe).

## 6. Resolved questions and remaining gates

- **Q-A (resolved)** `min_replicas` defaults to 1 with a nudge towards a
  second lender (EP-6).
- **Q-B (resolved)** Relays are reused, not built: DERP via tailcat
  (D5, NET-5). The project runs a few `derper` instances; the cost is a
  couple of small VPSes. Tailscale's hosted relays are a bootstrap
  convenience only, since they are throttled and revocable.
- **Q-C (gate, open)** Before v1 ships, run a field test with at least
  10 households on consumer NAT, including CGNAT and at least one
  symmetric NAT. Gate: ≥80% of peer pairs reach a direct path, 100% can
  replicate via relay, and a 100 GB initial seed completes unattended.
  If the gate fails and the cause is tailcat rather than the relays,
  the fallback is iroh and Rust (D10).
- **Q-D (resolved)** Lender-only profile exists (PEER-9).
- **Q-E (open)** Does tailcat's two-minute UDP idle timeout or its
  short-lived positioning cause problems for day-long transfers and
  weeks-idle peers? Needs a soak test early: two nodes replicating for
  72 hours, then idle for 7 days, then replicating again without a
  restart.
- **Q-F (resolved, with a gate)** versitygw embedded via `embedgw`
  (D13); the embeddability question that drove this is answered by
  `embedgw.RunVersityGW(ctx, backend.Backend, *Config)` at v1.10.0.
  Garage was the sidecar alternative and is now an external replica
  target only. Remaining gate: measure on a Raspberry Pi 4 and a
  Synology-class NAS: RSS, end-to-end write throughput, `ListObjects`
  time at 500k objects in one bucket, and behaviour on a full disk. If
  listing fails the gate, the decorator serves listings from lendisk's
  own index.
- **Q-G (resolved)** No proxy, so no SigV4 pass-through. versitygw
  terminates authentication in-process.
- **Q-H (open)** The decorator must be checked against the full
  `backend.Backend` interface at the pinned version: every method that
  can create, replace or remove an object (including `CopyObject`,
  `UploadPartCopy`, `PutObjectTagging`, `RestoreObject` if present) is
  either captured or disabled. A method that mutates objects without
  being captured is a replication hole. Re-run on every versitygw
  upgrade.

## 7. Acceptance scenarios

1. **First backup.** Two nodes, A behind NAT at home, B behind NAT at a
   friend's. B invites A with 500 GB. A adds bucket `laptop --profile restic` with replica B.
   `restic backup` on A finishes at local disk speed. Within the bandwidth
   available, `lendisk status` shows the snapshot offsite-complete.
2. **Friend offline.** B is powered off for three days. `restic backup` on
   A keeps working. Status shows lag; at 24h an alert fires. When B
   returns, the queue drains and the snapshot becomes offsite-complete.
3. **Total loss.** A's house burns down. On a new machine: `lendisk
   recover` with the phrase and B's address from the paper sheet;
   `restic -r rest:... restore latest` succeeds via read-through without
   downloading the whole repo.
4. **Ransomware.** An attacker on A runs `restic forget --keep-last 0
   --prune` and wipes A's disk. Within 30 days, A recovers as in scenario 3
   and runs `lendisk undelete laptop`; all snapshots are back.
5. **Bad lender.** B silently deletes half of A's packs. Within a day, the
   daily challenge fails and A is alerted that the replica is unverified.
6. **Quota.** A exceeds 500 GB at B. The local backup still succeeds;
   status shows replication paused for quota; B raises the quota and
   replication resumes without intervention on A.
7. **Zero lock-in.** With both daemons stopped: on B, `restic -r
   /var/lib/lendisk/lent/<A-id>/laptop snapshots` works on the plain
   files; on A, `restic -r /var/lib/lendisk/store/laptop snapshots`
   works the same way. No service is running on either side.
9. **Kopia.** A creates bucket `photos --profile kopia`, runs `kopia
   repository create s3 …` against the S3 endpoint and takes a snapshot. Status
   shows the manifest offsite-complete at B. Kopia maintenance deletes
   old packs; B tombstones them. A restores a file on a fresh machine
   via read-through after `lendisk recover`.
8. **Partial trust.** A lends to B and C; B lends to D; A and D have no
   relationship. D cannot connect to A (rejected at the WireGuard
   handshake), cannot list or read A's directory on B, and does not
   appear anywhere in A's state. A's `peer list` shows B and C only.
   Revoking A→B leaves A→C untouched and B's replica of A's data is
   purged after the retention window.

## 8. Capacity planning

Mirroring (D6) sets the disk cost. Let `R` be the restic repository
size: source data after deduplication and compression (typically
0.5–1.0× the source; ~1× for photos and video), plus the snapshot
history the user's `forget` policy keeps. Let `k` be the number of
lenders.

**What a borrower consumes elsewhere:** `k × R`, plus headroom at each
lender for tombstones (RET-1) and the lender's free-space floor
(LEND-2). A lender should set quota ≈ `1.2 × R` per borrower.

**What a node needs on its own disk:**

| | `full` mode (default) | `evict` mode |
|---|---|---|
| Local copy | `R` | ~1% of `R` for metadata, plus a buffer for packs not yet acknowledged by `min_replicas` lenders (at least one backup run of new data) |
| Space lent | user's choice; fair share is `1.2 × k × R` | same |

Worked example, 100 GB of source data, `R ≈ 100 GB`, two lenders:

- consumes ~200 GB at friends (240 GB with headroom);
- needs ~100 GB locally in `full` mode, ~1 GB plus buffer in `evict`;
- fair share to lend back: ~240 GB;
- total own disk: ~340 GB (`full`) or ~240 GB (`evict`).

Two friends backing each other up (`k = 1`): ~220 GB each per 100 GB
of data.

**Rule of thumb for users:** lend `k` times what you back up, plus 20%.
Reciprocity is not enforced (D7); this is the number that makes a
symmetric group work. An S3 replica (§4.11) counts in `k` but costs
money instead of lent disk: one friend plus one bucket means lending
~1.2 × R and paying for ~R.

Erasure coding would reduce the multiplier to roughly 1.5–2×; D6 rejects
it because restores would then depend on several friends being online at
once.
