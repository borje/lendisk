# lendisk — design

Status: draft v0.7, 2026-10-28. Companion to REQUIREMENTS.md.

## 0. How to read this document

Everything here is a choice, and every choice may be revisited. A
decision stays as long as it is the best known way to meet the
requirements it serves; when a better way appears it is replaced and
REQUIREMENTS.md does not change. If no known design can meet a
requirement, that is a requirements problem and is settled in
REQUIREMENTS.md first, with its own version number.

Each decision in §2 names the requirements it serves and the
alternatives it beat. §3 says how each requirement area is realised
with those decisions: this is where component names, ports, paths,
headers, formats and algorithms live, and it is the first place to
edit when a decision changes. §4 derives the capacity numbers. §5
holds the open gates on the current design and the questions already
closed.

History: v0.4 generalised the product from restic-only to any
S3-speaking backup tool. v0.5 replaced the Garage sidecar and capturing
proxy with versitygw embedded in the daemon (D4, D13). v0.6 made every
replica an S3 target: a friend's node serves S3 over the tunnel and the
borrower uses one replica implementation for friends and external
buckets alike (D12). v0.7 split requirements from design; the trust
model is unchanged throughout.

## 1. Name

"lendisk" is a working title. It says what the product does but is
awkward to pronounce and names only the lender's side. Candidate
replacement: **belay** (a climbing partner holds your rope: pairwise,
directional, friends only, and the whole point is the fall you hope never
happens; `belay invite`, `belay recover`, `belayd`). Runners-up: `kith`
(friends you know), `lodge` (lodge your backups with a friend), `escrow`
(a holder who cannot open the box). Rejected for collisions: `attic`
(borg's predecessor), `safekeep` (rdiff-backup wrapper, packaged in
Fedora), `stow`, `burrow`, `cellar`. Decide before the first public
release; the command name appears throughout REQUIREMENTS.md §4.

## 2. Decisions

IDs are stable. D1, D1a, D7, D8 and D9 were decisions in earlier
versions and are now principles P-1, P-2, P-8, P-6 and P-7 in
REQUIREMENTS.md: they turned out to be what the product *is*, not how it
is built, so there is nothing left to decide here. Their mechanisms are
in §3.

### D2. Packaging: one static binary, a container image, one TOML file

*Serves:* P-9, COMPAT-2, OPS-6.

- Single static binary for Linux amd64 and arm64, plus a Docker image for
  Synology, TrueNAS and Unraid. A systemd unit file ships with it.
  `network_mode: host` is recommended for the image so the overlay is
  not behind a second NAT.
- Configuration is one TOML file (`BurntSushi/toml`); the CLI is
  `cobra`. Every setting has a flag and an env var (OPS-6).

### D3. Write model: local staging with asynchronous replication

*Serves:* P-4, P-5, REPL-9, EP-9, EP-10, ID-6.

The backup tool writes to the local daemon at disk speed. The daemon keeps
the full repository locally and replicates every object to replicas in
the background.

*Rejected:* synchronous pass-through to a peer. Backups would run at WAN
upload speed, fail whenever the peer is offline, and give the user no local
copy. Local staging gives 3-2-1 (one local copy, N offsite) for free and
makes most restores fast.

The two storage modes (`full`, `evict`) are a requirement (EP-9); this
decision is how they are served: in `evict` mode metadata (`config`,
`keys`, `index`, `snapshots`, `locks`) stays local and evicted packs are
fetched from a replica on demand through the same read path (EP-10).

### D4. Endpoint: S3 served in-process, restic REST as a shim

*Serves:* EP-1 to EP-5, EP-11, COMPAT-1, P-5.

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

What changed versus a restic-only design, and the replacement for each:

- *Name-equals-hash validation* (restic names every object by SHA-256;
  Kopia names pack blobs `p<random>`) is replaced by the **immutability
  policy** (EP-11): once a key is acknowledged by a replica, a write to
  the same key with different content is a violation. The daemon hashes
  each object when the replication worker reads it from the store to
  send it, which it must do anyway, and sends the hash with the object;
  the lender verifies the bytes against it (LEND-4).
- *"Snapshot X is offsite-complete"* becomes a per-bucket high-water mark
  in the write sequence (REPL-11). Tool profiles (`restic`, `kopia`)
  recognise snapshot or manifest keys and present the watermark as
  "snapshot X"; `generic` shows a timestamp.
- *Write ordering* (REPL-1) generalises unchanged: restic and Kopia both
  write packs, then indexes, then the snapshot or manifest, so FIFO
  replication keeps the replica consistent at every watermark.
- *Multipart uploads* are enqueued on `CompleteMultipartUpload` only;
  parts and aborted uploads never reach the queue (EP-2).
- *Zero lock-in* (P-5) holds on both sides. The local store keeps
  objects as plain files at their key paths, so with the daemon stopped
  `restic -r /var/lib/lendisk/store/<bucket>` opens the borrower's own
  copy, and Kopia's filesystem backend opens a Kopia bucket. The lender's
  replica is the same keys-as-paths layout.
- *The trust promise* is per tool profile (EP-7, EP-8). restic and Kopia
  always encrypt client-side, so the "lender never sees plaintext"
  guarantee holds. A `generic` bucket (rclone, `aws s3 cp`) is outside
  the guarantee.

*Rejected:* sftp (needs an SSH server); writing an S3 implementation
(`gofakes3` is a test double and the protocol surface is large);
embedding rest-server's filesystem handler as the store (a second store
beside the S3 one, so the restic path would diverge from the Kopia
path); a separate S3 store behind a capturing proxy (v0.4's design: a
second process, a compose requirement, SigV4 pass-through risk, and a
crash window between the store's acknowledgement and the queue entry).

### D5. Overlay network: Tailscale's data plane via `tailcat`

*Serves:* NET-1 to NET-5a, NET-8, P-2, G-1, G-2.

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
  S3 and the peer control API are plain HTTP inside an authenticated
  WireGuard tunnel. No custom RPC framing, no second transport security
  layer.
- The relay server (`cmd/derper`) is mature and has been self-hosted by
  Headscale users for years.

Risks, and how the design handles them:

- tailcat is two months old and states no API stability guarantee. The
  underlying components are mature; the wrapper is thin. Pin the version
  and keep it behind the internal `Transport` interface (NET-8, §3.7).
- Tailscale positions it for short-lived connections and its hosted
  relays are throttled, carry no SLA and "may be revoked". The product
  therefore never depends on them: the project runs its own `derper`
  fleet, any user can run one, and a peer with a public address never
  needs a relay (NET-5).
- Its two-minute UDP idle timeout and short-lived positioning may hurt
  day-long transfers and weeks-idle peers; the soak gate G-2 and Q-E
  exist for this.

*Alternatives considered*, roughly in order of how close they came:

- **iroh** (Rust, 1.0, derived from Tailscale's magicsock and DERP): the
  credible second choice, with a comparable traversal design and a stable
  wire protocol. n0's public relays are explicitly for hobby use only and
  n0 is a startup; self-hosting `iroh-relay` is easy. iroh gives QUIC
  streams rather than TCP, so S3 and the control API would need HTTP/3
  or a TCP-over-QUIC shim. Pick this if D10 is reversed to Rust.
- **Syncthing's stack** (Go, MPL-2.0): device IDs are certificate hashes,
  trust is pairwise exactly as in P-2, and the project has a volunteer
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
  everyone. That is exactly the transitive trust P-2 rules out.
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
as such in status output (NET-4). Nodes ship with the project's DERP
map, can be pointed at any `derper`, and may optionally include
Tailscale's hosted relays as a last-resort bootstrap (NET-5).

### D6. Replication: whole-repository mirrors, configurable factor

*Serves:* P-5, REPL-1, REPL-4, LEND-6, LEND-7.

Each bucket is mirrored in full to each replica in its replication set.
`min_replicas` (default 1, recommended 2) defines when a snapshot counts as
offsite-complete.

*Rejected:* erasure coding across peers. It reduces storage cost but
makes a restore depend on k-of-n peers being online, and friends' NASes
are not reliable enough for that. Mirrors also keep the zero-lock-in
guarantee: a lender can hand you a disk. The capacity cost is derived
in §4.

### D10. Language and licence: Go, because of D5

*Serves:* COMPAT-2 (static cross-compiled binaries); otherwise a
consequence of D5 and D13.

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

If field testing fails gate G-1 and the cause is tailcat rather than the
relays, the fallback is iroh, and that means a Rust rewrite of a thin
daemon (REST handler, SQLite queue, replication loop). The requirements
are written so that nothing in them changes; in this document only D5,
D10 and §3.7 would.

Licence: Apache-2.0.

### D11. Build on existing software

*Serves:* every requirement, by keeping the daemon small enough to
finish.

Everything that already exists and fits is used as a library, not
reimplemented. The daemon is glue around these:

| Concern | Component | Licence | Notes |
|---|---|---|---|
| Backup format, client, encryption | **restic**, **Kopia**, unmodified | BSD-2 / Apache | Never imported; only their S3 (or REST) usage and key naming conventions are relied on, via tool profiles. |
| S3 protocol and local store | **`github.com/versity/versitygw`**: `embedgw` + `backend/posix` | Apache-2.0 | D13. Embedded in-process; lendisk wraps the backend to capture writes and deletes and serve read-through. Pinned at a tested version. |
| restic REST shim | protocol per restic's `doc/REST_backend.rst` | — | Five operations translated to calls on the wrapped backend. ~300 lines. |
| S3 client (all replicas) | `minio-go` | Apache | D12. |
| Overlay network and NAT traversal | **`github.com/tailscale/tailcat`** | BSD-3 | D5. |
| Relay | **`tailscale.com/cmd/derper`** | BSD-3 | `lendisk relay` is a thin wrapper. |
| State and queues | **SQLite** via `modernc.org/sqlite` | MIT / BSD | Pure Go, no cgo, so static builds stay simple. |
| Recovery phrase | `github.com/tyler-smith/go-bip39` | MIT | |
| CLI, config | `cobra`, `BurntSushi/toml` | Apache / MIT | |
| Metrics | `prometheus/client_golang` | Apache | |

What is written from scratch, because nothing exists: the capturing
backend decorator (D4, D13), the replication queue and its ordering
guarantee (REPL-1), the lender-side policy decorator (quota,
immutability, retention, deny-by-default; D12, §3.5, §3.6), the
challenge protocol (VER-1), the node manifest and recovery flow (§3.1),
invites (PEER-1), tool profiles, an `auth.IAMService` over lendisk's
SQLite with derived per-pair credentials (§3.7), and the CLI.

*Considered and rejected as the foundation:* **Syncthing** itself. The
obvious "existing software" answer is: run an S3 or REST server locally,
make its data directory a Syncthing send-only folder, and have each
friend hold it as a receive-only folder with file versioning turned on.
That is a working prototype in an afternoon and is worth doing as a
baseline. It falls short as the product because Syncthing does not
preserve write order (a snapshot file can arrive before the packs it
references, so the replica is inconsistent until sync completes;
REPL-1), has no per-peer quota (LEND-1), treats versioning as a
user-side convenience rather than a guarantee the borrower cannot
bypass (RET-2), gives no "offsite-complete" signal (REPL-4), and cannot
serve read-through restores (EP-10). Those are the product's core
guarantees, so wrapping Syncthing would mean fighting it.

*Also rejected:* **rest-server's `repo` package** as the local store. It
is a fine embeddable restic REST handler, but it is a second,
filesystem-based store beside the S3 one, and keeping both means the
restic path and the Kopia path diverge. The REST shim over the S3 store
replaces it. **rclone `serve restic`** and **rclone `serve s3`**: same
objection, plus they are processes without capture hooks.

### D12. Every replica is S3

*Serves:* NET-7, NET-7a, NET-7b, LEND-2, LEND-4, RET-1 to RET-3, S3-3,
S3-4, P-5, P-7.

A replication target is anything that implements the replica interface
(`Put`, `Get` with range, `Head`, `List`, `Delete`, `Undelete`, `Verify`).
There is **one implementation** of it, over the S3 API with `minio-go`,
and two kinds of target behind it:

- an **external S3-compatible bucket** (Backblaze B2, Wasabi, Hetzner,
  R2, Garage, MinIO), dialled over the internet with the user's
  credentials (§3.11);
- a **friend's node**, dialled through the tailcat tunnel (§3.7). The
  friend's daemon serves S3 from the versitygw it already embeds for its
  own store (D13), rooted at `lent/<borrower-id>/`, through a lender-side
  policy decorator that adds what S3 lacks: per-borrower quota (LEND-1),
  immutability (LEND-4), retention that the borrower cannot bypass
  (RET-1, RET-2) and a deny-by-default operation allow-list (SEC-9).

Both kinds appear in a bucket's replication set and count towards
`min_replicas`.

Why one implementation rather than a custom peer protocol beside S3
(v0.5's design):

- The custom protocol was S3 with the names changed: `PUT` with a hash
  header, ranged `GET`, `HEAD`, paged `LIST`, `DELETE`. Everything it
  needed that S3 lacks (quota, immutability, tombstones) was going to
  live in the lender's daemon anyway, and it still does, as the decorator
  over the backend that D13 already requires. versitygw supplies SigV4,
  multipart, listings and checksums on the lender side for free.
- One replica implementation, one resume strategy (multipart), one
  verification sampler (VER-2) and one undelete (delete markers) for
  every target.
- The friend's replica becomes a plain S3 endpoint, so **restic and
  Kopia can restore from a friend directly** (`restic -r s3:…` over the
  tunnel) with no lendisk replica code in the path (NET-7b), which
  extends the second recovery path of the external bucket to friends.
- The lender-only profile (PEER-9) is the same binary and the same
  store engine as everyone else, with one fewer code path to test.

What stays outside S3: the **peer control API**, four lendisk endpoints
served beside the S3 listener inside the same tunnel and authenticated
by the tunnel peer key alone: `challenge` (VER-1, S3 has no HMAC
challenge), `manifest` (ID-4, which must be fetchable before any S3
credential exists), `quota` (LEND-3) and `version` (NET-6).

**Credentials.** versitygw requires SigV4, and D5 says the tunnel key is
the identity. These are reconciled by deriving the pair's S3 credential
from the tunnel keys rather than storing it (NET-7a, §3.7): access key
is the borrower's node ID, secret is a key derivation over the X25519
shared secret of the two nodes' keys. Both sides compute it, nothing is
exchanged or written down, and recovery (P-6, ID-5) needs nothing
beyond the phrase and one lender's address. The lender refuses a
connection whose tunnel peer key does not match the access key it
presents, so there is still no second trust decision, only a second
encoding of the same one.

**Retention for external buckets** is delegated to the provider (S3-4):
versioning plus a lifecycle rule provide tombstones, and the borrower
is told to use a credential **without delete permission** (S3-5), which
makes the copy immune to a compromised borrower machine, a stronger
promise than a friend's node can make (RET-2 relies on the friend's
daemon being honest and intact). For friends, versitygw's versioning
directory holds the non-current versions and delete markers, and
lendisk's retention purger plays the lifecycle rule (versitygw has
none).

Costs accepted:

- versitygw's request surface is now reachable by invited peers, so the
  hostile-peer test (SEC-7) covers every S3 operation the decorator
  admits, not a two-page protocol. The allow-list (§3.7) is short and
  everything else is refused before reaching the backend.
- The "listable with plain `curl`" property of the old protocol is
  lost; any S3 client works instead.
- A node that both lends and borrows runs one gateway with two
  listeners (D13).

*Not used for:* the backing store of a friend's lender (the lending store
is a local filesystem; a lender who wants object storage behind it can
use a FUSE mount, but it is unsupported), nor as the borrower's local
store, which is the embedded POSIX backend (D13).

External buckets are tested in CI against MinIO and documented for
Backblaze B2, Wasabi, Hetzner Object Storage, Cloudflare R2 and Garage.
Friend replicas are tested in CI with two nodes over tailcat.

### D13. Local store: versitygw's POSIX backend, embedded

*Serves:* EP-1 to EP-4, EP-10, EP-12, LEND-6, COMPAT-4, SEC-3, SEC-8,
P-5.

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
  admin API (SEC-3).

The trust model (P-2) is untouched: the store is a directory, the
borrower-facing S3 listener binds loopback only (`127.0.0.1:9000` and a
unix socket), and everything that crosses between households is S3 plus
the peer control API, both inside the tailcat tunnel and both served by
the same process (D12).

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
- *Listings are `readdir` plus sort.* Gate G-3 measures listing time at
  500k objects; if it fails, the decorator serves `ListObjects` from
  lendisk's own index, which it maintains anyway (Q-F).
- *No scrub.* The daemon runs a daily local sample hash check against
  its index (VER-5).
- *One `RunVersityGW` per process.* One gateway per node with two
  listeners: loopback for the node's own backup tool and the tunnel
  for borrowers (D12). The local store and the lending store are two
  bucket sets under one backend root; `IAMService` accounts keep them
  apart, since the local credential owns only local buckets and each
  borrower's account owns only its own (PEER-8). Gate Q-I checks that
  `embedgw` accepts a `net.Listener` for the tunnel side.

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

## 3. Realisation, by requirement area

Section numbers mirror REQUIREMENTS.md §4. Each entry says how the
requirement is met with the decisions above. Nothing here adds to or
narrows a requirement; if a line seems to, the requirement is wrong or
this line is.

### 3.1 Identity and recovery

- **ID-1** The identity is an Ed25519 keypair; the same key, converted,
  is the WireGuard key tailcat uses (D5), so node ID and tunnel identity
  are one key. The display form is base32 with a checksum.
- **ID-2** The 24 words are BIP-39 (`go-bip39`); the identity is derived
  from the BIP-39 seed with a fixed derivation path. Two nodes from the
  same phrase derive the same key (the AC).
- **ID-3** The private key file is encrypted with a key stored beside
  it in the config directory, mode 0600. The phrase is never written.
- **ID-4** The manifest is encrypted with a key derived from the
  identity (HKDF over the private key with a fixed info string) and
  pushed to every lender through the control API `manifest` endpoint
  (D12), which the lender stores under `lent/<borrower-id>/.manifest`.
  Addressing hints are the peer's `tc…` string plus any explicit
  addresses (NET-3).
- **ID-5** A lender's address is its tailcat `tc…` string, which carries
  the public key and relay bootstrap info. `recover` derives the key
  from the phrase, dials the lender, fetches the manifest over the
  control API (no S3 credential needed yet), then derives the per-pair
  S3 credentials (§3.7) for every lender listed.
- **ID-6** Read-through is the decorator's `GetObject`/`HeadObject`
  miss path (EP-10); after `recover` every object is a miss until
  fetched.
- **ID-7** The sheet is generated from the manifest; a lender's `tc…`
  string changes only when its key changes, so regeneration is rare.

### 3.2 Peers and invites

- **PEER-1** The invite string carries the lender's `tc…` address, the
  quota, the retention offer and a single-use secret. The lender adds a
  key derived from the secret to its tailcat `AllowedClients` until the
  invite is redeemed or expires; the borrower dials with that key to
  perform the handshake, after which its real key replaces it.
- **PEER-8** Each borrower is one `IAMService` account that owns only
  its own buckets under `lent/<borrower-id>/`; versitygw's bucket
  ownership check plus the policy decorator refuse everything else.
- **PEER-9** The lender-only profile skips the loopback S3 listener and
  the REST shim; versitygw still runs, on the tunnel listener only.
- **PEER-10** Peers are rows in lendisk's SQLite; nothing in the control
  API or the S3 surface returns them, and `ListBuckets` is filtered to
  the caller's own account.

### 3.3 Endpoint and local store

- **EP-1** Embedded versitygw (D13) over the decorated POSIX backend,
  listening on `127.0.0.1:9000` and a unix socket. Path-style
  addressing is versitygw's default.
- **EP-2** The decorator enqueues on a successful `PutObject` or
  `CompleteMultipartUpload`, and a delete on a successful
  `DeleteObject` or each successful entry of `DeleteObjects`. Parts and
  aborted uploads do not pass through those methods.
- **EP-3** The decorator writes a pending queue row before calling the
  backend and marks it complete after the backend returns success; on
  failure it deletes the row. At startup, pending rows are resolved by
  checking the filesystem. The decorator does not buffer or hash
  request bodies; hashing happens when the replication worker reads the
  object to send it (REPL-10).
- **EP-4** The REST shim listens on `127.0.0.1:8000` and a unix socket
  and translates the five REST operations (and v1/v2 listings) into
  calls on the same decorated backend. About 300 lines.
- **EP-6** Bucket credentials are one versitygw access/secret pair per
  bucket, issued by lendisk's `IAMService`.
- **EP-10** The decorator's `GetObject`/`HeadObject` catch a not-found
  from the POSIX backend, fetch from a replica through the replica
  interface (D12), write the object into the store, and return it.
- **EP-12** The filesystem property tested is xattr support (versitygw
  keeps ETags in xattrs). The daemon pins the versitygw version it was
  built with and records it in `status`.
- **EP-14** The reconciler walks `<store>/<bucket>/` and compares with
  the SQLite index.

### 3.4 Replication

- **REPL-1 to REPL-3** The queue is a SQLite table (`modernc.org/sqlite`,
  pure Go) with one FIFO sequence per replica.
- **REPL-6** Byte-offset resume is S3 multipart upload: objects above
  16 MiB are sent in parts and an interrupted upload resumes at the
  last completed part, the same for friends and external buckets
  (D12).
- **REPL-10** The hash is SHA-256, computed while streaming the object
  from the store, sent as `x-amz-checksum-sha256`, and stored in the
  SQLite index on acknowledgement.
- **REPL-11** The watermark is the queue sequence number of the last
  acknowledged write per replica.

### 3.5 Lending and quotas

- **LEND-1** Quota is counted by the lender's policy decorator, which
  sums live objects and non-current versions per borrower account.
- **LEND-2** The distinguishable error is S3 error code `QuotaExceeded`
  with HTTP 507, returned by the lender's decorator since versitygw has
  no quota. The borrower's replica implementation maps it to a pause of
  that queue. Fallback if versitygw cannot return a custom error (Q-I):
  `AccessDenied` with a distinguishing message the borrower parses.
- **LEND-4** The received-bytes check is versitygw's own
  `x-amz-checksum-sha256` verification if it verifies checksums at the
  pinned version, otherwise the decorator's, which hashes the stream
  anyway for its index (Q-I). The existing-key check is always the
  decorator's, using a conditional put (`If-None-Match: *`) where
  versitygw supports it, returning `PreconditionFailed`.
- **LEND-6** The lending store is `lent/<borrower-id>/<bucket>/<key>`
  under the same backend root as the local store, plus versitygw's
  versioning directory holding tombstoned versions and delete markers,
  and the SQLite index of hashes. It is served by the same embedded
  versitygw as the local store, on the tunnel listener only.
- **LEND-7** The suggestion is `1.2 × k × R`; the derivation is §4.

### 3.6 Deletion, retention and verification

- **RET-1** A delete places an S3 delete marker and the object becomes
  a non-current version in versitygw's versioning directory. lendisk's
  retention purger removes non-current versions older than the window;
  versitygw has no lifecycle rules of its own. Fallback if versitygw's
  versioning lacks the needed semantics (Q-I): lendisk-side tombstones
  in a `tombstones/` sidecar directory as in v0.5, with undelete over
  the control API.
- **RET-2** The decorator refuses `DeleteObject` with a version id
  unless that version is a delete marker, and admits no lifecycle,
  bucket-deletion or admin call (§3.7 allow-list).
- **RET-3** `undelete` lists versions (`ListObjectVersions`) and removes
  the delete markers created within the window; the same `minio-go`
  code path serves friends and external buckets.
- **VER-1** The challenge is a control API call: the borrower sends an
  object key, a byte range and a nonce; the lender returns
  HMAC-SHA256(nonce, bytes of the range); the borrower computes the
  same over its local copy.
- **VER-5** The sampler hashes files in the store and compares with the
  SQLite index; versitygw has no scrub of its own.

### 3.7 Network

- **NET-1** Every peer connection is a WireGuard tunnel via tailcat; the
  lender's `AllowedClients` is the set of known peer keys plus pending
  invite keys.
- **NET-2 to NET-4** magicsock hole punching with UPnP/NAT-PMP/PCP, DERP
  fallback; `PathInfo` reports direct or relayed for `status`.
- **NET-5** Relays are DERP servers. The project publishes a DERP map;
  `relays.map_url` or `relays.list` replaces it; `lendisk relay` wraps
  `tailscale.com/cmd/derper`. Tailscale's hosted DERP map may be
  included as a last-resort bootstrap.
- **NET-5a** `relays.accept_peer_hints` controls whether a peer's
  `tc…` relay hints are merged into the node's DERP map.
- **NET-6** The version is carried by the control API `version`
  endpoint; the S3 surface is governed by its own stability.
- **NET-7** The lender's decorator admits exactly these S3 operations:
  `PutObject`, `CreateMultipartUpload`, `UploadPart`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `GetObject` with
  Range, `HeadObject`, `HeadBucket`, `ListObjectsV2`,
  `ListObjectVersions`, `ListBuckets` (own buckets only), `CreateBucket`
  (counted against quota), `DeleteObject` without a version id (creates
  a delete marker) and `DeleteObject` with the version id of a delete
  marker (undelete). Every other operation is refused with
  `AccessDenied` before reaching the backend. The control API
  (`challenge`, `manifest`, `quota`, `version`) is plain HTTP beside
  the S3 listener inside the tunnel, authenticated by the tunnel peer
  key alone. The borrower replicates to a friend with the same `minio-go`
  replica implementation it uses for external buckets, with a dialer
  that routes through the tunnel. AC check: from an allowed peer,
  `restic -r s3:http://<lender>:9000/<bucket> snapshots` works with the
  derived credential.
- **NET-7a** The S3 credential for a borrower–lender pair is derived:
  access key is the borrower's node ID; secret key is
  `HKDF-SHA256(X25519(borrower key, lender key), info="lendisk-s3-v1")`.
  The lender's `IAMService` resolves the access key to the borrower's
  account and computes the secret on demand; it refuses a request whose
  tunnel peer key differs from the access key.
- **NET-7b** `peer forward` opens a local TCP listener and proxies each
  connection through the tunnel to the lender's S3 listener.
- **NET-8** The internal `Transport` interface is `Listen`,
  `Dial(peerKey)`, `AllowedPeers`, `PathInfo`. tailcat is the only
  implementation; Syncthing's stack or iroh would be the second (D5).

### 3.8 Operations and observability

- **OPS-4** `prometheus/client_golang` on a loopback port.
- **OPS-6** The configuration file is TOML; state lives in SQLite.

### 3.9 Security

- **SEC-3** Per-bucket S3 keys live in lendisk's SQLite; versitygw's
  `IAMDir` is not used.
- **SEC-8** versitygw's admin, web UI and website listeners are
  disabled; the only listeners are loopback S3, loopback REST, loopback
  metrics and the tailcat tunnel.
- **SEC-9** The classification table covers every `backend.Backend`
  method at the pinned versitygw version (Q-H).

### 3.10 Compatibility

- **COMPAT-2** The Docker image recommends `network_mode: host`.
  versitygw also builds on macOS, BSD and Windows, which keeps post-v1
  ports open.
- **COMPAT-4** The documented layout is `<store>/<bucket>/<key>` with
  versitygw's `.sgwtmp/` and `.vgwlocks/` housekeeping directories and
  its versioning directory.

### 3.11 External S3 replicas

- **S3-3** The checksum is `x-amz-checksum-sha256` where the provider
  supports it, otherwise Content-MD5. Objects above 64 MiB use
  multipart upload with resume.
- **S3-6** Stored checksums are read via `GetObjectAttributes` or HEAD.
- **S3-9** Adoption lists the bucket with `minio-go` and seeds the
  acknowledged-hash index from the provider's checksums.

## 4. Capacity planning

Mirroring (D6) sets the disk cost. Let `R` be the repository size:
source data after deduplication and compression (typically 0.5–1.0× the
source; ~1× for photos and video), plus the snapshot history the user's
`forget` policy keeps. Let `k` be the number of lenders.

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

**Rule of thumb for users (LEND-7):** lend `k` times what you back up,
plus 20%. Reciprocity is not enforced (P-8); this is the number that
makes a symmetric group work. An external S3 replica counts in `k` but
costs money instead of lent disk: one friend plus one bucket means
lending ~1.2 × R and paying for ~R.

Erasure coding would reduce the multiplier to roughly 1.5–2×; D6 rejects
it because restores would then depend on several friends being online at
once.

## 5. Gates and resolved questions

Release gates on the product (field connectivity, soak, small hardware)
are G-1 to G-3 in REQUIREMENTS.md §6. The gates here are on the current
design and disappear with it.

- **Q-A (resolved)** `min_replicas` defaults to 1 with a nudge towards a
  second replica (EP-6).
- **Q-B (resolved)** Relays are reused, not built: DERP via tailcat
  (D5). The project runs a few `derper` instances; the cost is a couple
  of small VPSes. Tailscale's hosted relays are a bootstrap convenience
  only, since they are throttled and revocable.
- **Q-C (now G-1)** If G-1 fails and the cause is tailcat rather than
  the relays, the fallback is iroh and Rust (D10).
- **Q-D (resolved)** Lender-only profile exists (PEER-9).
- **Q-E (open, tested by G-2)** Does tailcat's two-minute UDP idle
  timeout or its short-lived positioning cause problems for day-long
  transfers and weeks-idle peers? If G-2 fails here, options are
  application-level keepalives over the tunnel or a different
  `Transport`.
- **Q-F (resolved, measured by G-3)** versitygw embedded via `embedgw`
  (D13); the embeddability question that drove this is answered by
  `embedgw.RunVersityGW(ctx, backend.Backend, *Config)` at v1.10.0.
  Garage was the sidecar alternative and is now an external replica
  target only. If G-3's listing measurement fails, the decorator serves
  listings from lendisk's own index.
- **Q-G (resolved)** No proxy, so no SigV4 pass-through. versitygw
  terminates authentication in-process.
- **Q-H (open)** The decorator must be checked against the full
  `backend.Backend` interface at the pinned version: every method that
  can create, replace or remove an object (including `CopyObject`,
  `UploadPartCopy`, `PutObjectTagging`, `RestoreObject` if present) is
  either captured or disabled. A method that mutates objects without
  being captured is a replication hole. Re-run on every versitygw
  upgrade. Since v0.6 the same review classifies the lender-facing
  surface (SEC-9).
- **Q-I (gate, open)** The lending store over embedded versitygw (D12,
  §3.7) depends on five properties of versitygw at the pinned version.
  Each has a fallback, so none blocks the decision, but each must be
  checked before the lender is built:
  1. `embedgw` accepts a `net.Listener` (the tailcat listener) and not
     only port strings. Fallback: bind a second loopback port and
     forward the tunnel listener to it in-process.
  2. The backend call can identify the authenticated account, so the
     decorator can root paths at `lent/<borrower-id>/` and count quota
     per borrower. Fallback: a vendored patch that puts the account in
     the request context, carried as a Go module `replace`.
  3. `PutObject` and `UploadPart` verify `x-amz-checksum-sha256`,
     including the trailing-checksum form `aws-chunked` clients send.
     Fallback: the decorator hashes the stream and compares it to the
     header itself; it hashes anyway for its index.
  4. The versioning directory produces delete markers on `DeleteObject`
     and lists non-current versions, with the semantics RET-1 to RET-3
     need. Fallback: lendisk-side tombstones as in v0.5 (`tombstones/`
     sidecar directory), with undelete over the control API.
  5. A backend can return a custom error (`QuotaExceeded`, HTTP 507)
     and a conditional-put refusal (`PreconditionFailed`). Fallback:
     `AccessDenied` with a distinguishing message the borrower parses.
  Also to measure: the cost of SigV4 signing and verification on a
  Raspberry Pi 4 for 4 MiB objects, against the v0.5 hash-header
  design. Expected to be negligible next to the SHA-256 both designs
  compute.
