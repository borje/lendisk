# lendisk — design

Status: draft v0.8, 2026-10-28. Companion to REQUIREMENTS.md v0.8.

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
proxy with versitygw embedded in the daemon. v0.6 made every replica an
S3 target: a friend's node serves S3 over the tunnel and the borrower
uses one replica implementation for friends and external buckets alike
(D12). v0.7 split requirements from design. v0.8 replaced embedded
versitygw with Garage as the storage engine, behind a lendisk-owned S3
proxy that terminates authentication, captures writes and enforces the
lender's policy (D14); this needed REQUIREMENTS.md v0.8, which gave up
"plain files with nothing running" for "readable with the storage
engine alone, plus an export". The trust model is unchanged throughout.

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
in §3. D13 (embedded versitygw) was replaced by D14 in v0.8 and is
recorded there as the rejected alternative.

### D2. Packaging: one service, two static binaries, one TOML file

*Serves:* P-9, COMPAT-2, OPS-6, OPS-8.

- One package per platform (Linux amd64, arm64; ARMv7 best effort)
  holding two static binaries: `lendisk` and the pinned `garage`
  release it was tested with (D14). One systemd unit runs `lendisk`,
  which starts and supervises `garage` itself; the user never
  configures Garage. The container image for Synology, TrueNAS and
  Unraid holds both binaries; `network_mode: host` is recommended so the
  overlay is not behind a second NAT.
- Configuration is one TOML file (`BurntSushi/toml`); the CLI is
  `cobra`. Every setting has a flag and an env var (OPS-6). lendisk
  generates Garage's own configuration file from it at every start
  (§3.8), so there is still one file the user edits.
- The two binaries are upgraded as a pair. lendisk records the Garage
  version and binary hash it was built against, refuses to start a
  different one, and runs Garage's documented migration steps as part
  of its own state migration (OPS-8).

*Rejected:* embedding the Garage binary inside the lendisk binary with
`go:embed` and writing it out at start. It gives a one-file download at
the cost of a 20–30 MB larger binary and an AGPL-aggregation question
nobody wants to argue; two files in one tarball is plain aggregation.

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

### D4. Endpoint: lendisk's S3 proxy in front of Garage

*Serves:* EP-1 to EP-3, EP-5, EP-10, EP-11, COMPAT-1, P-5.

The backup tool talks S3 to the lendisk daemon on `127.0.0.1:9000`. The
listener is **lendisk's own streaming S3 reverse proxy** (§3.3): it
verifies the request's SigV4 signature against the per-bucket
credential lendisk issued, records the operation in the replication
queue, forwards the body unbuffered to Garage over a unix socket
(D14), and relays the response. It is the only tool-facing endpoint:
there is one local copy and one capture point.

The same proxy, on a second listener inside the tunnel, is what a
friend's node talks to (D12). Everything the product adds to S3
(capture, read-through, quota, immutability, tombstones, the
deny-by-default allow-list) lives in this one component; Garage only
stores bytes.

Why S3 and only S3:

- Every backup tool that matters speaks S3 (restic, Kopia, Duplicacy,
  Duplicati, Velero, rclone). restic's REST protocol is restic-only, and
  a REST shim beside the S3 endpoint (v0.4 to v0.7) bought nothing but a
  second credential, a second listener and a second test matrix; it was
  dropped with REQUIREMENTS.md v0.8 (EP-4).
- lendisk does not implement S3 storage: multipart assembly, listings,
  ETags and the on-disk format are Garage's. lendisk implements the
  parts of the *protocol* that sit in front of storage: signature
  verification and an operation allow-list, both of which it needs for
  the lender side regardless (§3.7).

What a proxy costs versus the embedded gateway of v0.5–v0.7, and why it
is accepted:

- *SigV4 is terminated twice*: the proxy verifies the client's
  signature (including the `aws-chunked` streaming form restic and Kopia
  send over plain HTTP) and re-signs the forwarded request with a
  node-local Garage key, with `UNSIGNED-PAYLOAD`. The chunk framing is
  stripped as it streams; nothing is buffered (EP-3). The cost is one
  HMAC per 64 KiB chunk, negligible next to the SHA-256 the replication
  worker computes anyway (Q-L measures it).
- *A crash between Garage's success and the queue entry* is handled the
  same way the decorator did it: a pending row is written before the
  request is forwarded and completed when Garage answers; pending rows
  are resolved against Garage at startup (EP-3).
- *Two processes* instead of one (D2).

What changed versus a restic-only design, and the replacement for each:

- *Name-equals-hash validation* (restic names every object by SHA-256;
  Kopia names pack blobs `p<random>`) is replaced by the **immutability
  policy** (EP-11): once a key is acknowledged by a replica, a write to
  the same key with different content is a violation. The replication
  worker hashes each object as it reads it from Garage to send it, and
  sends the hash with the object; the lender verifies the bytes against
  it (LEND-4).
- *"Snapshot X is offsite-complete"* becomes a per-bucket high-water mark
  in the write sequence (REPL-11). Tool profiles (`restic`, `kopia`)
  recognise snapshot or manifest keys and present the watermark as
  "snapshot X"; `generic` shows a timestamp.
- *Write ordering* (REPL-1) generalises unchanged: restic and Kopia both
  write packs, then indexes, then the snapshot or manifest, so FIFO
  replication keeps the replica consistent at every watermark.
- *Multipart uploads* are enqueued on `CompleteMultipartUpload` only;
  parts and aborted uploads never reach the queue (EP-2).
- *Zero lock-in* (P-5) holds on both sides in its v0.8 form: with
  lendisk stopped, an unmodified `garage server` on the same directories
  serves the buckets to `restic -r s3:…`, and `lendisk export` writes a
  plain-file repository (§3.5, §3.10).
- *The trust promise* is per tool profile (EP-7, EP-8). restic and Kopia
  always encrypt client-side, so the "lender never sees plaintext"
  guarantee holds. A `generic` bucket (rclone, `aws s3 cp`) is outside
  the guarantee.

*Rejected:* sftp (needs an SSH server); writing an S3 implementation
(`gofakes3` is a test double and the storage side of the protocol is
large); embedding rest-server's filesystem handler as the store (a
second store beside the S3 one, so the restic path would diverge from
the Kopia path); embedded versitygw (D14, alternatives); pass-through
of the client's signature to Garage unchanged, which would need the
per-bucket and the derived per-pair credentials to be imported into
Garage's key store (SEC-3, NET-7a).

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
consequence of D5.

The language follows the transport, not the other way round. Nothing else
in the product forces a choice: the daemon stores opaque objects by key
and never parses a backup repository, so neither restic's Go code nor
Rust's `rustic_core` is needed. Garage (D14) is Rust, but it runs as a
separate process behind a unix socket and is never linked, so it has no
bearing on the daemon's language.

- **Go** is required by tailcat (D5). It also matches the restic ecosystem
  (restic, rest-server) and cross-compiles to static Linux amd64/arm64/
  armv7 binaries trivially.
- **Rust** would be the choice with iroh as transport. It would give a
  slightly smaller and safer daemon at the cost of a custom stream
  protocol over QUIC. It is a reasonable product; it is not a better one
  for this problem.

If field testing fails gate G-1 and the cause is tailcat rather than the
relays, the fallback is iroh, and that means a Rust rewrite of a thin
daemon (S3 proxy, SQLite queue, replication loop, Garage supervisor). The requirements are written so that nothing in them
changes; in this document only D5, D10 and §3.7 would.

Licence: Apache-2.0 for lendisk. Garage is AGPL-3.0 and is distributed
unmodified as a separate executable in the same package (D2); nothing
of it is linked into lendisk.

### D11. Build on existing software

*Serves:* every requirement, by keeping the daemon small enough to
finish.

Everything that already exists and fits is used as a library or a
bundled program, not reimplemented. The daemon is glue around these:

| Concern | Component | Licence | Notes |
|---|---|---|---|
| Backup format, client, encryption | **restic**, **Kopia**, unmodified | BSD-2 / Apache | Never imported; only their S3 usage and key naming conventions are relied on, via tool profiles. |
| Storage engine: local store and lending store | **Garage** (`garagehq.deuxfleurs.fr`), unmodified, pinned release | AGPL-3.0 | D14. Separate process supervised by lendisk, reached over unix sockets. Supplies the S3 storage model, multipart, indexed listings, per-bucket quotas, per-key bucket permissions, block scrub and the admin API. |
| SigV4 verification (both listeners) | an existing Go verifier, reused: candidates are versitygw's `s3api/auth` package and MinIO's `pkg/signer`; failing those, ~600 lines of own code | Apache | §3.3. Must handle `aws-chunked` streaming signatures. |
| S3 client (all replicas, and lendisk → Garage) | `minio-go` | Apache | D12. |
| Garage admin API client | generated from Garage's admin API v2 OpenAPI spec | — | Keys, bucket creation, quotas, bucket statistics, block errors. |
| Overlay network and NAT traversal | **`github.com/tailscale/tailcat`** | BSD-3 | D5. |
| Relay | **`tailscale.com/cmd/derper`** | BSD-3 | `lendisk relay` is a thin wrapper. |
| State and queues | **SQLite** via `modernc.org/sqlite` | MIT / BSD | Pure Go, no cgo, so static builds stay simple. |
| Recovery phrase | `github.com/tyler-smith/go-bip39` | MIT | |
| CLI, config | `cobra`, `BurntSushi/toml` | Apache / MIT | |
| Metrics | `prometheus/client_golang` | Apache | Garage's own metrics are scraped over its admin socket and re-exposed. |

What is written from scratch, because nothing exists: the S3 proxy
(signature termination and re-signing, operation allow-list, capture,
read-through, bucket mapping, quota, immutability, tombstones; D4, D12,
D14, §3.3, §3.5, §3.6), the Garage supervisor and config generator
(§3.8), the replication queue and its ordering guarantee (REPL-1), the
challenge protocol (VER-1), the node manifest and recovery flow (§3.1),
invites (PEER-1), tool profiles, `lendisk export`/`import` (REPL-8,
LEND-6) and the CLI.

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
restic path and the Kopia path diverge. **rclone `serve restic`** and
**rclone `serve s3`**: same objection, plus they are processes without
capture hooks.

### D12. Every replica is S3

*Serves:* NET-7, NET-7a, NET-7b, LEND-2, LEND-4, RET-1 to RET-3, S3-3,
S3-4, P-5, P-7.

A replication target is anything that implements the replica interface
(`Put`, `Get` with range, `Head`, `List`, `Delete`, `Undelete`, `Verify`).
There is **one S3 implementation** of it, over `minio-go`, and two kinds
of target behind it:

- an **external S3-compatible bucket** (Backblaze B2, Wasabi, Hetzner,
  R2, Garage, RustFS, MinIO), dialled over the internet with the user's
  credentials (§3.11);
- a **friend's node**, dialled through the tailcat tunnel (§3.7). The
  friend's daemon serves S3 through the same lendisk proxy that fronts
  its own Garage (D4, D14), on a second listener bound to the tunnel.
  The proxy maps the borrower's bucket names onto Garage buckets that
  belong to that borrower and adds what S3 lacks: per-borrower quota
  (LEND-1), immutability (LEND-4), retention that the borrower cannot
  bypass (RET-1, RET-2) and a deny-by-default operation allow-list
  (SEC-9).

Both kinds appear in a bucket's replication set and count towards
`min_replicas`. The friend kind differs from the external kind in two
methods only: `Verify` is the challenge (VER-1) and `Undelete` is a
control API call (RET-3), because a friend's store has no object
versioning to express tombstones with (D14). Everything that moves
bytes is the same code.

Why one implementation rather than a custom peer protocol beside S3
(v0.5's design):

- The custom protocol was S3 with the names changed: `PUT` with a hash
  header, ranged `GET`, `HEAD`, paged `LIST`, `DELETE`. Everything it
  needed that S3 lacks (quota, immutability, tombstones) was going to
  live in the lender's daemon anyway, and it still does, in the proxy
  that D4 already requires. Garage supplies multipart, listings and the
  storage side for free.
- One replica implementation, one resume strategy (multipart) and one
  verification sampler (VER-2) for every target.
- The friend's replica becomes a plain S3 endpoint, so **restic and
  Kopia can restore from a friend directly** (`restic -r s3:…` over the
  tunnel) with no lendisk replica code in the path (NET-7b), which
  extends the second recovery path of the external bucket to friends.
- The lender-only profile (PEER-9) is the same binaries and the same
  store as everyone else, with one fewer code path to test.

What stays outside S3: the **peer control API**, five lendisk endpoints
served beside the S3 listener inside the same tunnel and authenticated
by the tunnel peer key alone: `challenge` (VER-1, S3 has no HMAC
challenge), `manifest` (ID-4, which must be fetchable before any S3
credential exists), `quota` (LEND-3), `version` (NET-6) and `undelete`
(RET-3, since the lender's tombstones are not S3 versions).

**Credentials.** The proxy requires SigV4, and D5 says the tunnel key is
the identity. These are reconciled by deriving the pair's S3 credential
from the tunnel keys rather than storing it (NET-7a, §3.7): access key
is the borrower's node ID, secret is a key derivation over the X25519
shared secret of the two nodes' keys. Both sides compute it, nothing is
exchanged or written down, and recovery (P-6, ID-5) needs nothing
beyond the phrase and one lender's address. The lender's proxy refuses
a request whose tunnel peer key does not match the access key it
presents, so there is still no second trust decision, only a second
encoding of the same one. Because the proxy terminates the signature
itself, the derived secret never has to exist inside Garage.

**Retention for external buckets** is delegated to the provider (S3-4):
versioning plus a lifecycle rule provide tombstones, and the borrower
is told to use a credential **without delete permission** (S3-5), which
makes the copy immune to a compromised borrower machine, a stronger
promise than a friend's node can make (RET-2 relies on the friend's
daemon being honest and intact). For friends, the lender's proxy keeps
tombstones as deferred deletes in its own index (§3.6): the object stays
in Garage, hidden from the borrower, until the window expires.

Costs accepted:

- The proxy's admitted S3 surface is reachable by invited peers, so the
  hostile-peer test (SEC-7) covers every S3 operation the allow-list
  admits. The list (§3.7) is short and everything else is refused
  before reaching Garage; Garage's own `501 Not Implemented` for
  operations it lacks is a second backstop.
- The "listable with plain `curl`" property of the old protocol is
  lost; any S3 client works instead.
- A node that both lends and borrows runs one proxy with two listeners
  and one Garage.

*Not used for:* the backing store of a friend's lender (that is the
friend's own Garage on local disk), nor the borrower's local store
(same). A lender who wants object storage behind Garage's data
directory can mount it; it is unsupported.

External buckets are tested in CI against Garage and documented for
Backblaze B2, Wasabi, Hetzner Object Storage, Cloudflare R2, Garage and
RustFS. Friend replicas are tested in CI with two nodes over tailcat.

### D14. Storage engine: Garage, supervised, behind the lendisk proxy

*Serves:* EP-1 to EP-3, EP-10, EP-12, EP-13, EP-14, LEND-1, LEND-6,
VER-5, COMPAT-4, SEC-3, SEC-5, SEC-8, P-5, G-3. Replaces D13.

The node's local store and its lending store are buckets in one
**Garage** instance (Rust, AGPL-3.0, `garagehq.deuxfleurs.fr`; v2.3.0,
April 2026) that lendisk starts as a child process, configures, and
reaches only over unix sockets in a directory of its own (§3.8). No
client ever talks to Garage directly: the lendisk proxy (D4) sits in
front of both listeners.

What Garage brings that the product would otherwise build:

- **Per-bucket size quotas** (`maxSize`, `maxObjects` through the admin
  API), used as a hard backstop under the proxy's per-borrower
  accounting (LEND-1, SEC-5).
- **Per-key bucket permissions** (read, write, owner), so a second,
  Garage-enforced wall stands between borrowers even if the proxy's
  bucket mapping has a bug (PEER-8).
- **Indexed listings** (LMDB or SQLite metadata). Listing 500k objects
  is a database range scan, not a `readdir` and sort; the G-3 listing
  measurement no longer rides on the store (EP-14, G-3).
- **Whole-store block scrub** every 25–35 days, verifying every block
  against its hash, with a tranquility setting to keep it off the
  backup's way (VER-5).
- **Small-hardware design**: single static binary, low idle memory,
  SQLite metadata option for 32-bit, built for exactly the Raspberry Pi
  and NAS class of machine G-3 measures.
- **S3 storage done**: multipart assembly, ETags, range reads, bucket
  management and an admin API with scoped tokens; lendisk writes none
  of it.

What Garage lacks, and which part of the proxy covers it:

| Gap | Covered by |
|---|---|
| Object versioning, `ListObjectVersions` | Deferred-delete tombstones in lendisk's index (§3.6). |
| Bucket notifications or a change feed | Capture in the proxy (EP-2, EP-3); listing-based reconcile (EP-14). |
| Bucket policies, IAM | The proxy's allow-list and bucket mapping (§3.7); Garage keys are node-local and never given to clients. |
| Conditional writes (`If-None-Match`), unconfirmed at the pinned version | The proxy's index is authoritative (it is the only writer) and refuses before forwarding (LEND-4, EP-11). |
| Upload checksum verification (`x-amz-checksum-sha256`), unconfirmed (Q-J) | The proxy hashes the stream it is already consuming for signature verification and compares after Garage answers. |
| Repair of a corrupted block on a single node | lendisk reads Garage's block-error list and re-fetches the object from a replica (Q-K, EP-10). |
| Plain-file on-disk layout | Given up in REQUIREMENTS.md v0.8 (P-5); `lendisk export` writes it on demand (LEND-6). |

The trust model (P-2) is untouched: the borrower-facing listener binds
loopback only, the lender-facing listener binds the tailcat tunnel
only, Garage is reachable from neither, and everything that crosses
between households is S3 plus the peer control API, both inside the
tunnel and both served by the lendisk process (D12).

Known costs and their handling:

- *Second process.* Supervised by lendisk, invisible to the user (D2,
  §3.8). A Garage crash is a lendisk alert and a restart; the proxy
  returns `503` meanwhile and the backup tool retries.
- *AGPL.* Garage is distributed unmodified beside lendisk and never
  linked; the proxy talks to it over a socket. The project's own code
  stays Apache-2.0 (D10).
- *Metadata engine.* LMDB (Garage's default) is memory-mapped and
  counts towards RSS as pages are touched; on 32-bit it is limited to a
  small map and not recommended by Garage. lendisk configures LMDB on
  64-bit and SQLite on ARMv7; Q-M measures both against OPS-7.
- *Garage version churn.* The admin API is v2 since Garage 2.0 and the
  S3 surface is stable; the binary is pinned, upgraded as a pair with
  lendisk, and CI runs restic and Kopia end to end on every bump.
- *Compression and dedup are useless here* (the data is encrypted) and
  are turned off or ignored; block hashing still costs CPU (Q-L).
- *Garage's RPC listener* exists even on a single node; it binds
  loopback with a random secret (SEC-8).

*Alternatives considered:*

- **versitygw embedded via `embedgw` over its POSIX backend** (v0.5 to
  v0.7's design, formerly D13). One process, plain files, xattr ETags,
  `readdir` listings. Everything a lender needs beyond storage (quota,
  per-borrower isolation, scrub, indexed listings) had to be written in
  the backend decorator and was gated on five unconfirmed properties of
  a young `embedgw` API (former Q-I). Garage ships those four as
  features. The price was plain files, paid in REQUIREMENTS.md v0.8.
- **RustFS** (Rust, Apache-2.0, 1.0 GA on 2026-09-16). MinIO-compatible,
  with object versioning, lifecycle rules, IAM and event notifications,
  all of which would have simplified the lender side. Rejected for v1's
  hardware: its documentation strongly recommends XFS and says to avoid
  ext4, btrfs and ZFS, which are what Synology, TrueNAS and most
  homelabs run; its stated test-environment minimum is 2 GB of memory;
  it ships a web console that would have to be disabled (SEC-8); and it
  is six weeks past GA. Its one-directory-per-object layout is no more
  restic-readable than Garage's blocks. Kept as a documented external
  replica target, where its versioning works with S3-4 as written.
  Revisit if Garage fails Q-L or Q-M.
- **MinIO**: AGPL, heavier, its community edition shed features through
  2025 and its repository is reported archived in early 2026.
- **SeaweedFS**: capable, larger than needed, and a filer plus volume
  server topology for a single-node job.
- **Any of these as a cluster spanning friends**: rejected for the trust
  reasons in D11; a cluster is one trust domain.

## 3. Realisation, by requirement area

Section numbers mirror REQUIREMENTS.md §4. Each entry says how the
requirement is met with the decisions above. Nothing here adds to or
narrows a requirement; if a line seems to, the requirement is wrong or
this line is.

Paths used below: `<config>` is the config directory (mode 0700),
`<data>` the data directory. Garage owns `<data>/garage/` entirely:
`data/` (blocks), `meta/` (LMDB or SQLite), `garage.toml` (generated),
`s3.sock` and `admin.sock`. lendisk's state is `<data>/lendisk.db`
(SQLite). Garage bucket naming: the node's own buckets keep their names;
a borrower's bucket `b` is Garage bucket `lent-<id16>-b`, where `id16`
is the first 16 characters of the borrower's node ID in lowercase
base32. `bucket add` refuses own names beginning with `lent-` and
limits bucket names to 40 characters so the lent form fits S3's 63.
The mapping table in `lendisk.db` is authoritative; the name is a
stable label for reading the store with Garage alone (P-5).

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
  (D12), which the lender stores as a blob in `lendisk.db`, keyed by
  borrower; it never enters Garage. Addressing hints are the peer's
  `tc…` string plus any explicit addresses (NET-3).
- **ID-5** A lender's address is its tailcat `tc…` string, which carries
  the public key and relay bootstrap info. `recover` derives the key
  from the phrase, dials the lender, fetches the manifest over the
  control API (no S3 credential needed yet), then derives the per-pair
  S3 credentials (§3.7) for every lender listed.
- **ID-6** Read-through is the proxy's `GetObject`/`HeadObject` miss
  path (EP-10); after `recover` every object is a miss until fetched.
- **ID-7** The sheet is generated from the manifest; a lender's `tc…`
  string changes only when its key changes, so regeneration is rare.

### 3.2 Peers and invites

- **PEER-1** The invite string carries the lender's `tc…` address, the
  quota, the retention offer and a single-use secret. The lender adds a
  key derived from the secret to its tailcat `AllowedClients` until the
  invite is redeemed or expires; the borrower dials with that key to
  perform the handshake, after which its real key replaces it.
- **PEER-2** On redemption the lender creates, through Garage's admin
  API, one node-local Garage key for the borrower, with `createBucket`
  denied; every bucket the borrower later creates is created by lendisk
  under the `lent-<id16>-` name and granted to that key alone, with
  `maxSize` set to the borrower's quota.
- **PEER-8** Two walls. The proxy resolves the tunnel peer key to the
  borrower, maps bucket names through the mapping table, and forwards
  nothing it did not map. The forwarded request is signed with that
  borrower's Garage key, which Garage has granted on that borrower's
  buckets only, so a mapping bug cannot cross into another borrower's
  data. `ListBuckets` is answered from the mapping table.
- **PEER-9** The lender-only profile skips the loopback S3 listener;
  Garage and the proxy's tunnel listener still run.
- **PEER-10** Peers are rows in `lendisk.db`; nothing in the control API
  or the S3 surface returns them.

### 3.3 Endpoint and local store

- **EP-1** The proxy listens on `127.0.0.1:9000` and a unix socket, and
  forwards to Garage over `<data>/garage/s3.sock`. Path-style
  addressing is the only form the proxy accepts. The proxy is a
  `net/http` handler around `httputil.ReverseProxy` with a verifying
  body reader: it parses the SigV4 authorization header, resolves the
  access key to a lendisk-issued credential, verifies the seed
  signature and, for `aws-chunked` bodies, each chunk signature as the
  chunk streams past, stripping the framing. The outgoing request is
  signed with the node-local Garage key and `x-amz-content-sha256:
  UNSIGNED-PAYLOAD`, with `Content-Length` set to the decoded length
  the client declared. Headers Garage does not know are dropped.
- **EP-2** The proxy enqueues on a `200` to `PutObject` or
  `CompleteMultipartUpload`, and a delete on a `204` to `DeleteObject`
  or each deleted entry of `DeleteObjects`. Parts and aborted uploads
  never reach those branches.
- **EP-3** The proxy writes a pending queue row (bucket, key, operation,
  Garage upload id if any) before forwarding and marks it complete when
  Garage answers success; on failure it deletes the row. At startup,
  pending rows are resolved by `HeadObject` against Garage. The proxy
  neither buffers nor hashes request bodies; hashing happens when the
  replication worker reads the object from Garage to send it (REPL-10).
- **EP-6** Bucket credentials are one access/secret pair per bucket,
  generated and stored by lendisk in `lendisk.db` and verified by the
  proxy. Garage never sees them. Creating a bucket creates the Garage
  bucket through the admin API and grants it to the node's own Garage
  key.
- **EP-10** The proxy's `GetObject`/`HeadObject` turn a `NoSuchKey` from
  Garage into a fetch from a replica through the replica interface
  (D12), a `PutObject` into Garage with the node's own key, and the
  response to the client. In `evict` mode the eviction worker issues
  the Garage delete once `min_replicas` have acknowledged and no tool
  lock is present.
- **EP-12** `init` checks that `<data>` is a local filesystem (not NFS,
  SMB or FUSE, which LMDB and Garage's data directory are not safe on),
  has the free-space floor, and that the bundled `garage` binary
  matches the pinned version and hash. `status` reports the Garage
  version.
- **EP-13** Throughput is the proxy's chunk-signature HMAC plus Garage's
  per-block hashing and writes. `block_size` is set to 4 MiB
  (configurable), `compression_level = "none"`, `data_fsync` off by
  default; Q-L measures on NVMe and on a Pi 4.
- **EP-14** The reconciler pages through `ListObjectsV2` of every Garage
  bucket over the socket and compares with the index in `lendisk.db`.
  Since nothing but lendisk can write into Garage, drift comes only from
  crashes and from `lendisk import`.
- **EP-15** The user guide shows how to start `garage server` on
  `<data>/garage/garage.toml` with lendisk stopped, create a read-only
  key with `garage key create` and `garage bucket allow --read`, and
  point `restic -r s3:` at it (P-5, scenario 7).

### 3.4 Replication

- **REPL-1 to REPL-3** The queue is a SQLite table with one FIFO
  sequence per replica.
- **REPL-6** Byte-offset resume is S3 multipart upload: objects above
  16 MiB are sent in parts and an interrupted upload resumes at the
  last completed part, the same for friends and external buckets
  (D12).
- **REPL-8** `export` pages through the bucket over the Garage socket
  and writes one file per key at its key path (or Kopia's sharded
  layout); `import` on the lender walks the directory and `PutObject`s
  each file into the borrower's Garage bucket with the node-local key,
  recording hashes in the index as acknowledged.
- **REPL-10** The hash is SHA-256, computed while streaming the object
  from Garage's `GetObject`, sent as `x-amz-checksum-sha256`, and
  stored in the `lendisk.db` index on acknowledgement.
- **REPL-11** The watermark is the queue sequence number of the last
  acknowledged write per replica.

### 3.5 Lending and quotas

- **LEND-1** The proxy keeps a per-borrower byte counter in `lendisk.db`
  (live objects plus tombstones, updated on every completed put and
  every purge) and reconciles it daily against the admin API's
  `GetBucketInfo` byte totals. Each lent bucket also carries a Garage
  `maxSize` equal to the borrower's quota, so Garage refuses on its own
  if the counter ever drifts. The optional global cap is a second
  counter over all borrowers.
- **LEND-2** The proxy refuses `PutObject`, `CreateMultipartUpload` and
  `CreateBucket` with S3 error code `QuotaExceeded`, HTTP 507, when the
  counter would exceed quota or `statfs` on `<data>` is below the
  floor. A quota error from Garage itself is mapped to the same code.
  The borrower's replica implementation maps it to a pause of that
  queue.
- **LEND-4** The proxy hashes the body as it streams it to Garage
  (SHA-256 over the de-chunked bytes), compares with
  `x-amz-checksum-sha256` when Garage answers, and on mismatch deletes
  the object and returns `BadDigest`; if Garage verifies the header
  itself at the pinned version (Q-J) the mismatch never reaches that
  branch. The existing-key check happens before any byte is forwarded:
  a key present in the index with a different hash than the header is
  refused with `PreconditionFailed`. A put without the header is
  refused on the tunnel listener.
- **LEND-6** The lending store is the set of Garage buckets
  `lent-<id16>-<bucket>`, plus the tombstone and hash tables in
  `lendisk.db`. With lendisk stopped, `garage server` serves them to
  `restic -r s3:` as in EP-15; `lendisk export <bucket> --borrower <id>
  <path>` writes the plain-file layout (REPL-8).
- **LEND-7** The suggestion is `1.2 × k × R`; the derivation is §4.

### 3.6 Deletion, retention and verification

- **RET-1** A `DeleteObject` from a borrower does not reach Garage. The
  proxy inserts a row in `tombstones` (borrower, bucket, key, hash,
  deleted at, purge after) and answers `204`. From then on the key is
  hidden from that borrower's `GetObject`, `HeadObject` and
  `ListObjectsV2` (the proxy filters listing pages against the table).
  The purger issues the real Garage delete when `purge after` passes
  and decrements the quota counter. A put to a tombstoned key with the
  same hash removes the row (an undelete by re-upload); with a
  different hash the proxy first `CopyObject`s the stored object to
  `.lendisk/tomb/<key>/<unix-time>` inside the same bucket and points
  the row at it, then admits the put (Q-O). Keys beginning with
  `.lendisk/` are refused from borrowers.
- **RET-2** No admitted operation (§3.7) can touch the tombstone table
  or the `.lendisk/` prefix, and the real delete is issued by the purger
  alone. The borrower holds no Garage credential, so nothing it can
  present reaches Garage except through the proxy.
- **RET-3** `undelete` on the borrower calls the control API `undelete`
  endpoint (bucket, since) for friends; the lender removes the matching
  tombstone rows, moving any `.lendisk/tomb/` copies back to their keys.
  For external buckets it removes delete markers through `minio-go` as
  before (§3.11).
- **VER-1** The challenge is a control API call: the borrower sends an
  object key, a byte range and a nonce; the lender reads the range from
  Garage and returns HMAC-SHA256(nonce, bytes); the borrower computes
  the same over its local copy.
- **VER-5** Garage's scheduled scrub verifies every block hash every
  25–35 days on both sides, which exceeds the 0.1%-per-week rate. After
  each scrub lendisk reads the block-error list through the admin API,
  maps errored blocks to objects, raises the alert and, on the borrower
  side, deletes and re-fetches the object (EP-10). A weekly 0.1% sample
  through `GetObject` compared with the index stays as the end-to-end
  check that also covers the index itself.

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
- **NET-7** The proxy's tunnel listener admits exactly these S3
  operations: `PutObject`, `CreateMultipartUpload`, `UploadPart`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`,
  `GetObject` with Range, `HeadObject`, `HeadBucket`, `ListObjectsV2`,
  `ListBuckets` (own buckets only), `CreateBucket` (counted against
  quota) and `DeleteObject` without a version id (tombstones). Every
  other operation, including `DeleteObjects`, `CopyObject`,
  `UploadPartCopy`, any versioned or lifecycle call and any bucket
  configuration call, is refused with `AccessDenied` before anything is
  forwarded. The control API (`challenge`, `manifest`, `quota`,
  `version`, `undelete`) is plain HTTP beside the S3 listener inside the
  tunnel, authenticated by the tunnel peer key alone. The borrower
  replicates to a friend with the same `minio-go` replica implementation
  it uses for external buckets, with a dialer that routes through the
  tunnel. AC check: from an allowed peer, `restic -r
  s3:http://<lender>:9000/<bucket> snapshots` works with the derived
  credential.
- **NET-7a** The S3 credential for a borrower–lender pair is derived:
  access key is the borrower's node ID; secret key is
  `HKDF-SHA256(X25519(borrower key, lender key), info="lendisk-s3-v1")`.
  The lender's proxy resolves the access key to the borrower, computes
  the secret on demand and verifies the signature itself; it refuses a
  request whose tunnel peer key differs from the access key. The
  derived secret is never stored and never given to Garage.
- **NET-7b** `peer forward` opens a local TCP listener and proxies each
  connection through the tunnel to the lender's S3 listener.
- **NET-8** The internal `Transport` interface is `Listen`,
  `Dial(peerKey)`, `AllowedPeers`, `PathInfo`. tailcat is the only
  implementation; Syncthing's stack or iroh would be the second (D5).

### 3.8 Operations and observability

- **Garage supervision.** At start lendisk writes
  `<data>/garage/garage.toml` (mode 0600) from its own config:
  `replication_factor = 1`, `metadata_dir`, `data_dir`, `db_engine`
  (`lmdb` on 64-bit, `sqlite` on 32-bit), `block_size`,
  `compression_level = "none"`, `rpc_bind_addr = "127.0.0.1:3901"`, a
  random `rpc_secret`, `[s3_api] api_bind_addr = "<data>/garage/s3.sock"`,
  `[admin] api_bind_addr = "<data>/garage/admin.sock"` with
  `admin_token` and `metrics_token` from `<config>`, and no `[s3_web]`
  or K2V section. It then starts `garage server`, waits for the admin
  socket, and on first run assigns the single-node layout and creates
  the node's own Garage key. The `<data>/garage/` directory is mode
  0700 so the sockets, which Garage creates world-writable, are
  reachable only by the lendisk user. Garage is restarted with backoff
  if it exits; three exits in ten minutes raise an alert.
- **OPS-4** `prometheus/client_golang` on a loopback port. Garage's
  metrics are scraped over the admin socket and re-exposed under a
  `garage_` prefix, so one endpoint covers both.
- **OPS-6** The configuration file is TOML; state lives in `lendisk.db`.
  Garage settings the user may want (`block_size`, scrub tranquility,
  metadata engine) are lendisk settings under `[engine]`.
- **OPS-7** Idle RSS is lendisk plus Garage; Q-M measures it.
- **OPS-8** lendisk and Garage upgrade as a pair; lendisk's migration
  runs Garage's documented migration for the version step, and refuses
  to start a Garage binary other than the pinned one.

### 3.9 Security

- **SEC-3** Per-bucket S3 keys live in `lendisk.db`. Garage's key store holds only node-local keys (one for
  the node's own buckets, one per borrower), each created by lendisk,
  stored in `lendisk.db` and granting nothing beyond this node's disk.
  The admin and metrics tokens are files in `<config>`, mode 0600.
- **SEC-7** The hostile-peer test exercises every S3 operation against
  the tunnel listener and every control API endpoint, from an allowed
  peer's identity and from an unknown one.
- **SEC-8** Garage has no web endpoint configured, no K2V, S3 and admin
  on unix sockets, RPC on loopback. lendisk's listeners are loopback S3,
  loopback metrics and the tailcat tunnel. Q-N confirms
  the listener set with `ss -lntp` and `ss -lxp`.
- **SEC-9** The classification table is over S3 operations (every
  action name in the S3 API reference), each marked admitted, refused
  or unreachable at the tunnel listener, with a test that sends one
  request per action and asserts the outcome. It does not depend on
  Garage's version, since refused actions never reach Garage; the
  Garage upgrade review checks only that admitted actions still behave.

### 3.10 Compatibility

- **COMPAT-2** Garage publishes static builds for amd64, arm64 and
  32-bit ARM, so the package exists for every v1 platform. The Docker
  image recommends `network_mode: host`.
- **COMPAT-3** `bucket import` walks the directory and puts each file
  through the proxy's internal path, so it is captured but marked
  unacknowledged.
- **COMPAT-4** The documented layout is `<data>/garage/{data,meta}` as
  Garage's own, the bucket naming rule (own names as-is, borrowers'
  under `lent-<id16>-`), the `.lendisk/tomb/` prefix, `lendisk.db` and
  its tables, and the export layout (one file per key at its key path;
  Kopia's sharded form with `--kopia-layout`).

### 3.11 External S3 replicas

- **S3-3** The checksum is `x-amz-checksum-sha256` where the provider
  supports it, otherwise Content-MD5. Objects above 64 MiB use
  multipart upload with resume.
- **S3-4** Providers without versioning (Garage among them) get
  lendisk-side tombstones: the delete is recorded locally and issued
  after the window; `status` labels the replica "retention:
  client-side".
- **S3-6** Stored checksums are read via `GetObjectAttributes` or HEAD.
- **S3-9** Adoption lists the bucket with `minio-go` and seeds the
  acknowledged-hash index from the provider's checksums.

## 4. Capacity planning

Mirroring (D6) sets the disk cost. Let `R` be the repository size:
source data after deduplication and compression (typically 0.5–1.0× the
source; ~1× for photos and video), plus the snapshot history the user's
`forget` policy keeps. Let `k` be the number of lenders.

Garage adds metadata of a few hundred bytes per object and per block;
for a 1 TiB restic repository in 16 MiB packs that is well under 1 GB.
Its block deduplication saves nothing, since every repository is
encrypted with its own key.

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
- **Q-F (superseded)** Asked whether versitygw could be embedded. It
  could; v0.8 chose Garage instead (D14), and listings are Garage's
  indexed ones, so the G-3 listing measurement no longer has a store
  fallback to plan for.
- **Q-G (superseded)** There is a proxy again, but no SigV4
  pass-through: the proxy terminates the client's signature and signs
  the forwarded request itself (D4, §3.3).
- **Q-H (superseded)** The replication-hole review was over versitygw's
  `backend.Backend` interface. The equivalent in v0.8 is SEC-9's table
  over S3 actions at the proxy, which is complete by construction
  (every action not listed is refused) and does not change with Garage
  upgrades (§3.9).
- **Q-I (superseded)** The five versitygw properties are gone with it.
  Their Garage counterparts are Q-J to Q-P below.
- **Q-J (gate, open)** Does Garage at the pinned version verify
  `x-amz-checksum-sha256` on `PutObject` and `UploadPart`? Garage 2.0
  added checksum algorithms and `x-amz-checksum-type`, but upload-side
  verification is unconfirmed. Not blocking: the proxy verifies either
  way (LEND-4); the answer decides only whether its check is a second
  line or the first.
- **Q-K (gate, open)** On a single node, how does Garage report a block
  whose hash no longer matches: block-error list through the admin API,
  metrics, both? Does `GetObject` of the affected object fail cleanly?
  Needed for VER-5's re-fetch path. Fallback: lendisk's own weekly
  sample is the only detector, at a higher rate.
- **Q-L (gate, open, measured with G-3)** End-to-end write throughput
  through proxy plus Garage on NVMe (EP-13, 200 MB/s) and on a
  Raspberry Pi 4, as a function of `block_size`, `data_fsync` and
  metadata engine; and the cost of the proxy's chunk-signature
  verification on the Pi. Fallback if the NVMe number fails: larger
  `block_size`, then RustFS (D14, alternatives).
- **Q-M (gate, open, measured with G-3)** Idle RSS of lendisk plus
  Garage with a 1 TiB, 500k-object store and 10 peers, with LMDB and
  with SQLite metadata, against OPS-7's 256 MiB. Fallback: SQLite on
  every platform.
- **Q-N (gate, open)** With no `[s3_web]` section and no K2V feature,
  Garage opens exactly the S3 socket, the admin socket and the loopback
  RPC port; the sockets are unreachable from other users given the 0700
  directory. Checked with `ss -lntp` and `ss -lxp` (SEC-8).
- **Q-O (open)** Is `CopyObject` in Garage a metadata operation (new
  object version referencing the same blocks) or a data copy? Expected
  metadata-only, since blocks are content-addressed. It matters only
  for the rare overwrite of a tombstoned key with different content
  (RET-1); if it copies data, that case is still correct, only slower.
- **Q-P (open)** How current are the admin API's per-bucket byte totals
  under write load (Garage's counters are maintained asynchronously)?
  The proxy keeps its own counter and uses Garage's for daily
  reconciliation and as a backstop, so lag is tolerable; the gate sets
  how large a drift the reconciler should alert on.
