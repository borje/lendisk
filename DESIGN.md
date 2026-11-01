# lendisk — design

Status: draft v0.11.1, 2026-11-01. Companion to REQUIREMENTS.md v0.10.1.
REQUIREMENTS.md v0.9 made the two requirement changes this design
needed (Q-T, resolved); its later versions moved third-party
credentials to the environment,
then let backup tools on other machines use the node (EP-17; the LAN
bridge, §3.3), then hid bucket names from lenders (SEC-16; sealed
names, §3), and then replaced per-bucket replica lists with placement
over a pool of symmetric friendships, with external buckets as
overflow (D6, §3.4, §4).

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
closed. `architecture.html` is an overview drawn from this document.

History: v0.4 generalised the product from restic-only to any
S3-speaking backup tool. v0.5 replaced the Garage sidecar and capturing
proxy with versitygw embedded in the daemon. v0.6 made every replica an
S3 target: a friend's node serves S3 over the tunnel and the borrower
uses one replica implementation for friends and external buckets alike
(D12). v0.7 split requirements from design. v0.8 replaced embedded
versitygw with Garage as the storage engine, behind a lendisk-owned S3
proxy; this needed REQUIREMENTS.md v0.8, which gave up "plain files
with nothing running" for "readable with the storage engine alone,
plus an export". v0.8.1 realised the fail-closed peer admission of
REQUIREMENTS.md v0.8.1 (SEC-10 to SEC-15) with tailcat's admission
hook, per-connection peer key and disconnect call. v0.9 returns to
embedded versitygw, now with a choice of two of its backends per store:
`posix` (plain files, the default) or `s3` (any S3-compatible service
the user names) (D15). The lendisk proxy and its signature code are
gone. v0.9 also moved replication to rclone as a supervised child
process, so that a replica could be any rclone remote. v0.10, the same
day, took that back after a review against REQUIREMENTS.md: every
replica is S3 again and an in-process client, **minio-go**, moves the
bytes (D12); one store serves both roles, so a friend's buckets live
beside the node's own, on disk or in Garage (D15); the object hash is
taken at capture; the tunnel has one port. One process, one binary.
v0.10.1 made the identity a bare X25519 key, with no Ed25519 and no
signatures anywhere, and reads third-party credentials from the
environment. v0.10.3 sends a borrower's buckets to lenders under
sealed names, so a lender no longer learns what they are called.
v0.11 follows REQUIREMENTS.md v0.10: a bucket asks for `copies`
instead of naming replicas, each data object is placed on that many
friends by free quota and metadata on all of them (D6), the owner's
node re-places copies when the pool changes, every friendship is one
symmetric quota, and an external bucket only takes what friends
cannot hold. v0.11.1 corrects contradictions a review found in v0.11:
the metadata barrier waits for actual placements, placement prefers an
unreachable friend over overflow only until `replace_after`, metadata
is backfilled to new places, capacity counts metadata at every place,
undelete at overflow targets copies a version instead of removing
markers, and the global quota cap cannot be overridden. The trust
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
in §3. D13 (embedded versitygw over its POSIX backend) was replaced by
D14 (Garage) in v0.8, and D14 by D15 in v0.9; each is recorded in its
successor as a rejected alternative.

**Three roles.** REQUIREMENTS.md asks for three different things, and
the decisions below follow that split rather than one component per
feature:

| Role | Requirements | What they stress | Served by |
|---|---|---|---|
| **Local store**: the node's own copy, written by the user's backup tool | EP-1 to EP-15, P-5 | Synchronous and protocol-critical: full S3 behaviour for restic and Kopia, crash-safe exactly-once capture with no buffering, 200 MB/s, loopback only unless the user enables the LAN bridge (EP-17). The writer is trusted. | versitygw with the decorator's capture policy (D4, D15) |
| **Replication**: placing each object in the pool and moving it there | REPL-1 to REPL-13, §4.11, NET-2 to NET-5 | Asynchronous and robustness-critical: placement by free quota, re-placement when the pool changes, strict FIFO per place with a metadata barrier, survival of days-long outages, resume, rate limits, a hash per object, reach behind NAT and to providers. Almost no S3 semantics. | an in-process S3 client (minio-go) driven by lendisk's queue (D12), dialling friends through tailcat (D5) |
| **Lending store**: a friend's copy, written by a borrower who may be compromised | LEND-1 to LEND-7, RET-1 to RET-3, VER-1, NET-7, SEC-4 to SEC-16 | Enforcement-critical: quota, immutability, retention the borrower cannot bypass, a deny-by-default operation list, isolation between borrowers. | versitygw with the decorator's lender policy (D12, D15), behind the tunnel bridge (§3.7) |

The two stores need the same S3 surface and differ only in policy, so
one embedded gateway serves both and the decorator applies a different
policy per bucket. Replication needs none of that surface and all of a
transfer client's robustness, so it is a separate component behind its
own interface, even though it runs in the same process. The split also
says where a component does not belong: the replica client lacks what
the stores need (synchronous capture, versioning, object lock,
conditional writes), and versitygw has nothing replication needs beyond
what the decorator already exposes.

### D2. Packaging: one service, one static binary, one TOML file

*Serves:* P-9, COMPAT-2, OPS-6, OPS-8.

- One package per platform (Linux amd64, arm64; ARMv7 best effort)
  holding one static binary, `lendisk`, which links versitygw, tailcat
  and minio-go (D12). One systemd unit runs it. There is no child
  process.
- The container image for Synology, TrueNAS and Unraid holds that
  binary and is one compose service. `network_mode: host` is
  recommended so the overlay is not behind a second NAT (G-1), and so
  `127.0.0.1:9000` is the host's loopback, which is where the backup
  tool on the host looks. With host networking, an `endpoint.listen`
  address (EP-17) is the host's own LAN address and needs no port
  mapping; on a bridge network it is the container's address and the
  user publishes that port on the host's LAN address, never on
  `0.0.0.0`. The image runs as an unprivileged user with
  all capabilities dropped and a read-only root; the config directory
  and the data directory are the only volumes (plus the store
  directory when the user names one outside `<data>`), and third-party
  credentials arrive as environment variables, not files (SEC-3). A
  user who already runs
  an S3 store adds it as a second service on the bridge network,
  publishing its S3 port on the host's loopback only, and names it as
  the `s3` backend (D15).
- Configuration is one TOML file (`BurntSushi/toml`); the CLI is
  `cobra`. Every setting has a flag and an env var (OPS-6). One
  `[store]` section serves both roles (§3.8).
- An upgrade is a file swap; the only state with migrations is
  `lendisk.db` (OPS-8).

*Rejected:* a second process for transfers, rclone as a supervised
child (v0.9; D12, alternatives). It brought a second binary, a
supervisor, a socket and a per-friend loopback forward, for a breadth
of targets REQUIREMENTS.md does not ask for.

### D3. Write model: local staging with asynchronous replication

*Serves:* P-4, P-5, REPL-9, EP-9, EP-10, ID-6.

The backup tool writes to the local daemon at disk speed. The daemon keeps
the full repository locally and places every object in the pool in the
background (D6).

*Rejected:* synchronous pass-through to a peer. Backups would run at WAN
upload speed, fail whenever the peer is offline, and give the user no local
copy. Local staging gives 3-2-1 (one local copy, `copies` offsite) for free and
makes most restores fast.

The two storage modes (`full`, `evict`) are a requirement (EP-9); this
decision is how they are served: in `evict` mode metadata (`config`,
`keys`, `index`, `snapshots`, `locks`) stays local and evicted packs are
fetched from a replica on demand through the same read path (EP-10).

### D4. Endpoint: S3 served in-process by embedded versitygw

*Serves:* EP-1 to EP-3, EP-5, EP-10, EP-11, COMPAT-1, P-5.

The backup tool talks S3 to the lendisk daemon on `127.0.0.1:9000`. The
S3 protocol is served by **versitygw embedded in the daemon** (D15): it
terminates every SigV4 signature, including the `aws-chunked`
streaming form restic and Kopia send over plain HTTP, routes every
request, and implements multipart, listings, versioning and object
lock. Under it sits a lendisk-owned **decorator** of versitygw's
`backend.Backend` interface, which captures every write and delete
into the replication queue and enforces the lender's policy. It is the
only tool-facing endpoint: there is one local copy and one capture
point.

The same gateway, reached through a lendisk bridge inside the tunnel
(§3.7), is what a friend's node talks to (D12). Everything the product
adds to S3 (capture, read-through, quota, immutability, retention, the
deny-by-default allow-list) lives in the decorator, behind the tunnel
bridge's per-request peer checks (SEC-11, SEC-12); versitygw parses
and authenticates, its backends store.

Why S3 and only S3:

- Every backup tool that matters speaks S3 (restic, Kopia, Duplicacy,
  Duplicati, Velero, rclone). restic's REST protocol is restic-only, and
  a REST shim beside the S3 endpoint (v0.4 to v0.7) bought nothing but a
  second credential, a second listener and a second test matrix; it was
  dropped with REQUIREMENTS.md v0.8 (EP-4).
- With versitygw embedded, lendisk implements no part of the S3
  protocol: no signature code, no request parsing, no multipart
  assembly. lendisk supplies an `IAMService` and wraps the storage
  calls underneath.

What changed versus a restic-only design, and the replacement for each:

- *Name-equals-hash validation* (restic names every object by SHA-256;
  Kopia names pack blobs `p<random>`) is replaced by the **immutability
  policy** (EP-11): once a key is acknowledged by a replica, a write to
  the same key with different content is a violation. The decorator
  takes the SHA-256 of the write stream at capture and keeps it on the
  queue row; the hash travels with the object, and the lender verifies
  the bytes against it (LEND-4).
- *"Snapshot X is offsite-complete"* becomes a per-bucket high-water mark
  in the write sequence (REPL-11). Tool profiles (`restic`, `kopia`)
  recognise snapshot or manifest keys and present the watermark as
  "snapshot X"; `generic` shows a timestamp.
- *Write ordering* (REPL-1): restic and Kopia both write packs, then
  indexes, then the snapshot or manifest. No single place holds every
  pack, so per-place FIFO is not enough on its own; the metadata
  barrier of §3.4 holds each metadata object until the data before it
  is placed, which keeps the pool consistent at every watermark.
- *Multipart uploads* are enqueued on `CompleteMultipartUpload` only;
  parts and aborted uploads never reach the queue (EP-2).
- *Zero lock-in* (P-5) holds on both sides, at its v0.7 strength on the
  default store: objects are plain files at their key paths, so with
  lendisk stopped `restic -r <store>/<bucket>` opens the borrower's own
  copy and the lender's replica alike. Behind the `s3` backend, any S3
  client with the store's credential reads the same layout.
- *The trust promise* is per tool profile (EP-7, EP-8). restic and Kopia
  always encrypt client-side, so the "lender never sees plaintext"
  guarantee holds. A `generic` bucket (rclone, `aws s3 cp`) is outside
  the guarantee.

*Rejected:* sftp (needs an SSH server); writing an S3 implementation
(`gofakes3` is a test double and the protocol surface is large);
lendisk's own S3 proxy in front of a store (v0.8's design: ~500–800
lines of security-critical signature verification and re-signing,
including the streaming form, plus an HTTP hop between capture and
storage); a separate store behind a capturing proxy with SigV4
pass-through (v0.4's design: a crash window between the store's
acknowledgement and the queue entry); `rclone serve s3` as the front
end (experimental, built on gofakes3, no versioning, object metadata
held in memory, multipart parts must arrive in order).

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
server admits clients through an `AllowClient` hook that consults
`lendisk.db` and rejects anything it cannot find (§3.9).

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
  The admission API this design needs is newer than the latest tag
  (Q-Q).
- tailcat's defaults are open, because its first use is a netcat run by
  hand: a nil `AllowClient` hook admits every client (and in v0.7.0 an
  empty `AllowedClients` list did too), and a pre-shared key is
  generated by default and embedded in the address, which makes the
  address a secret. lendisk relies on neither default: the hook is
  always set and fails closed, and the pre-shared key is disabled
  (SEC-10, SEC-14, §3.9).
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

### D6. Replication: per-object placement over a pool, by free quota

*Serves:* P-5, P-8, REPL-1, REPL-4, REPL-11 to REPL-13, LEND-6, LEND-7,
§4.11. Replaces v0.10's whole-repository mirrors.

A node's **pool** is its friends plus its overflow targets. Every
place in the pool has a capacity (the friendship's quota, or an
overflow target's `max_bytes`), a use (live plus tombstoned bytes,
counted by the borrower and reconciled with the lender's `quota`
answer), a last-reachable time and a failure domain (the friend's
`domain`, or a domain of its own).

- **Metadata objects** (by the tool profile, EP-7) are placed at every
  friend, and at an overflow target once it holds any data object of
  the bucket. They are about 1% of a restic or Kopia repository, and
  having them everywhere makes each replica a readable, listable
  repository of what it holds and makes recovery's listing complete
  from any lender.
- **Data objects** are placed at `copies` places, chosen at capture
  time:
  1. Candidates are friends with free capacity for the object.
     Free capacity is the quota less what is stored there, less the
     bucket's metadata not yet sent there.
  2. Take the candidate with the largest *free fraction*
     (free ÷ capacity), friends reachable within the last hour
     (`replication.reroute_after`) before the others, then the next in
     a domain not yet used, and so on, until `copies` are chosen. If
     domains run out, continue in used domains and mark the object
     "same domain" for `status`. An unreachable friend with room is
     still chosen when the reachable ones are not enough, and its copy
     waits for it (§3.4), unless it has been unreachable longer than
     `replace_after` or is being drained: then it is not a candidate
     at all.
  3. Only if fewer than `copies` friends, reachable or not, have room,
     fill the rest from overflow targets by free bytes. An overflow
     target that gets its first data object of a bucket gets every
     metadata object of the bucket first (§3.4).
  4. If there are still too few places, place what can be placed and
     record the deficit for the re-placement worker (§3.4).

  Largest free fraction fills every friend at the same rate, so no
  friend fills early while others are empty. For the REPL-12 AC
  (quotas 100, 55 and 55 GB; 100 GB with about 1 GB of metadata at two
  copies) equal fractions put about 96 GB at B and 53 GB each at C and
  D, metadata included, all within quota.
- The placement is the table `placement` in `lendisk.db` (bucket,
  sequence, key, place, state: `queued`, `acked`, `moving`,
  `dropping`). It is never in the manifest: after `recover` it is
  rebuilt by listing each place's copy of each sealed bucket, with the
  hash from `x-amz-meta-lendisk-sha256` (§3.10).
- Copies are moved only by the borrower's own node (P-2, PEER-11): the
  re-placement worker of §3.4.

*Rejected:*
- **Whole-repository mirrors to named replicas** (v0.10 and earlier).
  They needed one friend with room for the whole repository per copy,
  and the user had to name friends per bucket and re-plan by hand when
  one left. Their one advantage, that a single lender holds a complete
  repository, REQUIREMENTS.md v0.10 gave up (P-5).
- **Erasure coding across peers.** It reduces storage cost but makes a
  restore depend on k-of-n peers being online, and friends' NASes are
  not reliable enough for that. Every object would be shards, so no
  replica would hold anything the backup tool can read; placement keeps
  whole objects in the tool's layout.
- **Deterministic placement** (weighted rendezvous hashing of key and
  friend list). It needs no table, but every quota or friend change
  moves objects, and it cannot react to a friend being full or offline.
  Recovery lists the replicas anyway, so the table costs nothing there.
- **Placement by absolute free bytes.** It also meets the AC, but a
  large friend takes all the early objects and a small one stays empty,
  which makes friendships look unbalanced in `status` (LEND-7).

The capacity cost and the feasibility rule are derived in §4.

### D10. Language and licence: Go, because of D5 and D15

*Serves:* COMPAT-2 (static cross-compiled binaries); otherwise a
consequence of D5 and D15.

The language follows the libraries, not the other way round. The daemon
stores opaque objects by key and never parses a backup repository, so
neither restic's Go code nor Rust's `rustic_core` is needed. What does
force the choice is that the two components lendisk links, tailcat
(D5) and versitygw (D15), are both Go; Go is the only language in which
both halves of the design come as libraries today. minio-go (D12) is
Go as well, but an S3 client exists in every language, so it has no
bearing on the choice.

- **Go** cross-compiles to static Linux amd64/arm64/armv7 binaries
  trivially and matches the restic ecosystem.
- **Rust** would mean iroh for the transport and `s3s` for the S3 front
  end, whose streaming-signature (`aws-chunked`) decoder is still a
  placeholder, so lendisk would write it; the store would be versitygw
  or Garage as a sidecar. The shape survives at the cost of the decoder
  and a second process. A reasonable product; not a better one for this
  problem.

If field testing fails gate G-1 and the cause is tailcat rather than the
relays, the fallback is iroh, and that means the Rust shape above. The
requirements are written so that nothing in them changes; in this
document D5, D10, D15 and §3.7 would.

Licence: Apache-2.0 for lendisk. versitygw (Apache-2.0), tailcat
(BSD-3) and minio-go (Apache-2.0) are linked.

### D11. Build on existing software

*Serves:* every requirement, by keeping the daemon small enough to
finish.

Everything that already exists and fits is used as a library or a
bundled program, not reimplemented. The daemon is glue around these:

| Concern | Component | Licence | Notes |
|---|---|---|---|
| Backup format, client, encryption | **restic**, **Kopia**, unmodified | BSD-2 / Apache | Never imported; only their S3 usage and key naming conventions are relied on, via tool profiles. |
| S3 front end and stores | **versitygw** via `embedgw`, pinned and vendored | Apache-2.0 | D15. SigV4 incl. `aws-chunked`, routing, multipart, listings, versioning, object lock; `backend/posix` and `backend/s3`. |
| Replication transfers, every place in the pool | **minio-go** | Apache-2.0 | D12. In-process S3 client, confined to one package behind the replica interface; the AWS SDK for Go v2 is the replacement if it is withdrawn. |
| Rate limiting | `golang.org/x/time/rate` | BSD-3 | REPL-7's token bucket around each uploaded part. |
| Overlay network and NAT traversal | **`github.com/tailscale/tailcat`** | BSD-3 | D5. |
| Relay | **`tailscale.com/cmd/derper`** | BSD-3 | `lendisk relay` is a thin wrapper. |
| State and queues | **SQLite** via `modernc.org/sqlite` | MIT / BSD | Pure Go, no cgo, so static builds stay simple. |
| Recovery phrase | `github.com/tyler-smith/go-bip39` | MIT | |
| CLI, config | `cobra`, `BurntSushi/toml` | Apache / MIT | |
| Metrics | `prometheus/client_golang` | Apache | Transfer counters come from the replication worker. |

What is written from scratch, because nothing exists: the backend
decorator (capture, read-through, bucket mapping, quota, immutability,
retention, the operation allow-list, the mux between two backends;
D4, D15, §3.3, §3.5, §3.6), the `IAMService` (§3.3, §3.7), the tunnel
bridge and the peer control API (§3.7, §3.9), the capability probe
(D15), the replication queue and its ordering guarantee (REPL-1) with
the replica client's multipart resume loop, rate limiter and timetable
(D12, §3.4), the challenge protocol (VER-1), the node manifest and recovery flow
(§3.1), invites (PEER-1), tool profiles, `lendisk export`/`import`
(REPL-8, LEND-6) and the CLI.

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
restic path and the Kopia path diverge. **rclone** as the replication
engine (v0.9): see D12. **minio-go** was itself set aside in v0.9 for
its owner risk, since MinIO archived its server and `mc` in 2026;
v0.10 takes it back with that risk accepted on 2026-10-30. It is
confined to one package, and if it is withdrawn a new decision is
taken, with the AWS SDK for Go v2 as the obvious replacement.

### D12. Every replica is S3; an in-process client moves the bytes

*Serves:* NET-7, NET-7a, NET-7b, REPL-1, REPL-6, REPL-7, REPL-10,
LEND-2, LEND-4, RET-1 to RET-3, S3-1 to S3-8, P-5, P-7.

A replication target is anything that implements the replica interface
(`Put`, `Get` with range, `Head`, `List`, `Delete`, `Undelete`,
`Verify`). There is **one implementation** of it, an S3 client linked
into the daemon (**minio-go**, §3.4), and two kinds of target behind
it:

- an **overflow target**: any S3-compatible bucket the user adds to the
  pool, in support tiers (§3.11), with versioning and lifecycle where
  the provider has them;
- a **friend's node**, dialled through the tailcat tunnel (§3.7). The
  friend's daemon serves S3 from the same embedded versitygw that fronts
  its own store (D15), through the lender-side policy in the decorator:
  per-borrower quota (LEND-1), immutability (LEND-4), retention that the
  borrower cannot bypass (RET-1, RET-2) and a deny-by-default operation
  allow-list (SEC-9).

Both kinds are places in the pool and count towards `copies`; D6
decides which objects each one gets, and overflow targets only get
what friends cannot hold. The friend kind differs from the overflow
kind in two methods only: `Verify` is the challenge (VER-1), and `Undelete` goes
over the control API when the friend's store has no versioning
(§3.6). Everything that moves bytes is the same code.

Why one implementation rather than a custom peer protocol beside S3
(v0.5's design):

- The custom protocol was S3 with the names changed: `PUT` with a hash
  header, ranged `GET`, `HEAD`, paged `LIST`, `DELETE`. Everything it
  needed that S3 lacks (quota, immutability, tombstones) was going to
  live in the lender's daemon anyway, and it still does, in the
  decorator that D4 already requires.
- One replica implementation, one resume strategy and one verification
  sampler (VER-2) for every target.
- The friend's replica is a plain S3 endpoint, so **restic and Kopia can
  restore from a friend directly** (`restic -r s3:…` over the tunnel)
  with no lendisk replica code in the path (NET-7b).
- The lender-only profile (PEER-9) is the same binary and the same
  store as everyone else, with one fewer code path to test.

Why an in-process client, and why minio-go:

- Under REQUIREMENTS.md every target is S3: a friend serves S3 over the
  tunnel (NET-7) and §4.11 is written for S3 buckets. A client in the
  process reaches both with one HTTP transport, and for a friend that
  transport's dial function is the tailcat `Client` (§3.7). No loopback
  forward, no second process, no socket, no second binary.
- Each object is read once per replica. The decorator takes the SHA-256
  of the write stream at capture (§3.3) and the worker streams the
  object from the store straight into the upload, with the hash as
  metadata (REPL-10).
- Credentials never leave the process: the derived per-pair secret
  (NET-7a) and every external credential are used by the client
  directly, not handed to another program.
- A friend's `507`, `403` and `412` arrive as S3 errors the worker maps
  itself (LEND-2, SEC-15, EP-11).
- minio-go is written for S3-compatible stores first: path-style, no
  AWS-only request defaults, and multipart, ranged reads, versioned
  listings, delete-marker removal and object-lock calls all present.
  Its low-level multipart calls are what the resume loop needs.

What lendisk writes because the client does not: the resume loop,
which keeps the multipart upload ID and the ETag of each completed part
on the queue row so a restart continues at the next part (REPL-6); a
token-bucket limiter (`golang.org/x/time/rate`) around each part and a
time-of-day timetable checked before each part (REPL-7); and the FIFO
queue, which was lendisk's in every version.

What stays outside S3: the **peer control API**, six lendisk endpoints
served under `/_lendisk/` on the same tunnel port as S3 and
authenticated by the tunnel peer key, checked against `lendisk.db` on
every request (SEC-11): `redeem` (PEER-2, the invite handshake and the
only endpoint an invite key may call, SEC-13), `challenge` (VER-1, S3
has no HMAC challenge), `manifest` (ID-4, which must be fetchable
before any S3 credential exists), `quota` (LEND-3), `version` (NET-6)
and `undelete` (RET-3, for a friend whose store keeps lendisk-side
tombstones).

**Credentials.** versitygw requires SigV4, and D5 says the tunnel key is
the identity. These are reconciled by deriving the pair's S3 credential
from the tunnel keys rather than storing it (NET-7a, §3.7): access key
is the borrower's node ID, secret is a key derivation over the X25519
shared secret of the two nodes' keys. Both sides compute it, nothing is
exchanged or written down, and recovery (P-6, ID-5) needs nothing
beyond the phrase and one lender's address. The lender's tunnel bridge
refuses a request whose access key is not the tunnel peer key (SEC-12),
so there is still no second trust decision, only a second encoding of
the same one. The borrower's client uses the derived secret directly;
it is never written to a file.

**Retention for overflow targets** is delegated to the provider where
the bucket has versioning (S3-4): versioning plus a lifecycle rule
provide tombstones, and the borrower is told to use a credential
**that cannot remove versions** (S3-5), which makes the copy immune to a
compromised borrower machine, a stronger promise than a friend's node
can make (RET-2 relies on the friend's daemon being honest and intact).
A bucket without versioning (Garage) gets lendisk-side tombstones and
the "retention: client-side" label. For friends, the lender's decorator
keeps retention, with delete markers in versitygw's versioning or its
own tombstones depending on the store (§3.6). Overflow stays second
choice all the same (REQUIREMENTS.md §4.11): the strong case needs a
tier-1 provider and a correctly scoped credential, both of which are the
user's to get right, a tier-2 target only has client-side tombstones,
and every byte there costs money.

Costs accepted:

- Non-S3 targets (SFTP, B2's native API, WebDAV, OAuth remotes) are
  not in v1. REQUIREMENTS.md §4.11 does not ask for them; rclone behind
  the same interface is the v1.1 route (alternatives below).
- REPL-6 and REPL-7 are lendisk code, a few hundred lines, exercised
  by the two-node CI and against every tier-1 and tier-2 bucket (Q-Y).
- minio-go's owner risk: MinIO archived its server and `mc` in 2026,
  and the client's future rests on their commercial product. Accepted
  on 2026-10-30. The client is confined to one package behind the
  replica interface; if it is withdrawn a new decision is taken, with
  the AWS SDK for Go v2 as the obvious replacement (D11).
- REPL-10's SHA-256 travels as user metadata
  (`x-amz-meta-lendisk-sha256`) and, where the target verifies one, as
  the request checksum. The lender's decorator hashes the stream anyway
  and compares with the metadata (LEND-4).
- Stored-checksum verification is per target (an MD5 ETag on
  single-part objects, nothing usable on multipart at many providers);
  sample downloads carry the real check (VER-2, S3-6).

*Alternatives considered:*

- **rclone as a supervised child process driven over its rc API**
  (v0.9). It made any rclone remote a target and brought a widely used
  transfer engine, at the price of a second binary and process with a
  supervisor, a per-friend loopback forward because rclone cannot dial
  through tailcat (a listener class REQUIREMENTS.md SEC-8 would have
  had to admit), secrets passed in rc request bodies, two reads of
  every object, errors mapped across a process boundary, and eight
  unverified rc checks (former Q-U). Dropped on 2026-10-30 because
  REQUIREMENTS.md asks for S3 targets only. It remains the v1.1 route
  to non-S3 targets, behind the same interface.
- **The AWS SDK for Go v2.** Equivalent coverage and no owner risk, but
  AWS-first defaults: since 2025 it adds a CRC32 request checksum that
  several non-AWS stores reject, so every tier-2 store and every friend
  would need it configured off. The replacement if minio-go is
  withdrawn.
- **Linking rclone's Go packages** (`fs.Fs` and its backends): an
  unguaranteed API on a path nobody else uses, and a lendisk binary two
  to three times larger.
- **An S3-only CLI** (s5cmd, the AWS CLI, `mc`): the cost of a child
  process without rclone's breadth; `mc` is archived.
- **Go CDK's `blob` package**: no versioning or delete-marker
  operations, and it sits on the AWS SDK anyway.

Overflow targets are tested in CI per the support tiers of §3.11;
friend replicas with two nodes over tailcat.

### D15. Stores: versitygw embedded, `posix` or `s3` backend per store

*Serves:* EP-1 to EP-3, EP-10, EP-12, EP-14, EP-15, LEND-1, LEND-6,
VER-5, COMPAT-4, SEC-3, SEC-8, P-5, G-3. Replaces D14.

The daemon embeds **versitygw** (Go, Apache-2.0) through its `embedgw`
package: `embedgw.RunVersityGW(ctx, backend, *Config)` serves the S3 API
over any `backend.Backend`. lendisk passes its decorator (D4), which
wraps one of versitygw's own backends, chosen once per node:

- **`backend/posix`, the default.** Objects are plain files at
  `<dir>/<bucket>/<key>`, written temp-then-rename, with ETags and
  metadata in xattrs, a versioning directory for non-current versions
  and delete markers, and object lock. One process, no network, and P-5
  at its v0.7 strength: `restic -r <dir>/<bucket>` works with nothing
  running.
- **`backend/s3`, any store.** The same gateway becomes a proxy that
  re-signs each call with the store's own credential and forwards it to
  an S3-compatible endpoint the user names: a Garage, MinIO-compatible
  store or versitygw on the LAN, or a provider (B2, R2, Wasabi,
  Hetzner). lendisk still writes no signature code.

**One store serves both roles.** The node picks `posix` or `s3` once,
at `init`, and its own buckets and every borrower's `lent-*` bucket go
into that store: sibling directories on a disk, sibling buckets in a
Garage. This is the point of the product for the user who runs storage:
a friend's data lands where their own does. It also means one backend
instance, no mux, and Q-W closed. A lender who wants lent buckets on a
second disk mounts it at the bucket directory (Q-Z). A store behind
`backend/s3` that is not on the local network is allowed with a warning
at `init` and `store set`: backups then run at WAN speed (EP-16).

What this buys over the v0.8 Garage design:

- No signature code in lendisk: the ~500–800 security-critical lines of
  the v0.8 proxy are versitygw's, tested against restic, Kopia and the
  AWS SDKs by its own users.
- Capture is synchronous and in the same process as the backend call. A
  pending queue row is written before the backend call and completed
  after it (EP-3), with no HTTP hop between capture and storage.
- The default store is plain files again, and the store engine is
  replaceable by configuration: anything S3-compatible fits under
  `backend/s3`.
- One lendisk process instead of lendisk plus a supervised Garage, and
  no admin socket, RPC port or metadata engine to configure.

**The capability profile.** Behind `backend/posix` every feature the
lender uses is native except lifecycle, which lendisk's purger does
(RET-1). Behind `backend/s3` they depend on the upstream,
so each store has a profile. For a tested store (Garage, RustFS, B2)
the profile ships as data and the probes below confirm it at `init` and
`store set`; for any other store the probes decide it. The result is
recorded in `lendisk.db`, shown in `status`, and chooses the mode per
feature:

| Capability | Probe | Upstream has it | Upstream lacks it | `status` label |
|---|---|---|---|---|
| Versioning | `GetBucketVersioning` | Deletes are delete markers; undelete removes the marker. | lendisk tombstones (§3.6). | retention: store / client-side |
| Lifecycle | `PutBucketLifecycleConfiguration` on a probe prefix | Store expires non-current versions after the window. | lendisk's purger issues the deletes. | purge: store / daemon |
| Object Lock | `GetObjectLockConfiguration` | Retain-until set on delete; the lender's own credential cannot purge early. | RET-2 rests on the lender's daemon. | retention: store-enforced / daemon-enforced |
| Checksum verification | PUT with a wrong `x-amz-checksum-sha256`, expect `BadDigest` | The store checks too; the decorator still hashes the stream (LEND-4). | The decorator's hash is the only check. | integrity: store and daemon / daemon |
| Conditional PUT | `If-None-Match: *` on an existing probe key | Store refuses overwrites itself. | The decorator checks its index first. | immutability: store / daemon |
| Server-side copy | `CopyObject` of a probe object, timed | Cheap tombstone moves, undelete and exports. | Download and re-upload for those. | copy: server / client |

The probe runs through the decorator and versitygw's `backend/s3`, so it
measures the pair, not the upstream alone (Q-V).

**Support tiers for stores:** tier 1 is `backend/posix` on a local
directory (every requirement and scenario, full CI including G-3 on a
Pi 4); tier 2 is `backend/s3` against Garage, a MinIO-compatible store
(RustFS) and one provider (B2), with the full suite per store and the
declared profile asserted by the probes; tier 3 is anything else that
passes the probe, best effort.

The trust model (P-2) is untouched: the borrower-facing listener binds
loopback only, the lender-facing side is reachable only through the
tailcat tunnel and lendisk's bridge (§3.7), a store behind `backend/s3`
is reached with a credential the borrower never holds, and everything
that crosses between households is S3 plus the peer control API.

Known costs and their handling:

- *`embedgw` and `backend.Backend` churn.* The embeddable package was
  extracted in versitygw v1.5.0 (June 2026), external IAM arrived in
  v1.8.0 (September) and v1.10.0 shipped on 22 October: five minor
  releases in four months, with no stability statement. The version is
  pinned and vendored, the decorator is the only file that touches the
  interface, the SEC-9 table and the replication-hole review (Q-H) are
  re-run on every bump, and CI runs restic and Kopia end to end. This is
  the risk that made the v0.8 sidecar attractive; the price of avoiding
  it was the signature code.
- *No `net.Listener`.* `embedgw`'s listener fields are address strings
  (TCP, file-backed or abstract unix sockets). The loopback listener is
  versitygw's own; the tunnel side is bridged by lendisk to an internal
  unix socket in a 0700 directory (§3.7).
- *xattrs are required* on the `posix` store. ext4, xfs, btrfs, zfs and
  Docker overlay2 volumes support them; SMB and some NAS exports do not.
  `init` tests the store directory and refuses one without them (EP-12).
- *Multipart completion copies parts into the final file.* On btrfs and
  xfs `copy_file_range` is a reflink; on ext4 it is a real copy.
  Accepted.
- *Listings on `posix` are `readdir` plus sort.* G-3 measures 500k
  objects; if it fails, the decorator serves `ListObjectsV2` from
  lendisk's index, which it maintains anyway (Q-X).
- *No scrub.* The daemon's own sampler hashes stored objects against
  the index (VER-5).

*Alternatives considered:*

- **Garage as a supervised sidecar behind lendisk's own proxy** (v0.8,
  formerly D14). Indexed listings, per-bucket quotas, block scrub and a
  small-hardware design, but lendisk had to verify and re-sign every
  signature itself, the default store was Garage's block format, and
  two processes were always needed. Garage remains a tier-2 store
  behind `backend/s3` and an overflow target; it has no
  versioning, so lendisk-side tombstones apply there.
- **versitygw as a supervised child** over a unix socket, with
  lendisk's proxy in front (the sidecar variant considered on
  2026-10-29). Removes the `backend.Backend` coupling at the cost of the
  signature code and an HTTP hop between capture and storage.
- **RustFS** (Apache-2.0, 1.0 GA 2026-09-16): versioning, lifecycle and
  IAM, but its documentation recommends XFS and says to avoid ext4,
  btrfs and ZFS, needs 2 GB of memory to test, and ships a web console.
  A tier-2 store behind `backend/s3`, not the engine.
- **MinIO**: AGPL, heavier, community edition archived in 2026.
- **SeaweedFS**: a filer plus volume servers for a single-node job.
- **Any of these as a cluster spanning friends**: rejected for the
  pairwise trust of P-2 (REQUIREMENTS.md); a cluster is one trust
  domain.

## 3. Realisation, by requirement area

Section numbers mirror REQUIREMENTS.md §4. Each entry says how the
requirement is met with the decisions above. Nothing here adds to or
narrows a requirement; if a line seems to, the requirement is wrong or
this line is.

Paths used below: `<config>` is the config directory (mode 0700),
`<data>` the data directory. On the default `posix` backend the store
is `<data>/store/` (or a directory the user names, such as a USB disk),
holding the node's own buckets and the `lent-*` buckets side by side,
with versitygw's versioning directory beside it; `lendisk.db` (SQLite)
is in `<data>`; `<data>/run/` (mode 0700) holds the internal tunnel
socket. Bucket naming: the node's own buckets keep their names. A borrower
never sends a bucket's own name to a lender (SEC-16); it sends the
bucket's **sealed name** `s<26>`, where `<26>` is the first 26
characters, lowercase base32, of
`HMAC-SHA256(K_name, bucket name)` and
`K_name = HKDF-SHA256(identity private key, info="lendisk-bucket-name-v1")`.
That is 130 bits, the same at every lender, fixed for the bucket's
life, and recomputable from the phrase and the name alone, so `recover`
and the recovery sheet need nothing else. The lender stores the
borrower's sealed bucket `t` as `lent-<id16>-t`, where `id16` is the
first 16 characters of the borrower's node ID in lowercase base32: 49
characters whatever the real name, so own bucket names may use S3's
full 63. `bucket add` refuses own names beginning with `lent-` and the
reserved name `lendisk-manifests`. The borrower's `lendisk.db` and its
encrypted manifest (ID-4) hold the name-to-sealed-name map; the
lender's mapping table holds only sealed names and is authoritative on
its side; the stored name is a stable label for reading the store with
no lendisk running (P-5). Behind `backend/s3` the same names are upstream
bucket names; placing them as prefixes inside one upstream bucket is
Q-V.

### 3.1 Identity and recovery

- **ID-1** The identity is an X25519 key: the WireGuard key tailcat
  uses (D5). Node ID, tunnel key and S3 access key (NET-7a) are
  literally the same key; there is no Ed25519 and no conversion.
  Nothing in the design needs a signature today: invites carry a
  symmetric secret (PEER-1), the challenge is an HMAC (VER-1), the
  manifest is AEAD-encrypted under a derived key (ID-4), and the
  per-pair S3 secret is a key agreement (NET-7a). If a feature ever
  needs signatures, it gets a separate signing key derived from the
  same seed, not a change to the identity. The display form is base32
  with a checksum.
- **ID-2** The 24 words are BIP-39 (`go-bip39`); the private key is
  HKDF-SHA256 of the BIP-39 seed with a fixed info string, clamped as
  X25519 requires. Two nodes from the same phrase derive the same key
  (the AC).
- **ID-3** The private key file is encrypted with a key stored beside
  it in the config directory, mode 0600. The phrase is never written.
- **ID-4** The manifest is encrypted with a key derived from the
  identity (HKDF over the private key with a fixed info string) and
  pushed to every lender through the control API `manifest` endpoint
  (D12). The lender writes it through its backend as the object
  `<borrower-id>` in the reserved bucket `lendisk-manifests`, which no
  borrower's account can name or list (PEER-8), so it survives the loss
  of `lendisk.db` and is part of a lender's disk handed over (P-5).
  Addressing hints are the peer's `tc…` string plus any explicit
  addresses (NET-3).
- **ID-5** A lender's address is its tailcat `tc…` string, which carries
  the public key and relay bootstrap info and no pre-shared key
  (SEC-14). `recover` derives the key
  from the phrase, dials the lender, fetches the manifest over the
  control API (no S3 credential needed yet), then derives the per-pair
  S3 credentials (§3.7) for every lender listed. Overflow target
  credentials found in the manifest are shown once, for the user to
  put in the environment (SEC-3); they are not written to disk. The
  `placement` table (D6) is then rebuilt by listing each reachable
  place's copy of each sealed bucket; a place that is offline is
  listed when it returns, and until then its objects count as held
  wherever else a listing found them.
- **ID-6** Read-through is the decorator's `GetObject`/`HeadObject`
  miss path (EP-10); after `recover` every object is a miss until
  fetched, from the first reachable place `placement` names.
- **ID-7** The sheet is generated from the manifest; a lender's `tc…`
  string changes only when its key or its configured bootstrap region
  changes (SEC-14), so regeneration is rare. Each bucket is listed with
  its sealed name and the `lent-<id16>-<sealed>` name it has in every
  lender's store (SEC-16), which is what a restore with lendisk stopped
  (P-5) needs, together with the number of replicas it must combine
  (N − c + 1, from the pool size and the bucket's `copies`) and the
  command that combines them: `rclone copy` or `aws s3 sync` from each
  replica into one directory, `--ignore-existing` after the first.

### 3.2 Peers and invites

- **PEER-1** The invite string carries the inviting node's `tc…`
  address, the symmetric quota, the retention offer and a single-use
  secret. Its admission hook admits a key derived from the secret, in
  the `invite` role, until the invite is redeemed or expires (§3.9);
  the invited node dials with that key to call `redeem`, after which
  its real key replaces it.
- **PEER-2** `redeem` carries the invited node's `tc…` address, its
  retention offer and the user's confirmation of the quota. Both nodes
  then write the same friendship row in `lendisk.db` (peer key,
  address, quota, each side's retention, ceiling, domain), and each
  becomes an account in the other's `IAMService`: each is the other's
  lender and borrower. The derived secret (NET-7a) serves both
  directions: the X25519 shared secret is the same either way, the
  access key is the borrowing side's node ID, and the tunnel bridge
  binds that access key to the connection's peer key (SEC-12), so
  neither side can use the other's credential. It is computed on
  demand and never stored. Buckets a borrower
  creates, always under their sealed names (SEC-16), are created by the
  decorator under the `lent-<id16>-` name and owned by that account
  alone.
- **PEER-3, PEER-6, LEND-8** The quota is one column of the friendship
  row and both sides hold the same value. Changes go through the
  control API `quota` endpoint: `propose(n)` from either side, answered
  `accepted` when `n` is within the receiver's `lending.max_per_peer`
  and keeps the sum of its quotas within `lending.max_total`; `pending`
  when only the per-friend ceiling is exceeded, until its user runs
  `lendisk peer quota <peer> --accept` (or rejects it); `refused` when
  the global cap would be exceeded, which `--accept` cannot override. A decrease
  is not a proposal: it is applied by the side that makes it and pushed
  as `lower(n)`, which the other side applies at once (SEC-15's 60
  seconds apply). If the push cannot be delivered, the lowering side
  enforces the lower number on its side alone and repeats the push on
  every connection until it is acknowledged, so the two rows converge.
  The automatic proposer runs after each placement pass: when a bucket
  has a deficit (D6, step 4), it proposes to the friends with the
  smallest free fraction an increase that would cover the deficit at
  `copies`, never beyond its own ceilings.
- **PEER-8** Two walls. The decorator maps every bucket name through
  the borrower's own mapping table, so a borrower can only name its own
  buckets; and versitygw's bucket-ownership check refuses an account
  any bucket it does not own, so a mapping bug cannot cross into another
  borrower's data or the lender's own. `ListBuckets` is answered from
  the mapping table.
- **PEER-9** The lender-only profile skips the loopback S3 listener and
  runs no replication worker; versitygw still runs, behind the tunnel
  bridge only. Its friendship rows are like any other; the quota its
  friends lend it back has no buckets to fill, and `status` shows it as
  unused. Adding a bucket later is a change of `role` and a restart.
- **PEER-10** Peers are rows in `lendisk.db`; nothing in the control API
  or the S3 surface returns them.

### 3.3 Endpoint and local store

- **EP-1** Embedded versitygw (D15) over the decorated backend,
  listening on `127.0.0.1:9000`. No unix socket: no backup tool can use
  one. Path-style addressing is versitygw's default.
- **EP-5** versitygw is given `127.0.0.1:9000`, the internal
  `tunnel.sock` (§3.7) and nothing else, whatever the configuration
  says; network addresses are served only by the LAN bridge (EP-17),
  so no versitygw option or default can open one.
- **EP-17** The LAN bridge. versitygw is not given the network
  addresses: its TLS settings apply to every listener it runs, which
  would put the loopback listener behind TLS too, and a per-address
  TLS choice is what EP-17 asks for. Instead lendisk listens on each
  `[[endpoint.listen]]` entry itself (`address = "192.168.1.10:9000"`
  or `interface = "eth0"` with a port; `tls = "generate"`, the
  default, `"files"` with `cert` and `key` paths, or `"off"`). An
  interface name is resolved to its addresses at start and on change;
  `0.0.0.0`, `::` and an empty host are refused at config load. Each
  accepted connection is served as HTTP (after a `crypto/tls`
  handshake where TLS is on; a plaintext request on a TLS port gets
  the TLS alert, nothing is proxied) and every request is proxied,
  unbuffered, to versitygw's loopback listener with the `Host` header
  and every signed header unchanged, as the tunnel bridge does (§3.7,
  Q-G). versitygw verifies the signature against the per-bucket
  credential exactly as on loopback; lendisk adds no check of its own
  and needs none, since the writer is trusted (§2). A request under
  `/_lendisk/` is refused on this bridge. With `tls = "generate"`,
  lendisk creates a self-signed ECDSA P-256 certificate whose SANs are
  the configured addresses and the host name, valid ten years, at
  `<config>/endpoint.crt` with its key at `<config>/endpoint.key`
  (0600); it is regenerated when the address set changes, and
  `lendisk endpoint cert` prints its path and SHA-256 fingerprint.
  `bucket add` prints, for each network address, `restic -r
  s3:https://<address>/<bucket>` with `--cacert <path to a copy of
  endpoint.crt>` for a generated certificate, or `s3:http://…` with the
  plaintext warning for `tls = "off"`. With TLS off the request is
  still SigV4-signed, and restic and Kopia send the `aws-chunked`
  streaming form, which signs every chunk, so a machine on the LAN can
  read object bytes and replay a captured request for up to SigV4's
  15-minute window, but cannot forge one or learn the secret key.
  Contents of restic and Kopia buckets are encrypted by the tool;
  `generic` buckets are not, which is what the warning says.
  In the lender-only profile (PEER-9) an `endpoint.listen` entry is
  refused at config load.
- **EP-2** The decorator enqueues on a successful `PutObject` or
  `CompleteMultipartUpload`, and a delete on a successful
  `DeleteObject` or each successful entry of `DeleteObjects`. Parts and
  aborted uploads do not pass through those methods.
- **EP-3** The decorator writes a pending queue row (bucket, key,
  operation) before calling the backend and marks it complete after the
  backend returns success; on failure it deletes the row. At startup,
  pending rows are resolved with `HeadObject` against the backend. The
  decorator does not buffer request bodies; it wraps the stream in a
  SHA-256 as it passes to the backend and writes the hash to the
  completed row, so the object is never read again to hash it
  (REPL-10). EP-11 needs the hash at write time in any case, to notice
  a changed acknowledged key.
- **EP-6** Bucket credentials are one access/secret pair per bucket,
  generated and stored by lendisk in `lendisk.db` and resolved by
  lendisk's `IAMService` (`Config.IAMService`, which takes precedence
  over versitygw's own IAM options). versitygw's `IAMDir` is not used.
- **EP-10** The decorator's `GetObject`/`HeadObject` turn a not-found
  from the backend into a fetch through the replica interface (D12; a
  ranged `GetObject` by the replica client) from a place `placement`
  names as `acked` for the key, friends before overflow targets and
  the most recently reachable first, falling through to the next on
  failure; they write the object into the store and return it. In
  `evict` mode the eviction worker deletes a pack from the store once
  `copies` places have acknowledged it and no tool lock is present
  (restic `locks/*`); for Kopia, which has no server-side lock, once
  the bucket has also had no writes for the configured idle interval
  (EP-9).
- **EP-12** On `posix`, `init` checks xattr support and the free-space
  floor on the store directory and refuses otherwise. On `s3`, it runs
  the capability probe (D15) and warns when the endpoint is not on the
  local network. `status` reports the pinned versitygw and tailcat
  versions.
- **EP-13** Throughput is versitygw's chunk-signature verification plus
  the backend's writes; on `posix` with NVMe this is file I/O. Measured
  with G-3.
- **EP-14** The reconciler pages through `ListObjectsV2` of every bucket
  through the backend and compares with the index in `lendisk.db`.
- **EP-15** On `posix` the store is plain files: `restic -r
  <data>/store/<bucket>` opens it with nothing running, and a borrower's
  replica on the lender, `restic -r <data>/store/lent-<id16>-<sealed>`,
  likewise (scenario 7).
  Behind `backend/s3`, any S3 client with the store's credential reads
  the same keys.

### 3.4 Replication

- **REPL-12, placement at capture.** When the decorator completes a
  queue row (EP-3), the placer runs D6's choice for it in the same
  SQLite transaction: a metadata key gets a `placement` row per friend
  (and per overflow target already holding data of the bucket), a data
  key gets `copies` rows. Each `placement` row in state `queued` is an
  entry in that place's FIFO.
- **REPL-1 to REPL-3** The queue is a SQLite table with one FIFO
  sequence per place, ordered by the bucket's write sequence. The
  worker uploads one object at a time per place in sequence order,
  several places in parallel. A delete (REPL-2) is a row in the FIFO of
  every place whose `placement` row for the key is `acked` or
  `queued`; a `queued` put that has not started is simply cancelled.
- **REPL-1, the metadata barrier.** A metadata row at sequence `m` is
  eligible to send only when every data object before `m` has no
  `queued` placement row left: each is `acked` at every place it is
  placed at, which is `copies` places or, where D6 recorded a deficit,
  fewer. A deficit therefore lowers the copies but does not stop the
  pool: places with room keep receiving data and metadata. The bucket's
  watermark (REPL-11) still requires `copies`. Because the place's FIFO
  waits behind a held metadata row, a lagging data copy holds back
  every place's metadata, so data rows are re-routed early: a `queued`
  data row at a friend unreachable for longer than
  `replication.reroute_after` (1 hour) is given to another *friend*
  with room by D6's choice, never to an overflow target while the
  unreachable friend still has room (REPL-12); with no such friend it
  waits, the lag shows in `status`, and `replace_after` applies.
  Re-routing an unsent row moves no bytes.
  Restic and Kopia write packs, then indexes, then the snapshot or
  manifest, so the barrier costs one round of acknowledgements per
  backup run, not per object.
- **REPL-13, the re-placement worker.** It runs after every change to
  the pool (friend added or removed, quota changed, a verification
  failed, an overflow target added) and hourly. It computes for each
  object the places that hold or will hold it (`acked`, `queued`, not
  `dropping`) and:
  - for each object with fewer than `copies` such places, places the
    missing copies by D6's choice, excluding the current holders; the
    put reads from the local store, or in `evict` mode streams from a
    holder through the replica client;
  - for each metadata object missing at a friend, or at an overflow
    target holding data of the bucket, adds the row. A new friend thus
    receives the bucket's existing metadata, in sequence order and
    ahead of any newer data, before the placer counts it as holding a
    copy;
  - for each copy at a place being drained (a friend removed, failed,
    unreachable for longer than `replication.replace_after`, 7 days,
    or above its lowered quota; an overflow target when a friend has
    room; any place beyond `copies` after `copies` is lowered), first
    places the replacement as above and, only when that is `acked`,
    marks the old row `dropping` and queues a delete there. A copy
    never drops below what it had, and an object never has fewer than
    `copies` places except while there is nowhere to put one.
  - Drain-back from overflow targets is lowest priority and runs only
    inside the replication schedule (REPL-7), oldest objects first.
  A friend that returns after `replace_after` has its rows already
  `dropping`; its queue sends the deletes, and what it held becomes
  tombstones for the window (RET-1) like any other delete. The worker
  and the placer share one lock per bucket, so the two never decide
  about one object at once.
- **The replica client.** One minio-go client per place in the pool, built from
  the configuration and the manifest. A friend is the friend's tunnel
  address as endpoint, path-style, the derived credential (NET-7a), the
  bucket's sealed name in place of its own (SEC-16), and an HTTP
  transport whose dial function is the tailcat `Client` for
  that peer (§3.7). An overflow target is the endpoint, bucket, prefix
  and credential the user registered (S3-1). The clients are built in
  one package, which is the only code that imports minio-go (D12).
- **Operations.** `Put` streams the object from the store through the
  decorator into `PutObject`, or for objects above the part size into
  the low-level multipart calls (`NewMultipartUpload`, `PutObjectPart`,
  `CompleteMultipartUpload`), with `x-amz-meta-lendisk-sha256` from the
  queue row and the SHA-256 as the request checksum where the target
  verifies one. `Get` with a range is `GetObject` with a range option.
  `Head`, `List` and `Delete` map to `StatObject`, `ListObjects` (with
  versions where needed) and `RemoveObject`. `Undelete` is §3.6.
- **Errors.** The client's S3 errors are mapped to the replica
  interface's: a `507 QuotaExceeded` from a friend's decorator becomes
  "paused for quota" (LEND-2); a `403` after a removal becomes
  "removed" (PEER-5, SEC-15); a `412` becomes an immutability alert
  (EP-11); anything else is a retry with backoff.
- **REPL-6** The upload ID and the ETag of each completed part are
  written to the queue row as parts complete. After a restart or an
  interruption the worker lists the parts of the open upload, checks
  them against the row, and continues at the next part. Objects below
  the part size (16 MiB, configurable) restart whole. The same for
  friends and overflow targets.
- **REPL-7** Per-peer and global limits are token buckets
  (`golang.org/x/time/rate`) wrapped around the reader of each part;
  the time-of-day schedule is checked before each part, so a closing
  window stops a transfer within one part. Both change at runtime from
  the configuration.
- **REPL-8** `export --for <peer>` pages through the bucket through the
  decorator and writes one file per key at its key path (or Kopia's
  sharded layout), for each key with a `placement` row at that peer;
  `import` on the lender walks the directory and puts each file into
  the borrower's bucket through the decorator, and the borrower marks
  those rows `acked` once a listing of the peer confirms them with the
  right hashes.
- **REPL-10** The hash is SHA-256, taken by the decorator from the
  write stream at capture (EP-3) and kept on the queue row; the worker
  sends it with the object as `x-amz-meta-lendisk-sha256` and stores
  it in the `lendisk.db` index on acknowledgement. The object is read
  once per replica, as it is sent. The lender compares what it received
  with the hash (LEND-4), so a local copy that rotted between capture
  and send is rejected and alerted rather than replicated.
  REQUIREMENTS.md v0.9 reworded REPL-10 to allow the hash to be taken
  when the tool writes the object, for exactly this.
- **REPL-11** The bucket's watermark is the highest sequence `w` such
  that every object up to `w` has `copies` `acked` placement rows; it
  advances as acknowledgements arrive and is the offsite-complete point
  (REPL-4). Per-place lag (the oldest `queued` row's age) is kept beside
  it for `status` and OPS-2.

### 3.5 Lending and quotas

- **LEND-1** The decorator keeps a per-friend byte counter in
  `lendisk.db` (live objects plus non-current versions or tombstones,
  updated on every completed put and every purge) and reconciles it
  daily against a listing, and enforces the friendship's quota on it.
  `lending.max_total` bounds the sum of all friendships' quotas; a
  quota proposal that would exceed it is refused (LEND-8). The borrower side keeps its own count of what
  it has stored at each place, for D6's free fraction, and corrects it
  from the lender's `quota` answer.
- **LEND-2** The decorator refuses `PutObject`, `CreateMultipartUpload`
  and `CreateBucket` with S3 error code `QuotaExceeded`, HTTP 507, when
  the counter would exceed quota or `statfs` on the store is below the
  floor. Whether a backend error can carry a custom code
  through versitygw is Q-I.5; the fallback is `AccessDenied` with a
  distinguishing message the borrower parses.
- **LEND-4** The decorator hashes the body as it streams it to the
  backend (SHA-256), compares with `x-amz-meta-lendisk-sha256` when the
  backend answers, and on mismatch deletes the object and returns
  `BadDigest`. The existing-key check happens before any byte is
  stored: a key present in the index with a different hash is refused
  with `PreconditionFailed`, using a conditional put where the store
  supports one (D15, capability profile). A put without the hash is
  refused on the tunnel side.
- **LEND-6** The lending store is the set of `lent-<id16>-<sealed>`
  buckets in the node's one store, beside its own, plus versioning or
  tombstones and the hash index. With lendisk stopped they are plain
  files on `posix` (EP-15); `lendisk export <bucket> --borrower <id>
  <path>` writes the tool's own layout from either backend kind
  (REPL-8).
- **LEND-7** `init` suggests `lending.max_total` = `c × S` plus the
  retention margin of §4. The control API `quota` answer carries the
  quota, both sides' use and the answering node's repository size (sum
  of its own buckets' live bytes, rounded to 1 GB). The protectable
  size is computed by bisection on `S` over Σ min(D, quotaⱼ − m) +
  Σ (overflow `max_bytes` − m) ≥ c × D, with `m` the bucket's metadata
  bytes from the index and `D = S − m`, and the fair-quota bound with
  `quotaⱼ` replaced by `sizeⱼ`; the derivation is §4. The
  "under half used for 30 days" note compares the daily use samples
  both sides already report in `quota`.

### 3.6 Deletion, retention and verification

- **RET-1** A borrower's `DeleteObject` on a store with versioning
  (`posix` always; `s3` per the probe) places a delete marker and the
  object becomes a non-current version; lendisk's purger removes
  non-current versions older than the window where the store has no
  lifecycle of its own. On object-lock-capable stores the decorator
  sets retain-until at delete time, so the lender's own credential
  cannot purge early. On a store without versioning the decorator keeps
  tombstones instead: a row in `tombstones` (borrower, bucket, key,
  hash, deleted at, purge after), the key hidden from that borrower's
  reads and listings, and the real delete issued by the purger after
  the window; a put with different content to a tombstoned key first
  moves the stored object to `.lendisk/tomb/<key>/<unix-time>`.
  Whether versitygw's `posix` versioning gives these semantics is
  Q-I.4.
- **RET-2** The decorator refuses `DeleteObject` with a version id
  unless that version is a delete marker created within the window, and
  admits no lifecycle, bucket-deletion, object-lock or admin call (§3.7
  allow-list). Keys beginning with `.lendisk/` are refused from
  borrowers.
- **RET-3** `undelete` for a friend removes delete markers with
  `RemoveObject` by version id where the friend's store versions (the
  one versioned delete NET-7 admits), and calls the control API
  `undelete` (bucket, since) where it keeps tombstones. For an
  overflow target that versions, it does not remove the delete marker,
  since S3-5's credential cannot remove versions: it copies the last
  version before the marker onto the same key (`CopyObject` with the
  source version id), which makes it current again and leaves the
  marker to the lifecycle rule. Elsewhere it clears the borrower's own
  client-side tombstones (S3-4).
- **VER-1** The challenge is a control API call: the borrower sends an
  object key, a byte range and a nonce; the lender reads the range
  through its decorator and returns HMAC-SHA256(nonce, bytes); the
  borrower computes the same over its local copy.
- **VER-2** Samples are whole-object `GetObject` calls by the replica
  client, hashed by lendisk and compared with the index.
- **VER-5** The sampler hashes a weekly sample of at least 0.1% of the
  stored bytes through
  the decorator and compares with the index, on both sides; neither
  backend has a scrub of its own. On a mismatch the borrower deletes
  and re-fetches the object (EP-10).

### 3.7 Network

- **NET-1** Every peer connection is a WireGuard tunnel via tailcat; the
  lender's `AllowClient` hook admits known peer keys and pending invite
  keys and nothing else (SEC-10).
- **NET-2 to NET-4** magicsock hole punching with UPnP/NAT-PMP/PCP
  (Q-R), DERP fallback; `PathInfo` is built on `Client.DiscoPing`,
  whose result names an endpoint only when the pong came over a direct
  path, and reports direct or relayed for `status`. `Client.Ping`
  always measures the DERP path and is not used for this.
- **NET-5** Relays are DERP servers. The project publishes a DERP map;
  `relays.map_url` or `relays.list` replaces it; `lendisk relay` wraps
  `tailscale.com/cmd/derper`. Tailscale's hosted DERP map may be
  included as a last-resort bootstrap.
- **NET-5a** `relays.accept_peer_hints` controls whether a peer's
  `tc…` relay hints are merged into the node's DERP map.
- **NET-6** The version is carried by the control API `version`
  endpoint; the S3 surface is governed by its own stability.
- **The tunnel bridge.** `embedgw` takes listener addresses, not a
  `net.Listener` (D15), so the lender side is bridged. The tailcat
  `Server` listens on one tunnel port, 9000. lendisk accepts each
  connection itself, resolves its peer key (SEC-12) and serves HTTP on
  it. A request whose path begins with `/_lendisk/` is the control API
  and is answered by lendisk directly; no S3 bucket can have that name,
  since bucket names begin with a letter or digit, so the two surfaces
  cannot collide. Every other request is an S3 request: it is checked
  (SEC-11, SEC-12) and then proxied, unbuffered, to versitygw's
  internal listener on
  `<data>/run/tunnel.sock`, with the `Host` header and every signed
  header passed unchanged so the client's signature still verifies.
  versitygw terminates the signature; lendisk never computes one on
  this path.
- **NET-7** The decorator admits exactly these S3 operations from a
  borrower's account: `PutObject`, `CreateMultipartUpload`,
  `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`,
  `ListParts`, `GetObject` with Range, `HeadObject`, `HeadBucket`,
  `ListObjectsV2`, `ListObjectVersions`, `ListBuckets` (own buckets
  only), `CreateBucket` (counted against quota), `DeleteObject` without
  a version id (a delete marker or a tombstone) and `DeleteObject` with
  the version id of a delete marker inside the window (undelete). Every
  other operation, including `DeleteObjects`, `CopyObject`,
  `UploadPartCopy`, tagging, lifecycle, object-lock and any bucket
  configuration call, is refused with `AccessDenied` before reaching the
  backend. The control API (`redeem`, `challenge`, `manifest`, `quota`,
  `version`, `undelete`) is plain HTTP under `/_lendisk/` on the same
  tunnel port, authenticated by the tunnel peer key and checked per
  request against `lendisk.db` (SEC-11). The borrower replicates to a
  friend with the same replica client it uses for overflow targets,
  dialling through tailcat as below. AC check: from an allowed peer,
  `restic -r s3:http://<lender>:9000/<bucket> snapshots` works with the
  derived credential.
- **NET-7a** The S3 credential for a borrower–lender pair is derived:
  access key is the borrower's node ID; secret key is
  `HKDF-SHA256(X25519(borrower key, lender key), info="lendisk-s3-v1")`.
  The lender's `IAMService` resolves a node-ID access key only when a
  relationship exists for it (SEC-11), since any node can derive a valid
  secret for its own key, and computes the secret on demand. The bridge
  refuses a request whose access key is not the tunnel peer key
  (SEC-12). The derived secret is never stored.
- **Dialling a friend.** The replica client's HTTP transport uses the
  tailcat `Client.Dial` for that peer as its dial function, so
  replication traffic enters the tunnel from inside the process. No
  listener is opened on the borrower's side for replication.
- **NET-7b** `peer forward` opens a listener on the loopback address
  the user names, for as long as the command runs, and copies each
  accepted connection into a tailcat dial to the friend's tunnel port,
  so a backup tool can read a friend's replica directly. It is the only
  forward in the design, and it is the listener NET-7b itself names.
  Since the forward is a byte copy, the tool must name the sealed
  bucket; the command prints, from the borrower's own map, one line per
  bucket held at that peer, e.g.
  `laptop  restic -r s3:http://127.0.0.1:9100/s4k2…`.
- **NET-8** The internal `Transport` interface is `Listen`,
  `Dial(peerKey)`, `Admit(func(peerKey) bool)`, `PeerOf(conn)`,
  `Disconnect(peerKey)`, `PathInfo`. `Admit` must be called before
  `Listen`, and an implementation with no admission function refuses
  every connection; `PeerOf` returns the key that authenticated an
  accepted connection (SEC-12). These three are the contract any
  second implementation has to meet, whatever its own defaults are.
  tailcat is the only implementation; Syncthing's stack or iroh would
  be the second (D5).

### 3.8 Operations and observability

- **OPS-4** `prometheus/client_golang` on a loopback port. Transfer
  counters (bytes, objects, retries, lag, per place) come from the
  replication worker.
- **OPS-6** The configuration file is TOML; state lives in `lendisk.db`.
  Store settings are one `[store]` section (`backend = "posix"` with a
  `path`, or `backend = "s3"` with endpoint; its credential comes from
  the environment, SEC-3), used for both roles; replication settings,
  including the limits, the timetable, `reroute_after` and
  `replace_after`, are `[replication]`; the ceilings are `[lending]`
  (`max_total`, `max_per_peer`); a friend's domain is `domain` in its
  `[[peer]]` entry; overflow targets are `[[overflow]]` entries; network
  addresses for the endpoint are `[[endpoint.listen]]` entries (EP-17),
  none by default.
- **OPS-7** Idle RSS is lendisk alone (versitygw, tailcat and the
  replica client included); active RSS adds one part buffer (16 MiB by
  default) per transfer in flight. Q-M measures both.
- **OPS-8** One binary; versitygw, tailcat and minio-go are vendored,
  so their upgrades are lendisk releases. State migrations are
  `lendisk.db`'s, forward and one minor version back.

### 3.9 Security

- **SEC-3** Per-bucket S3 keys live in `lendisk.db`, which lendisk
  creates at mode 0600; versitygw's
  `IAMDir` is not used. A `backend/s3` store credential and overflow
  target credentials are read from environment variables at start:
  `LENDISK_STORE_ACCESS_KEY` and `LENDISK_STORE_SECRET_KEY` for the
  store, `LENDISK_OVERFLOW_<NAME>_ACCESS_KEY` and
  `LENDISK_OVERFLOW_<NAME>_SECRET_KEY` for each overflow target, with
  the variable names overridable per entry in the config file. They
  are supplied by systemd's `EnvironmentFile=` (a root-owned file the
  service user cannot read) or the container runtime's secrets;
  lendisk never writes them to disk, redacts them from logs and
  `status`, and refuses to start a store or overflow target whose variables
  are unset. Replica credentials are also carried in the encrypted
  node manifest (S3-1) for recovery. They are read by the process that
  uses them and never passed to another. The endpoint's TLS key
  (EP-17), where TLS is on, is `<config>/endpoint.key` at mode 0600
  when generated; a user-supplied key stays at the path the config
  names, and lendisk refuses to start if that file is readable by
  anyone but the service user. It is not in the manifest: a recovered
  node generates a new certificate, and clients are given the new one.
- **SEC-7** The hostile-peer test exercises every S3 operation through
  the tunnel bridge and every control API endpoint, from an allowed
  peer's identity and from an unknown one.
- **SEC-8** versitygw's admin API, web UI and website endpoints are not
  configured. lendisk's listeners are loopback S3 (TCP), the LAN
  bridge on each configured `endpoint.listen` address (EP-17), loopback
  metrics, the tailcat tunnel (one port inside it), the internal
  `tunnel.sock`, and the loopback listener `peer forward` opens while
  that command runs (NET-7b). There is no child process and no other
  socket. Every listener is one REQUIREMENTS.md names (SEC-8 as
  reworded in v0.9.2 for EP-17). Checked with `ss -lntp` and `ss
  -lxp`, with and without an `endpoint.listen` entry.
- **SEC-9** The classification table covers every `backend.Backend`
  method at the pinned versitygw version, each marked admitted,
  refused or unreachable for a borrower's account, with a test that
  sends one request per S3 action through the bridge and asserts the
  outcome. It is re-reviewed on every versitygw upgrade, together with
  the replication-hole review (Q-H).
- **SEC-10** `Server.AllowClient` is always set, before `Start`, to a
  lendisk function that looks the key up in `lendisk.db`: a
  relationship row admits it in the `peer` role, an unexpired invite
  key in the `invite` role, and anything else, including a database
  error or a closed database, is rejected. The hook is never nil and
  tailcat's `AllowedClients` list is never used, since both admit
  everyone when unset or empty. tailcat retries a rejected client
  about once a second, so the hook keeps a negative cache per key,
  cleared whenever a relationship or invite changes. The server starts
  even with zero rows, so the address exists from `init` on and every
  connection is refused until the first invite. The SEC-7 harness runs
  an unknown identity against zero peers, one peer, and zero after
  `peer remove`.
- **SEC-11** The tunnel port sits behind one middleware in the bridge,
  in front of both the control API and the S3 proxy, that takes the
  connection's peer key (SEC-12), loads its relationship
  and role from `lendisk.db` and answers `AccessDenied` (403) before
  routing or proxying if there is none. It reads the row on every
  request, not once per connection, so quota and state changes apply
  to the next request (SEC-15). The `IAMService` repeats the
  relationship check when it resolves a node-ID access key, so a
  request reaching versitygw by any path is refused without one. A
  build tag `admitall`, used only by the SEC-11 test, replaces the
  admission hook with one that admits everyone, to show the middleware
  alone refuses an unknown peer.
- **SEC-12** The bridge resolves each accepted connection with
  `Server.PeerKey(conn.RemoteAddr())` and puts the key in the request
  context through `http.Server.ConnContext`; a connection without a key
  is closed before any byte is read. For an S3 request it reads the
  access key from the `Authorization` header's `Credential=` or a presigned
  URL's `X-Amz-Credential`, without verifying anything, and refuses the
  request unless it equals the peer key; versitygw then verifies the
  signature for that access key. A request with neither form is
  refused. The control API takes the borrower from the context, never
  from a parameter.
- **SEC-13** In the `invite` role the middleware admits `redeem` and
  refuses every other S3 and control API request. `redeem` checks the
  invite secret, marks it used in the same transaction that creates the
  relationship row, and the invite key's connection is then closed;
  the borrower reconnects with its own key.
- **SEC-14** `DisablePresharedKey` is set and `Key` is the identity's
  WireGuard key (ID-1), so the `tc…` address is a function of the
  public key and the bootstrap region only. `Region` is set from the
  region chosen at `init` and saved in the config, rather than picked
  by latency at each start, because the address embeds it and would
  otherwise change on restart. Without a pre-shared key the tunnel is
  WireGuard's standard Noise IK handshake; what is given up is the
  pre-shared key's hedge against a future break of X25519, which for
  lendisk exposes S3 headers and control API traffic, never backup
  contents (P-3).
- **SEC-15** On `peer remove`, a quota change, or an invite's
  redemption or expiry, lendisk commits the change to `lendisk.db`,
  clears the hook's cache, cancels the contexts of in-flight requests
  from that key and closes its connections (the bridge tracks them per
  key, including the proxied half to `tunnel.sock`), and, when no
  relationship is left, calls `Server.DisconnectClient`. tailcat's
  disconnect alone does not reset connections, which would stall until
  they time out, so lendisk closes its own. Later requests are answered
  according to the relationship's new state (PEER-5, PEER-6).
- **SEC-16** The sealed name (§3, bucket naming) is the only bucket
  name that crosses the tunnel: in S3 paths and in any control API
  call that names a bucket (`challenge`, `undelete`). A keyed hash
  rather than a counter or a random label keeps it stateless: a node
  rebuilt from the phrase can reach its buckets at every lender before
  its manifest is fetched, and because the key is per identity a lender
  cannot tell that two borrowers both have a bucket called `laptop`.
  Not hidden, and listed under SEC-2: the number of buckets per
  borrower, and the tool, which shows in the key layout (`data/`,
  `snapshots/`, `kopia.repository`). Hiding keys as well would mean
  rewriting them on the lender's disk and losing P-5, so it is not
  done.

### 3.10 Compatibility

- **COMPAT-2** lendisk is one static Go binary for amd64, arm64 and
  32-bit ARM, so the package exists for every v1 platform.
  The container image recommends `network_mode: host` (D2). versitygw
  also builds on macOS, BSD and Windows, which keeps post-v1 ports
  open.
- **COMPAT-3** `bucket import` walks the directory and puts each file
  through the decorator, so it is captured but marked unacknowledged.
- **COMPAT-4** The documented layout on `posix` is
  `<store>/<bucket>/<key>` as plain files with versitygw's xattrs, its
  `.sgwtmp/` and `.vgwlocks/` housekeeping directories and its
  versioning directory; borrowers' buckets under `lent-<id16>-` and
  their sealed names (SEC-16); the
  reserved `lendisk-manifests` bucket; the `.lendisk/tomb/` prefix where
  tombstones apply; `lendisk.db` and its tables; and the export layout
  (one file per key at its key path; Kopia's sharded form with
  `--kopia-layout`). Every object lendisk stores carries
  `x-amz-meta-lendisk-sha256`, so the hash index can be rebuilt from
  the store.

### 3.11 Overflow targets

Support tiers, all through the replica client (D12):

| Tier | Targets | Retention | Testing |
|---|---|---|---|
| 1 | S3 with versioning (B2, Wasabi, Hetzner, R2, RustFS) | provider-enforced (S3-4), credential without permanent delete (S3-5) | CI per provider |
| 2 | S3 without versioning (Garage) | client-side tombstones, labelled | CI against Garage |

Non-S3 targets (SFTP, B2's native API, WebDAV, OAuth remotes such as
Google Drive) are not in v1: REQUIREMENTS.md §4.11 asks for S3
buckets, and rclone behind the same replica interface is the v1.1
route to the rest (D12).

An overflow target is a place in the pool like a friend (D6), with
`max_bytes` as its capacity, and is only a candidate for a data copy
when fewer than `copies` friends have room (D6, step 3). The
re-placement worker drains it back to friends whenever they have room
(§3.4). `status` shows what it holds, the objects for which it is one
of the `copies`, and the estimated cost (S3-7).

- **S3-1** `overflow add s3 <name> --endpoint … --bucket … --prefix …
  --max-bytes …` writes an `[[overflow]]` entry to the config file and
  reads the credential from the environment (SEC-3), testing it before
  the target is accepted; the credential is carried in the encrypted
  manifest for recovery, and the client is built from all of these at
  start. Each of the node's buckets is a sub-prefix
  `<prefix>/<sealed name>/` (SEC-16), with keys below it as the tool
  wrote them (S3-2).
- **S3-3** Uploads carry the SHA-256 as the request checksum where the
  provider verifies one and Content-MD5 otherwise, plus lendisk's
  SHA-256 as metadata. Large objects use multipart with the resume of
  §3.4.
- **S3-4** Buckets without versioning get lendisk-side tombstones: the
  delete is recorded locally and issued after the window; `status`
  labels the target "retention: client-side". Drain-back deletes go
  the same way.
- **S3-5** Drain-back has to delete at the target, so the recommended
  credential may create delete markers (`s3:DeleteObject` without a
  version id) and read earlier versions (`s3:GetObjectVersion`, for
  undelete, §3.6) but not remove versions (`s3:DeleteObjectVersion`),
  and the lifecycle rule purges non-current versions after the window.
  `overflow add` tests both permissions and reports them. Where a
  provider cannot separate the two (Q-AC), the choice is the user's:
  with no delete permission at all, drained copies stay at the target
  as dead weight until the user removes them, and `status` counts them.
- **S3-6** Stored checksums are read with `StatObject` (the ETag,
  where it is an MD5 of a single-part object) and compared where the
  type matches; sample downloads (VER-2) are the check everywhere.

## 4. Capacity planning

Placement (D6) and symmetric quotas (P-8) set the disk cost. Let `S`
be the repository size: source data after deduplication and
compression (typically 0.5–1.0× the source; ~1× for photos and video),
plus the snapshot history the user's `forget` policy keeps. Let `c` be
`copies` and `n` the number of friends.

**What a node stores elsewhere:** `c × S` of data objects, plus the
metadata (~1% of `S`) at each of the other `n − c` friends, plus
tombstones for the retention window (RET-1). Tombstones are the
margin: restic `prune` and Kopia maintenance rewrite packs, and the
old ones stay at every place that held them for the window. Weekly
pruning of a repository with a few percent churn a week gives 10–20%
of `S` in tombstones at 30 days; this document plans with 20%.

**What it lends:** by symmetry, the same total, spread over its
friendships: `1.2 × c × S`. That is `init`'s suggestion for
`lending.max_total` (LEND-7).

**What a node needs on its own disk:**

| | `full` mode (default) | `evict` mode |
|---|---|---|
| Local copy | `S` | ~1% of `S` for metadata, plus a buffer for packs not yet acknowledged by `copies` places (at least one backup run of new data) |
| Space lent | `1.2 × c × S` | same |
| Total, `c = 2` | `3.4 × S` | `2.4 × S` plus the buffer |

**Feasibility.** Each friend can hold at most one copy of an object,
and every place also holds the metadata `m` (about 1% of `S`). With
`D = S − m`, quotas `qⱼ` and overflow room `Oₖ`, the pool holds `S` at
`c` copies only if

    Σⱼ min(D, qⱼ − m) + Σₖ (Oₖ − m) ≥ c × D

Below, `m` is left out where the example has margin to spare, and
counted where a fit is close.

and the protectable size (LEND-7) is the largest `S` that satisfies it.
A friend has no use for more of your disk than its own repository
`Sⱼ` needs, so fair quotas satisfy `qⱼ ≤ Sⱼ`; with friends no bigger
than you, the rule becomes: **you are fully protected while `S ≤ ΣSⱼ /
c`**, your backup at most half your friends' combined at two copies.
Beyond that, quotas cannot fix it fairly; overflow or more friends
can.

Worked examples, `c = 2`:

1. **Four friends, 100 GB each.** Every pair at 80 GB. Each node lends
   and receives 240 GB against the 200 GB its two copies take, so 40 GB
   of the room is tombstone and metadata margin; the protectable size is
   about 120 GB. Disk per node: 100 + 240 = 340 GB.
2. **E with 200 GB among four friends with 100 GB.** E's friendships at
   100 GB each. Counting the ~2 GB of metadata at each friend, the data
   room is 4 × 98 = 392 GB against 2 × 198 = 396 needed: the protectable
   size is about 198 GB, just below E's 200, so about 4 GB of data sits
   at one copy and OPS-2 alerts. E needs a little overflow, or quotas
   of about 102 GB, and more than that for tombstone margin. Each small friend gets 100 GB from E and needs about 140 GB
   more, ~47 GB with each of the other three. E's disk: 200 + 400 = 600
   GB; each small friend's: 100 + 240 = 340 GB.
3. **E with 380 GB among the same four** (scenario 12). Fair quotas
   cap each friendship at 100 GB: with `m` ≈ 4 GB, 4 × 96 = 384 GB of
   data room, so `D` ≤ 192 and the protectable size is just under 200
   GB. An overflow target at `max_bytes` 450 GB adds 446: 384 + 446 =
   830 ≥ 2 × 376, so every data object has one copy at a friend and the
   other at a friend or the target. At about $6 per TB-month that is
   under $3 a month. A fifth friend with 400 GB of their own, at a 400
   GB quota, empties the target: 384 + min(376, 396) = 760 ≥ 752.

**Rule of thumb for users (LEND-7):** lend `c` times what you back up,
plus 20% (2.4× at two copies), and your disk holds about 3.4× your
backup. You are fully protected while your backup is at most your
friends' backups combined divided by `c`; past that, `status` says so
and an overflow bucket covers the rest.

Erasure coding would reduce the multiplier to roughly 1.5–2×; D6
rejects it because restores would then depend on several friends
being online at once, and no replica would hold anything the backup
tool can read.

## 5. Gates and resolved questions

Release gates on the product (field connectivity, soak, small hardware)
are G-1 to G-3 in REQUIREMENTS.md §6. The gates here are on the current
design and disappear with it.

- **Q-A (resolved)** `copies` defaults to 2, with a nudge when set to 1
  (EP-6). Until v0.10 it was `min_replicas`, defaulting to 1.
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
- **Q-F (resolved, measured by G-3)** versitygw is embedded again
  (D15), via `embedgw.RunVersityGW(ctx, backend.Backend, *Config)` at
  v1.10.0. If G-3's listing measurement fails on `posix`, the decorator
  serves listings from lendisk's own index (Q-X).
- **Q-G (resolved)** No lendisk proxy and no SigV4 code in lendisk:
  versitygw terminates every signature, and the tunnel bridge passes
  requests to it with the signed headers untouched (§3.7).
- **Q-H (open, again since v0.9)** The decorator must be checked against
  the full `backend.Backend` interface at the pinned version: every
  method that can create, replace or remove an object (including
  `CopyObject`, `UploadPartCopy`, `PutObjectTagging`, `RestoreObject`
  if present) is either captured or disabled. A method that mutates
  objects without being captured is a replication hole. Re-run on every
  versitygw upgrade, together with SEC-9's table.
- **Q-I (gate, open)** The embedded design depends on five properties of
  versitygw at the pinned version:
  1. *Resolved:* `embedgw` takes listener address strings, not a
     `net.Listener`. The tunnel side is bridged through an internal
     unix socket (§3.7).
  2. *Half resolved:* `Config.IAMService` takes an external
     `auth.IAMService` and overrides every built-in IAM option
     (confirmed). Open is whether the authenticated account reaches the
     backend call, so the decorator can map bucket names per borrower
     and count quota. Fallback: a vendored patch that puts the account
     in the request context, carried as a Go module `replace`.
  3. *No longer needed:* whether versitygw verifies
     `x-amz-checksum-sha256` on upload. REPL-10's hash travels as
     metadata and the decorator compares it itself (LEND-4).
  4. The `posix` versioning directory produces delete markers on
     `DeleteObject` and lists non-current versions with the semantics
     RET-1 to RET-3 need. Fallback: the tombstone path of §3.6, which
     exists anyway for stores without versioning.
  5. A backend can return a custom error (`QuotaExceeded`, HTTP 507)
     and a conditional-put refusal (`PreconditionFailed`). Fallback:
     `AccessDenied` with a distinguishing message the borrower parses.
- **Q-J, Q-K, Q-N, Q-O, Q-P (superseded)** Questions about Garage as the
  storage engine (upload checksum verification, block-error reporting,
  listener set, `CopyObject` cost, byte-total freshness). They went with
  D14; Garage is now a tier-2 store behind `backend/s3` and an external
  replica target, covered by the capability probe.
- **Q-L (gate, open, measured with G-3)** End-to-end write throughput
  through versitygw and the decorator on `posix` on NVMe (EP-13, 200
  MB/s) and on a Raspberry Pi 4, including chunk-signature
  verification, and the replication rate from the store to a friend
  through the replica client and the tunnel on the Pi. Also the LAN
  bridge (EP-17) on the Pi over gigabit, with TLS on and off, to see
  whether the extra proxy hop or the TLS cost, not the link, is the
  limit.
- **Q-M (gate, open, measured with G-3)** Idle and active RSS of lendisk
  with a 1 TiB, 500k-object store and 10 peers, against OPS-7's 256 MiB
  idle, and with four transfers in flight.
- **Q-Q (gate, open)** Which tailcat to pin. The admission API §3.9
  relies on (`Server.AllowClient`, `KeySet`, `Server.PeerKey`,
  `Server.DisconnectClient`) is on `main` as of 2026-10-07 but in no
  tag: v0.7.0 (2026-09-18), the latest, has only `AllowedClients`,
  which admits everyone while empty, an add method with no remove, and
  no per-connection peer key. Pin a `main` commit until a tag carries
  this API, then move to the tag. Every bump re-runs the SEC-10 to
  SEC-13 tests, since admission semantics have already changed once.
- **Q-R (open)** Does tailcat's engine run Tailscale's port mapper
  (UPnP, NAT-PMP, PCP)? D5 and NET-2 assume so, but tailcat's
  documentation mentions only STUN and hole punching. It matters for
  NET-3 and for G-1's direct-path rate. Checked by reading the pinned
  source and with a UPnP router in the G-1 field test.
- **Q-S (open, measured with G-3)** Each tailcat `Client` creates its
  own userspace WireGuard engine, with its own UDP socket, DERP
  connection and netstack, so a node with ten lenders runs eleven
  engines with the server. Measure their RSS with Q-M against OPS-7.
  Fallback: dial lenders on demand and close a `Client` after an idle
  period, at the cost of a fresh hole punch per replication burst.
- **Q-T (resolved)** REQUIREMENTS.md v0.9 made the two changes this
  design needed: EP-16 admits a store behind an S3 service the user
  names, with EP-12, EP-15, LEND-6, COMPAT-4 and scenarios 1 and 7
  extended to match, and SEC-3 admits that store's credential. It also
  reworded REPL-10 so the hash may be taken at capture (§3.4). The two
  further changes v0.9 of this document needed, §4.11 generalised to
  non-S3 remotes and replication forwards in SEC-8, went with the
  rclone design.
- **Q-U (superseded)** The eight rclone-over-rc checks. They went with
  the v0.9 replication design (D12, alternatives); Q-Y replaces them.
- **Q-V (gate, open)** `backend/s3` pass-through. Multipart and
  checksums are documented; whether versioning, object lock and
  conditional headers pass to the upstream unchanged is not, and
  neither is whether lendisk's buckets can be placed as prefixes inside
  one upstream bucket rather than one upstream bucket each. The
  capability probe runs through the decorator and `backend/s3`, so it
  measures the pair.
- **Q-W (resolved)** One store serves both roles (D15), so one
  `RunVersityGW` with one backend and no mux. The mux survives only as
  Q-Z's fallback.
- **Q-X (gate, open, measured with G-3)** Listing time at 500k objects
  in one bucket on `posix`, and object lock semantics on `posix`
  (retain-until honoured against the lender's own credential). Both
  testable in a day.
- **Q-Y (gate, open)** The replica client (D12, §3.4), against the
  pinned minio-go, before the worker is built on it:
  1. A multipart upload killed with `SIGKILL` mid-part resumes at the
     next part after restart, against a friend (versitygw), RustFS, B2
     and Garage.
  2. Each tier-1 and tier-2 target accepts the request checksum lendisk
     sends, or the client is configured per target to send none.
  3. A friend's `507`, `403` and `412` reach the worker as
     distinguishable errors through the tailcat transport.
  4. The limiter and the timetable hold to within one part.
  5. RSS per transfer in flight (with Q-M) and throughput to a friend
     on a Pi 4 (with Q-L).
  If minio-go fails these in a way configuration cannot fix, the AWS
  SDK for Go v2 behind the same interface (D12).
- **Q-Z (open)** Lent buckets on a second disk under one `posix` root.
  A lender who mounts or links another disk at a `lent-*` bucket
  directory needs versitygw's temp directory and versioning directory
  to stay on the same filesystem as the bucket, or its temp-then-rename
  and version moves cross a mount. Check in the pinned source and with
  a bind mount in CI. Fallback: a second `posix` backend instance for
  `lent-*` buckets with a mux by bucket name, for that configuration
  only.
- **Q-AA (open, tuned with G-1)** Placement and re-placement
  parameters (D6, §3.4): `reroute_after` (1 hour), `replace_after` (7
  days), the hourly re-placement pass, and free fraction as the
  placement key. G-1's households give real offline patterns; the
  question is how much re-placement traffic a friend's normal weekend
  away causes, and whether `replace_after` should scale with how often
  that friend has come back before.
- **Q-AB (resolved)** The repository size exchanged for LEND-7 tells a
  friend one number it could already estimate from what it holds of
  the bucket's metadata. REQUIREMENTS.md v0.10 lists it in SEC-2; it is
  rounded to 1 GB (§3.5).
- **Q-AC (gate, open)** Drain-back deletes at overflow targets (§3.11,
  S3-5). Check per tier-1 provider (B2, Wasabi, Hetzner, R2) whether a
  credential can allow delete markers but not version removal, and
  whether the lifecycle rule then purges the drained copies after the
  window. Where it cannot, the user chooses between a credential that
  can remove versions and drained copies left in place. Check also that
  undelete's server-side copy of an earlier version onto the same key
  (§3.6) works with that credential at each provider.
