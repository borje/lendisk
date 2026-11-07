# lendisk — design

Status: draft v0.16, 2026-11-07. Companion to REQUIREMENTS.md v0.11.2.
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
markers, and the global quota cap cannot be overridden. v0.12 follows
REQUIREMENTS.md v0.11 and takes the design down to what v1 needs: the
decorator is the only retention mechanism, with tombstones moved
aside under `.lendisk/tomb/` and versitygw's versioning and object
lock left off, so the capability profile is gone; `backend/s3` as the
node's own store, and evict mode, are post-v1; quotas are two numbers
people set and the smaller wins; failure domains are gone; lock files
stay local; one lendisk bridge owns every listener in front of one
internal socket; the control API gains `list`; the identity key is a
plain file. v0.13 takes in the first three tickets: the identity's
encodings and the state migration rule (§3.1, OPS-8), and the
versitygw and tailcat spikes (Q-I, Q-Q, Q-R). The operation
allow-list moves from the decorator to a gate in front of versitygw's
routing; versitygw's admin API, which cannot be left out, is made
unreachable; object lock is refused rather than "left off"; the
tunnel region is saved whole in `lendisk.db`; and v1 does without
port mapping. v0.14 takes in the minio-go spike (Q-Y items 1, 2 and
4): minio-go v7.4.1 is pinned, every target is sent unsigned payloads,
and the SHA-256 request checksum is the queue row's hash on a single
PUT and a per-part SHA-256 in an unsigned trailer on multipart (§3.4,
S3-3). v0.15 follows the first capture code (#6): a multipart object's
hash is the SHA-256 of the whole object, read once at completion,
rather than a composite of its parts, so every object has one hash and
replication picks part sizes freely (EP-3, REPL-10); and dependencies
are pinned by `go.sum` rather than vendored (OPS-8). v0.16 gives
each node a dial key per friend for its tunnel clients, beside the
identity key its server keeps, since a relay delivers a key's packets to the last
of its connections to write (§3.1, §3.2, §3.7, §3.9; #69). The trust
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
| **Lending store**: a friend's copy, written by a borrower who may be compromised | LEND-1 to LEND-7, RET-1 to RET-3, VER-1, NET-7, SEC-4 to SEC-16 | Enforcement-critical: quota, immutability, retention the borrower cannot bypass, a deny-by-default operation list, isolation between borrowers. | versitygw with the decorator's lender policy (D12, D15), behind the bridge's tunnel listener (§3.7) |

The two stores need the same S3 surface and differ only in policy, so
one embedded gateway serves both and the decorator applies a different
policy per bucket. Replication needs none of that surface and all of a
transfer client's robustness, so it is a separate component behind its
own interface, even though it runs in the same process. The split also
says where a component does not belong: the replica client lacks what
the stores need (synchronous capture, tombstones, quota), and versitygw
has nothing replication needs beyond what the decorator already
exposes.

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
  credentials arrive as environment variables, not files (SEC-3).
  Post-v1, a user who already runs an S3 store names it as the `s3`
  backend (D15, EP-16).
- Configuration is one TOML file (`BurntSushi/toml`); the CLI is
  `cobra`. Every setting has a flag and an env var (OPS-6). One
  `[store]` section serves both roles (§3.8). The file holds what the
  user sets; friendships, buckets and overflow targets are rows in
  `lendisk.db`, never written into the file.
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

v1 has one storage mode: the local copy is complete (EP-9). The read
path that serves a missing object from a replica (EP-10) exists for
recovery and bit-rot repair; the post-v1 `evict` mode would reuse it
(§3.3).

### D4. Endpoint: S3 served in-process by embedded versitygw

*Serves:* EP-1 to EP-3, EP-5, EP-10, EP-11, COMPAT-1, P-5.

The backup tool talks S3 to the lendisk daemon on `127.0.0.1:9000`. The
S3 protocol is served by **versitygw embedded in the daemon** (D15): it
terminates every SigV4 signature, including the `aws-chunked`
streaming form restic and Kopia send over plain HTTP, routes every
request, and implements multipart and listings; its versioning is off and
object lock is refused at the gate (§3.6). In front of its routing sits
lendisk's **gate**, a fiber middleware passed in through
`Config.S3Options`, which refuses by S3 action before versitygw routes
or authenticates (NET-7, SEC-8). Under it sits a lendisk-owned **decorator** of versitygw's
`backend.Backend` interface, which captures every write and delete
into the replication queue and enforces the lender's policy. It is the
only tool-facing endpoint: there is one local copy and one capture
point.

The same gateway, reached through lendisk's bridge from every
listener (§3.7), is what a friend's node talks to (D12). Everything the product
adds to S3 (capture, read-through, quota, immutability, retention)
lives in the decorator, and the deny-by-default allow-list in the gate,
both behind the bridge's per-request peer checks (SEC-11, SEC-12); versitygw parses
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
  copy and the lender's replica alike. Post-v1, behind the `s3`
  backend, any S3 client with the store's credential reads the same
  layout.
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
traversal, DERP relays) with no control plane, no account and no admin
panel. Tailscale's UPnP/NAT-PMP/PCP port mapper is not linked by
tailcat, and v1 of lendisk does not link it either: many NAS owners
keep UPnP off, so v1 cannot rely on it anyway (Q-R; post-v1, #46). Each node runs a tailcat
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
answer) and a last-reachable time.

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
     (`replication.reroute_after`) before the others, then the next,
     until `copies` are chosen. An unreachable friend with room is
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
  hash from `x-amz-meta-lendisk-sha256` (§3.10), into done queue rows
  and `acked` placement rows for the version of each key most friends
  list, a tie being a conflict the user resolves (§3.1 ID-5).
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
- **Failure domains** (v0.11 and earlier): a label per friend and a
  tiebreaker so that two friends in one house would not both hold a
  copy. Dropped from v1 with REQUIREMENTS.md v0.11 because nobody had
  asked for it; it returns as a tiebreaker in step 2 when someone
  does.

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
| S3 front end and store | **versitygw** via `embedgw`, pinned by `go.sum` | Apache-2.0 | D15. SigV4 incl. `aws-chunked`, routing, multipart, listings; `backend/posix` (`backend/s3` post-v1). Versioning off; object lock refused at the gate; admin API mounted but unreachable (SEC-8). |
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
tombstones and the purger; D4, D15, §3.3), the gate with the operation allow-list (D4, NET-7, SEC-8; §3.3,
§3.5, §3.6), the `IAMService` (§3.3, §3.7), the bridge and the peer
control API (§3.7, §3.9), the replication queue and its ordering guarantee (REPL-1) with
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
kind in three methods only: `Verify` is the challenge (VER-1), and
`Undelete` and `List` go over the control API (§3.6, §3.7), since a
friend's store has no versioning and its index knows more than a
listing does. Everything that moves bytes is the same code.

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
  Its low-level multipart calls are what the resume loop needs. By
  default it sends no request checksum at all (spike #4, Q-Y.2).

What lendisk writes because the client does not: the resume loop,
which keeps the multipart upload ID and the ETag of each completed part
on the queue row so a restart continues at the next part (REPL-6); a
token-bucket limiter (`golang.org/x/time/rate`) around each part and a
time-of-day timetable checked before each part (REPL-7); and the FIFO
queue, which was lendisk's in every version.

What stays outside S3: the **peer control API**, seven lendisk
endpoints served under `/_lendisk/` on the same tunnel port as S3 and
authenticated by the tunnel peer key, checked against `lendisk.db` on
every request (SEC-11): `redeem` (PEER-2, the invite handshake and the
only endpoint an invite key may call, SEC-13), `challenge` (VER-1, S3
has no HMAC challenge), `manifest` (ID-4, which must be fetchable
before any S3 credential exists), `quota` (LEND-3, LEND-8), `version`
(NET-6), `undelete` (RET-3, the lender's tombstones are not S3
versions) and `list` (the lender's index of a borrower's bucket: key,
size, hash, tombstoned-at; a listing carries no hash and would need a
request per object, §3.7).

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
keeps retention with its own tombstones, the same way on every store
(§3.6). Overflow stays second
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
  (`x-amz-meta-lendisk-sha256`) and, on a single PUT, as the request
  checksum. S3 has no whole-object SHA-256 for multipart, so a part
  carries its own SHA-256, taken as it streams (§3.4, Q-Y.2). The
  lender's decorator hashes the stream anyway and compares with the
  metadata (LEND-4).
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

- **`backend/posix`, the only v1 store.** Objects are plain files at
  `<dir>/<bucket>/<key>`, written temp-then-rename, with ETags and
  metadata in xattrs. Versioning is off on every bucket (no
  `VersioningDir`) and object lock is refused at the gate (§3.6).
  lendisk sets `PosixOpts.AbsolutePaths`; without it `posix.New`
  changes the whole process's working directory to the store root.
  Temporary files live in `<bucket>/.sgwtmp/` (Q-Z). One process, no network, and P-5 at its v0.7
  strength: `restic -r <dir>/<bucket>` works with nothing running.
- **`backend/s3`, post-v1 (EP-16).** The same gateway becomes a proxy
  that re-signs each call with the store's own credential and forwards
  it to an S3-compatible endpoint the user names: a Garage,
  MinIO-compatible store or versitygw on the LAN, or a provider (B2,
  R2, Wasabi, Hetzner). lendisk would still write no signature code.
  Deferred with REQUIREMENTS.md v0.11; what this document says about
  it is kept for that version and marked post-v1.

**One store serves both roles.** The node's own buckets and every
borrower's `lent-*` bucket go into the one store: sibling directories
on a disk. This is the point of the product for the user who runs
storage: a friend's data lands where their own does. It also means one
backend instance, no mux, and Q-W closed. A lender who wants lent
buckets on a second disk mounts it at the bucket directory (Q-Z).

**Store features lendisk does not use.** versitygw's versioning,
object lock and conditional puts are left off. Its verification of a
request checksum cannot be turned off and is not relied on: it refuses
a part whose SHA-256 is wrong (Q-Y.2), which only catches damage in
transit earlier. The decorator keeps retention (§3.6), immutability (§3.5)
and the hash check (LEND-4) itself, in one way on every store, so
nothing the lender promises depends on what a store provides and
nothing needs a probe. Object lock in particular would only guard
against the lender's own daemon, which SEC-7 already trusts. Until
v0.12 a capability profile probed six features per store and labelled
the weaker mode in `status`; with the posix store alone and the
decorator doing the work, the profile is gone. One probe would remain
for the post-v1 `s3` store: whether `CopyObject` is cheap, which
decides whether tombstone moves and exports copy server-side or
through the client.

**Support tiers for stores:** tier 1, the only tier in v1, is
`backend/posix` on a local directory (every requirement and scenario,
full CI including G-3 on a Pi 4). Post-v1: tier 2 is `backend/s3`
against Garage, a MinIO-compatible store (RustFS) and one provider
(B2), with the full suite per store; tier 3 is anything else that
passes the copy probe, best effort.

The trust model (P-2) is untouched: every listener is lendisk's
bridge (§3.7), the tool-facing ones bind loopback or an address the
user named, the lender-facing one is reachable only through the
tailcat tunnel, and everything that crosses between households is S3
plus the peer control API.

Known costs and their handling:

- *`embedgw` and `backend.Backend` churn.* The embeddable package was
  extracted in versitygw v1.5.0 (June 2026), external IAM arrived in
  v1.8.0 (September) and v1.10.0 shipped on 22 October: five minor
  releases in four months, with no stability statement. The version is
  pinned by `go.sum`, the decorator is the only file that touches the
  interface, the SEC-9 table and the replication-hole review (Q-H) are
  re-run on every bump, and CI runs restic and Kopia end to end. This is
  the risk that made the v0.8 sidecar attractive; the price of avoiding
  it was the signature code.
- *No `net.Listener`.* `embedgw`'s listener fields are address strings
  (TCP, file-backed or abstract unix sockets). versitygw is given one
  internal unix socket in a 0700 directory and nothing else; every TCP
  listener is lendisk's bridge in front of it (§3.7).
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
  two processes were always needed. Garage remains an overflow target
  (and, post-v1, a tier-2 store); it has no versioning, so client-side
  tombstones apply to it as an overflow target.
- **versitygw as a supervised child** over a unix socket, with
  lendisk's proxy in front (the sidecar variant considered on
  2026-10-29). Removes the `backend.Backend` coupling at the cost of the
  signature code and an HTTP hop between capture and storage.
- **RustFS** (Apache-2.0, 1.0 GA 2026-09-16): versioning, lifecycle and
  IAM, but its documentation recommends XFS and says to avoid ext4,
  btrfs and ZFS, needs 2 GB of memory to test, and ships a web console.
  A post-v1 tier-2 store behind `backend/s3`, not the engine.
- **MinIO**: AGPL, heavier, community edition archived in 2026.
- **SeaweedFS**: a filer plus volume servers for a single-node job.
- **trove** (statewright/trove, MIT, reviewed 2026-11-03): a small Go S3
  server with PostgreSQL metadata and SHA-256 content-addressed blobs.
  Requires PostgreSQL; its packages are `internal/`, so it cannot be
  embedded; no `aws-chunked` decoding or payload-hash check; no
  versioning, object lock or conditional writes; S3 keys exist only in
  the database, so the store is not plain files (P-5); a single commit
  from one author.
- **Any of these as a cluster spanning friends**: rejected for the
  pairwise trust of P-2 (REQUIREMENTS.md); a cluster is one trust
  domain.

## 3. Realisation, by requirement area

Section numbers mirror REQUIREMENTS.md §4. Each entry says how the
requirement is met with the decisions above. Nothing here adds to or
narrows a requirement; if a line seems to, the requirement is wrong or
this line is.

Paths used below: `<config>` is the config directory (mode 0700),
`<data>` the data directory. The store is `<data>/store/` (or a
directory the user names, such as a USB disk), holding the node's own
buckets and the `lent-*` buckets side by side, each lent bucket with a
`.lendisk/tomb/` directory for its tombstones (§3.6), and
`.lendisk/manifests/` for the friends' node manifests (§3.1 ID-4); `lendisk.db`
(SQLite) is in `<data>`; `<data>/run/` (mode 0700) holds the internal
gateway socket `gw.sock`; `<config>/locks/` (mode 0700) holds the
`peer add` redeem locks (§3.2, #64). `init` creates a missing `<config>` or
`<data>` at 0700. An existing one with no group or other permission
bits, such as the installer's `install -d -m 0700` in `examples/`, is
kept private (its owner bits set to 0700); one that group or others can reach is set to 0700 only if it holds
nothing but lendisk's own entries (`lendisk.toml`, `identity.key`,
`endpoint.crt`, `endpoint.key`, `profiles`, `locks`, `store`, `run`,
`lendisk.db*`), as a fresh bind mount or the example config does, and
otherwise `init` refuses, naming the directory and a foreign entry:
it is used for other things, such as `$HOME` with `./lendisk.toml`,
and lendisk does not change its permissions (`cli.ensureDir`, #53).
The store directory gets the same treatment in `store.Prepare`
(EP-12), stricter: generic buckets hold plaintext, so an existing
store others can reach is set to 0700 only when it is empty, and
otherwise refused with a `chmod 700` hint. Bucket naming: the node's own buckets keep their names. A borrower
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
name `lendisk-manifests`, which stays reserved though manifests are kept
under `.lendisk/` (§3.1 ID-4). The borrower's `lendisk.db` holds the
bucket names and recomputes each sealed name (`identity.SealedName`);
its encrypted manifest (ID-4) lists each bucket with its sealed name,
for `recover` and the recovery sheet (ID-7); the
lender's mapping table holds only sealed names and is authoritative on
its side; the stored name is a stable label for reading the store with
no lendisk running (P-5). On the wire the borrower names its bucket
by the sealed name alone, in path-style S3 requests
(`/s<26>/<key>`), signed with the pair's derived credential (§3.7
NET-7a); it never sends `lent-` or its `id16`. The lender maps the
name in the decorator's outer layer, `gateway.Lending`, from the
account versitygw authenticated (Q-I.2): for an access key that is a
node ID, a name not of the form `s<26>` is `NoSuchBucket` (and
`InvalidBucketName` on `CreateBucket`), a sealed name with no row for
that borrower is `NoSuchBucket`, and otherwise the backend is called
with `lent-<id16>-<sealed>`; names in answers (`ListObjectsV2`'s
`Name`, the multipart results' `Bucket`) are mapped back. The mapping
table is `lent_bucket` (migration 6: borrower node ID, sealed name,
created at). Code: `identity.SealedName`, `identity.IsSealedName`,
`gateway.LentName`, `state.DB.LentBuckets`. Post-v1, behind `backend/s3`, the same names
would be upstream bucket names; placing them as prefixes inside one
upstream bucket is Q-V.

### 3.1 Identity and recovery

- **ID-1** The identity is an X25519 key: the WireGuard key tailcat
  uses (D5). Node ID, the tunnel server's key and S3 access key
  (NET-7a) are literally the same key; there is no Ed25519 and no
  conversion. The node's tunnel client for each friend uses an X25519
  key of its own, the dial key for that friend
  (`Identity.DialKeyFor`): HKDF-SHA256 over the private key with the
  info `"lendisk-dial-v1"` followed by the friend's 32 key bytes,
  clamped (§3.7 says why). It is derived from the private key rather
  than the BIP-39 seed because the daemon holds only the key file
  (ID-3), as with the other derivations below; the phrase and the
  friend's ID recover it. It is not an identity: the friend learns it
  at redeem (PEER-2) and maps it to the node ID (SEC-10, SEC-12), and
  the address, the S3 credential and every control API answer stay
  keyed by the node ID.
  Nothing in the design needs a signature today: invites carry a
  symmetric secret (PEER-1), the challenge is an HMAC (VER-1), the
  manifest is AEAD-encrypted under a derived key (ID-4), and the
  per-pair S3 secret is a key agreement (NET-7a). If a feature ever
  needs signatures, it gets a separate signing key derived from the
  same seed, not a change to the identity. The display form is
  lowercase unpadded base32 of the 32 key bytes followed by a 3-byte
  checksum, the first three bytes of
  SHA-256(`"lendisk-node-id-v1"` ‖ key): 56 characters. Base32 works in
  5-byte groups, so the first 16 characters depend on the key alone
  and are the `id16` of lent bucket names. Parsing ignores case,
  spaces and dashes and rejects a checksum mismatch.
- **ID-2** The 24 words are BIP-39 (`go-bip39`); the private key is
  HKDF-SHA256 of the BIP-39 seed (empty passphrase, no salt) with the
  info string `"lendisk-identity-v1"`, clamped as X25519 requires.
  `init` shows the phrase once and asks for three of its words back
  before writing anything; `init --restore` reads a phrase from stdin
  instead. Two nodes from the same phrase derive the same key
  (the AC).
- **ID-3** The private key is `<config>/identity.key`, base64 of the
  32 bytes, at mode 0600; lendisk refuses to load it if group or others
  can read it. The phrase is never written.
- **ID-4** Package `manifest` (#28). *Contents*, JSON, `format` 1:
  the node ID, a version `seq` and `updated_at`, and the content:
  `settings` (the node's own `tc…` address and the saved relay region
  it embeds, so a rebuilt node keeps its address, SEC-14; the config
  file is the user's and is not in it), `friends` (node ID, `tc…`
  address as the addressing hint, both numbers, both retentions, since
  when, and `dial_key`, the key the friend dials this node with, #69,
  so a rebuilt node admits the friend again; added in #29 within
  format 1, since readers ignore a field they do not know and an
  absent one reads as none), `overflow` (name, endpoint, bucket, prefix and credential,
  S3-1; empty until #38 fills it) and `buckets` (name, sealed name,
  `copies`, profile, created at). Placement (REPL-12) and the buckets'
  own S3 credentials (SEC-3) are not in it. *Envelope*: `ldm1` ‖
  24-byte nonce ‖ XChaCha20-Poly1305 (`golang.org/x/crypto`) of the
  JSON padded with spaces to a multiple of 4 KiB, with `ldm1` ‖ the
  node's 32 key bytes as additional data,
  under `identity.ManifestKey` =
  `HKDF-SHA256(private key, info="lendisk-manifest-v1")`. The fourth
  byte versions the envelope and `format` the plaintext; `Open`
  refuses a version it does not know, strips the padding, and checks
  the node ID inside. The padding keeps the size from telling a lender
  how many friends and buckets the node has, beyond which 4 KiB step
  their entries reach. *Version*: the daemon's `manifest.Pusher` builds the content from
  `lendisk.db` at start, every 15 s and when `redeem` commits a
  friendship, and hashes it (SHA-256 of the JSON); a new hash counts
  `seq` up (`node` keys `manifest_seq`, `manifest_hash`,
  `manifest_updated_at`), so a change made by another process
  (`bucket add`, `peer quota`) is pushed within 15 s. `seq` never goes
  back: a recovered node continues from the version it fetched
  (`state.ContinueManifest`, ID-5), so its next manifest is above every
  copy its friends hold. *Push*: a friend is due the sealed manifest
  when it has not acknowledged the current `seq` (differs, not below:
  any other copy is stale), or last acknowledged it 24 h ago or more,
  since it may have lost its store and only a push puts the manifest
  back. It gets it by `PUT /_lendisk/manifest`, 30 s at most; the
  lender answers with the size and SHA-256 it stored, which the pusher
  checks. Each friend's push runs on its own and the round does not
  wait for it, nor start a second one to a friend still in flight, so a
  hung friend holds up no other. After failed pushes in a row a friend
  is due again only 1 min after the last, doubling per failure to at
  most 1 h. The table `manifest_push`
  (migration 14) keeps per friend the `seq` acknowledged, when, the
  last attempt, its failure and the failures in a row, so a friend
  that missed a push (it was offline, or this node was) gets it at the
  first round after its backoff in which it answers. Committing a
  friendship, new or again (the inviter's side of a redeem by a friend
  it kept, #51/#56), drops that friend's row, so it is pushed to at
  once: a friend that redeems again has usually lost its disk. The
  inviter's daemon is kicked by its control API's redeem; the invited
  side commits in `peer add`, another process, so its daemon pushes at
  its next poll. `status` shows each friend not known to hold the
  current version (`manifest.pending`): never acknowledged, or the last
  push, the daily one included, failed. Both profiles push: a lender-only node
  is rebuilt from its manifest too. *Lender*: the call is open to the
  `peer` role only (SEC-11, SEC-13) and keyed by the resolved caller
  (`Caller.Peer`, the friend's node ID, never the dial key it connected with),
  so a friend stores and reads its own manifest and no other; no
  parameter names whose. The body is at most 1 MiB
  (`control.MaxManifest`) and must have the envelope's shape (`ldm`,
  any version, at least header, nonce and tag), so a friend can make
  the lender keep one small file and nothing else; it is not counted
  against the quota. `manifest.Store` writes it to
  `<store>/.lendisk/manifests/<node ID>`, 0600 in 0700 directories,
  through a synced temporary file (`.put-*`) renamed over the old one;
  `serve` sweeps temporary files an hour old at start, left by a crash.
  A former friend's manifest stays until ending the friendship removes
  it with its replica (#25). It is a
  file, not an object in a `lendisk-manifests` bucket as planned: a
  name beginning with a dot is no valid bucket name, so versitygw
  serves it to no account at all (no borrower, no bucket key, not the
  gateway's root) and lists it in no `ListBuckets`, with no reliance on
  ACLs. Being in the store, it survives the loss of `lendisk.db` and
  goes with a lender's disk handed over (P-5). The lender sees that a
  manifest exists, its size and when it changes, nothing else.
  *Lender's database loss*: the manifest files stay, but the
  friendships go with `lendisk.db`, so the lender admits the borrower
  again only once a friendship is back (SEC-10 admits no identity
  without one): by the lender's own `recover` (its manifest names its
  friends), or by a new invite, which a borrower that kept its row
  answers with the stored terms (§3.2 PEER-2). The relay region saved
  in `lendisk.db` goes too; the lender picks one again, the same
  nearest region in practice, and `tunnel.region` pins it.
- **ID-5** A lender's address is its tailcat `tc…` string, which carries
  the public key and relay bootstrap info and no pre-shared key
  (SEC-14). `lendisk recover <address>` (#29, package `recovery`)
  runs on a fresh machine only (it refuses a config directory holding
  an identity), and first asks the user to confirm that the old node
  is stopped or gone (below); then it reads the phrase from stdin and
  derives the key from it, dials the lender, and fetches the manifest
  over the control API (`GET /_lendisk/manifest`, `manifest.Fetch`: no
  bucket, no S3 credential and no `lendisk.db` needed). A lender can
  serve an older copy, since the envelope binds node and format, not
  freshness, so `recovery.Newest` then asks every friend any fetched
  manifest names, the newest one's included, and keeps the highest
  `seq`; each lender's answer is printed. `recovery.Restore` passes
  that `seq` to `state.ContinueManifest` before the daemon first runs,
  so the version never goes back and later changes count on from it;
  restores each friendship with its terms and the friend's dial key
  from the manifest, so the tunnel admits each friend again under the
  key it always dialled this node with; and saves the relay region the
  manifest carries (`transport.RestoreRegion`), so the address is
  unchanged. *Friends' dial keys*: the manifest carries them (ID-4)
  rather than each friend being asked at recovery, because a friend
  offline then would stay locked out, and the manifest's copy is the
  one redeem proved (DialProof), where a friend's answer later would be
  its word alone. A friend missing from a manifest's dial keys (one
  written before #29; no release has one) is restored without one:
  `recover` names it, and only a new invite lets it reach this node.
  Buckets are restored with new S3 credentials of their own (SEC-3:
  those are not in the manifest), printed by `bucket show`, and created
  empty in the store; nothing is downloaded (ID-6 reads on demand,
  #30). The identity key is saved after everything else recover writes
  but `initialised_at` and the initial config file, and `recover`
  refuses a directory holding an identity key: so a `recover` that
  stopped before saving the key runs again on the same directories and
  keeps what it wrote, and one that stopped after saving it has
  restored everything, and the daemon's lister reads what it did not.
  The per-pair S3 credentials (§3.7) for every lender are derived when
  used and never written (NET-7a), and the node re-derives its dial
  key for each friend the manifest lists (§3.1 ID-1), which the friend
  still admits.
  *How recovery dials*: with `transport.NewFriendDialer(identity)`, a
  dial-only transport that dials each friend with
  `DialKeyFor(friend)`, the key the daemon dials it with (#69). The
  fresh machine has no friendships yet, but it needs none: the dial key
  depends on the phrase and the lender's node ID alone, and the
  lender's ID is in its `tc…` address. The lender's tunnel resolves the
  dial key to the friend (`state.TunnelPeer`), so the manifest call,
  keyed by `Caller.Peer`, answers with the friend's own manifest. The
  lender admits the dial key wherever its friendship row holds it:
  every friendship made since #69, and one made again after the lender
  lost `lendisk.db` (the redeem exchanges dial keys again, as the end
  to end test checks). Dialling with the identity key was the other
  choice: the lender admits it too, but it is the key the recovered
  node's own tunnel server holds at its relay, and one key held by two
  of a node's engines at one relay lets one take the other's packets
  (§3.7, the reason for #69); the dial key is held by nothing else
  once the old machine is gone. The one friendship it misses is one
  made before #69 whose row has no dial key, which no release has; such
  a friend admits the identity key, through `transport.NewDialer`.
  A separate fetch key, learnt by the lender, was rejected earlier: the
  lender would have to keep it, and recovery dials each friend anyway.
  `recover` (#29) is for a node whose machine is gone or stopped: a
  still running old node dials its friends with the same dial keys, and
  two clients of one key at one relay stall each other. It says so and
  refuses to go on until the user answers `y` to "Is the old node
  stopped, or its machine gone?" (stdin, before the phrase); nothing
  can tell from outside whether the old node runs, since it admits
  only its friends. It uses one friend dialler for every call of the
  recovery, and closes it before writing the identity key, so before
  the daemon can start. Two nodes running
  with one identity are wrong in any case (both would replicate and
  push manifests). Overflow target
  credentials found in the manifest are shown once, for the user to
  put in the environment (SEC-3); they are not written to disk
  (`printOverflow`; the list is empty until #38). The
  `placement` table (D6) and the hash index are then rebuilt from the
  control API `list` of each reachable place for each sealed bucket
  (§3.7): one paged call per bucket and place, not a request per
  object (`recovery.Lister`).
  *A friend's list is not trusted.* The control client refuses a page
  whose keys do not rise strictly past the key it started after, or
  whose `next` is not past them; the lister rejects, logs and counts
  each entry with a key a borrower could not name (SEC-5's check,
  `gateway.CheckKey`), a size below 0 or above 5 TiB, a hash that is
  not 64 lower-case hex digits, or, for a key the bucket's profile
  names by its hash (`hash_named`), a hash that is not the name:
  that friend's entry for the key counts as not held, whatever order
  the friends are listed in. A list is capped per bucket and place at
  10 million entries (`relist.seen`) and, apart from that, at 21,000
  pages (`relist.pages`, migration 17): an honest lender fills its
  pages (1,000 keys), so 10 million keys take 10,000 pages, and twice
  that plus 1,000 leaves room for short pages while a friend serving
  empty pages without end is stopped within hours. A list past either
  cap is abandoned (`relist.abandoned`): it is no longer read, what it
  recorded is dropped, and a key only it listed is forgotten rather
  than left a conflict nobody could resolve. Only the lender's
  `NoSuchBucket` code means it holds nothing of the bucket; any other
  refusal, a 404 from a node without the call included, is a failure
  tried again.
  *Which version counts* (user decision of 2026-11-07, #29 review).
  Each page is recorded in one transaction (`state.RecordListed`):
  every place's size and hash for each key goes in the table `listed`
  (migration 16; local-only keys skipped, tombstoned keys only
  counted), and the key's verdict (table `recovered_key`) is made
  again from all places' entries: the hash most places list counts
  (`held`); a tie between hashes is a `conflict`; a key one place
  lists is a `single` source and counts, except a metadata or snapshot
  key not named by its hash that another place lacks although it
  listed the bucket to its end, which is a conflict too, since REPL-12
  puts metadata at every friend (a hash-named key is that object
  whoever holds it). Only lists read normally to their end count for
  that: a pending or abandoned friend's silence is no evidence, nor
  that of a friend added since recovery, which has no list at all. For a
  key not named by its hash, a colluding majority of friends therefore
  wins: that is the rule decided, and #70 covers restic's `config` and
  `keys/*`. The
  verdict is made again as each further list arrives (a friend
  returning breaks a tie 2 to 1), and whenever a list ends. Only the
  version that counts has a queue row (`put`, `done`, with the listed
  size, hash and the class the bucket's profile gives) and `acked`
  placement rows, at exactly the places that list that hash, sized as
  each place counts it, so `place_use` holds what each friend holds; a
  conflict has neither, so no place counts as holding it, and
  `state.KeyConflict` is what the read-through (#30) checks to refuse
  it with the reason. A key this node has written since recovery is
  its own: lists only add the places holding that version. `status`
  shows the conflicts, with each friend's hash and the command that
  picks it (`recovery.conflicts`), and, for buckets outside the
  encryption guarantee only (EP-8: a restic or Kopia repository's MAC
  catches a forged object), the count of single-source objects as
  unverified (`recovery.unverified`). `lendisk bucket resolve <bucket>
  <key> <friend>` picks that friend's version (`state.ResolveKey`):
  it counts from then on, at every friend holding it. It refuses a key
  that is not in conflict ("not a conflict; nothing to resolve"),
  including one this node has written since recovery, which is no
  conflict any more and is not shown as one. Restic's
  `config` and `keys/*` are not named by their hash, so one friend's
  stale but genuine copy is accepted; verifying them against hashes in
  the manifest is #70 (post-v1).
  The lists still to read are the table
  `relist` (migration 15: place, bucket, the key to resume after, the
  last failure and failures in a row; migration 16 adds `seen`,
  migration 17 `pages` and `abandoned`, migration 18 `done` and
  `settled`),
  filled by `Restore` for every
  friend and bucket; the last page of a list marks its row `done` in
  its transaction, a page before it moves the resume key. A done row
  is kept: "listed this bucket to its end" means exactly a place with
  a done, not abandoned, row for that bucket that is still a friend,
  so a friend added after recovery (no row) is no evidence. A page's
  keys are decided in its own transaction: a page holds at most 1,000
  keys (`gateway.MaxListPage`), and a longer one fails the list, in the
  control client and in the lister alike. What an ended list affects
  beyond its pages (an abandoned list's keys, a finished list's lone
  metadata keys) is decided afterwards by `state.SettleFinished`, which
  the lister runs at the start and the end of every round, in write
  transactions of 2,000 keys with a 100 ms pause between them, longer
  than SQLite's busy handler polls, so the gateway's captures wait a
  fraction of a second, never the whole bucket (200,000 keys abandon in
  about 25 s with no other write waiting over 0.2 s). `settled` marks a
  list's settling finished. Settling is bounded by the round's budget
  like the listing: what it does not finish (or a crash, a shutdown or
  a busy database interrupts) is left, and the daemon's lister counts
  every ended list not yet settled as pending (`state.Unsettled`), so
  it goes on making rounds until settling finishes, though no list is
  left to read. A list read to its end is never reported failed for
  its settling; `RelistFailed` touches no ended row. Places are listed
  side by side (4 at a time), each place's buckets one after another,
  so a slow friend holds up no other, and each round starts 4 places
  further on, so slow friends that use every round's budget cannot
  starve those after them. A database too busy to mark a list failed
  or abandoned is logged and not fatal: the list stays pending, and is
  read (and, past its cap, abandoned) again later. A place that fails is
  skipped for the rest of the round, and `recover` says it is not
  listed. `recover`'s round has a time budget (3 min, injectable):
  what is not finished by then, and every list of a friend named with
  `recover --skip <node ID>`, stays pending, untried, with what it
  recorded; `recover` says so, saves the identity and finishes, and
  the daemon's lister reads the rest. A `--skip` naming no friend in
  the manifest is warned of. The daemon's rounds have a
  budget of 10 min, so a slow friend delays other friends' retries by
  that at most. When a friend did not serve its manifest,
  `recover` warns that it may hold a newer version and names it. A
  floor at the lender (refusing a manifest push below the stored
  `seq`) is not possible as things stand: the lender cannot read `seq`
  inside the sealed envelope, and putting it outside would be an
  envelope change.
  *Known gaps, follow-ups to file:* a list read while a write of the
  same key is still pending here (captured, not yet done) races the
  write; a friend can list objects this node deleted before it was
  lost, unless the friend tombstoned them (#17: its list then carries
  `tombstoned_at` and recovery counts them as deleted); and a friend that never returns keeps its lists
  pending for ever (backing off to an hour), with no point at which
  recovery gives up on it. A place that is offline is listed when it
  returns: the daemon runs the same lister (every minute, a failed
  list due again after 1 min doubling to 1 h), and `status` shows the
  lists still to read (`recovery.pending`). Until then its objects
  count as held wherever else a list found them, and are below
  `copies` where no other place holds enough of them.
  *Limit: a lender that lost its own `lendisk.db`* keeps the manifest
  file but admits no one without a friendship (SEC-10, ID-4 above), so
  it serves neither the manifest nor a list to a recovering node: that
  friend must be befriended again with a new invite, once the node is
  recovered from another friend or with `init --restore`. `recover`
  says so when no friend serves the manifest; the recovery demo must
  give the address of a friend that kept its database.
- **ID-6** Read-through is the decorator's `GetObject`/`HeadObject`
  miss path (EP-10, §3.3, #30); after `recover` every object is a miss
  until fetched, from a place recovery recorded `acked` with the
  version that counts, and a recovered key in conflict is refused with
  the command that resolves it. Listing the bucket shows the keys the
  index holds before any is read, so restic finds its keys, snapshots
  and indexes on a store that is still empty. `restic restore latest`
  then fetches the config, keys, snapshots and indexes, and the packs
  of the snapshot it restores; nothing else (scenario 3). The AC is
  `TestResticRestoreThroughFriendsAfterRecovery` in `internal/cli`:
  three friends, a restic bucket at two copies, two snapshots (files
  removed between them), replication drained; the node's directories
  deleted; `recover` with the phrase and one friend's address; one
  friend stopped; `serve`; `restic restore latest` with the credential
  `bucket show` prints restores the files exactly, and the store then
  holds only objects restic read, each the version the node wrote,
  and some data packs (those only the older snapshot needs) are never
  fetched; the test asserts that some stay unread, not which.
- **ID-7** The sheet is generated from the manifest; a lender's `tc…`
  string changes only when its key or its configured bootstrap region
  changes (SEC-14), so regeneration is rare. Each bucket is listed with
  its sealed name and the `lent-<id16>-<sealed>` name it has in every
  lender's store (SEC-16), which is what a restore with lendisk stopped
  (P-5) needs, together with the number of replicas it must combine
  (N − c + 1, from the pool size and the bucket's `copies`) and the
  command that combines them: `rclone copy` or `aws s3 sync` from each
  replica into one directory, excluding `.lendisk/`, `--ignore-existing`
  after the first.

### 3.2 Peers and invites

- **PEER-1** The invite string carries the inviting node's `tc…`
  address, its number for the friendship (LEND-8), the retention offer
  and a single-use secret, and nothing else: five fields joined by dots,
  `li1.<address>.<number in bytes>.<retention in seconds>.<secret>`,
  the secret being 32 random bytes in unpadded base64url (package
  `invite`). `lendisk invite --quota 80G [--retention 30d] [--expires
  7d]` prints it on stdout; `--retention` and `--expires` take days
  (`30d`) or a Go duration. The invite key is the X25519 key
  `HKDF-SHA256(secret, info="lendisk-invite-key-v1")`, clamped. The
  inviting node stores only that key's node ID, its own number and
  retention, and the expiry, in the `invite` table; the secret itself
  is written nowhere. Its admission hook admits the invite key, in
  the `invite` role, until the invite expires, while it is unused or
  its redeemer is still a friend (the retry of PEER-2, §3.9);
  the invited node dials with that key to call `redeem`, after which
  it dials with its dial key for the inviting node (ID-1). Expired invite rows, used or not, are
  deleted by `invite` before it adds one, by the daemon at start and
  every minute after, which then clears the admission cache.
- **PEER-2** `peer add <invite>` shows the inviter's number and
  retention and reads the user's own number from stdin (Enter keeps
  the inviter's); `--retention` is its own offer, 30 days by default.
  `redeem` (POST, JSON) carries the invited node's `tc…` address, its
  retention offer, the number its user chose (LEND-8) and a proof that
  it holds the key its address names, since the connection is
  authenticated by the invite key, not by that key: HMAC-SHA256 over
  the invite key's 32 bytes and then the dial key's 32 bytes
  (`dial_key`, ID-1: the redeemer's dial key for the inviting node),
  keyed with
  `HKDF-SHA256(X25519(own key, other key), info="lendisk-redeem-v1")`,
  which either side computes and no one else can (ID-1: no
  signature). So the proof binds the dial key, and a request seen on
  the way cannot carry another. `dial_proof` shows the redeemer holds
  that dial key for that node: HMAC-SHA256 over the invite key and
  then the redeemer's node ID, keyed with
  `HKDF-SHA256(X25519(dial key, inviter's key),
  info="lendisk-redeem-dial-v1")`; without it a node could report
  another's dial key and be named by that key's connections, and
  without the node ID it could copy another redeemer's `dial_proof`
  (#69 review). The inviting node refuses a missing or wrong proof, its
  own address, a missing dial key (`400`, "upgrade lendisk on the
  redeeming node"), a dial key that is either node's ID, its own dial
  key for the redeemer or the invite key, and a wrong `dial_proof`; it then marks the invite used by the proven key
  (`invite.redeemed_by`, migration 8) and inserts the friendship row,
  with `dial_key` (migration 13), in one transaction. A dial key that
  is already another friend's dial key or node ID is refused (`409`;
  `state.ErrDialKeyTaken`, and a unique index on `dial_key`). It
  answers with its own number and retention, its own dial key for the
  redeemer and its `dial_proof` (keyed by X25519 of that dial key and
  the redeemer's key, over the invite key and the inviter's node ID;
  `RedeemResponse.InviterDialKey` checks it), and closes the
  connection (`Connection: close`). The invited node writes its row,
  with the inviter's dial key, only after that answer, and refuses an
  answer without a proven dial key. `control.RedeemRequest.Prove`
  fills the redeemer's three fields.
  The answer can be lost, or the invited node can stop before writing
  its row, after the inviting node committed (#51, #56). So the
  invited node first records the terms it offers in `pending_redeem`
  (keyed by the invite key's node ID, with the inviter's ID) and
  writes its friendship row and deletes every record with that
  inviter in one transaction (`state.DB.FinishRedeem`). Running
  `peer add` with the same invite again finds the record, repeats its
  number and retention without asking, and redeems again; a record
  with the same inviter from another invite is named on stdout (its
  invite key's node ID and start time) and settled by this redeem.
  Only one `peer add` per invite reads its redeem record or dials at a time, since
  two dialling with the invite key at one relay stall each other
  (#52, #64): before it reads the record or asks for the number it
  takes `flock(LOCK_EX|LOCK_NB)` on
  `<config dir>/locks/redeem-<invite key node ID>.lock` (directory
  0700, file 0600, the file left in place), which the kernel drops
  when the process ends, even on a crash. A second run that cannot
  take it stops with "a redeem of this invite is already in progress"
  and does not dial. If the record of this invite still appears while
  `peer add` asks for the number (a backstop: a run not holding the
  lock, such as an older binary), it stops with the same message.
  A proven key that is already a friend is answered, not refused: any
  unexpired invite, used by it, unused or used by another, answers
  `redeem` with the stored friendship's terms, the request's address,
  number, retention and dial key applied as the friend's latest
  report, and an unused invite is marked used by it
  (`state.DB.RedeemInvite`). The
  proof binds the invite key of this connection, so a proof seen
  elsewhere does not carry over; `redeem` is served to the `invite`
  role only, never over a friend's own key. An expired invite's key
  is no longer admitted (SEC-10), so a re-run after expiry cannot
  reach the inviting node. When an invite-key redeem times out or is
  refused (`4xx`) and a `pending_redeem` record with that inviter
  existed before this run, `peer add` keeps the records and says that
  the invite has expired or cannot be reached, that the inviting node
  may already count this node as a friend, and to ask it for a new
  invite; redeeming that one heals through the friend answer above.
  The invited node never dials with its own key here: a second
  process holding the daemon's key on the relay stalls both (#52).
  Every other proven key finds a used invite used (`409`) and an
  expired one not admitted, so the PEER-2 AC holds: a replay never
  makes a friendship. Without an earlier record, a refusal (`4xx`)
  drops the invite's `pending_redeem` record; a
  timeout, a lost answer or a `5xx` keeps it and tells the user to
  run the same `peer add` again. Both rows
  hold peer key, address, own number, the other's number as last
  reported and each side's retention (migration 5), and the other's
  dial key (migration 13), and each
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
- **PEER-3, PEER-6, LEND-8** Each side's number is a column of the
  friendship row, beside the other side's number as last reported; the
  quota is the smaller, recomputed whenever either changes, and it is
  what the decorator enforces (LEND-1). `lendisk peer quota <peer>
  <bytes>` sets the own number, refusing a value that would take the
  sum of all numbers past `lending.max_total`. The control API `quota`
  call carries, in each direction, the caller's number, its use of the
  other's space and its repository size (LEND-7). A node applies the
  other's number the moment it sees it, so a lowering is in force on
  both sides after one call, which the lowering side makes at once
  (SEC-15's 60 seconds) and repeats on every connection until
  answered; a raising is in force once both numbers allow it. Nothing
  is proposed, nothing is pending and the daemon never changes a
  number by itself. When a bucket has a deficit (D6, step 4), `status`
  and the OPS-2 alert name the friends with the smallest free fraction
  and, per friendship, the number that would cover the deficit at
  `copies`; the two people set it.
- **PEER-8** Two walls. The decorator maps every bucket name through
  the borrower's own mapping table, so a borrower can only name its own
  buckets; and versitygw's bucket-ownership check refuses an account
  any bucket it does not own, so a mapping bug cannot cross into another
  borrower's data or the lender's own. `ListBuckets` is answered from
  the mapping table. `gateway.Lending` writes out every
  `backend.Backend` method rather than embedding the next layer, so a
  method a versitygw upgrade adds fails to compile instead of passing a
  sealed name through unmapped. For a borrower's account it maps every
  bucket-named call, and refuses (`AccessDenied`) the ones that could
  weaken the ownership wall or name a bucket outside the mapping:
  `PutBucketAcl`, `PutBucketPolicy`, `DeleteBucketPolicy`, the
  ownership-control writes, `PutObjectAcl`, `ChangeBucketOwner`,
  `ListBucketsAndOwners`, `CopyObject` and `UploadPartCopy` (their
  source is a bucket inside a string), and `DeleteBucket` (its row and
  quota count would go stale). The rest of NET-7's narrowing is the
  gate's (§3.7 NET-7, SEC-9). `CreateBucket` creates the bucket with lendisk's own
  private ACL owned by the borrower's account and `BucketOwnerEnforced`,
  whatever ACL headers came with it, then inserts the row; a retry that
  finds `BucketAlreadyOwnedByYou` inserts it too, so a crash between
  the two heals on the client's retry. The borrower's account has
  versitygw's `userplus` role, whose one addition to `user` is
  `CreateBucket`; own bucket keys keep `user`. Own bucket keys cannot
  reach a `lent-*` bucket, since the borrower owns it.
- **PEER-9** The lender-only profile skips the loopback S3 listener,
  the metrics listener and the replication worker, so nothing listens
  on loopback (its AC); an `endpoint.listen` or `metrics` entry is
  refused at config load in this profile. versitygw still runs, behind
  the bridge's tunnel listener only. Its friendship rows are like any other; the quota its
  friends lend it back has no buckets to fill, and `status` shows it as
  unused. Adding a bucket later is a change of `role` and a restart.
- **PEER-10** Peers are rows in `lendisk.db`; nothing in the control API
  or the S3 surface returns them. `redeem` answers with the inviting
  node's own terms only. `peer list` (`--json` too) shows each friend's
  node ID, address, quota, both numbers, both retentions and since
  when. The AC test runs A–B and B–D in one process and searches A's
  and D's files, logs, `status` and `peer list` for the other's node
  ID, address and raw key.

### 3.3 Endpoint and local store

- **EP-1** lendisk's bridge (§3.7) listens on `127.0.0.1:9000` and
  proxies to embedded versitygw (D15) over the decorated backend on
  the internal socket. No unix socket is offered to tools: no backup
  tool can use one. Path-style addressing is versitygw's default. The
  address is the `endpoint.loopback` setting, refused at config load
  unless its host is a loopback IP or `localhost`; it exists for a
  second node on one machine and for tests. CI runs restic (`init`,
  `backup`, `check --read-data`, `restore`), the AWS CLI and rclone
  against it. A bucket credential cannot create buckets, so rclone
  needs `no_check_bucket = true`, which the generic profile's lines say.
- **EP-5** versitygw is given the internal `gw.sock` (§3.7) and
  nothing else, whatever the configuration says; every TCP listener,
  loopback included, is lendisk's bridge, so no versitygw option or
  default can open one.
- **EP-17** The bridge on the LAN. versitygw is not given the network
  addresses, or any address: its TLS settings would apply to every
  listener it ran, and a per-address TLS choice is what EP-17 asks
  for. The bridge listens on each
  `[[endpoint.listen]]` entry (`address = "192.168.1.10:9000"`
  or `interface = "eth0"` with a port; `tls = "generate"`, the
  default, `"files"` with `cert` and `key` paths, or `"off"`). The
  flag and variable (OPS-6) are the repeatable `--endpoint-listen` and
  `LENDISK_ENDPOINT_LISTEN`, an entry written as comma-separated
  `key=value` pairs with the table's keys and entries separated by
  `;` (`address=192.168.1.10:9000,tls=off;interface=eth0,port=9000`);
  either replaces the file's whole list. An `address` is an IP
  literal with its port (a host name is refused: use `interface`);
  `0.0.0.0`, `::`, an empty host, port 0, and `port` beside `address`
  are refused at config load. An interface name is resolved to its
  addresses (link-local ones left out, since a URL cannot carry the
  zone) with `net.InterfaceByName`, pure Go, at start and every 10 s
  after: listeners open and close as addresses come and go, and an
  interface with no address is logged and waited for. A fixed address
  that cannot be bound stops `serve`; an interface address that cannot
  is retried. Each
  accepted connection is served as HTTP (after a `crypto/tls`
  handshake where TLS is on, TLS 1.2 or later; a plaintext request on
  a TLS port gets Go's `400 Client sent an HTTP request to an HTTPS
  server`, nothing is proxied) and every request is proxied,
  unbuffered, to `gw.sock` with the `Host` header and every signed
  header unchanged, as on every other listener (§3.7, Q-G). versitygw verifies the signature against the per-bucket
  credential exactly as on loopback; lendisk adds no check of its own
  and needs none, since the writer is trusted (§2). A request under
  `/_lendisk/` is refused on a loopback or LAN listener, as are
  `/_lendisk` itself and every path that cleans to one of them
  (`//_lendisk/x`, `/b/../_lendisk/x`); the path is only tested
  cleaned, never rewritten (#53). With `tls = "generate"`,
  lendisk creates one self-signed ECDSA P-256 certificate whose SANs are
  the addresses of every `generate` entry (interfaces resolved) and the
  host name, valid ten years, at
  `<config>/endpoint.crt` (0644) with its key at `<config>/endpoint.key`
  (0600, written first, each through a rename); it is its own CA
  (`CA:TRUE`), so it is what a client trusts. It is regenerated when
  that SAN set changes, at start or when an interface's addresses
  change, and swapped into the running listeners; otherwise the
  existing pair is reused, after the key passes the same mode and
  owner check as a `tls = "files"` key (#53). `lendisk endpoint cert` prints its path,
  SHA-256 fingerprint (colon-separated hex, as `openssl x509
  -fingerprint -sha256`), names and expiry, and says to run `serve`
  when there is none yet. With `tls = "files"` the key must be owned
  by the service user with no group or other permission bits, or
  `serve` refuses to start (SEC-3).
  `bucket add` and `bucket show` print, after the loopback lines, for
  each network address: for restic `RESTIC_REPOSITORY=s3:https://<address>/<bucket>`
  and `restic --cacert endpoint.crt init`, after a line saying to copy
  `<config>/endpoint.crt` to the other machine and check it with
  `lendisk endpoint cert`, for a generated certificate (Kopia gets
  `--root-ca-pem-path=endpoint.crt`, the generic profile a `CA cert`
  line); `s3:http://…` after the
  plaintext warning for `tls = "off"`. Loading a configuration with a
  `tls = "off"` entry logs that warning, on every command. `status`
  lists every non-loopback listener under `endpoint.listen` in
  `--json` (`url`, `interface`, `tls`, `plaintext`; an interface with
  no address has an empty `url`), and a `LAN:` line each in text,
  ending "(plaintext)" when TLS is off. With TLS off the request is
  still SigV4-signed, and restic and Kopia send the `aws-chunked`
  streaming form, which signs every chunk, so a machine on the LAN can
  read object bytes and replay a captured request for up to SigV4's
  15-minute window, but cannot forge one or learn the secret key.
  Contents of restic and Kopia buckets are encrypted by the tool;
  `generic` buckets are not, which is what the warning says.
  In the lender-only profile (PEER-9) an `endpoint.listen` entry is
  refused at config load.
- **EP-2** The decorator (`internal/gateway`, `capture.go`) enqueues
  on a successful `PutObject` or `CompleteMultipartUpload`, and a
  delete on a successful `DeleteObject` or each entry `DeleteObjects`
  reports deleted. Only the node's own buckets (rows in `buckets`) are
  captured; `lent-*` buckets pass through. Parts and aborted uploads
  are neither hashed nor queued (EP-3). The other ways to put content into an object (Q-H at
  v1.10.0) are refused rather than captured, since their bytes never
  pass the decorator as a stream: `CopyObject` (including the same-key
  metadata replace) and `UploadPartCopy` answer `501 NotImplemented` on
  an own bucket, from the decorator, which knows the bucket map; a
  browser-form `POST` upload (a `POST` to a bucket that is not
  `?delete`) is refused by the gate. restic, Kopia and the AWS CLI's
  and rclone's uploads use none of them. Decided 2026-11-06 to keep
  copies refused for every profile, generic included: a copy is a new
  key that friends store and receive in full anyway, so capturing it
  would save only local I/O. rclone's server-side copy and move
  (`--backup-dir`, `move`, `--track-renames`) and its in-place modtime
  update fail against a lendisk bucket, so the generic profile's
  `bucket add` lines tell rclone users to pass `--disable Copy,Move`
  (download and re-upload instead) and `--no-update-modtime` (#7).
  *Measured (EP-2 AC):* `TestCaptureOverhead` in `internal/gateway`
  (opt-in, `LENDISK_OVERHEAD_MB`) times `restic backup` of random data
  through the gateway with capture on and off, alternating, after a
  warm-up run, and compares medians. On a 6-vCPU VM (Ryzen 7 8745HS,
  SHA-NI, 3 GB RAM), restic 0.19.1, 2026-11-06: 512 MiB to ext4, five
  runs each, 2.400 s with capture and 2.404 s without (−0.2%); 256 MiB
  to tmpfs, where the disk hides nothing, seven runs each, 1.582 s and
  1.610 s (−1.7%); a 1 GiB pair to ext4 gave +1.8% and +0.4% before the
  disk filled. The difference is inside the run-to-run spread (about
  ±5%): the hash runs on the gateway's goroutine at SHA-NI speed and
  two SQLite writes per 16 MiB pack are noise. The AC's 10 GB did not
  fit on the test machine's disk; the gate machine (G-3) should repeat
  it at that size, and for Kopia with #7.
- **EP-3** The decorator writes a pending queue row (bucket, key,
  operation, class) before calling the backend and marks it done after
  the backend returns success; on failure it deletes the row. A request
  whose row cannot be written or completed fails with a retryable 500
  rather than go uncaptured. The decorator does not buffer request
  bodies; it wraps the stream in a SHA-256 as it passes to the backend
  (keeping versitygw's `ChecksumReader` visible through the wrapper, so
  posix still verifies a trailing checksum) and writes the hash and the
  byte count to the done row, so the object is never read again to hash
  it (REPL-10). EP-11 needs the hash at write time in any case, to
  notice a changed acknowledged key.
  - *Which write is on disk.* Each captured `PutObject` stores a random
    token as user metadata `lendisk-capture` (it is visible on `HEAD`
    as `x-amz-meta-lendisk-capture`; tools ignore it), and the pending
    row keeps it. A pending completion keeps its upload ID (and the ETag
    the upload will have, `backend.ComputeMultipartETagFromPartETags`).
    posix records the upload ID on the object it assembles, in the
    `mp-metadata` attribute (`user.mp-metadata` xattr; versitygw v1.10.0
    `posix.go`, which checks it to answer a retried completion), and the
    decorator reads the same attribute (`Store.UploadID`), so a row
    matches its own upload exactly; an ETag would also match an earlier
    upload of the same parts (#59). A `PutObject` drops the attribute.
    `OpenBackend` returns the `Store`: the posix backend with the
    metadata storer it was opened with, so the attribute is read through
    the same storer and bucket paths posix writes it with. The gateway
    hands the `Store` to the decorator as `Decorator.Uploads`, apart from
    the backend it wraps, so a wrapper between the decorator and posix
    cannot hide it (#62); quota is enforced above the
    decorator, in `Lending` (§3.5), and the gateway gives `Lending` the
    same `Store`. The ETag never
    stands in for a missing attribute. versitygw v1.10.0's
    `CompleteMultipartUpload` writes `mp-metadata` with the ETag, so an
    object without one is either a `PutObject`'s (a plain MD5 ETag:
    the upload did not take effect, the row is removed) or one whose
    attributes were lost outside versitygw, as when a store is copied
    without extended attributes (a multipart `-N` ETag, or none, since
    posix keeps the ETag in `user.etag` too). Taking the latter as landed
    by ETag would let a retry of a never-landed upload of the same parts
    be answered from the store, and posix would then assemble it with no
    row (#62). So it is treated like an attribute that cannot be read (an
    I/O error, or a value that does not parse): the row is undecided, it
    is logged and stays pending and stranded, is never dropped for it,
    and a retried completion of its upload is refused. `ResolveStranded`
    retries it every round but warns of it once per row while the daemon
    runs; `status` counts it among the bucket's pending writes
    (`queue.pending`). Restoring the attributes settles it, and so does
    any later write of the key (a plain ETag, or another upload's ID).
    The rule relies on posix's `DataIntegrityEtag` being off: with it, a
    `PutObject`'s ETag contains `-` too.
  - *Startup.* Before the gateway serves anything, `ResolvePending`
    settles every pending row with `HeadObject`: a put is done if the
    object carries the row's token (or upload ID), and is otherwise removed;
    a delete is done if the key is gone. A `PutObject` that reached the
    store but not its done row is hashed by reading the object once; a
    `PutObject`'s object is read only then (after a crash, or for a
    stranded row), while a completed upload's is always read once
    (*Multipart*). A completion's row whose upload ID cannot be read does
    not stop the start: it is logged and left pending and stranded for
    `ResolveStranded` (#62).
  - *While running.* A row whose request ended without completing or
    removing it (the state database refused the write, or a multipart
    hash failed) is remembered by the decorator as stranded, and
    `ResolveStranded` settles it against the store as startup would,
    every minute (`Gateway.ResolveEvery`) until it succeeds (a row whose
    upload ID cannot be read waits for its attributes or a later write of
    the key), so a
    failed request does not leave its row pending until the next start.
    The list is in memory; after a crash startup settles every pending
    row anyway.
  - *Concurrent requests.* Capture writes `lendisk.db` from many
    requests at once, alongside the replication worker. Write
    transactions begin `IMMEDIATE` (`_txlock=immediate` in the modernc
    DSN), so `FinishPut`, which reads the key's latest done row and then
    writes, holds the write lock from its start and cannot fail with
    `SQLITE_BUSY_SNAPSHOT` when another connection commits in between
    (#47); writers wait for the lock up to `busy_timeout`, 30 s, which is
    a queue of millisecond transactions, never a held lock, since no
    transaction does I/O other than SQLite's own.
  - *Same-key ordering.* `seq` is assigned when the row is written,
    before the store call, so two overlapping writes to one key could
    reach the store in the other order and leave the key's last done row
    describing an object that is not on disk (#48). The decorator
    therefore holds a per-key lock (`keyLocks`, a map of one-slot
    channels that exist only while held or awaited) from before a
    captured request writes its row until the row is done or removed:
    writes and deletes of one key are applied in `seq` order, and the
    key's last done row always describes the object on disk. Different
    keys never wait for each other. A request waiting for a key gives
    up when its context ends (the client went away), so a key held for
    minutes by a completed upload's hash keeps no abandoned goroutines
    (#55). `DeleteObjects` blocks on one of its captured keys at a time
    holding none of the others, then takes the rest only if they are
    free (else it lets go and waits for the busy one), so two batches
    cannot deadlock and a batch never holds a key while it waits for
    another. restic and Kopia never write one key concurrently, so the
    lock costs them one uncontended channel send per request.
    `ResolveStranded` settles a row only if its key's lock is free (and,
    for a completion's row, the upload is unclaimed, *Retried
    completions*), and otherwise leaves it for the next round.
  - *Multipart.* There is no single stream to hash, and clients upload
    parts in parallel, so parts are not hashed. The completion writes a
    pending row with its upload ID (and the ETag the completion will give
    the object), the store assembles it, and the decorator then reads the finished object
    once and records its whole-object SHA-256 and size (decided
    2026-11-06: one canonical hash for every object is worth the second
    read, and replication may then use any part size). If the key holds
    another object by then (it changed outside lendisk), the row is
    dropped.
    *The completion answers before the hash finishes* (decided
    2026-11-06, #49). On a Pi a multi-GB object can take longer to hash
    than a client's read timeout (the AWS CLI's is 60 s), and a client
    that times out retries the completion and may report the upload as
    failed. So the client gets its answer as soon as the store has
    assembled the object, and the hash runs in the background, still
    holding the key's lock (same-key ordering above), so a later write of
    the key waits for it. Nothing is lost by answering first: the pending
    row was written before the store call, so if the process dies before
    the hash is recorded, startup finds the object carrying the row's
    upload ID and hashes it (*Startup*); a graceful stop cancels the hash
    with the same result. `Gateway.Run` waits for running hashes after
    versitygw has shut down; a completion handler that outlives
    versitygw's 10 s shutdown starts no hash once that wait has begun
    and leaves its row pending for startup (#55). A failed read leaves the row stranded for
    `ResolveStranded`. A `FinishPut` that answers `ErrNotFound` (the row
    was settled elsewhere) counts as success.
    *Retried completions.* A completion of an upload that already has a
    queue row is a client's retry (#55). posix claims an upload by
    renaming its directory: a retry while the first is in the store is
    answered "done" at once, a retry after the first linked the object
    is answered from the object without changing it (or `NoSuchUpload`
    if its parts give another ETag), but a retry after the first failed
    finds the upload rolled back and assembles the object itself. So
    one request per upload at a time *claims* it (a per-upload signal,
    registered before it waits for the key's lock), and the decorator
    decides by the row and the claim:
    - *done row*: the object is linked; the retry goes to the store
      uncaptured, without the key's lock.
    - *pending or no row, claimed by another request*: the retry waits
      (with its request context) for the claimant's store call to
      return, not for the key's lock, which the claimant holds while it
      hashes. Stored: as *done*. Failed (or the claimant gave up): the
      claimant has removed its row (or stranded it) and released the
      claim, and the retry looks again. Two retries that both found no
      row therefore never both wait for the lock: the second finds the
      first's claim (#59).
    - *pending, unclaimed* (stranded), or *no row, unclaimed*: the retry
      claims the upload and takes the key's lock. A stranded row is
      checked against the store with a `HEAD` and the upload-ID
      attribute only: landed, the retry is answered from the store and
      the row stays pending and stranded for `ResolveStranded` (or
      startup) to hash; not landed, the row is removed. The request
      never hashes the object inline, so a client's timeout cannot
      waste a multi-GB read (#59). Anything else is captured as a first
      completion, with its own row, upload ID and ETag.
    `ResolveStranded` settles a completion's stranded row holding the
    upload's claim as well as the key's lock, and settles the claim as
    stored as soon as the `HEAD` and the attribute show the object
    landed, before it hashes. A retry arriving meanwhile finds the claim
    and is answered from the store; one that claimed the upload first
    keeps the resolver off the row until the next round (#62). So a
    retried completion waits at most for another request's store call or
    a `HEAD` and an attribute read, never for the hash of its own upload.
    It can wait for another upload's hash: a retry of an older upload of
    the key, superseded by a newer one that is being hashed, claims its
    own upload and then waits for the key's lock behind that hash, as
    any write of the key does; it then finds its row not landed, removes
    it, and goes to the store as a first completion, where posix answers
    `NoSuchUpload` if the upload was consumed. (A retried
    `PutObject` writes the key, so it does wait for the key's lock, as
    any later write of the key does.)
    So no completion stores an object without a row. The claim, the
    key's lock and an unsettled row are released (the row stranded) by
    a deferred step, so a panic in the store leaks neither (#59). Every
    hash of a stored object (`hashStored`, the background hash) stops
    when its context ends.
  - *Exactly once.* A put whose hash equals the key's latest done row,
    a put, is not queued again (`FinishPut` drops it): this is the
    client's retry after a crash it saw as a failure, and an identical
    re-upload EP-11 accepts silently anyway.
  - *AC.* `TestCrashDuringResticBackup` in `internal/cli` runs the
    daemon as a real process (the test binary re-executed), SIGKILLs it
    at random points during restic backups and restarts it, twelve
    times, while the daemon also kills itself with probability 0.1 per
    request in the window between the store's answer and the row's
    completion, and restic is killed with it a third of the time (so
    nothing retries what was in flight). Afterwards every object in the
    store has exactly one queue entry, a put with the SHA-256 of its
    bytes; no row is pending; every queued put is in the store; and no
    `locks/*` key is queued. restic 0.19.1 sent no multipart upload in
    these runs, even with 40 MiB packs, so the multipart paths are
    covered by `TestCaptureWritesAndDeletes` and
    `TestPendingResolvedAtStartup` instead.
  - *Durability.* `lendisk.db` runs WAL with `synchronous=NORMAL`: a
    process crash loses no committed row, a power loss may lose the
    last ones. The store's own writes are not fsynced either; the
    reconciler (EP-14) catches what a power loss leaves.
- **EP-6** Bucket credentials are one access/secret pair per bucket,
  generated and stored by lendisk in `lendisk.db` (table `buckets`)
  and resolved by lendisk's `IAMService` (`Config.IAMService`, which
  takes precedence over versitygw's own IAM options). versitygw's
  `IAMDir` is not used. The access key is `LDK` and 17 base32
  characters, and the secret is 240 random bits as 40 URL-safe base64
  characters. The account has versitygw's plain `user` role, and
  `bucket add` creates the bucket with that access key as its ACL
  owner, so versitygw's own access check confines each key to its
  bucket: another bucket is `AccessDenied`, `ListBuckets` shows only
  its own, and it cannot create buckets. `bucket add` writes the row
  and then creates the bucket directly through the posix backend, so it
  needs no running daemon; `bucket show` prints the lines again.
  `--copies N` sets the bucket's `copies`; without it the bucket gets
  `replication.copies` (default 2). A bucket at `copies` 1, however it
  was set (flag or setting; EP-6 v0.11.1, decided 2026-11-06), gets a
  warning that is never suppressed, never an alert and never a
  daily-summary line: `bucket add` prints "Warning: copies = 1. Each
  object is held by one friend only, so if that friend's disk is lost,
  this bucket has no offsite copy left. Add a second friend, or
  recreate the bucket with --copies 2." (`bucket set` comes with
  REPL-13), and `status` prints "! one copy: losing that friend's disk
  loses the offsite copy; 2 is recommended" under the bucket (`buckets[].single_copy` in
  `status --json`). `bucket add` also says when the pool has fewer
  places than `copies` (OPS-2), as does the `pool_too_small` alert
  (§3.8).
- **EP-7** Profiles are TOML files compiled into the binary
  (`internal/profile/profiles/<name>.toml`) and overridable in
  `<config>/profiles/`: a file there replaces the compiled-in profile
  of its name or adds one, read when `serve` starts; a file with an
  unknown key or an empty pattern stops the start, naming the file.
  Each has `encrypted` and four lists of patterns, `local_only`,
  `snapshot`, `metadata` and `data`, in which `*` matches any run of
  characters, `/` included, and `?` one. A key's class is local-only,
  else snapshot, else metadata, by the first list it matches, and data
  otherwise; a snapshot key is placed as metadata. A fifth list,
  `hash_named`, names the keys the tool names by the SHA-256 of their
  content as their last segment (restic: `data/*`, `index/*`,
  `snapshots/*`; not `config` or `keys/*`), which recovery checks
  (§3.1 ID-5). A bucket whose
  profile has no file classifies every key as data. The compiled-in
  files are `restic` and `generic`; `kopia` comes with #7. The
  decorator classifies a key at capture: a local-only key (restic
  `locks/*`, Kopia `_log*`) gets no queue row at all; the others get a
  queue row with their class, and placement rows as §3.4 says.
- **EP-10** The read-through (`gateway/readthrough.go`, #30). The
  decorator's `GetObject` and `HeadObject` turn the store's not-found
  for an own bucket's key (not a local-only one; `lent-*` buckets pass
  through) into a fetch:
  - *Refused or not found.* A recovered key in conflict
    (`state.KeyConflict`, §3.1 ID-5) is refused with `409 KeyConflict`,
    whose message gives the reason and `lendisk bucket resolve <bucket>
    <key> <friend>`. A key with no version that counts
    (`state.Source`: no done queue row, or its latest is a delete) keeps
    the store's `NoSuchKey`.
  - *Where from.* The version that counts is the key's latest done
    queue row, a put, with its size and SHA-256; the places are those
    whose latest `acked` placement row for the key is a put of that
    hash, which after `recover` are exactly the friends that listed the
    winning hash. They are tried most recently reachable first: by the
    later of `place.last_ok` (replication's last acknowledgement) and
    the read-through's own last successful fetch from the place, kept in
    memory; a place whose last fetch failed goes after every other, so
    an offline friend costs one dial timeout, not one per object. A
    failure falls through to the next place. Friends only until overflow
    targets exist (#38), which would come after them.
  - *Verified as it passes.* The body (`replica.Fetcher`, which builds
    a `replica.Friend` per friend and calls `Friend.Get`: a whole-object
    `GetObject` from the sealed bucket, with the stall watchdog, and a
    dial timeout of 10 s rather than replication's 30 s, since a reader
    waits) is streamed into the store with a plain backend `PutObject`
    through a verifying reader: more bytes than the recorded size, fewer
    at the end, or another SHA-256 once the size has passed fail the
    read before posix links the object, so wrong bytes are never stored.
    That is the place's fault: logged as such (`read-through: a friend
    served bytes that are not the version it acknowledged`, with the
    place), counted as its failed fetch, and the next place tried. It is
    not an alert: VER-1's challenge (#27) is the verdict on a friend. A
    failure of the local store stops the fetch (`500`); no place would
    do better.
  - *Stored without capture.* The object goes to the backend under the
    decorator, so it gets no queue row and no placement: the places it
    came from hold it already. It carries no `lendisk-capture` token,
    which nothing needs, since only queue rows are settled against the
    store.
  - *Then served.* The request is answered from the store. A ranged
    `GetObject` fetches the whole object, verifies and stores it, then
    serves the range: restic's packs are about 16 MiB, so the simple way
    costs little, and the next range of the pack is local. A `HEAD`
    fetches the whole object too.
  - *Once per key.* The fetch holds the key's lock (`keyLocks`, as a
    captured write does) and, holding it, looks at the store again: of
    concurrent misses of one key one fetches, the rest find it stored;
    a write of the key waits for the fetch, and a fetch after a delete
    finds no version and answers not-found.
  - *No place could serve it*: `503 ServiceUnavailable` saying so, which
    restic retries; nothing is stored.
  - *Listing.* `ListObjectsV2` of an own bucket merges the store's page
    with the index's live keys (`state.LiveKeys`: each key whose latest
    done write is a put, and each recovered key in conflict, so reading
    it says why) from the same marker, in byte order, folding keys under
    a delimiter into common prefixes as posix does; the store's entry
    wins a name both have, and the next continuation token is the last
    name returned, which posix takes as its marker. An index entry has
    the size and capture (or listing) time and no ETag. On a node that
    lost nothing the index adds no key the store lacks. `ListObjects`
    (v1) is not merged: restic, Kopia, rclone and the AWS CLI use v2.
  - A miss happens after `recover` (ID-6) and after VER-5 has removed a
    rotten object; nothing else removes an object from the local store
    (EP-9). Only what a tool reads is fetched; no download runs in the
    background.
  - *Tests:* `TestMissFetchesFromAFriendAndRejectsWrongBytes`,
    `TestNoFriendServingTheVersionStoresNothing`,
    `TestHeadAndRangedReadFetchTheWholeObject`,
    `TestConflictRefusedAndUnknownNotFound`,
    `TestConcurrentMissesFetchOnce` and
    `TestListShowsRecoveredKeysBeforeTheyAreRead` (`internal/gateway`);
    the restic AC is §3.1 ID-6.
  *Post-v1, evict mode:* an eviction worker would delete a pack once
  `copies` places have acknowledged it and no restic lock is present,
  or for Kopia once the bucket has been idle for a configured
  interval; re-placement would then stream from a holder through the
  replica client, and the evicted part would be verified by VER-2's
  sampling.
- **EP-12** `init` checks xattr support (versitygw's own
  `XattrMeta.Test`) and the free-space floor (`statfs`, as a percentage
  of the filesystem or a size) on the store directory, before showing
  the phrase, and refuses otherwise, naming the filesystem from its
  `statfs` magic number. `serve` repeats the check at start. Post-v1, on `s3`, it
  would run the copy probe (D15) and warn when the endpoint is not on
  the local network. `status` reports the pinned versitygw and tailcat
  versions, read from the binary's build information
  (`version.Module`; `versions.versitygw` and `versions.tailcat` in
  `status --json`).
- **EP-13** Throughput is versitygw's chunk-signature verification plus
  the backend's writes; on `posix` with NVMe this is file I/O. Measured
  with G-3.
- **EP-14** The reconciler pages through `ListObjectsV2` of every bucket
  through the backend and compares with the index in `lendisk.db`.
- **EP-15** On `posix` the store is plain files: `restic -r
  <data>/store/<bucket>` opens it with nothing running, and a borrower's
  replica on the lender, `restic -r <data>/store/lent-<id16>-<sealed>`,
  likewise (scenario 7), with the `.lendisk/` directory ignored as
  restic and Kopia ignore unknown names. Post-v1, behind `backend/s3`,
  any S3 client with the store's credential would read the same keys.

### 3.4 Replication

- **REPL-12, placement at capture (#19).** When the decorator completes a
  queue row (EP-3), `FinishPut` or `FinishDelete` places it in the same
  SQLite transaction (`state.DB.placeTx`, `internal/state/pool.go`):
  - a metadata or snapshot key (the queue row's `class`, EP-7) gets a
    `placement` row at every friend with an address (and, once overflow
    targets exist, every one already holding data of the bucket);
  - a data key gets rows at `copies` friends (the bucket's row;
    `state.DefaultCopies`, 2, for a bucket with none) by D6's choice
    (`state.DB.choose`): candidates are friends with an address and
    free capacity for the object; reachable ones first, then by largest
    free fraction (free ÷ quota), compared exactly, ties by node ID
    (`state.byFraction`, a total order; room is counted in hundredths
    of a byte, so the reserve's 1% of a quota is exact and equal
    fractions tie; a quota, use or size above `state.MaxQuota`, 32
    PiB, counts as 32 PiB, so that arithmetic never overflows 64
    bits). Free capacity is the
    friendship's quota less the borrower's count of the place's use
    (below) less the bucket's live metadata with no row at that place
    (a friend added later, which re-placement, #26, fills) less the
    metadata reserve (below). A friend
    that is `removed`, whose last answer was `507` (condition `quota`,
    below), or that is gone past `replace_after` is not a candidate;
  - a delete goes to every friend whose latest row of the key is a put,
    in any state (REPL-2): every place that holds or will hold it, and
    one that refused it, which may hold older bytes;
  - a healing re-upload of a data key (*Healing*, below) goes where the
    row it heals was placed, topped up to `copies` by the choice.

  *Decision (#19, the user, 2026-11-06): data leaves headroom for
  metadata.* The metadata reserve at a friend is the larger of
  `state.MetadataReserveFactor` (3) times the live metadata of all the
  node's buckets (the key's latest done write a put, each counted as
  the place counts it) and `state.MetadataReservePercent` (1%) of the
  friend's quota. Data placement and re-routing leave it free; metadata
  placement ignores it and may use it. It exists because restic's prune
  writes the new index before it deletes the packs it no longer lists:
  were a friend filled with data, it would refuse that index with
  `507`, the deletes behind the index in its FIFO would never be sent,
  and it would never free room. Residual risk: metadata that grows
  faster than the reserve between backups (a large first index, many
  snapshots at once) can still fill a friend; the reconcile from the
  lender's own count (follow-up, #68) corrects the borrower's count, not
  the room.

  With fewer candidates than `copies`, what fits is placed: the deficit
  is not stored, it is the object's count of `queued` and `acked` rows
  below `copies`, which `status` shows (`state.DB.BelowCopies`,
  `buckets[].below_copies` in `status --json`: live objects and bytes,
  the key's latest done write a put). Each `placement` row in state
  `queued` is an entry in that place's FIFO.
  *Thin pools (EP-6, #19).* Two `status` lines, neither an alert: a
  bucket at `copies` 1 gets "! one copy: losing that friend's disk
  loses the offsite copy; 2 is recommended" under it (`buckets[].single_copy`; `bucket
  add` warns too, see EP-6 below), and a pool of exactly one friend,
  counted as `state.DB.PoolSize` counts (not removed, not gone past
  `replace_after`), whatever the buckets' `copies`, gets "Friends (1:
  one lost disk loses every offsite copy)" above the places
  (`pool: {"friends": N, "single_friend": true|false}` in
  `status --json`).
  *Reachability* until the probe (#65) is the place's condition: a
  friend is unreachable once it has been `retrying` or `removed` for
  `replication.reroute_after` (1 hour; a shorter failure is a blip),
  and gone once, in addition, `replication.replace_after` (7 days) has
  passed since its last acknowledgement (`last_ok`, or the failure's
  start if it never answered). An unreachable friend with room is still
  chosen when the reachable ones are not enough; a gone one is not.
  *The borrower's count* (`place_use`, schema 12) is the sum over a
  place's `queued` and `acked` rows of `placement.size`: a put's bytes
  plus `state.ObjectOverhead` (4 KiB, the lender's per-object overhead,
  LEND-1), or 0 when the place's latest row of the key acknowledged the
  same hash (a healing re-upload, which the lender stores nothing for);
  a delete minus what the place acknowledged of the key since its last
  delete there. SQLite triggers on `placement` keep it as rows are
  inserted, acknowledged, refused, re-queued, cancelled and moved, so
  placing an object reads one row per friend. A row acknowledged after
  its delete was placed (a send in flight) stays counted: the count
  errs towards less room. A lender keeps a deleted object counted
  until its purge (§3.6), so this count frees it a retention window
  early, erring towards more room until the `507` below or the `quota`
  answer corrects it. *Correction from the lender's answers:* the lender's only
  answer about room so far is `507` (LEND-2; the control API's `quota`
  call is not built): a place whose last answer was `507` counts as
  full, gets no new data and has its unsent data re-routed or dropped
  (below), until it acknowledges a row again. Each backup run queues
  metadata at every friend, so a full friend learns of freed room within
  a `PauseRetry` (an hour) of the next run.
  *Follow-up (#68): reconcile the count from the lender.* Nothing
  corrects `place_use` but the rows, so it drifts from what the lender
  holds. Known sources: a send that landed but whose answer was lost
  (a network error), whose row is then re-routed, cancelled as
  superseded or dropped, leaves bytes the count does not hold; a row
  acknowledged after its delete was placed stays counted; a lender's
  real per-object overhead and its retention of deleted objects (RET-1)
  differ from the fixed 4 KiB and the immediate free; a refused row's
  place may still hold older bytes of the key; and partial multipart
  uploads (#23) count nothing. The control API's `quota` answer (LEND-7)
  is the source to reconcile against.
- **REPL-1 to REPL-3** The queue is a SQLite table with one FIFO
  sequence per place, ordered by the bucket's write sequence. The
  worker uploads one object at a time per place in sequence order,
  several places in parallel. A delete (REPL-2) is a row in the FIFO of
  every place whose `placement` row for the key is `acked` or
  `queued`; a `queued` put that has not started is simply cancelled.
  The capture side is table `queue` (schema 4): `seq` (an
  autoincrement, the write sequence, assigned when the request reaches
  the decorator, so a tool that waits for its packs before writing the
  index gets the index after them), `bucket`, `key`, `op` (`put`,
  `delete`), `class` (`snapshot`, `metadata`, `data`), `state`
  (`pending`, `done`), and for a put `size` and `sha256`; `token`,
  `etag` and `upload_id` serve EP-3. `sha256` is always the
  whole-object SHA-256. Done rows are never removed, by capture or by
  the worker (#18), which consumes `placement` rows instead; capture
  removes only pending rows, of a failed request or of an identical
  re-upload (`FinishPut`'s duplicate), and exactly-once capture (EP-3)
  still compares with the key's latest done row. `status` shows each
  bucket's capture count: `buckets[].queue.objects` and `.bytes` in
  `status --json` count every done row the bucket ever captured, puts
  and deletes, and the bytes of its puts, not what is still to send;
  `.pending` counts writes in flight to the store. The backlog, what
  waits to be sent, is per place: `places[].queue` (below). The field
  names stay, as `status --json` never renames.
- **The placement table (schema 7).** `FinishPut` and `FinishDelete`
  place the row they complete in the same transaction (above). Table
  `placement`: `seq` (the queue row), `place` (the
  friend's node ID), `bucket`, `key` and `op` copied from the queue row
  so the index outlives it, `state` (`queued`, `acked`, `refused`, and
  from #19 `dropped`),
  and on acknowledgement `sha256` and `acked_at`: acked put rows are
  the index of acknowledged hashes (REPL-10, `state.AckedHash`).
  `refused` (with `detail`) is a refusal that would repeat; the row
  leaves the FIFO. Its `cause` (schema 9) is NULL when the place
  refused (a `412`), `store` when the node's own store could not
  supply the object as captured (`state.RefuseObject`, #50), and
  `oversize` when the only reason was that the object is larger than
  one PUT carries (#61): such a refusal takes the row out of every
  place's FIFO at once and counts against the bucket
  (`state.RefusedObjects`, both causes), not any place.
  *Healing (#57):* a refusal is healed by writing the same bytes again.
  `FinishPut` drops an identical re-upload as a retry (EP-3) only while
  the key's last done row is refused nowhere; when it is refused at
  some place, the re-upload is queued and placed again (where the
  healed row was, for a data key; at every friend for metadata), but
  sent only where the object is known to be held (#61): at a place
  that acknowledged the very row being healed (the key's previous done
  row, a put of the same hash) and has nothing of the key queued, the
  new row is `acked` at placement, without a send (`placeTx`).
  Everywhere else it is queued: an older acknowledgement of the same
  hash is not enough, because a row of other bytes sent since may have
  landed with its answer lost and then been cancelled as superseded,
  which leaves no row behind. A row of the key still queued there may
  be in flight and could still replace what the place holds, so then
  the new row is queued and the older one is cancelled as superseded.
  So a friend that keeps refusing gets every re-upload,
  and an honest friend that holds the hash gets none: exactly-once
  (EP-3) holds for it. A refusal counts in
  `status` (both kinds) only while its row is the key's latest done
  write: once a later write or delete of the key is done, it no longer
  does. A client's ordinary retry, of an object queued or acknowledged
  everywhere, is still dropped, so exactly-once capture (#6) holds. Rows
  refused only as too large for one PUT (cause `oversize`) are put back
  in their places' FIFOs by `state.RequeueOversize` when multipart
  replication lands: #23 calls it once, at the start of a worker that
  can send them, with `replica.MaxPutSize`; it also re-queues rows
  refused before #61 told the causes apart (cause `store`, size above
  that limit). A row a later write of the key replaced stays as it
  is. A re-upload also heals them.
  Table `place` keeps each place's last answer:
  `condition` (`ok`, `retrying`, `quota`, `removed`, and `store` while
  it waits because this node's store cannot be read), `detail`, `since`
  and `last_ok`, D6's last-reachable time. A sender whose FIFO is empty,
  including at daemon start, sets `store` back to `ok` without touching
  `last_ok` (`state.ResetPlaceCondition`): the rows it waited on went
  for another reason than its answer, such as a refusal for the store.
  `retrying`, `quota` and `removed` are the place's own last answer and
  stand until it answers again, at its next row (#61). A friend
  added after a row was placed gets no copy of it until re-placement
  (#26).
- **REPL-1, the metadata barrier.** A metadata row at sequence `m` is
  eligible to send only when every data object before `m` has no
  `queued` placement row left: each is `acked` at every place it is
  placed at, which is `copies` places or, where D6 recorded a deficit,
  fewer. A deficit therefore lowers the copies but does not stop the
  pool: places with room keep receiving data and metadata. The bucket's
  watermark (REPL-11) still requires `copies`. The sender checks it
  (`state.DB.Held`) when a metadata or snapshot row, put or delete,
  is the head of its FIFO, and polls every second while it holds; the
  check reads the bucket's queued rows before `m` (index
  `placement_barrier`). *Decision (#19, from the #20 review):* only
  `queued` rows hold the barrier. A row refused for good (by the place,
  `BadDigest` or `412`, or for the store) is not queued, so one hostile
  friend that refuses everything cannot stall metadata for the others;
  the object is then below `copies` and shows so. A queued put that a
  done write of the key before `m` replaced does not hold it either: it
  will be cancelled, not sent. One replaced only by a write after `m`
  still holds `m`, which was written while the put was the key's bytes.
  Because the place's FIFO
  waits behind a held metadata row, a lagging data copy holds back
  every place's metadata, so data rows are re-routed early: a `queued`
  data row at a friend unreachable for longer than
  `replication.reroute_after` (1 hour), or whose last answer was `507`,
  is given to another *reachable friend* with room by D6's choice
  (`state.DB.Reroute`, which the worker runs every minute,
  `Worker.RerouteEvery`), never to one that has or had a row of the
  object, never to an unreachable one (rows would move back and forth
  between friends that both stopped answering), and never to an overflow
  target while the unreachable friend still has room (REPL-12).
  The moved row keeps its sequence, so it takes its turn in the new
  place's FIFO ahead of later rows. Re-routing an unsent row moves no
  bytes. A row its sender has taken and not yet recorded an outcome for
  is not moved: should the send land, the place's row must be there to
  record it, or no later delete would reach the copy. The worker keeps
  that set (`Worker.sending`) under a lock that the sender takes its row
  under and a re-routing pass reads it under, so neither misses the
  other; a send that failed releases its row before the sender waits
  to retry. A pass reads without a transaction: the waiting rows of
  each friend to re-route, with their holders, in one query; and, once,
  each reachable friend's room before any bucket's metadata (quota less
  use less the reserve). A row's room at a friend is that, less its
  bucket's unsent metadata there (read once per bucket and friend),
  less what the pass has already moved there for any bucket, so moves
  of several buckets fill a friend once. Friends are scanned in node ID
  order and compared exactly, so the choice does not depend on the
  order of a map; a friend with no room for one more object leaves the
  candidates, so the remaining rows cost nothing. A row never goes
  to a friend that holds the object, nor to one this pass already gave
  another copy of it (an object waiting at two unreachable friends, with
  one friend with room, moves there once; its other copy waits).
  It then applies the moves in write transactions of at most 64 rows,
  each under the worker's lock, re-checking that each row is still
  queued and not being sent, so captures never wait behind a long pass
  (3000 waiting rows in a bucket of 1200 metadata objects take tens of
  milliseconds). A batch that fails is rolled back and the pass goes on
  with the next, returning the errors with what the others did, so one
  bad row cannot stop re-routing for the node. With no reachable friend with room the row waits at
  its friend, and the lag shows in `status`. *Decision (#19,
  review):* the row is dropped instead of waiting (state `dropped`:
  out of the FIFO, not counted in `place_use`) when its friend is gone
  past `replace_after`, or has answered `507` for
  `replication.reroute_after` and the row holds a metadata barrier
  (`Held`'s rule): a full friend would refuse it, and while queued it
  would hold every place's metadata and the full friend's later
  deletes for as long as the friend stays full. A shorter `507` is
  not enough, since the lender's count includes writes in flight and
  its floor (LEND-2) and may pass, and a dropped copy stays dropped
  until re-placement; a row that holds no barrier waits for the friend
  to accept it. The object is then below `copies`, which
  `status` shows and the `copies_dropped` alert raises (§3.8);
  re-placement (#26) places the copy when there is room. A dropped row
  still counts as the place's latest row of the key, so a later delete
  is sent there too; it is not a target for a healing re-upload.
  `dropped` is a copy never sent; it is not REPL-13's `dropping`, a
  copy the place holds that re-placement is deleting.
  Restic and Kopia write packs, then indexes, then the snapshot or
  manifest, so the barrier costs one round of acknowledgements per
  backup run, not per object.
- **REPL-13, the re-placement worker.** It runs after every change to
  the pool (friend added or removed, quota changed, a verification
  failed, an overflow target added) and hourly. It computes for each
  object the places that hold or will hold it (`acked`, `queued`, not
  `dropping`; a `dropped` row is a copy that was never sent, §3.4) and:
  - for each object with fewer than `copies` such places, places the
    missing copies by D6's choice, excluding the current holders; the
    put reads from the local store;
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
  and credential the user registered (S3-1). Every client is built
  with `DisableContentSha256` (an `UNSIGNED-PAYLOAD` body: over the
  tunnel's plain HTTP minio-go would otherwise SigV4-sign every 64 KiB
  chunk, and over HTTPS it sends unsigned anyway) and `MaxRetries: 1`
  (minio-go retries only seekable bodies, and the limited reader is
  not one; the worker owns retries). `TrailingHeaders` stays off. The
  clients are built in one package, `internal/replica`, which is the
  only code that imports minio-go (D12). Pinned at **minio-go v7.4.1**
  (Q-Y). In v7.4.1 `DisableContentSha256` is an option of each put
  (`PutObjectOptions`, `PutObjectPartOptions`), not of the client, so
  every put sets it. A friend's client (`replica.NewFriend`) sends
  `Host: lendisk-friend:9000` (the dial ignores it; versitygw checks
  the signature over it) and region `us-east-1`, so minio-go never asks
  for a bucket's location. It creates the sealed bucket at the friend
  before its first put to it (`BucketAlreadyOwnedByYou` is success).
  The dial is bounded by 30 s, the wait for a response's headers by
  2 minutes, and a body the transport takes no bytes from for 2 minutes
  cancels the request (a friend switched off mid-transfer).
- **Operations.** `Put` streams the object from the store through the
  decorator into `PutObject`, or for objects above the part size into
  the low-level multipart calls (`NewMultipartUpload`, `PutObjectPart`,
  `CompleteMultipartUpload`), with `x-amz-meta-lendisk-sha256` from the
  queue row. Until #23 every object is one `PutObject` (up to 5 GiB)
  and an interrupted one is sent again whole; the multipart path and its
  resume come with #23. The request checksum is SHA-256 (S3-3, Q-Y.2): a single
  PUT carries the queue row's hash as `x-amz-checksum-sha256`, at no
  extra read; a multipart upload is created with
  `x-amz-checksum-algorithm: SHA256` and each part carries its own
  SHA-256 as an unsigned trailer (`PutObjectPartOptions.Trailer`, a
  placeholder of the final length filled by a hashing reader at EOF),
  so each byte is still read once. Every object's capture hash is a
  whole-object SHA-256 (EP-3), so the worker chooses part sizes freely;
  the lender hashes the whole stream it receives and compares with
  `x-amz-meta-lendisk-sha256` (LEND-4). minio-go's *signed* trailer is never
  used: its framing is refused by versitygw and Garage. A target that
  refuses checksums gets none, per target. `Get` is `GetObject` of the whole object
  (`Friend.Get`, the read-through's fetch, §3.3 EP-10; a range would be
  a range option, which nothing needs yet).
  `Head`, `List` and `Delete` map to `StatObject`, `ListObjects` (with
  versions at an overflow target) and `RemoveObject`. For a friend,
  `List` is the control API call of §3.7 and `Undelete` is §3.6.
- **Errors.** The client's S3 errors are mapped to the replica
  interface's: a `507 QuotaExceeded` from a friend's decorator becomes
  "paused for quota" (LEND-2); a `403` that says this node is not a
  friend becomes "removed" (PEER-5, SEC-15): the tunnel middleware's
  `AccessDenied` whose message is `state.NoRelationship`, or
  versitygw's `InvalidAccessKeyId` (its IAM no longer resolves the
  node-ID key). Other `403`s are retried like any other error and
  reported as what they are: `RequestTimeTooSkewed` says the two
  clocks differ by more than 15 minutes and to enable NTP, and
  `SignatureDoesNotMatch` and the rest name their S3 code (#53). A
  friend that removed this node normally refuses the tunnel
  connection at admission (SEC-10) before any `403`, so until ending
  a friendship tells the other side (#25) such a place shows as
  `retrying` with a dial error, not `removed`; `removed` appears only
  on a connection opened before the removal, which the middleware
  still refuses request by request (SEC-11). A `412` becomes an
  immutability alert
  (EP-11); anything else is a retry with backoff. A `BadDigest` (also
  `InvalidDigest`, `XAmzContentSHA256Mismatch`) says the bytes the
  place received do not match the capture hash (REPL-10); the place's
  word alone never refuses the object for the others (SEC-7, #57). The
  worker re-hashes the store's copy (SHA-256 of the whole file, read
  again): if it still matches the capture hash, the place is at fault
  (buggy, version-skewed or hostile) and the row is refused at that
  place only, counted against it, with an alert, the place `ok`; if it
  does not, the store's copy rotted or changed outside lendisk, a local
  error (below) refused for every place with an alert (VER-5); if it
  cannot be read now, the row waits as for an unreadable store. A
  refused row is logged at error level with
  `alert=true`, and the refusal it records raises the
  `place_refused` or `objects_not_replicated` alert (§3.8, #13). A paused (`quota`) or
  `removed` place tries its head row again after an hour (LEND-2);
  other errors back off from 1 s, doubling to 5 minutes, with the place
  `retrying`.
- **Local errors (#50, #57).** The worker tells the store's failures apart
  from the network's and the place's. A row whose object the store
  definitely cannot supply as captured is a *local error*: the file is
  gone (`ENOENT`, or `ENOTDIR` for a path through a file), its size
  differs from the capture, it is a directory or special file, its key
  names no file under the bucket, it is larger than one PUT carries
  (5 GiB, `replica.MaxPutSize`, until multipart replication, #23;
  refused with cause `oversize`, *Healing* above), or
  its bytes no longer match the capture hash after a place answered
  `BadDigest` (above). Every other failure to open, stat or read the
  object (`EMFILE` under load, `EACCES` until a restore's ownership is
  fixed, `ENOMEM`, `EIO` from a disk dropping out) may pass and is
  never a local error: the row waits as for an unreadable store, below,
  since a refusal would break every snapshot that uses a restic pack
  for good. Errors are told apart with `errors.Is` on `fs.ErrNotExist`
  and `syscall.ENOTDIR`; tests inject open, stat and read failures
  through the worker's `open` hook. A local error is explained by a later write of the key only while a
  later capture exists (`state.LaterCapture`); since a done later
  capture supersedes the row first, that means while one is in flight
  (pending), which lasts until its write ends or, if the write ended
  without settling it, until `ResolveStranded` or the next start settles
  it (§3.3). Every write the decorator passes to the store has a row
  first; a retried multipart completion is passed on uncaptured only
  when its upload already has a done row or a pending row whose object
  landed, and posix then answers from the finished object without
  writing (§3.3 *Retried completions*). So a store that changed with no
  later capture changed outside lendisk. Only then does the row wait,
  polling every second, with the place's condition untouched; it is
  then cancelled as superseded (or sent, if that write was dropped and
  the store holds the captured bytes again). With no later capture the
  row is refused at once with cause `store`, for every place, with an
  alert, so a rotted or vanished object never stops a FIFO. Only the
  sender whose `RefuseObject` took rows out of the FIFOs raises the
  alert; another place's sender that comes second finds nothing queued
  and stays quiet. A directory
  marker (a key ending in `/` captured with no bytes, which the posix
  store keeps as a directory) is not an error: it is sent as an empty
  object. When the bucket's directory itself is missing (a store disk
  not mounted), or the object cannot be read now (above), no object is
  blamed: the row waits with backoff (1 s doubling to 5 minutes, for as
  long as it lasts), and the place's condition is `store` with the
  error as its detail, so `status` names the cause on every place the
  store holds up. Nothing is refused. The first store failure after any
  other outcome (an acknowledgement, a refusal, a network error at any
  backoff) is logged at error level with `alert=true` and starts its
  backoff at 1 s; the network's backoff never decides it (#61). The
  `store` condition raises the `store_unreadable` alert (§3.8, #13). A read
  of the store that does not return while an object is sent, or while
  it is re-hashed after a `BadDigest`, is such a failure: the store is
  read on a goroutine of its own and handed to the client through a
  pipe, and when one read does not return within
  `replica.Worker.StoreStall` (1 minute, half the client's 2-minute
  stall timeout) the pipe is closed with that error, so the request
  body fails at once. (Cancelling the put's context is not enough:
  net/http does not return while the body's `Read` is blocked.) The
  goroutine stuck in the read is left behind until the disk answers;
  the sender goes on, and a hung disk shows as `store`, not as the
  friend's `retrying`. Every open and stat before a read is
  guarded by the same rule (#63): opening and stating the object (one
  call), and stating the bucket's directory (to tell a missing object
  from a missing store) each run on a goroutine of their own
  (`replica.storeCall`) and fail as `store` when they do not return
  within `StoreStall`; when the sender stops, they return at once, so a
  wedged disk (a hard NFS mount) never holds up `Worker.Run`'s stop. A
  stuck open or stat goroutine is left behind until the disk answers,
  as a read is; a file it then opens is closed there, and one opened
  just as the sender stops is closed by the sender. Closing a file is
  not guarded (a regular file's close does not wait for a stuck read),
  and each stalled attempt leaves a goroutine and its thread behind, so
  a disk wedged for weeks piles them up (#67). A body that ends before the row's size
  (the file truncated under the sender) is the store's failure too,
  not the friend's: the read ends with "the object ended after n of
  size bytes" and the place shows `store`; the next try sees the size
  differ and treats it as a local error, above.
- **The worker** (`replica.Worker`, started by `serve` in the full
  profile only). It re-reads the friendships every 5 s and runs one
  sender per friend with an address, restarted when the address
  changes; a failed read keeps the senders as they are (#53). A sender takes the head of its FIFO (`state.NextPlacement`),
  sends it and waits for the answer before the next row, polling every
  second when the FIFO is empty. A put whose key a later completed
  capture replaced or deleted (`state.Superseded`) is cancelled, since
  the store no longer holds its bytes; deletes are always sent, and a
  key or bucket the friend lacks counts as deleted. A put reads
  `<store>/<bucket>/<key>` from the `posix` store as it sends it; what
  it cannot read as captured is a local error, above (#48, #50). On
  acknowledgement the row becomes `acked` with its hash and the place
  `ok`. Nothing is acknowledged before the place answers, so a restart
  of either side sends the head row again whole; the friend stores the
  same bytes under the same key, and nothing is sent twice that was
  acknowledged. `status` (and `places` in `status --json`) shows per
  place its condition, queued objects and bytes, lag (age of the oldest
  queued row) and the rows it refused; per bucket (`buckets[].refused`)
  the objects refused for a local error; refusals a later write healed
  are not counted (*Healing*, above).
- **REPL-6** The upload ID and the number, ETag and checksum of each
  completed part are written to the queue row as parts complete. After
  a restart or an interruption the worker lists the parts of the open
  upload (`ListObjectParts`), keeps the stored parts whose ETag
  matches, and continues at the next part, reading the store from that
  part's offset. A part the target acknowledged but the row missed is
  sent again; a `NoSuchUpload` (aborted or expired at the target)
  starts the object over. Shown with `SIGKILL` mid-part against
  versitygw and Garage (Q-Y.1). Objects below
  the part size (16 MiB, configurable) restart whole. The same for
  friends and overflow targets.
- **REPL-7** Per-peer and global limits are token buckets
  (`golang.org/x/time/rate`) wrapped around the reader of each uploaded
  part and of every download (read-through, VER-2 samples, `verify
  --full`);
  the time-of-day schedule is checked before each part, so a closing
  window stops a transfer within one part, which is part size over rate
  in time (16 s for 16 MiB at 1 MiB/s). Each read is cut to the
  smallest bucket's burst (64 KiB) before `WaitN`. Both change at
  runtime from the configuration: `SetLimit` applies from the next
  read (Q-Y.4).
- **REPL-8** `export --for <peer>` pages through the bucket through the
  decorator and writes one file per key at its key path (or Kopia's
  sharded layout), for each key with a `placement` row at that peer;
  `import` on the lender walks the directory and puts each file into
  the borrower's bucket through the decorator, and the borrower marks
  those rows `acked` once the peer's `list` (§3.7) returns them with
  the right hashes.
- **REPL-10** The hash is SHA-256, taken by the decorator from the
  write stream at capture (EP-3) and kept on the queue row; the worker
  sends it with the object as `x-amz-meta-lendisk-sha256` and stores
  it in the `lendisk.db` index on acknowledgement. The object is read
  once per replica, as it is sent. The lender compares what it received
  with the hash (LEND-4), so a local copy that rotted between capture
  and send is rejected and alerted rather than replicated.
  REQUIREMENTS.md v0.9 reworded REPL-10 to allow the hash to be taken
  when the tool writes the object, for exactly this. For an object the
  tool wrote as a multipart upload the hash is taken by reading the
  assembled object once at completion (EP-3), so every object has one
  whole-object SHA-256 whatever its part sizes, and EP-11 compares
  hashes directly.
- **REPL-11** The bucket's watermark is the highest sequence `w` such
  that every object up to `w` has `copies` `acked` placement rows; it
  advances as acknowledgements arrive and is the offsite-complete point
  (REPL-4). Per-place lag (the oldest `queued` row's age) is kept beside
  it for `status` and OPS-2.

### 3.5 Lending and quotas

The lender's policy on a borrower's requests lives in the decorator's
outer layer, `gateway.Lending` (`internal/gateway/quota.go`), which
knows the borrower from the request's account; it sits above the
capture decorator, so nothing stands between that and posix (#62).

- **LEND-1** `Lending` keeps a per-friend counter and enforces the
  friendship's quota (`Friendship.Quota`, the smaller number) on it.
  The counter holds the bytes of the borrower's objects and of the
  parts of its open multipart uploads, plus its tombstoned objects
  (§3.6), plus a fixed overhead of 4 KiB for each object, each bucket and each
  open upload. *Decision (#16 review):* the overhead stands for the
  inode, metadata and database row each item costs, so a borrower under
  quota cannot make empty objects, buckets or uploads without end. The
  counter lives in memory under one lock, loaded from `lent_usage` in
  `lendisk.db` once; every change is written to the database first
  and reaches memory only once the database took it. A put adds its
  bytes and overhead; a delete changes nothing (the object is
  tombstoned, §3.6) and the purge removes them; a bucket or upload adds
  its overhead; a part adds what it changes over the part of that
  number counted before (`lent_part`), in one transaction with
  `lent_upload.counted` and `lent_usage`, so settling a part costs the
  same whatever the upload holds and re-sent parts count once; a
  completion adds the object less its parts; an abort, or a refused
  completion, removes the parts and the upload's overhead. Each change
  is made holding the borrower's publication lock shared, from the
  moment the store may publish (a body's last read, or the store call)
  until it is counted; a part also holds its upload's lock over that
  span, so parts of one number are published and counted in the same
  order. The counter is reconciled against a listing (`ListObjectsV2`
  of each `lent-` bucket and its rows in `lent_bucket`, plus
  `ListParts` of each upload `ListMultipartUploads` shows, which also
  resets `lent_part`, plus the borrower's `tombstone` rows, since
  posix lists nothing under `.lendisk/`) once at every start, whatever the last run was,
  since one that did not finish leaves no trace, and then once a day,
  checked hourly. The reconcile holds the publication lock exclusively
  while it lists and sets the count, so every write is either counted
  or not yet published; it waits only for publications in progress,
  never for a body still streaming. *Decision (#16 review):* an upload
  that has had no part for 7 days (`Lending.AbandonAfter`, against
  posix's `Initiated`, which each part refreshes) is aborted by the
  reconcile and its `lent_upload` row dropped. Otherwise a row is
  dropped only when its upload is finished, aborted, or gone from the
  store; an upload begun long ago that still gets parts keeps it.
  `lending.max_total` bounds the sum of the node's own numbers;
  `peer quota` refuses a value that would exceed it (LEND-8). The
  borrower side keeps its own count of what it has stored at each
  place, for D6's free fraction (`place_use`, §3.4), and corrects it
  from the lender's answers: for now a `507`, which makes the place
  full until it acknowledges again; the control API's `quota` answer
  (LEND-7) will reconcile the count itself.
- **LEND-2** `Lending` refuses a borrower's `PutObject`,
  `CreateMultipartUpload`, `UploadPart` and `CreateBucket` of a new
  bucket with S3 error code `QuotaExceeded`, HTTP 507, when the counter
  plus what the borrower's admitted writes in flight may add plus the
  request (its `Content-Length`, plus the overhead for a new object,
  bucket or upload) would exceed quota, or when `statfs` on the store,
  less what every borrower's writes in flight may add and less the
  request, is below the floor (`store.free_space_floor`, default 5%).
  The check reads the counter under the same lock that admits the
  write. A completion is checked against the floor alone, for the size
  of its parts: posix assembles the object in a staging file while the
  parts still exist, so it needs that much again for a while; the
  parts are already counted against quota. A create of a bucket the
  borrower already has is not refused, and the same bytes put again
  add nothing and are not refused. The error is lendisk's own
  `s3err.APIError` (versitygw's `ErrQuotaExceeded` is a 403) and is
  returned unwrapped: versitygw type-asserts it, and a wrapped error
  becomes a 500 that clients retry (Q-I.5).
- **LEND-2, SEC-5 (stall bound)** The bridge bounds every request body
  on the tunnel, since nothing behind it does: fasthttp, behind
  `gw.sock`, reads the first min(`Content-Length`, 8 KiB) before any
  handler runs and the rest as the store asks, with no timeout.
  `TunnelS3Handler` sets the connection's read deadline
  (`http.ResponseController`) a stall period ahead before the body
  starts, and pushes it a stall period on each time 64 KiB more arrive
  (`bridge.Bridge.Stall`, 2 minutes, the borrower's own stall watchdog
  of §3.4, and `MinProgress`). A body that stops, trickles below that
  rate, or never starts though its headers promised one fails its read;
  the proxy drops the request to `gw.sock`, which ends it there, and
  answers `RequestTimeout` (400), which the borrower retries. The
  deadline is cleared when the body ends and when the handler returns.
  So a stalled body releases its quota and floor reservation within
  one stall period, and a body trickling at the minimum rate holds it
  until it ends (Content-Length ÷ the minimum rate, which can be long,
  but the reservation is within the borrower's own quota);
  an honest borrower whose lender's store stops reading for a stall
  period gets `RequestTimeout` and retries. Nothing reads the body but
  the store. A body longer
  than its `Content-Length` ends the write in `Lending` with
  `InvalidRequest`.
- **LEND-4** `Lending` hashes a borrower's body as it streams it to the
  backend (SHA-256) and compares with `x-amz-meta-lendisk-sha256` at
  the end of the stream: a mismatch fails that last read, so posix
  never publishes the object (it is deleted should it land anyway), and
  the answer is `BadDigest` (400). The hash is taken in lower case and
  stored so. There are no part or composite hashes: a multipart upload
  declares the whole object's hash at `CreateMultipartUpload`, which is
  kept in `lent_upload` with the upload ID, and after
  `CompleteMultipartUpload` the assembled object is read once, whole,
  and compared; a mismatch is removed and refused. The existing-key
  check happens before any byte is stored: the key's stored object
  carries its own `lendisk-sha256` (COMPAT-4), read with a `HEAD`;
  another hash is refused with `PreconditionFailed` (412), at
  `PutObject`, `CreateMultipartUpload` and again at completion. With
  the same hash the put is accepted without storing anything: its body
  is hashed and discarded, so a body that only claims the hash gets
  `BadDigest` and the stored copy is never touched; a completion with
  the same hash aborts the upload and answers with the stored object's
  ETag. A put takes no key lock, since a body that stalls (a borrower
  killed mid-transfer) would hold up its own retry. This relies on one
  store feature: a put of an absent key, and a completion, is stored
  with `If-None-Match: *`, which posix evaluates under its publish lock
  together with the publication, so of two racing writes of one key
  the second is refused (or accepted unstored if it carries the same
  bytes). Multipart create and completion and deletes hold a per-key
  lock; none of them waits on a body. A key with no object but a
  tombstone is checked against the tombstones' hashes the same way, at
  `PutObject`, `CreateMultipartUpload` and completion: no tombstone of
  that hash is `PreconditionFailed`, the same hash revives the object
  (§3.6). A retried completion of an upload already in the
  store (its `mp-metadata` upload ID) is answered by posix, and
  verified first if a crash came between completion and verification.
  A borrower's put or multipart create without the hash (64 hex
  digits) is refused with `InvalidRequest` (400); node-ID accounts are
  served on the tunnel only (SEC-12), so this is the tunnel side.
- **SEC-5** Every key a borrower names is checked, by every `Lending`
  method that takes one (writes, deletes, multipart calls, reads,
  heads, tagging, retention, legal hold, select; a test lists them all
  and finds any added later): 1 to 1024 bytes of UTF-8 without control
  characters, segments separated by single slashes, none empty, `.` or
  `..`, and not under `.lendisk/` (tombstones), `.sgwtmp/` or
  `.vgwlocks/` (posix's own), in any case; anything else is
  `InvalidArgument` (400). Besides, `OpenBackend` opens posix with
  `ExcludeDirs: [".lendisk"]` (#17), so for every account posix
  refuses a key with a `.lendisk` directory at any depth
  (`ExcludedObjectKey`), lists nothing under one, and does not take
  the store's own `.lendisk/` for a bucket. Buckets are confined by the mapping
  (PEER-8). `Lending` also refuses `DeleteObjects` to a borrower (NET-7
  lists it), so every delete is one counted `DeleteObject`.
- **LEND-6** The lending store is the set of `lent-<id16>-<sealed>`
  buckets in the node's one store, beside its own, plus versioning or
  tombstones and the hash index. With lendisk stopped they are plain
  files on `posix` (EP-15); `lendisk export --lent lent-<id16>-<sealed>
  <path>` writes the tool's own layout, excluding `.lendisk/`, naming
  the bucket as the store does since the lender does not know its real
  name (REPL-8, SEC-16).
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

- **RET-1** Retention is `Lending`'s (`internal/gateway/tomb.go`) on
  every store; versitygw's versioning is off on every bucket. A
  borrower's `DeleteObject` of a key that holds an object renames it to
  `.lendisk/tomb/<key>/<unix nanoseconds>` inside the same bucket (a
  rename on `posix`, so it never crosses a filesystem and keeps the
  inode and its xattrs, the hash among them; the tomb's directories are
  0700), removes the key's directories that leaves empty, and records a
  `tombstone` row (migration 19: borrower, sealed name, key, tomb path,
  hash, ETag, size, deleted at, purge after, pending). A delete of a
  key that holds nothing, tombstoned already or never stored, answers
  204 and changes nothing. From then on the key is absent from that
  borrower's GET, HEAD and listings (posix never lists `.lendisk/`),
  and the tomb's bytes and overhead count against quota as they did
  live (LEND-1: a delete moves no count). The `list` call carries each
  tombstoned key, with its newest tombstone's size, hash and
  `tombstoned_at`, merged in key order with the live keys; a key live
  again is listed live.
  *Window (#17):* the purge time is fixed at the delete: deleted at
  plus `gateway.Retention`, the longer of the friendship's two
  retention offers exchanged at redeem (`own_retention`, this node's
  offer as lender, its minimum; `peer_retention`, the borrower's offer,
  which is its request), 30 days when neither was recorded. So the
  lender's offer is a floor and a borrower can only lengthen the
  window, and both sides of a friendship use the same number. Changing
  an offer later leaves recorded purge times alone, as S3 Object Lock
  keeps an object's retain-until date.
  *Purge:* `Lending.purgeLoop`, at start and then hourly
  (`Gateway.PurgeEvery`), reads tombstones whose purge time is at or
  before `Lending.Now` (the gateway's injected clock) and, holding the
  key's lock and the borrower's publication lock, unlinks the tomb,
  removes the tomb directories that leaves empty, and drops the row
  and the bytes plus overhead from `lent_usage` in one transaction. A
  tomb that cannot be removed is logged and tried at the next purge.
  *Re-put:* a `PutObject` to a key holding no object but tombstones,
  with the hash of one of them, has its body hashed and discarded (a
  body only claiming the hash is `BadDigest`) and revives the newest
  tombstone of that hash: the row is marked pending, the tomb
  hard-linked back to its key (a link never replaces an object already
  there), the tomb unlinked and the row dropped; the count is
  unchanged. A completion of the same hash aborts the upload and
  revives the same way. Another hash is `PreconditionFailed` (412,
  LEND-4) before any byte is read, at `PutObject`,
  `CreateMultipartUpload` and completion, so a tombstoned key takes
  only its own bytes back until it is purged.
  *Crash safety:* the row is written, pending, before the rename and
  marked kept after it; a revival marks it pending before the link.
  At every start, before anything is served and before the reconcile,
  `settleTombs` settles each pending row from the store: no tomb at its
  path (the rename never happened, or the revival finished) drops the
  row, the object live where it was; a tomb beside a live object of
  the same hash (a revival linked back) unlinks the tomb and drops the
  row; a tomb alone, or beside other bytes, is kept. No order of steps
  leaves bytes without either a key or a kept row. *Accepted race:* a
  put of an absent key holds no key lock while its body streams, so a
  put that began before a delete of the same key can publish other
  bytes beside the new tomb; both are kept and counted, the list shows
  the key live, and a later delete makes a second tomb (tomb names are
  unique, never overwritten).
  A direct reader of the replica (P-5) sees the bucket as the tool left
  it after its last delete, plus `.lendisk/` and posix's `.sgwtmp/`,
  which restic ignores (`TestDirectReaderSeesTheToolsLayout` runs
  `restic snapshots` and `check --read-data` on a lent bucket); export
  and the recovery sheet's copy command exclude them.
  *Precedent (#17):* Backblaze B2's "hide" and lifecycle
  `daysFromHidingToDeleting`: a hidden file vanishes from listings, is
  still billed as stored, and is deleted after the days set; S3's
  delete markers likewise hide a version that stays stored and billed.
- **RET-2** The gate admits no versioned, lifecycle,
  bucket-deletion, object-lock, tagging or admin call from a borrower
  (§3.7 NET-7, `gateway.Surface`); `Lending` refuses a borrower's
  `DeleteObject` with a version ID, `DeleteObjects`, `DeleteBucket` and
  copies; keys beginning with `.lendisk/` are refused from borrowers by
  the gate and again by `checkKey` in `Lending`, and posix itself
  refuses and never lists a `.lendisk` directory at any depth
  (§3.5 SEC-5); and the purger is not reachable from any request. So
  nothing a borrower can send reaches the tomb directory or the
  purger. `TestBorrowerCannotPurgeEarly` is the RET-2 AC at the S3
  surface: with the pair's derived credential a borrower tries delete
  again, versioned deletes, multi-delete, overwrite, multipart over the
  key, completing an upload begun before the delete, every spelling of
  the tomb's path (read, write, delete, copy, tag), versioning then
  delete, a lifecycle rule and bucket deletion; the tomb's bytes are
  unchanged after each and after a purge one second before the window
  ends. `TestHostilePeer` (§3.9 SEC-7) runs the same list, in the same
  order, over the tunnel from both of the borrower's keys
  (`purgeAttacks`). Completing the upload begun before the delete
  while it is still open revives the tombstoned object, since it
  declared that object's hash, and stores none of the parts' bytes:
  nothing is lost, and a later delete starts a new window
  (`TestCompletingAnOldUploadRevivesTheObject`).
- **RET-3** `undelete` for a friend is the control API `undelete`
  (bucket, since): the lender moves each tomb copy deleted since that
  time back to its key and drops the row; `list` shows beforehand what
  that would restore. For an overflow target that versions, the
  borrower does not remove the delete marker, since S3-5's credential
  cannot remove versions: it copies the last version before the marker
  onto the same key (`CopyObject` with the source version id), which
  makes it current again and leaves the marker to the lifecycle rule.
  Elsewhere it clears the borrower's own client-side tombstones
  (S3-4).
- **VER-1** The challenge is a control API call: the borrower sends an
  object key, a byte range and a nonce; the lender reads the range
  through its decorator and returns HMAC-SHA256(nonce, bytes); the
  borrower computes the same over its local copy.
- **VER-2** Samples are whole-object `GetObject` calls by the replica
  client, hashed by lendisk and compared with the index; the method
  for overflow targets (S3-6), and for the evicted part of a bucket if
  evict mode arrives post-v1.
- **VER-5** The sampler hashes a weekly sample of at least 0.1% of the
  stored bytes through
  the decorator and compares with the index, on both sides; neither
  backend has a scrub of its own. On a mismatch the borrower deletes
  and re-fetches the object (EP-10).

### 3.7 Network

- **NET-1** Every peer connection is a WireGuard tunnel via tailcat; the
  lender's `AllowClient` hook admits known peer keys and pending invite
  keys and nothing else (SEC-10).
- **NET-2 to NET-4** magicsock hole punching, without port mapping in
  v1 (Q-R), and DERP fallback; `PathInfo` is built on `Client.DiscoPing`,
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
  endpoint; the S3 surface is governed by its own stability. The peer
  protocol version is `major.minor` (`control.Protocol`, now `0.1`);
  every control API request and answer carries it in the
  `Lendisk-Protocol` header. Two versions interoperate when the majors
  match and the minors are at most one apart. `GET
  /_lendisk/version` answers whatever the caller speaks, so it can
  learn why it is refused; every other call from a version outside
  that range, or without the header, is answered 400 with an error
  naming both versions and which side to upgrade, and the client
  refuses an answer whose header is outside the range the same way.
  The tunnel middleware's refusals carry the header too, on the
  control path and on S3, so every answer from a lendisk tunnel port
  has it; `control.Client` treats an answer without it, success or
  refusal, as an error: whatever answered is not a lendisk control
  API (#53).
- **The bridge.** `embedgw` takes listener addresses, not a
  `net.Listener` (D15), so versitygw is given one internal unix
  socket, `<data>/run/gw.sock`, and lendisk's bridge owns every
  listener in front of it. A unix socket path is limited to 108 bytes,
  so when `<data>` is too long for one, `gw.sock` goes in a private
  0700 directory from `os.MkdirTemp` instead, removed on stop. The
  listeners are `127.0.0.1:9000` (EP-1), each
  `[[endpoint.listen]]` address with its TLS setting (EP-17), and the
  one tunnel port, 9000, inside the tailcat `Server`. One code path
  accepts a connection, learns what it is allowed (a per-bucket
  credential on loopback and the LAN; a peer key on the tunnel,
  SEC-12), serves HTTP on it, and proxies every S3 request, unbuffered,
  to `gw.sock` with the `Host` header and every signed header passed
  unchanged so the client's signature still verifies. On the tunnel, a
  request whose path begins with `/_lendisk/` is the control API and
  is answered by the bridge directly; no S3 bucket can have that name,
  since bucket names begin with a letter or digit, so the two surfaces
  cannot collide. On loopback and the LAN that path is refused. Every
  other tunnel request is an S3 request: it is checked (SEC-11,
  SEC-12) and then proxied. versitygw terminates the signature;
  lendisk never computes one on any path. The hop costs one in-process
  proxy per request on every listener, loopback included (Q-L).
- **NET-7** The gate admits exactly these S3 operations from a
  borrower's account: `PutObject`, `CreateMultipartUpload`,
  `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`,
  `ListParts`, `GetObject` with Range, `HeadObject`, `HeadBucket`,
  `ListObjectsV2`, `ListBuckets` (own buckets only), `CreateBucket`
  (counted against quota) and `DeleteObject` without a version id (a
  tombstone, §3.6). Every other operation, including `DeleteObjects`,
  `CopyObject`, `UploadPartCopy`, every versioned call, tagging,
  lifecycle, object-lock and any bucket configuration call, is refused
  with `AccessDenied` before versitygw routes it. The refusal cannot
  live in the decorator, because versitygw itself calls
  `GetBucketAcl` and `GetBucketPolicy` on the backend for its access
  check on every request (Q-I). The gate runs before versitygw
  authenticates, so, like the bridge (SEC-12), it reads the access key
  from the request without verifying it and applies this list when the
  key is a node ID; versitygw then verifies the signature, so a forged
  key gains nothing. It admits a borrower's request only in the exact
  shape of one admitted entry of `gateway.Surface` (SEC-9): its method
  and level (service, bucket or object, from the path as versitygw's
  `DecodeURL` decodes it), that entry's query keys present (with
  `list-type=2` for `ListObjectsV2`), no other query key but the
  entry's `Allow` list, the SDK's `x-id` and `X-Amz-*` signature keys,
  and no header beginning `X-Amz-Copy-Source`, `X-Amz-Object-Lock-`,
  `X-Amz-Bypass-Governance-Retention`, `X-Amz-Tagging`, `X-Amz-Acl`,
  `X-Amz-Grant-` or `X-Amz-Server-Side-Encryption`. So a version id,
  another action's query key or a copy source turns an admitted
  request into a refused one, whatever versitygw would route it to; it
  is an allow-list of shapes, not a copy of versitygw's router. An
  object key must pass `checkKey` (nothing under `.lendisk/`, no empty,
  `.` or `..` segment), answered 400 as `Lending` answers it. The
  borrower list applies when any credential the request names is a
  node ID, read as versitygw reads it (`gateway.ReadCredentials`):
  versitygw v1.10.0 splits the Authorization header at its first space
  and removes every whitespace character from each part that holds a
  space before reading `Credential=` (`sigv4auth.ParseAuthorization`),
  so `Credential =<id>` or a node ID with a space or tab inside is that
  node ID to it, and to the gate. A whitespace-free spelling of any
  `Credential=` value, a SigV2 key and every `X-Amz-Credential` are
  checked too. A credential the gate cannot read the way versitygw does
  (two Authorization headers, another scheme, SigV2, a malformed header
  or `X-Amz-Credential`, an `AWSAccessKeyId`) is refused for every
  account, never read as "no friend"; versitygw would refuse it anyway.
  The control
  path is refused for every account both as sent and as decoded, so
  `/%5Flendisk/x` and `/_lendisk%2Fx` are refused as `/_lendisk/x` is
  (`TestGateRefusesControlPathAsFiberDecodes`). The control API
  (`redeem`, `challenge`, `manifest`, `quota`, `version`, `undelete`,
  `list`) is plain HTTP under `/_lendisk/` on the same tunnel port,
  authenticated by the tunnel peer key and checked per request against
  `lendisk.db` (SEC-11). `manifest` (`PUT` and `GET`, the caller's own
  sealed manifest, §3.1 ID-4) is served (#28). `list` is served (#29):
  `GET /_lendisk/list?bucket=<sealed>&after=<key>&limit=<n>` pages the
  lender's index for that borrower's bucket: key, size, SHA-256 and, if
  tombstoned, when (`tombstoned_at`, §3.6), as
  `{"objects": [...], "next": "<key>"}`, `next` absent after the last
  page; at most 1000 keys a page (`gateway.MaxListPage`), the default.
  Like `manifest` it is open to the `peer` role only and keyed by the
  resolved caller (`Caller.Peer`); no parameter names whose bucket, so
  a friend naming another's sealed name, or a bucket it never made,
  gets 404 with the code `NoSuchBucket` in the error body (`code`),
  the one answer a borrower reads as "holds nothing of it"; other 404s
  carry no code. The index is the store itself (`gateway.LentIndex`), not a
  table in `lendisk.db`: posix lists the `lent-<id16>-<sealed>` bucket
  in key order from `after` (as both `StartAfter` and the continuation
  token, since posix reads `StartAfter` only beside one) and each key's
  hash is the `x-amz-meta-lendisk-sha256` every borrower's object
  carries (COMPAT-4), read with a local head; keys under the reserved
  prefixes (SEC-5) are left out. So it needs no second copy kept in
  step with the store. Serving it still needs `lendisk.db`: the
  friendship that admits the caller and the `lent_bucket` row that
  makes the sealed name the caller's; a lender that lost its database
  serves no list until both are back (§3.1 ID-4). It is what `recover` (ID-5),
  `import` (REPL-8) and `undelete` previews read, so none of them
  needs a request per object. The borrower replicates to a friend with
  the same replica client it uses for overflow targets, dialling
  through tailcat as below. AC check: from an allowed peer,
  `restic -r s3:http://<lender>:9000/<bucket> snapshots` works with the
  derived credential.
- **NET-7a** The S3 credential for a borrower–lender pair is derived:
  access key is the borrower's node ID; secret key is
  `HKDF-SHA256(X25519(borrower key, lender key), info="lendisk-s3-v1")`,
  32 bytes in lowercase hex (`identity.S3Secret`). The access key is
  the node ID exactly in its display form; another spelling is not a
  node-ID key. The lender's `IAMService` resolves a node-ID access key
  only when a relationship exists for it in the `peer` role (SEC-11;
  an invite key gets nothing), since any node can derive a valid
  secret for its own key, and computes the secret on demand, reading
  `lendisk.db` on every lookup so a removed friend's next request is
  refused. The bridge refuses a request whose access key is not the
  tunnel peer key (SEC-12). The derived secret is never stored.
- **One key per engine: the identity key serves, a dial key per
  friend dials (#69).** A node's tailcat `Server` and each of its
  tailcat `Client`s hold their own connection to the relay. With one
  key for two of them, a node whose friends share its relay (the
  common case: friends in one region pick the same nearest relay) has
  two relay connections under one key, and derpserver delivers a
  key's packets to whichever of its connections last sent a frame
  (`lastWriterIsActive`, the default; Q-S noted the duplicate). A
  tailcat client drops the "meow" pings and WireGuard packets meant
  for the server. So once A's client had written last, B's client
  could not reach A's server at all, and B's failing client, pinging
  every second, then held B's key the same way: two failing clients
  kept each other failing until a restart dropped one's relay
  connection (#69; measured: A's server never logged B's ping, and
  closing A's client alone let B through at once). It is the #52 stall
  between a server and a client of one process. Two clients of one
  process with one key stall the same way: a review of the first fix,
  which gave every client one shared dial key, showed a download from
  B dropped while the client to C uploaded, every packet relayed
  (`TestTwoFriendsOnOneRelayCarryAtOnce`, with
  `TS_DEBUG_ALWAYS_USE_DERP`). So the identity key stays with the
  `Server` (the address, SEC-14) and each `Client` gets the dial key
  for its friend (ID-1): `transport.NewTailcat` derives it, and
  `transport.NewDialer` dials with the key it is given (the invite key
  of `peer add`). The listening side admits a friend's dial key with
  that friend's rights and resolves it to the friend's node ID before
  anything acts on it (`state.DB.TunnelPeer`, SEC-10, SEC-12).
  Friendships made before migration 13 have no dial key, and are given
  no fallback to the shared key: 0.1 is unreleased (no tag) and every
  install is a development one, while a fallback would keep the stall
  for those friendships and add a control call to swap keys later.
  Such a friend refuses this node's dials; drop the row on both sides
  and befriend again, which stores both dial keys.
- **A restarted friend.** tailcat admits a client as a WireGuard peer
  at its first ping only (the "meow" handshake, `Client.up`), and a
  restarted server has forgotten it while the client's session looks
  valid, so its dials time out. `Tailcat.Dial` keeps one client per
  friend, shared by every caller (replication now, the control API
  calls of #24 and #28 later), and counts its open connections and
  when one last moved bytes (a read or write through `tunnelConn`). A
  failed dial does not drop the client, since the caller's own context
  may have ended or the friend hiccupped: the transport checks the
  client with a dial of its own, bounded by 10 s, at most one at a
  time. A failed check closes and drops the client only once its open
  connections have moved no bytes for 30 s (`quietFor`); until then it
  waits out the rest of the 30 s and checks again (#58). A relay
  restart or Wi-Fi drop of 15 s thus leaves a multi-GB PUT running,
  whose TCP resumes; the price is that after a friend's restart a
  client with an open connection is replaced up to about 40 s later
  than a client with none, and a write that only fills the send buffer
  of a dead connection counts as bytes moved, which delays the close
  by one more round. Activity is stamped on the transport's monotonic
  clock (the time since the transport was made), so an NTP step of the
  wall clock on a board without a real-time clock neither delays a
  check nor cuts a live transfer (#60). The next dial after the close
  builds a new client, which pings first (#52), and not before the old
  one has finished closing; a dial that succeeded through the client
  as it was closed closes its connection and fails, so its caller
  dials again.

  Two clients of one key never run at one relay: tailcat's server
  keeps one WireGuard session per client key, so a live old client
  would fight its successor for that session (measured: a second
  client of the same key to the same server stalls the first one's
  connections). The client is keyed by the friend and its relay, not
  by the address string. tailcat's client connects to the first node
  of the address's region that answers, in order, and does not report
  which, so `relayOf` names every DERP server the address could reach:
  each node that is not STUN-only, by host name and by each IP it
  gives, with its DERP port (0 is 443); an address naming a public
  region by ID only gives that ID. tailcat strips region and node
  names from an address. Two addresses are one relay when those sets
  share a server, whatever the node order, so an address that keeps a
  server of the client's (a re-encoded region, a node added or removed
  by a re-init or a #51 re-redeem with a newer DERP map) reuses the
  client, and nodes with no host name are told apart by IP. The match
  errs towards one relay: a client wrongly kept fails its check and is
  replaced, while a second client wrongly built stalls the first. When
  the address moves to another relay, the old client takes no new
  dials and its check stops, but it stays up until its last connection
  closes, since clients on different relays coexist; if the address
  comes back to its relay first, that client is put back in service
  rather than a second one built. `Tailcat.Close` marks every client
  closed under its lock and closes them and the server after releasing
  it, since tailcat's shutdown may wait on the admission hook, which
  takes that lock.
- **Dialling a friend.** The replica client's HTTP transport uses the
  tailcat `Client.Dial` for that peer, on the dial key for it, as its dial
  function, so
  replication traffic enters the tunnel from inside the process. No
  listener is opened on the borrower's side for replication.
- **NET-7b** `peer forward` opens a listener on the loopback address
  the user names, for as long as the command runs, and copies each
  accepted connection into a tailcat dial to the friend's tunnel port,
  so a backup tool can read a friend's replica directly. It is the only
  forward in the design, and it is the listener NET-7b itself names.
  Since the forward is a byte copy, the tool must name the sealed
  bucket and sign with the derived credential; the command prints the
  credential as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` lines
  to paste into the shell, computed on demand (NET-7a) and never
  written, and then, from the borrower's own map, one line per bucket
  held at that peer, e.g.
  `laptop  restic -r s3:http://127.0.0.1:9100/s4k2…`.
- **NET-8** The internal `Transport` interface is `Listen`,
  `Dial(peerKey)`, `Admit(func(peerKey) bool)`, `PeerOf(conn)`,
  `Disconnect(peerKey)`, `PathInfo`. `Admit` must be called before
  `Listen`, and an implementation with no admission function refuses
  every connection; `PeerOf` returns the key that authenticated an
  accepted connection (SEC-12). These three are the contract any
  second implementation has to meet, whatever its own defaults are.
  tailcat is the only implementation; Syncthing's stack or iroh would
  be the second (D5). In code it is `internal/transport.Transport`,
  keyed by `identity.NodeID`, with two additions: `Address` and
  `Close`. `Listen` returns `ErrNoAdmission` until `Admit` is given a
  function, and the hook handed to tailcat is never nil and refuses
  while no function is set. `Dial` takes the friend's key and its
  address and refuses an address that names another key; it keeps one
  tailcat `Client` per friend (Q-S), on the dial key for that friend. `PeerOf` names
  the key a connection authenticated with, a friend's dial key
  included, and callers resolve it with `state.DB.TunnelPeer`. The tunnel port, 9000, is a
  constant, not a setting, since friends dial it without being told.
  `transport.NewDialer` is a dial-only transport with any key and no
  region, which never listens: `peer add` dials with the invite key
  through it. `serve` hands the tunnel listener to
  `bridge.ServeTunnel`. The tunnel listens before the gateway is
  ready, so its S3 handler is a `bridge.Late` that answers 503 until
  `serve` sets it to `Bridge.TunnelS3Handler` once `gw.sock` serves.

### 3.8 Operations and observability

- **OPS-2, OPS-3, alerts (#13).** Package `alert`. An alert is a
  condition read from `lendisk.db`, never an event: `alert.Service`
  re-reads every condition each minute (the first pass a minute after
  start, so the replication worker has contacted each friend again),
  and at once when the worker records a refusal or a store failure
  (`replica.Worker.Alerted` calls `Service.Check`, which never
  blocks). Table `alert` (migration 11) keeps one row per condition,
  keyed `kind` or `kind:subject`: `state.SyncAlerts` raises a row
  when its condition starts, keeps it (updating its message) while
  the condition holds, so an alert is raised once, and sets
  `cleared_at` when it ends. A later raise of the same key replaces the
  row, counts up its `raise_no` and is delivered again. The kinds so far:
  - `friend_unreachable`, subject the friend: raised at `now` when
    both hold: its place's condition is `retrying` (a network error)
    and `now − since ≥ alert.Service.MinRetrying` (1 hour), and
    `now − last_ok ≥ alerts.unreachable_after` (24h), using `since`
    when `last_ok` is unset (it never answered). A friend that went off
    at t0, tried next by a backup at t0+20h, is alerted at t0+24h; one
    tried next at t0+23h30m is alerted at t0+24h30m, when it has been
    retrying for the hour. The hour keeps a nightly backup that meets a one-minute
    blip 24h after the last answer from alerting at once, since
    reachability is learned only from replication attempts. So a friend
    with nothing queued is not judged until the periodic reachability
    probe (#65), which may let the hour be revisited;
  - `store_unreadable`, no subject: a place waits with condition
    `store` (§3.4 *Local errors*);
  - `objects_not_replicated`, subject the bucket: objects refused
    because the store could not supply them (VER-5, #50), until written
    again;
  - `place_refused`, subject the place: rows it refused for good
    (EP-11, a `BadDigest` for bytes that match, #57), until written
    again;
  - `pool_too_small`, subject the bucket: its `copies` exceed the
    places in the pool, friends with an address that do not refuse this
    node and are not gone past `replace_after` (`state.DB.PoolSize`,
    #19);
  - `copies_dropped`, subject the bucket: live objects below `copies`
    with a copy dropped at a full or gone friend (§3.4,
    `state.DB.DroppedBelowCopies`, #19), until written again or
    re-placed (#26). It raises at once: the worker calls `Alerted` when
    it drops one. A deficit from placement (the pool had no room for
    `copies` when the object was captured) shows in `status` as bytes
    below `copies` but raises no alert until #26's threshold alert.
  Later tickets add a kind to `alert.kinds` and its condition to the
  evaluator. Each raise is delivered at least once to each configured
  channel: success is recorded in `hook_at` or `webhook_at` against
  that raise's `raise_no` (so the send of an earlier raise never marks
  a later one, even within the same second), and a restarted daemon
  delivers what is not recorded. A crash between a send and its
  record, or a record that fails, delivers that raise again. A cleared alert
  is not delivered (decided for #13: the minimal option; the
  `event` field leaves room for `cleared`). The body is
  `alert.Message`, JSON:
  `{"schema":1,"event":"raised","kind":…,"subject":…,"message":…,"raised_at":RFC 3339 UTC,"node":node ID}`
  (`subject` omitted when empty; fields are added, never renamed).
  `alerts.command` is the absolute path of one executable program, run
  with no arguments and no shell, the body on stdin, delivered on exit
  status 0; `serve` refuses at startup anything else
  (`alert.CheckCommand`). The path may hold spaces when the whole
  string names such a file; when only its first word does, the rest
  are arguments and the refusal says to put them in a small script;
  a missing file is refused with "create it, or clear
  alerts.command". The hook runs in a process group of its own,
  killed whole when its attempt times out and again once it returns,
  so nothing it started outlives it, except a child that left the
  group with `setsid`. A hook that exits 0 but leaves a child holding
  its output is delivered once the 5 s wait for the output ends, with
  a warning, rather than sent again. Its output (the first 512 bytes) is logged
  when it fails, so a hook should not print secrets. `alerts.webhook`
  receives the same body as a `POST` with `Content-Type:
  application/json`, delivered on a 2xx; redirects are not followed,
  so a 3xx is a failure. Each attempt has 30 s; a failure is logged at
  warn level and every undelivered channel is retried after a backoff
  of 1 s doubling to 5 minutes, for as long as the alert holds.
  Delivery runs on its own goroutine and the evaluator on another,
  both only reading and writing state, so neither replication nor the
  gateway ever waits for a hook or a webhook. The webhook URL may hold
  a secret (a healthchecks.io UUID, an ntfy topic): it is logged as
  scheme and host only (`alert.Redact`), also inside connection and
  redirect errors, and a config refusal never echoes it. `status` and `status --json`
  (`alerts`: kind, subject, message, raised_at) show the pending
  alerts, also while the daemon is stopped.
- **OPS-4** `prometheus/client_golang` on a loopback port, not started
  in the lender-only profile (PEER-9). Transfer
  counters (bytes, objects, retries, lag, per place) come from the
  replication worker.
- **OPS-6** The configuration file is TOML and holds what the user
  sets; everything the daemon creates or learns is state in
  `lendisk.db`: friendships with both numbers, buckets, overflow
  targets, placement, probe results. The daemon never writes the
  configuration file. Store settings are one `[store]` section
  (`backend = "posix"` with a `path`; post-v1 `backend = "s3"` with an
  endpoint and a credential from the environment, SEC-3), used for
  both roles; replication settings, including the limits, the
  timetable, `reroute_after` and `replace_after`, are `[replication]`;
  the cap is `[lending]` (`max_total`); network addresses for the
  endpoint are `[[endpoint.listen]]` entries (EP-17), none by default;
  relays are `[relays]`, the address's bootstrap region `[tunnel]`
  (`region`, SEC-14) and alerts `[alerts]` (`command`, `webhook`,
  `unreachable_after`). There are no
  `[[peer]]` or `[[overflow]]` entries: `peer add`, `peer quota` and
  `overflow add` write `lendisk.db`.
- **OPS-7** Idle RSS is lendisk alone (versitygw, tailcat and the
  replica client included); active RSS adds one part buffer (16 MiB by
  default) per transfer in flight. Q-M measures both.
- **OPS-8** One binary; versitygw, tailcat and minio-go are pinned to
  exact versions by `go.mod` and `go.sum`, so their upgrades are lendisk
  releases. They are not vendored (decided 2026-11-06): the Go module
  proxy keeps every published version, and a vendor tree would add
  about 80 MB to the repository. State migrations are
  `lendisk.db`'s, forward and one minor version back. Each migration
  is one integer schema step that records a *compat* level: the oldest
  schema whose binary still runs safely on the result. A binary that
  finds a newer database opens it unchanged if that level is at or
  below its own schema, and refuses otherwise. Migrations in a minor
  release are additive (new tables, nullable columns), so the previous
  minor keeps working; a breaking step may lock out only schemas two
  or more minors old, which a test enforces. The database is copied to
  `lendisk.db.schema-<n>.bak` (`VACUUM INTO`) before migrating. The
  copy holds bucket secrets, so lendisk creates it empty at 0600
  first (`VACUUM INTO` accepts an empty file) and a failure to do so
  stops the migration (#53).

### 3.9 Security

- **SEC-3** Per-bucket S3 keys live in `lendisk.db`, which lendisk
  creates at mode 0600; versitygw's `IAMDir` is not used. An invite's
  secret is not stored at all: the inviting node keeps only the public
  invite key derived from it, and deletes that row once the invite
  expires (§3.2, PEER-1). Overflow target credentials (and, post-v1, a
  `backend/s3` store credential) are read from environment variables
  at start: `LENDISK_OVERFLOW_<NAME>_ACCESS_KEY` and
  `LENDISK_OVERFLOW_<NAME>_SECRET_KEY` for each overflow target, the
  prefix overridable with `overflow add --env-prefix`, and
  `LENDISK_STORE_ACCESS_KEY` and `LENDISK_STORE_SECRET_KEY` for a
  post-v1 store. They
  are supplied by systemd's `EnvironmentFile=` (a root-owned file the
  service user cannot read) or the container runtime's secrets;
  lendisk never writes them to disk, redacts them from logs and
  `status`, and refuses to start a store or overflow target whose variables
  are unset. Replica credentials are also carried in the encrypted
  node manifest (S3-1) for recovery. They are read by the process that
  uses them and never passed to another. The endpoint's TLS key
  (EP-17), where TLS is on, is `<config>/endpoint.key` at mode 0600
  when generated; a user-supplied key stays at the path the config
  names, and lendisk refuses to start if that file, or a generated key
  it would reuse, is readable by anyone but the service user. It is not in the manifest: a recovered
  node generates a new certificate, and clients are given the new one.
- **SEC-7** The hostile-peer harness (`internal/cli/hostile_test.go`,
  run by `TestHostilePeer`) drives real nodes over the local relay
  (`transporttest`). Its attacks are one request per S3 action in
  `gateway.Surface` (`s3Attacks`), SigV4-signed with whatever
  credential the attacker chooses, and every control API call
  (`controlAttacks`); a later ticket adds an attack or a phase there.
  With friendships A–B and A–C, a hostile B, holding every credential
  its node holds, sends A all of them from its node ID and from its
  dial key: everything outside NET-7's list is refused and A's store is
  unchanged; the admitted operations reach nothing but B's own
  buckets; each answer on the dial key equals the answer on the node
  ID; a request signed with the dial key, C's credential or A's node ID
  as the access key is refused; RET-2's attack list leaves B's
  tombstone intact; the redeem abuses of #69 are refused; and after B's
  friendship row is deleted, both keys are refused everything on their
  open connections. An unknown identity, and B at C (not its friend),
  are refused the connection. The `admitall` build
  (`TestMiddlewareAloneRefusesStranger`) sends the whole list from an
  unknown identity whose connection is admitted, and the middleware
  refuses every request (SEC-11). `peer remove` (#25) replaces the
  row deletion and adds the zero-peers-after-removal state of SEC-10.
- **Per-friend caps.** fasthttp's `MaxConnections` and `MaxRequests` at
  `gw.sock` are shared by every listener, so the tunnel bounds each
  friend before it gets there (`bridge.Caps`, #15): at most 32 open
  connections per connection key (`DefaultConns`; a friend has two
  keys, its node ID and its dial key), a connection over that closed
  unread at accept; and at most 16 requests in flight per friend,
  counted on the resolved node ID (`DefaultRequests`), one over that
  answered 503 `SlowDown` with `Retry-After: 1` (a JSON 503 on the
  control path), which S3 clients and the replica worker retry. The
  borrower's replica client moves one object per bucket at a time over
  a couple of connections (§3.4), well inside both. Loopback and LAN
  listeners are not counted. The caps are constants, not settings.
- **SEC-8** versitygw's web UI, website, metrics and event endpoints
  are not configured, and versitygw opens no TCP port at all. Its admin
  API cannot be left out: with `AdminPorts` empty, `embedgw` mounts it
  on the S3 socket, open to any account with the admin role, and
  `list-users` returns every secret the `IAMService` lists (Q-I). So
  the root account `RunVersityGW` requires gets a random secret per
  start, held only in memory; the `IAMService` never returns the admin
  role and refuses the user-management calls; and the gate refuses
  every `PATCH`, which S3 does not use and every admin route does. lendisk's
  listeners are the bridge (§3.7) on loopback S3, on each configured
  `endpoint.listen` address (EP-17) and on the one tunnel port inside
  tailcat; loopback metrics, except in the lender-only profile; the
  internal `gw.sock`; and the loopback listener `peer forward` opens
  while that command runs (NET-7b). tailcat's one UDP socket on all
  interfaces is the peer connection NET-1 names. There is no child
  process and no other socket. Every listener is one REQUIREMENTS.md
  names. Checked with `ss -lntup` and `ss -lxp`, with and without an
  `endpoint.listen` entry.
- **SEC-9** The classification is Go code: `gateway.Surface`, one entry
  per S3 action versitygw v1.10.0 routes (by its metrics action name,
  with a request that routes there), and `gateway.BackendMethods`, one
  per `backend.Backend` method. `TestSurfaceCoversEveryRoutedAction`
  reads the pinned `s3api/router.go` and fails on an action the table
  does not name, or one it names that is no longer routed;
  `TestBackendMethodsAllClassified` does the same against the
  interface by reflection; `TestSurfaceExamplesRouteAsNamed` sends each
  example through versitygw's own router and checks the action it
  reaches; `TestGateAdmitsNET7ListOnly` sends each, signed as a
  borrower, through the router behind the gate: an admitted one is
  routed, a refused one is answered by the gate and never routed. For a
  borrower's account:

  | Class | S3 actions | Backend methods |
  |---|---|---|
  | Admitted | `ListBuckets` (own only), `CreateBucket`, `HeadBucket`, `ListObjectsV2`, `PutObject`, `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `GetObject` (ranged), `HeadObject`, `DeleteObject` (no version id) | the same thirteen |
  | Refused at the gate | copies (`CopyObject`, `UploadPartCopy`), `DeleteObjects`, browser-form `PostObject`, `RenameObject`, `RestoreObject`, `SelectObjectContent`; list v1, `ListObjectVersions`, `ListMultipartUploads`, `GetObjectAttributes`, `GetBucketLocation`, torrent; ACLs, policy, ownership controls; bucket and object tagging; object-lock configuration, retention, legal hold; versioning; CORS and website; lifecycle, analytics, encryption, intelligent tiering, inventory, metrics, replication, public access block, logging, request payment, notification, accelerate; annotations; `DeleteBucket`; CORS preflight (`OPTIONS`); versitygw's error routes | every other method that an S3 action names |
  | Internal | none | `GetBucketAcl`, `GetBucketPolicy` (access check on every request), `GetBucketCors` (when a request carries `Origin`), `GetBucketVersioning`, `GetObjectLockConfiguration`, `GetObjectRetention`, `GetObjectLegalHold` (object-lock checks on writes and deletes), and `String`, `Shutdown`, `NormalizeObjectKey`, `StoresDotSegmentKeys`, `ValidateUploadID` |
  | Unreachable | none | `ChangeBucketOwner`, `ListBucketsAndOwners`: the admin API only, whose every route is a `PATCH` the gate refuses (SEC-8) |

  Every class but Admitted is decided at the gate, before versitygw
  routes or authenticates and before the backend is touched; `Lending`
  refuses the ownership-weakening calls, copies, `DeleteObjects` and
  `DeleteBucket` a second time. The table is re-reviewed on every
  versitygw upgrade, together with the replication-hole review (Q-H),
  and the two tests above fail when an upgrade adds an action or a
  method.
- **SEC-10** `Server.AllowClient` is always set, before `Start`, to a
  lendisk function that looks the key up in `lendisk.db`: a
  relationship row admits it in the `peer` role, an unexpired invite
  key in the `invite` role, and anything else, including a database
  error or a closed database, is rejected. The hook is never nil:
  tailcat admits everyone without one (its older `AllowedClients`
  list, which did the same while empty, is gone at the pinned commit). tailcat retries a rejected client
  about once a second, so the hook keeps a negative cache per key,
  cleared whenever a relationship or invite changes. The lookup is
  `state.DB.TunnelPeer` over the `friendship` table (keyed by the peer's
  node ID, and admitting the friend's `dial_key` in the same `peer`
  role, #69) and the `invite` table (keyed by the invite key's node ID,
  admitted while `expires_at` is in the future and either `used_at` is
  null or `redeemed_by` still has a friendship row, for PEER-2's
  retry). Deleting the friendship row refuses both keys at once.
  `state.DB.PeerRole`, which the `IAMService` uses, reads node IDs
  only, so a dial key is never an S3 account.
  The cache is `transport.Admission`: it remembers rejections only,
  never admissions or errors, each for 30 seconds
  (`transport.DeniedFor`), empties itself at 4096 keys, and
  `Admission.Clear` empties it after a commit. The expiry is for
  changes a one-shot command commits from another process (`peer add`
  writing the invited side's row), which cannot reach the daemon's
  cache. tailcat keeps a once-admitted key's tunnel, so a used invite
  key may still connect for a while; the middleware refuses it
  (SEC-11). The server starts
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
  to the next request (SEC-15). It is `bridge.TunnelHandler`: it puts
  the caller (the resolved peer's node ID and role; `bridge.RolesIn`)
  in the request context as a `control.Caller`, sends `/_lendisk/` to the `control.Server` and
  everything else to the S3 handler (#14). A refusal is a JSON error
  on the control path and S3 `AccessDenied` XML elsewhere; a failed
  lookup is a 503, never an admission. The `IAMService` repeats the
  relationship check when it resolves a node-ID access key, so a
  request reaching versitygw by any path is refused without one. A
  build tag `admitall`, used only by the SEC-11 test, replaces the
  admission hook with one that admits everyone, to show the middleware
  alone refuses an unknown peer; the daemon logs a warning when built
  with it, and CI runs that one test with the tag
  (`go test -tags admitall -run TestMiddlewareAloneRefusesStranger
  ./internal/cli`).
- **SEC-12** The bridge resolves each accepted connection with
  `Server.PeerKey(conn.RemoteAddr())` and puts the key in the request
  context through `http.Server.ConnContext`; a connection without a key
  is closed before any byte is read. For an S3 request it reads the
  access key from the `Authorization` header's `Credential=` or a presigned
  URL's `X-Amz-Credential`, without verifying anything, and refuses the
  request unless it equals the peer key; versitygw then verifies the
  signature for that access key. A request with neither form is
  refused, and so is an `Authorization` header in any scheme but
  `AWS4-HMAC-SHA256` (SigV2's `AWS key:sig` included) and a request
  whose header and query name two different keys
  (`bridge.AccessKeyOf`, `Bridge.TunnelS3Handler`). A friend dials
  with its dial key (ID-1), which the middleware resolves to the
  friend's node ID before anything acts on it; "the peer key" here is
  that node ID, so the access key and the per-pair secret stay keyed
  by node ID and a request signed with the dial key is refused. The control API takes the borrower from the context, never
  from a parameter. `redeem` is the one call whose connection is not
  the friend's own key; the friend's key comes from its address and is
  accepted only with the key-agreement proof of §3.2 (PEER-2).
  `bridge.ServeTunnel` does this: a listener wrapper resolves
  `Transport.PeerOf` at accept and closes a connection without a key
  unread, and `ConnContext` puts the key in the request context.
  *A friend's credential is served on the tunnel only (#15).* The
  loopback and LAN handler (`Bridge.LoopbackHandler`) refuses with
  `AccessDenied`, before the gateway sees it, any request whose
  Authorization header or presigned query names a node ID as the
  access key, SigV4 or SigV2 (`gateway.NamesFriend`). Off the tunnel,
  nothing binds the request to the connection's key (SEC-12), the
  per-friend caps do not apply and SEC-15's per-key connection closing
  cannot reach it; a borrower who could reach a lender's LAN listener
  and derive the pair's secret would bypass all three. No lendisk path
  needs it: replication, recovery and `peer forward` (NET-7b) all reach
  the lender's tunnel port, and the tool behind `peer forward` talks
  to a listener on the borrower's machine. *Precedent:* Syncthing
  accepts a device ID only as the TLS identity on its sync protocol
  listener, never on its GUI and REST listener, which has its own API
  key; Tailscale grants by node identity only for traffic that arrives
  over the tailnet interface, and `WhoIs` names no identity for an
  address outside it; MinIO is the counterexample, where a service
  account's key works on every listener unless a policy adds an
  `aws:SourceIp` condition. lendisk follows the first two: a
  credential is valid on the listener its identity was proven on.
- **SEC-13** In the `invite` role the middleware admits `POST
  /_lendisk/redeem` and refuses every other S3 and control API request,
  `version` included. `redeem` checks the role again; holding the
  invite key is the proof of the secret, so the secret never crosses
  the wire. It marks the invite used in the same transaction that
  creates the relationship row, and the invite key's connection is then
  closed; the borrower reconnects with its own key. A used invite key
  keeps the `invite` role until expiry while its redeemer is a friend,
  so the redeemer can retry a redeem whose answer it lost (§3.2,
  PEER-2); in that role it still reaches `redeem` alone. Over any
  unexpired invite, `redeem` answers a proven key that is already a
  friend with that friendship's terms (§3.2, #56); every other key
  finds a used invite used.
- **SEC-14** `DisablePresharedKey` is set and the `Server`'s `Key` is
  the identity's WireGuard key (ID-1), so the `tc…` address is a
  function of the public key and the bootstrap region only. The dial
  key that clients use is in no address; a friend learns it in the
  redeem, bound by the proof (PEER-2). The address embeds the
  region's relay nodes, not just its ID (Q-Q), so the whole
  `tailcfg.DERPRegion` is chosen once (the `tunnel.region` setting, or
  by latency when unset) and saved in `lendisk.db`, never in the config
  file (OPS-6), and `Region` is set from that saved copy at every
  start. Picking by latency, or re-reading the region from the DERP
  map, would change the address on restart or whenever the map
  changes. The region is saved as JSON under `tunnel.region` in the
  `node` table, chosen at `init` when the relays are reachable and
  otherwise at the first `serve`. `tunnel.region` takes a region ID or
  code; the saved copy is kept while the setting is empty or names it,
  and a setting naming another region chooses and saves that one,
  which changes the address (ID-7). The address is the CBOR `ConnInfo`
  of the node key, the disco key tailcat derives from it and the saved
  region, so `status` computes it from the key file and `lendisk.db`
  without the daemon (`transport.AddressOf`); a test checks it equals
  the running server's. Without a pre-shared key the tunnel is
  WireGuard's standard Noise IK handshake; what is given up is the
  pre-shared key's hedge against a future break of X25519, which for
  lendisk exposes S3 headers and control API traffic, never backup
  contents (P-3).
- **SEC-15** On `peer remove`, a quota change, or an invite's
  redemption or expiry, lendisk commits the change to `lendisk.db`,
  clears the hook's cache, cancels the contexts of in-flight requests
  from that key and closes its connections (the bridge tracks them per
  key, including the proxied half to `gw.sock`), and, when no
  relationship is left, calls `Server.DisconnectClient` for both the
  friend's node ID and its dial key, in that order. tailcat's disconnect alone resets nothing and cuts only the
  server-to-client direction: the client's packets still arrive on
  open connections for some seconds, and a connection closed after
  the disconnect never tells the client (Q-Q). So lendisk closes its
  own connections first, and SEC-11's per-request check refuses
  anything that arrives meanwhile. Later requests are answered
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
  `<store>/<bucket>/<key>` as plain files with versitygw's xattrs and
  its `.sgwtmp/` and `.vgwlocks/` housekeeping directories; borrowers'
  buckets under `lent-<id16>-` and their sealed names (SEC-16), each
  with its `.lendisk/tomb/` directory (§3.6: one file per tombstone at
  `.lendisk/tomb/<key>/<unix nanoseconds>`, ignorable by a direct
  reader); the friends' sealed
  manifests as `.lendisk/manifests/<node ID>` at the store's top (§3.1
  ID-4); `lendisk.db` and its tables; and the export layout
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
  --max-bytes …` writes a row in `lendisk.db` (state, not
  configuration, OPS-6) and reads the credential from the environment
  (SEC-3), testing it before the target is accepted; the credential is carried in the encrypted
  manifest for recovery, and the client is built from all of these at
  start. Each of the node's buckets is a sub-prefix
  `<prefix>/<sealed name>/` (SEC-16), with keys below it as the tool
  wrote them (S3-2).
- **S3-3** Uploads carry a SHA-256 request checksum, the object's on a
  single PUT and each part's on multipart (§3.4), plus lendisk's
  SHA-256 as metadata. Garage verifies both (Q-Y.2); a provider that
  refuses them is set per target to send none, and the sample
  downloads of VER-2 carry the check there. Large objects use
  multipart with the resume of §3.4.
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

| | v1 |
|---|---|
| Local copy | `S` |
| Space lent | `1.2 × c × S` |
| Total, `c = 2` | `3.4 × S` |

A post-v1 evict mode (EP-9) would bring the local copy down to about
1% of `S` for metadata plus a buffer for packs not yet acknowledged by
`copies` places, about `2.4 × S` in all.

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

- **Q-A (resolved)** `copies` defaults to 2, with a warning in
  `bucket add` and `status` when it is 1, however set (EP-6). Until v0.10 it was `min_replicas`, defaulting to 1.
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
  `Transport`. At the pinned commit the two-minute timeout applies
  only to incoming UDP flows; the tunnel port is TCP, which tailcat
  gives no idle timeout (spike #3, 2026-11-05).
- **Q-F (resolved, measured by G-3)** versitygw is embedded again
  (D15), via `embedgw.RunVersityGW(ctx, backend.Backend, *Config)` at
  v1.10.0. If G-3's listing measurement fails on `posix`, the decorator
  serves listings from lendisk's own index (Q-X).
- **Q-G (resolved)** No lendisk proxy and no SigV4 code in lendisk:
  versitygw terminates every signature, and the tunnel bridge passes
  requests to it with the signed headers untouched (§3.7).
- **Q-H (resolved at v1.10.0, #15; re-run on every upgrade)** The decorator must be checked against
  the full `backend.Backend` interface at the pinned version: every
  method that can create, replace or remove an object (including
  `CopyObject`, `UploadPartCopy`, `PutObjectTagging`, `RestoreObject`
  if present) is either captured or disabled. A method that mutates
  objects without being captured is a replication hole. Re-run on every
  versitygw upgrade, together with SEC-9's table. Seen at v1.10.0
  while answering Q-I: the methods that create, replace or remove
  object content are `PutObject` (also reached by a browser-form
  `POST`), `CompleteMultipartUpload`, `CopyObject` (including a
  same-key metadata replace), `DeleteObject` and `DeleteObjects`;
  `UploadPartCopy` writes a part only. `PutObjectTagging`,
  `DeleteObjectTagging`, `PutObjectAcl`, `PutObjectRetention` and
  `PutObjectLegalHold` change an object's metadata. `RestoreObject` and
  `SelectObjectContent` are not implemented on `posix`. *Settled for
  content (#6):* `PutObject`, `CompleteMultipartUpload`, `DeleteObject`
  and `DeleteObjects` are captured; `CopyObject` and `UploadPartCopy`
  are refused on own buckets by the decorator and the browser-form
  `POST` by the gate (§3.3 EP-2); posix's `DeleteObjects` deletes
  through its own `DeleteObject`, not the decorator's, so nothing is
  captured twice. *Settled for metadata (#15):* from a borrower, every
  metadata-only write (`PutObjectTagging`, `DeleteObjectTagging`,
  `PutObjectAcl`, `PutObjectRetention`, `PutObjectLegalHold`, the
  bucket-level tagging, ACL, policy, lock, versioning, CORS and website
  calls) is refused at the gate, and so are the headers that would do
  the same on an admitted write (`X-Amz-Tagging`, `X-Amz-Object-Lock-*`,
  `X-Amz-Acl`, `X-Amz-Grant-*`); `TestBorrowerRefusedPerCategory` has
  one case per category. On an own bucket they are neither captured nor
  refused, by decision: they create, replace or remove no content, a
  replica carries an object's bytes and its hash (REPL-10) and nothing
  else, and no supported tool sends them. In practice retention and
  legal hold fail there anyway (lock is never on) and so do ACLs
  (`BucketOwnerEnforced`); tags set by a tool live on the local copy
  only. `RestoreObject` and `SelectObjectContent` are refused for
  borrowers and not implemented on `posix`. Every `backend.Backend`
  method is now in SEC-9's table (`gateway.BackendMethods`), so an
  upgrade that adds one fails `TestBackendMethodsAllClassified` until
  it is reviewed here.
- **Q-I (gate, resolved 2026-11-05)** The embedded design depends on
  five properties of versitygw at the pinned version. Pinned at
  **v1.10.0** (tag commit `72f75bd`, 2026-10-22), pinned by `go.sum`; it needs Go
  1.26. Answered by the spike in `spikes/versitygw/` (issue #2): the
  `posix` backend behind a pass-through decorator and an external
  `IAMService`, tested with the AWS SDK for Go v2 and with restic.
  1. *Resolved:* `embedgw.RunVersityGW(ctx, backend.Backend, *Config)`
     takes listener address strings (`Config.Ports`), not a
     `net.Listener`. Given one unix socket path and nothing else, the
     process holds that socket, at the mode `SocketPerm` sets, and no
     TCP or UDP socket (`ss -lntupx`, and a test over `/proc/self/net`).
     Every listener is lendisk's bridge in front of it (§3.7).
  2. *Resolved, no patch:* `Config.IAMService` overrides every built-in
     IAM option, and the authenticated account reaches every backend
     call. versitygw passes the request's `*fasthttp.RequestCtx` as the
     `context.Context`; the auth middleware stores the `auth.Account`
     with fiber's `Locals`, which are fasthttp user values, so
     `ctx.Value(string(utils.ContextKeyAccount)).(auth.Account)` returns
     it inside the decorator. An unsigned request reaches
     `GetBucketPolicy` and `GetBucketAcl` (the public-bucket check) with
     a zero `Account`, so an empty `Access` means no account. The key
     is a string constant from an internal package and is re-checked on
     every bump.
  3. *No longer needed:* whether versitygw verifies
     `x-amz-checksum-sha256` on upload. REPL-10's hash travels as
     metadata and the decorator compares it itself (LEND-4). Spike #4
     found that it does (Q-Y.2).
  4. *No longer needed:* whether `posix` versioning gives RET-1 to
     RET-3 their semantics. Versioning is off; the decorator's
     tombstones are the only path (§3.6).
  5. *Resolved:* a backend error of type `s3err.APIError` reaches the
     client with its code, message and status: `QuotaExceeded` with
     507 and `PreconditionFailed` with 412 arrive intact, and a `HEAD`
     carries the status only. The error must be returned unwrapped: the
     controller uses a type assertion, not `errors.As`, so a wrapped one
     becomes 500 `InternalError`, which clients retry. versitygw's own
     `ErrQuotaExceeded` is a 403, so lendisk defines its own. The
     `AccessDenied` fallback is not needed.

  Found by the spike, and changing what SEC-8, NET-7 and D15 say:
  - The admin API cannot be left unconfigured. With `AdminPorts` empty,
    `embedgw` mounts it on the S3 socket (`PATCH /create-user`,
    `/list-users` and the rest), open to any account with the admin
    role. The root account `RunVersityGW` requires has that role, and
    `list-users` returns every secret the `IAMService` lists. lendisk
    sets a random root secret held only in memory, its `IAMService`
    never returns the admin role and refuses the user-management calls,
    and the gate below refuses every `PATCH`, which S3 does not use.
  - The decorator cannot refuse operations by `backend.Backend` method
    alone: versitygw calls `GetBucketAcl` and `GetBucketPolicy` on the
    backend for its own access check on every request. NET-7's refusal
    by S3 action goes in a gate, a fiber middleware passed through
    `Config.S3Options` (`s3api.WithMiddleware`), which runs before
    routing and authentication; the decorator keeps capture, quota and
    the bucket map.
  - Versioning is off by construction: without `VersioningDir`,
    `PutBucketVersioning` answers 501 `VersioningNotConfigured`. Object
    lock is not: `CreateBucket` with `x-amz-bucket-object-lock-enabled`
    succeeds on `posix` and the bucket then reports lock enabled. The
    gate refuses that header.
  - `posix.New` changes the process's working directory to the store
    root unless `PosixOpts.AbsolutePaths` is set; lendisk sets it.
  - restic 0.19.1 `init`, `backup`, `check --read-data` and `restore`
    work through a TCP-to-unix byte forward, and `restic -r
    <store>/<bucket>` opens the same repository with nothing running
    (P-5).
- **Q-J, Q-K, Q-N, Q-O, Q-P (superseded)** Questions about Garage as the
  storage engine (upload checksum verification, block-error reporting,
  listener set, `CopyObject` cost, byte-total freshness). They went with
  D14; Garage is now a tier-2 store behind `backend/s3` and an external
  replica target, covered by the capability probe.
- **Q-L (gate, open, measured with G-3)** End-to-end write throughput
  through versitygw and the decorator on `posix` on NVMe (EP-13, 200
  MB/s) and on a Raspberry Pi 4, including chunk-signature
  verification, and the replication rate from the store to a friend
  through the replica client and the tunnel on the Pi. Also the
  bridge's hop, now on every listener: loopback against a direct
  versitygw figure, and the LAN (EP-17) on the Pi over gigabit with
  TLS on and off, to see whether the hop or the TLS cost, not the
  link, is the limit.
- **Q-M (gate, open, measured with G-3)** Idle and active RSS of lendisk
  with a 1 TiB, 500k-object store and 10 peers, against OPS-7's 256 MiB
  idle, and with four transfers in flight.
- **Q-Q (resolved 2026-11-05, spike #3)** Pinned tailcat `main` at
  e00cdde (`v0.7.1-0.20261105151022-e00cdde6ff78`, with
  `tailscale.com v1.105.0-pre.0.20261013225543-fb79793d99f5`; Go
  1.27.2). The admission API §3.9 relies on (`Server.AllowClient`,
  `KeySet`, `Server.PeerKey`, `Server.DisconnectClient`) came in on
  2026-09-25 and 2026-09-26 and is still in no tag: v0.7.0
  (2026-09-18), the latest, has only `AllowedClients`, which admits
  everyone while empty, `AddAllowedClient` with no remove, and no
  per-connection peer key. On `main` `AllowedClients` is removed.
  Move to the first tag that carries this API. Every bump re-runs the
  SEC-10 to SEC-13 tests, since admission semantics have already
  changed once. The spike (`spikes/tailcat/`, local and real relays)
  showed: a hook rejecting everything is asked about once a second,
  the client never becomes a peer, and the listener accepts nothing;
  with the pre-shared key disabled the address is the same across
  restarts and holds only the node key, a disco key derived from it
  and the region's relay nodes; `PeerKey` names the admitted key.
  Two things §3.9 must keep in mind. The address embeds the region's
  nodes, so SEC-14 saves the whole `DERPRegion`, not its ID; with only
  `RegionID` the address follows the DERP map. And `DisconnectClient`
  cuts only the server-to-client direction: the WireGuard session
  stays, so the removed client's packets still reach open connections
  and can open new UDP flows (with no `PeerKey`) for some seconds
  after the call (8 in the spike); a new TCP connection does not
  complete. lendisk serves no UDP
  and SEC-11, SEC-12 and SEC-15 already cover the rest; SEC-15's
  order matters, since a connection closed after the disconnect never
  tells the client.
- **Q-R (resolved 2026-11-06: not in v1; post-v1, #46)** Does tailcat's engine
  run Tailscale's port mapper (UPnP, NAT-PMP, PCP)? No, at the pinned
  e00cdde. magicsock creates one only when
  `portmappertype.HookNewPortMapper` is set
  (`wgengine/magicsock/magicsock.go:681-697`), which only importing
  `tailscale.com/feature/portmapper` does; tailcat imports neither it
  nor `feature/condregister/portmapper`, and its release builds add
  `ts_omit_portmapper`. netcheck logs `portmap=?`. tailcat does not
  set `DisablePortMapper`, so lendisk turns it on with one blank
  import of `tailscale.com/feature/portmapper`; the spike's
  `-tags portmapper` build does, and netcheck then probes all three.
  Decided 2026-11-06: v1 does not link it. Many security-minded NAS
  owners keep UPnP off, so direct paths cannot depend on it; v1 relies
  on hole punching, explicit addresses (NET-3) and relays. Linking it
  behind a switch is post-v1 (#46), if G-1 shows too many relayed
  friendships among households that do have a mapping protocol on.
- **Q-S (open, measured with G-3)** Each tailcat `Client` creates its
  own userspace WireGuard engine, with its own UDP socket, DERP
  connection and netstack, so a node with ten lenders runs eleven
  engines with the server (confirmed in `Client.initLocked` at the
  pinned commit; if the port mapper is ever linked, Q-R, each would
  also run its own). Measure their RSS with Q-M against OPS-7.
  Fallback: dial lenders on demand and close a `Client` after an idle
  period, at the cost of a fresh hole punch per replication burst.
  A serving node that dialled a friend in its own region held two DERP
  connections with one key, its server's and its client's; derpserver
  keeps both and sends to the last writer. That stalled friendships
  (#69): a node's own client took the packets meant for its server.
  Since v0.16 each client dials with the dial key for its friend
  (ID-1, §3.7), so the server and each client have a key of their own
  at the relay, and no two connections share one: two clients to the
  same friend never run at one relay (#52), and a dial key belongs to
  one friend. What is left for G-1 is the cost of the engines, not the
  key.
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
- **Q-V (post-v1, deferred with EP-16)** `backend/s3` pass-through. Multipart and
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
  in one bucket on `posix`. Testable in a day. Object lock semantics
  were part of this gate until v0.12; object lock is no longer used.
- **Q-Y (gate, open: item 5, and 1 and 2 at RustFS and B2)**
  The replica client (D12, §3.4), against the pinned minio-go, before
  the worker is built on it. Pinned at **minio-go v7.4.1** (2026-10-12)
  with `golang.org/x/time` v0.16.0. Items 1, 2 and 4 were answered on
  2026-11-06 by the spike in `spikes/minio-go/` (issue #4), against a
  versitygw v1.10.0 friend over TCP and a `dxflrs/garage:v2.5.0`
  container; no item needed the AWS SDK.
  1. *Resolved for versitygw and Garage:* a multipart upload killed
     with `SIGKILL` mid-part resumes at the next part after restart,
     using only the upload ID and the part ETags. A subprocess sending
     six 5 MiB parts was killed 300 ms into part 4; the target listed
     parts 1 to 3 and not the partial one, the restarted process
     checked the stored parts with `ListObjectParts`, sent parts 4 to 6
     only (13.6 of 29.4 MB read again) and the object's SHA-256
     matched. Same with and without the SHA-256 trailer. The loop uses
     `minio.Core`'s `NewMultipartUpload`, `PutObjectPart`,
     `ListObjectParts` and `CompleteMultipartUpload` only (§3.4,
     REPL-6). RustFS and B2 are left to the tier-1 CI.
  2. *Resolved for versitygw and Garage:* both accept **and verify** a
     SHA-256 request checksum (a wrong one gets `400 BadDigest` and
     `400 InvalidDigest`; versitygw thereby answers Q-I.3 too), and both
     accept none. minio-go sends none by default, unlike the AWS SDK:
     over plain HTTP it signs every 64 KiB chunk
     (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`), over HTTPS it sends
     `UNSIGNED-PAYLOAD`. Its `TrailingHeaders` option turns on CRC32C,
     and on multipart over plain HTTP that goes in a *signed* trailer
     which both targets refuse, as they do a SHA-256 in one: minio-go
     ends the trailer line with `\n` before the `\r\n` the format
     expects. What works with one read of each byte is an unsigned
     payload with the part's SHA-256 in an unsigned trailer
     (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`), and on a single PUT the
     queue row's hash as a header. S3 has no whole-object SHA-256 for
     multipart. §3.4 and S3-3 are written to match. Tier-1 providers
     are left to their CI.
  3. *Resolved 2026-11-06 (#18):* a friend's `507 QuotaExceeded`,
     `403 AccessDenied`, `412 PreconditionFailed` and `400 BadDigest`
     reach the worker as four distinct errors (`replica.ErrQuota`,
     `ErrRemoved`, `ErrImmutable`, `ErrRejected`), and a `500` or `503`
     as none of them, through a real tailcat tunnel with minio-go
     v7.4.1 (`TestFriendRefusalsAreDistinct`). minio-go parses the
     error document of a `PUT` whatever the status. Since #53 not every
     403 means removed: only an `AccessDenied` whose message names
     `state.NoRelationship` and an `InvalidAccessKeyId` map to
     `ErrRemoved`; any other 403 (`RequestTimeTooSkewed`, a bad
     signature) is retried and reported as what it is
     (`replica.classify`).
  4. *Resolved:* the limiter and the timetable hold to within one part.
     At 4 MiB/s each 5 MiB part took 1.236 to 1.255 s at both targets
     and a 20 MiB download 4.99 to 5.00 s; `SetLimit` from 2 to 8 MiB/s
     mid-part gave 8.14 MiB/s within 250 ms; two places at 4 MiB/s
     under a 6 MiB/s global bucket ran at 6.006 MiB/s combined. A window
     closing 10, 50 and 90 % into a part stopped the transfer at the end
     of that part (0.95, 0.50 and 0.10 parts after the close at most)
     and the reopened transfer sent only the remaining parts.
  5. RSS per transfer in flight (with Q-M) and throughput to a friend
     on a Pi 4 (with Q-L). The client's per-chunk signing is gone with
     the unsigned payload, so the SHA-256 work per byte sent is the
     part checksum's alone.
  If minio-go fails these in a way configuration cannot fix, the AWS
  SDK for Go v2 behind the same interface (D12).
- **Q-Z (open)** Lent buckets on a second disk under one `posix` root.
  A lender who mounts or links another disk at a `lent-*` bucket
  directory needs versitygw's temp directory to stay on the same
  filesystem as the bucket, or its temp-then-rename crosses a mount;
  the tombstone move (§3.6) stays inside the bucket and is safe either
  way. Check in the pinned source and with
  a bind mount in CI. Fallback: a second `posix` backend instance for
  `lent-*` buckets with a mux by bucket name, for that configuration
  only. The pinned source (v1.10.0, read 2026-11-05) puts the temp file
  in `<bucket>/.sgwtmp` (`O_TMPFILE` where supported, then linked into
  place) and multipart parts in `<bucket>/.sgwtmp/multipart/`, so a
  disk mounted at the bucket directory keeps both on the bucket's
  filesystem and the fallback should not be needed; the bind-mount
  check in CI remains. A symlinked bucket directory is served only with
  `PosixOpts.BucketLinks` set.
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
