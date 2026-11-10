# lendisk — design

Status: draft v0.18, 2026-11-08. Companion to REQUIREMENTS.md v0.11.4.
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
of its connections to write (§3.1, §3.2, §3.7, §3.9; #69). v0.17
follows REQUIREMENTS v0.11.3 (#74): a profile names the keys its tool
rewrites in place; the borrower sends a changed version of one as a
rewrite marked with the hash of the version it replaces
(`x-amz-meta-lendisk-replaces`); the lender stores it and keeps the
replaced version as a tombstone for the retention window; and
recovery reads those tombstones to tell the newest version where
friends disagree (§3.3 EP-7, §3.4, §3.5 LEND-4, §3.6 RET-1, §3.1
ID-5). v0.18 follows REQUIREMENTS v0.11.4 (#79): a Kopia format-1
repository, told by the blobs only that format writes, is shown in
`status` and alerted until Kopia's upgrade, never refused (§3.3 EP-7,
§3.8, §3.10 COMPAT-1). The trust model is unchanged throughout.

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
  the same key with different content is a violation, unless the
  bucket's profile names the key rewritable (EP-7: a rewrite, whose
  replaced version each lender keeps as a tombstone). The decorator
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
     metadata object of the bucket first (§3.4). *Split* (REQUIREMENTS
     v0.11.5, Q-AD), for a bucket with `copies` ≥ 2 (a copies-1
     bucket's one copy goes to a friend while one has room): while
     the capacity plan says the friends cannot
     hold `copies` copies of the repository (`state.FriendsHold`, §4's
     rule without overflow) and an overflow target exists, step 2
     takes at most `copies − 1` friends and step 3 the rest, so the
     friends' room goes to one copy of every object instead of
     `copies` copies of the first ones; with no target that can take
     the copy, a friend takes it after all. Re-placement follows the
     same rule, and moves one friend copy of an object held only at
     friends to a target (§3.11).
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
| Metrics | none: the text exposition format written by hand (`internal/metrics`) | — | Decided for #43 (§3.8 OPS-4): `client_golang` v1.25.0 added 3.1 MB (5.2%) to the amd64 binary and 3.0 MB (5.2%) to armv7, and seven modules (protobuf, procfs among them), for counters and gauges a scrape reads from `lendisk.db`. |

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
with the queue row's placement at that place (table `transfer`, #23) so
a restart continues at the next part (REPL-6); a token-bucket limiter
(`golang.org/x/time/rate`, `internal/pace`) around each body and a
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
  S3-1, with region, `max_bytes`, `env_prefix`, retention and price
  added in #38 within format 1; the credential is read from the
  daemon's environment when the manifest is built, and absent while it
  is unset) and `buckets` (name, sealed name,
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
  A former friend's manifest is kept, but no longer served: the
  manifest call needs the `peer` role, and ending the friendship
  refuses both of its keys (SEC-10). *Decision (#25):* a removed
  friend cannot fetch its manifest here during the window; its other
  friends hold the same manifest (every friend gets it), so recovery
  does not need a former friend, and admitting the key for one call
  would undo SEC-15. The file is deleted with the friend's replica at
  the purge (`manifest.Store.Delete`, §3.2 PEER-5), and a push in
  flight when a friendship ended records no delivery row
  (`TestPusherStopsAtARemovedFriend`). It is a
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
  #30). The queue rows recovery writes are not held locally
  (`queue.local`), so the store reconciler never takes a recovered
  object's absence for a delete (§3.3 EP-14, #8). The identity key is saved after everything else recover writes
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
  (`printOverflow`, naming the variables each goes in). `recovery.Restore`
  adds each target's row back from the manifest, all but the
  credential (#38 review), so the rebuilt node's manifest still names
  the target rather than erasing the only copy of its endpoint, and its
  daemon refuses to start until the credential is in its environment.
  The target is not listed: its copies are placed again as if missing.
  The
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
  `keys/*`.
  *Rewritable keys (#74).* Friends may hold different versions of a
  key the profile names rewritable (one was offline for the last
  Kopia maintenance run, or a password change), and with two friends
  that is a tie. The tie is broken by the lenders' tombstones: each
  place lists with a live key the versions its rewrites replaced and
  still keeps (`replaced`, §3.6 RET-1), recorded per place
  (`listed.replaced`, `listed.rewritable`, migration 20). A version
  that a place listing another version replaced is older than that
  one. If exactly one listed version is replaced by no place, it
  counts (`state.VerdictNewest`, "newest"), held where it is listed,
  and the read-through serves it; `bucket resolve` may still pick
  another. The user sees each such key: `recover` prints them, naming
  the friend whose replaced versions decided it, and `status` lists
  them with every friend's hash and the `bucket resolve` command for
  each other version (`recovery.newest` in `status --json`: bucket,
  key, `sha256` used, `decided_by`, reason, places;
  `state.NewestKeys`; `TestStatusAndRecoverShowNewestVerdicts`). If
  none or more than one is left, the tie is a conflict as
  before: a friend that lagged by more than the retention window (its
  version's tomb is purged at the others), or places naming each
  other's version, which only a lying friend does. Only ties are
  broken: a strict majority still wins, so one hostile friend cannot
  outvote honest ones by claiming to have replaced their version. In a
  one-to-one tie it can make its own version count so; `bucket
  resolve` undoes that, and a forged version fails the tool's MAC
  (EP-8). The queue row recovery makes for the winner is marked
  rewritable, so the node's next write of the key is sent as a
  rewrite (§3.4), and a friend holding an older version takes it
  through the worker's ask-and-retry. Tests:
  `TestRewritableKeyTieBrokenByTheReplacedVersions` (`internal/state`),
  `TestRecoveryCountsTheNewestRewrittenVersion` (`internal/recovery`).
  The
  verdict is made again as each further list arrives (a friend
  returning breaks a tie 2 to 1), and whenever a list ends. Only the
  version that counts has a queue row (`put`, `done`, with the listed
  size, hash and the class the bucket's profile gives) and `acked`
  placement rows, at exactly the places that list that hash, sized as
  each place counts it, so `place_use` holds what each friend holds; a
  conflict has neither, so no place counts as holding it, and
  `state.KeyConflict` is what the read-through (#30) checks to refuse
  it with the reason. A key this node has written since recovery is
  its own: lists only add the places holding that version. The rows are
  acknowledged with the friends' hashes, so the node's first write of a
  recovered key with other bytes, the key not rewritable, is a changed
  key (§3.4 *EP-11*, #22). `status`
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
  transactions of 2,000 keys or 250 ms (`state.SettleBudget`),
  whichever ends first, so a slow machine (an armv7 board, the race
  detector) settles fewer keys per batch rather than holding the writer
  for seconds, with a 100 ms pause between them, longer than SQLite's
  busy handler polls, so the gateway's captures wait a fraction of a
  second, never the whole bucket (200,000 keys abandon in about 16 s
  with no other write waiting over 0.1 s; under the race detector
  20,000 take about 26 s with none over 0.35 s). A batch prepares each
  statement once (`preparedTx`): the same dozen statements run for
  every key, and compiling them each time cost most of a batch.
  `settled` marks a
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
- **ID-7** `lendisk recovery-sheet` (#33, package `sheet`) prints the
  sheet from the manifest's content (`manifest.Build`, ID-4): plain
  ASCII within 80 columns, a `tc…` address cut into lines to type as one
  string. In order: the phrase, the node ID, each friend's ID and
  address, each overflow target's name, endpoint, bucket and prefix with
  the note that its access key is needed (the keys themselves only with
  `--with-secrets`, S3-8; "none" while the list is empty, until #38),
  the restore commands (`lendisk recover <address>`, `lendisk serve`,
  `lendisk bucket show`, `restic restore`/`kopia snapshot restore`),
  each bucket with its profile, `copies`, sealed name, the
  `lent-<id16>-<sealed>` name it has in every friend's store (SEC-16)
  and `s3://<bucket>/<prefix>/<sealed>/` at each overflow target, and
  how many replicas a restore without lendisk combines: N − c + 1 with
  N the friends plus overflow targets, one when N ≤ c, none with no
  place. Then the lendisk-free restore (P-5, LEND-6): a
  friend hands over a replica made by `lendisk export --lent <stored
  name> <dir>` (`sheet.ExportCommand`, the arguments `lendisk export`
  takes, §3.5 LEND-6; `--kopia-layout` for Kopia), or, for restic, a copy of
  the bucket's store directory made with its lendisk stopped; an
  overflow target's keys are copied with any S3 client. The replicas,
  each in a directory `replica-1`, `replica-2`…, are combined as a plain
  union (one key has the same bytes everywhere, EP-11), leaving out the
  housekeeping directories `gateway.ReservedDirs` (`.lendisk/`,
  `.sgwtmp/`, `.vgwlocks/`): `cp -R "$r". repo/` per replica then `rm
  -rf` of the three, or `rsync -a --exclude=/.lendisk/ …`
  (`sheet.CombineCp`, `sheet.CombineRsync`; the test runs the printed
  commands). Last, the reminder that the restic or Kopia password is a
  separate secret. *Decision (#33): the phrase.* It is never stored
  (ID-3), so the sheet has a labelled blank for each of its 24 words;
  `--with-phrase` reads it from stdin (the prompt on stderr, stdout
  being the sheet), refuses one that does not derive this node, and
  prints it with a warning; nothing logs it and the record below omits
  it. `--json` prints the same `sheet.Sheet`.
  *Regeneration.* Each print records the sheet's content, without
  time, phrase or keys, in the `node` key `recovery_sheet` (with
  `printed_at`; no migration). The alert service compares it with the
  content built now (`sheet.Stale`, injected as `alert.Service.Sheet`)
  and raises `recovery_sheet_stale` (§3.8) naming what changed: a
  friend's address (a friend's `tc…` string changes only when its key
  or bootstrap region does, SEC-14, so this is rare), a friend,
  overflow target or bucket added or removed, a bucket's `copies`. A
  node that never printed a sheet is not alerted (*decision, #33*):
  nothing on paper can be stale. Former friends in their retention
  window are not on the sheet: they no longer admit this node.
  *Typing the address back (#35).* `lendisk recover` takes one or more
  arguments and removes all whitespace from them (`typedAddress`): an
  address never holds any, so one copied with the sheet's line breaks
  and indent, or typed with spaces the shell splits into several
  arguments, is the same string
  (`TestRecoverTakesAnAddressTypedWithTheSheetsLineBreaks`). A damaged
  one is still refused by `transport.KeyOf`. *The printed steps end to
  end:* `TestScenarioZeroLockIn` reads A's printed sheet for the
  bucket's name at every friend, runs its export command on two
  friends, its combine command and its restore line, and `restic check
  --read-data` (§3.10 COMPAT-4); the Kopia test does the same with
  `--kopia-layout` and the sheet's `kopia repository connect
  filesystem` line.

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
  number by itself. When the repository is above 90% of the protectable
  size, `status` and the OPS-2 alert name, per friendship with the least
  room, the number that would cover it at `copies` (§3.5 LEND-7, #37:
  the shortfall shared by water-filling); the two people set it.
  *Realised (#24).* `lendisk peer quota <node ID> <size>` commits
  `state.DB.SetOwnNumber` (a malformed ID or size is a usage error, a
  stranger "not a friend"); it refuses with `state.MaxTotalError` a
  number that takes the sum past `lending.max_total`, the sum being
  every friendship's own number, every unused, unexpired invite's
  number and every redeem in progress (`state.DB.LentNumbers`), since
  each becomes a friendship's number; `invite --quota` and `peer add`'s
  number are checked the same way, in the transaction that stores the
  invite or the redeem record (`state.DB.AddInviteWithin`,
  `StartRedeemWithin`; write transactions are `IMMEDIATE`, so two
  commands cannot both pass the check), a redeem leaving out other
  redeems with the same inviter, which it settles; the comparison is
  `n > max − others`, which cannot overflow. *Decision (#24 review):*
  unset, `lending.max_total` is what the store's filesystem can hold:
  its size, read with the store checker's `Statfs`, less the LEND-2
  free-space floor (`app.maxTotal`). LEND-1 requires a cap, and the
  disk is the one a node has before `init` asks for the repository
  size (LEND-7, whose suggestion is the value to set); a set value
  overrides it, for a filesystem that is shared or will grow. `peer
  list` and `status` (`lending` in `status --json`: `max_total`,
  `from`, `numbers`) show the effective cap and what the numbers and
  open invites take of it. `peer quota` prints the new quota each way, says
  when it stays until the friend raises theirs, and, when the quota is
  now below what either side stores, that new objects stop and nothing
  is deleted. Migration 25 adds to the friendship row what the quota
  call learns: `told_number` (this node's number as the friend last
  acknowledged it), `seen_at` (last call or answer either way),
  `peer_repo_size`, `peer_use`, `lent_there`, `peer_protocol`, `path`
  and `exchange_error`.
  The quota call is `POST /_lendisk/quota` (§3.7), made by the daemon's
  `quota.Exchanger` (`internal/quota`): a friend is called when its
  `told_number` differs from the own number (a change, made by `peer
  quota` in another process, is seen within the exchanger's 10 s poll,
  or at once after a friend's call or a redeem in this process), on
  `Exchanger.Due` (the replication worker's `Full`, on a `507`, #68),
  and otherwise an hour after its last answer, less a random jitter of
  up to a tenth of that (`Exchanger.Jitter`), so friends' calls spread
  out. A failed call is repeated after 10 s, doubling to 5 minutes,
  until answered.
  *Decision (#24): the call is also the periodic exchange.* An hourly
  call per friend is one small request and keeps the friend's number,
  use, repository size, protocol and reachability fresh for `peer list`
  and LEND-7, and feeds #68's reconcile. *Precedent:* HTTP/2's SETTINGS
  (RFC 9113 §6.5) has each endpoint announce its own limit, applied by
  the peer on receipt and acknowledged, and a lowered
  `SETTINGS_MAX_CONCURRENT_STREAMS` refuses new streams without resetting
  open ones, as a lowered quota refuses new objects and deletes
  nothing; Tahoe-LAFS storage servers announce their available space in
  the version answer a client fetches when it connects, rather than
  negotiating it. lendisk keeps HTTP/2's shape (each side's own number,
  the smaller in force, the change applied on receipt) and Tahoe's
  (figures carried in a call each side makes anyway). The callee
  applies the caller's number at once (`state.DB.QuotaReported`) and
  answers with its own number, the quota, its use of the caller's
  space, its lender's count of what the caller stores there
  (`lent_usage`, §3.5) and its repository size; the caller applies the
  answer (`state.DB.QuotaAnswered`), which marks the number it sent
  told only if it is still the own number. An answer does not
  overwrite the friend's number when the friend's own call applied one
  at or after this call was sent (`friendship.reported_at`, migration
  26, in nanoseconds): that number is newer, and an hourly call's
  answer could otherwise restore the old one for an hour
  (`TestAnAnswerOlderThanTheFriendsReportKeepsItsNumber`). Each pass compares every
  friendship's quota with the last it saw and hands a change to
  `Exchanger.Changed`, also when it first sees a friendship (before
  −1, since a friendship made and lowered within one poll must still
  reach writes in flight). The daemon then, on a lowering (or first
  sight), calls `gateway.Gateway.Requota` (§3.5 *Lowering*, SEC-15);
  on a raising, `state.DB.ResumePlace` (a place paused for quota is
  `ok` again, so placement counts its room at once) and
  `replica.Worker.Resume`, which wakes the place's sender from its
  hour-long `PauseRetry` so the copies queued there are sent with no
  further action (LEND-8 AC); and the alert service checks.
  *What a raising resumes.* Rows still `queued` at the friend resume.
  A copy that was `dropped` there (§3.4, a full friend for
  `reroute_after` with the row holding a barrier) and an object placed
  nowhere for want of room at capture (the deficit, D6) do not:
  placing them again is re-placement (§3.4 REPL-13, #26), which a
  changed quota starts at once, as it does when a lowering leaves a
  friend holding more than its quota: copies are moved away, oldest
  first, until what it keeps fits (PEER-6). Scenario 13 runs it end to
  end (`TestScenarioFriendLeaves`). `TestBothNumbersRaisedResumeCopiesWithNoFurtherAction` covers
  the queued case with two daemons.
- **PEER-4 (#24)** `peer list` (`--json` too, `PeerJSON`, fields only
  added) shows per friend: the quota; each side's number, the text
  column saying `same` or both, and "(yours not told yet)" while
  `told_number` differs; this node's use there (`place_use`, reconciled
  from the friend's count, §3.4) and the friend's use here
  (`lent_usage`); the remaining quota there (LEND-3); reachability
  (`direct` or `relayed` from `Transport.PathInfo` after an answered
  call, `offline` when the last call failed, `unknown` before the
  first); last seen; last verified (VER-4, #27: since when every
  replica it holds of this node's buckets has passed, the oldest of
  their last passes; `never` while one has not; `UNVERIFIED`, and
  `unverified` in JSON, while one failed, §3.6); the
  friend's protocol version; its repository size as reported (rounded
  up to a GiB); both retentions; and since when.
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
  quota count would go stale). Beyond those it refuses, as its first
  step, every other method SEC-9's table classifies Refused or
  Unreachable (#15 review), so NET-7's narrowing does not rest on the
  gate alone (§3.7 NET-7, §3.9 SEC-9;
  `TestLendingRefusesEveryRefusedMethodToABorrower`,
  `TestLendingRefusesTheUnreachableMethodsToABorrower`). `CreateBucket` creates the bucket with lendisk's own
  private ACL owned by the borrower's account and `BucketOwnerEnforced`,
  whatever ACL headers came with it, then inserts the row; a retry that
  finds `BucketAlreadyOwnedByYou` inserts it too, so a crash between
  the two heals on the client's retry. The borrower's account has
  versitygw's `userplus` role, whose one addition to `user` is
  `CreateBucket`; own bucket keys keep `user`. Own bucket keys cannot
  reach a `lent-*` bucket, since the borrower owns it.
- **PEER-9** The lender-only profile (#41) is `role = "lender"`, which
  `lendisk init --lender-only` writes into the config it creates. With
  a config already there that sets another role, `--lender-only` is a
  usage error naming the file, and so are `--repository-size` and
  `--copies` beside it; nothing is written. Such an init asks nothing
  about a repository (§3.5 LEND-7).
  - *What does not run* (`cli/serve.go`): the loopback S3 listener,
    the metrics listener (OPS-4, #43), the replication worker,
    verification, undelete and the store reconciler (EP-14), all of
    which serve or read the node's own buckets. Nothing listens on
    loopback (its AC): the only socket is tailcat's UDP one, and S3 for
    friends is versitygw behind the bridge's tunnel listener, inside
    the tunnel. `bucket add` refuses, naming `role = "full"`. Once the
    daemon lends it logs "lending only: nothing listens on loopback".
  - *What runs*, as on a full node: the tunnel's S3 and control calls
    (invites, redeem, quota, manifest, challenge, remove), the
    gateway's purge and borrower-usage loops, the bit-rot sample, which
    on a node with no buckets reads friends' replicas only (VER-5,
    #31), the farewell and purge (PEER-5), the quota exchange (LEND-8),
    alerts (the borrower-side conditions have nothing to raise), the
    recovery lister and the manifest pusher.
  - *Config.* An `endpoint.listen` entry and a non-empty
    `metrics.listen` (§3.8 OPS-4, #43) are refused at load in this
    profile (`Config.Validate`), from the file, a flag or the
    environment. #41's interim refusal of unknown `metrics` keys
    (`config.lenderUnknown`) went with #43: such a key is now only
    reported as unknown, in either role.
  - *Decision: the manifest is pushed* in both profiles: a lender-only
    node has no buckets, but its manifest holds its friendships and
    their quotas, which `lendisk recover` needs to rebuild it from its
    phrase and one friend's address (ID-4, ID-5), and the push costs
    one small file per friend.
  - *Decision: what `status` and `peer list` show.* Its friendship rows
    are like any other; the quota its friends lend it back has no
    buckets to fill. `status` prints `Role: lender`, no `S3:` line, and
    a "Lending only:" line with what it lends, what its friends use of
    it and the quota received (`capacity.lent`, `lent_used`,
    `received`, `received_unused`; #37, §3.5 LEND-7), then one line on
    switching (set `role = "full"` and restart). The "Places:" block
    and the single-friend warning are left out of the text: places are
    where this node's copies go, and it sends none; `status --json`
    keeps `places` and `pool` as they are (additive only). `peer list`
    is the same table and JSON, with one line saying the node lends
    only, and VERIFIED reads `n/a` for a friend never verified, since
    nothing of this node's is there to verify.
  - *Switching.* Adding a bucket later is `role = "full"` and a
    restart; no friendship row changes, nor anything at the friends.
    *Decision:* the reverse, `role = "lender"` on a node with buckets,
    makes `serve` refuse to start, naming a bucket and the config file:
    its buckets would otherwise silently stop going offsite.
  - *Tests:* `TestInitLenderOnlyWritesTheRole`,
    `TestLenderOnlyNothingListensOnLoopback` (serial: it reads the
    process's sockets), `TestLenderOnlyNodeLendsThenAddsABucket` (B
    lender-only invites A, holds A's backup and pushes its manifest,
    then switches to full and replicates a bucket of its own to A with
    both friendships unchanged), `TestLenderRoleRefusedWithBuckets`,
    `TestLenderOnlyRefusesMetricsSettings`.
- **PEER-5, PEER-12, ending a friendship (#25).** `lendisk peer
  remove <node ID>` (a malformed ID is a usage error, a stranger "not
  a friend") commits `state.DB.RemoveFriendship`: in one transaction
  the friendship row and its `manifest_push` row are deleted, the
  friend's unsent placement rows leave its FIFO (§3.4), and a
  `former_friend` row (migration 24) keeps its node ID, dial key and
  address, when it ended, whether the friend ended it (`by_peer`),
  when it was told, and `purge_after`: removal plus the friendship's
  retention window, the longer of the two sides' offers
  (`state.Friendship.Retention`, which the lender's tombs use too), so
  both sides purge at the same time. Both keys are refused from the
  commit (SEC-10); the daemon cuts their open connections (§3.9
  SEC-15). Only the one friendship changes (PEER-12, scenario 8:
  `TestEndingAFriendshipTellsTheFriendAndLeavesTheOthers`, where C's
  open connection at A is still served).
  *Telling the other side.* The daemon's farewell calls the friend's
  `POST /_lendisk/remove` over the tunnel, still admitted there,
  retried with a backoff of a minute doubling to an hour while the
  window lasts; a 403 means it holds no friendship either and counts
  as told. The callee, in the `peer` role, ends the friendship on its
  side with `by_peer` set (and so told), answers, and raises
  `friend_removed` (§3.8). Its daemon cuts and disconnects the caller
  (`Server.Ended`, which kicks the farewell) only once the caller has
  the answer (#84): an answer written and flushed is still in the
  tunnel's send buffer, and dropping the tunnel loses it, so the
  caller's tell timed out and its retries met a key refused at
  connect. The answer carries its length and `Connection: close`; the
  handler then hijacks the connection and reads it until the caller
  closes it, as a client does after such an answer, or for
  `Server.AnswerWait` (5 s) at the most, and only then calls `Ended`.
  A farewell pass that cuts the caller meanwhile leaves a hijacked
  connection's reads to its handler and waits for it to close: until
  `Conns.Grace` (2 s) after the cut or `Conns.Linger` after the hijack,
  whichever is later, then closes it anyway. `Linger` defaults to
  `AnswerWait` (`bridge.DefaultLinger` is `control.DefaultAnswerWait`),
  so the cut never closes the remove call's connection while its
  handler still waits (§3.9 SEC-15;
  `TestTheRemoveCallOverADialKeyConnectionEndsTheFriendship`, the call
  on the caller's dial key, resolved to its node ID;
  `TestRemoveCallEndsOnlyOnceTheCallerHasTheAnswer`,
  `TestCutLeavesAHijackedConnectionsReadsToItsHandler`).
  *Decision (#84, review): a refusal at connect after the call was
  sent makes the friend "probably told", and it is still asked until it
  answers.* A friend that ended it refuses the caller's key at
  connect, and tailcat ignores a refused key's handshake, so the
  caller cannot tell that from an offline friend: both are a dial that
  never connects. `control.Client` marks a failed call with how far it
  got, `control.ErrNotConnected` (no connection) or
  `control.ErrNoAnswer` (the request written whole, no answer), traced
  with `net/http/httptrace`. But "written whole" means only that the
  local end of the tunnel took it: a friend that fell asleep just then
  (a laptop closed) looks the same as one that ended it. So after a
  try that ended `ErrNoAnswer` and three tries in a row that never
  connect (`tellRefusals`; with the backoff, about seven minutes), the
  friend is not recorded as told. Its `notify_error` says it has
  probably ended the friendship too, and it is asked again every
  6 hours, give or take a tenth (`tellProbablyEvery`, jitter from
  `farewell.Rand`), until it answers 200 or 403, which records it as
  told at once, or the purge forgets the former friend. Any other
  answer (a 500) starts over on the backoff; a dial that never
  connects without such a try never makes it probable. *Precedent:*
  the farewell's own rule, like the manifest pusher's and the quota
  call's, that a notification is retried until it is confirmed or
  moot; a friend that only ever saw "A offline" would keep the
  friendship and A's replica. The mark is kept in memory only: after
  a restart the farewell retries on the backoff again, so no migration
  (`TestAFriendThatRefusesAfterTheRemoveCallIsAskedUntilItAnswers`).
  A `peer remove` run while the daemon is down is all in the database,
  so the daemon's first farewell pass at its next start revokes the
  friend, tells it, and keeps the purge time the removal recorded
  (`TestAPeerRemoveWhileTheDaemonIsDownIsAppliedAtTheNextStart`).
  `status` lists ended friendships (`former_friends`, with who ended
  it, whether the friend was told and the purge time).
  *Decision (#25): the other side ends it too, automatically, and is
  alerted.* PEER-3 has no one-way friendships and PEER-12 revokes a
  friendship as a whole, so a side that kept its half would only keep
  dialling a node that refuses it. *Precedent:* Syncthing removes a
  device on one side only and sends nothing; the other side keeps
  connecting and the remover's GUI keeps asking "device wants to
  connect" (syncthing#4086), and an introducer's removals reach other
  devices only when they connect. lendisk takes the opposite course
  for the reason Syncthing's users complain of: one explicit call, and
  an alert in place of a silent refusal.
  *Decision (#83, review): a side that never receives the call is
  alerted, and nothing ends by itself.* The worker records a place's
  403 "no relationship" (or versitygw not knowing this node's key) as
  the condition `removed`, with `since` (the first refusal) and
  `refused_at` (the last; migration 30). `friend_refuses`, subject the
  friend, is raised while the friend has refused over at least an hour
  (`refused_at − since ≥ alert.Service.MinRefusing`) and the last
  refusal is recent (within `RefusedRecent`, 2 hours, twice the
  worker's hourly re-ask of a `removed` place, `PauseRetry`), counted
  within this friendship; its message says the friend has probably
  ended the friendship, that copies are placed elsewhere meanwhile, and
  to run `lendisk peer remove <friend>` if it did
  (`TestAFriendThatKeepsRefusingIsAlerted`). A `removed` place keeps
  being asked as long as rows wait there: metadata goes to every friend
  and is never re-routed, so a node that still backs up keeps asking;
  and while nothing waits, the reachability probe asks it every 15
  minutes (§3.4, #65), each refusal refreshing `refused_at`.
  *Why not end it:* `since` changes only with the condition, and a
  `removed` place's data rows are re-routed away, so one stray 403 (an
  IAM lookup error, a stale session) could freeze the condition and,
  a window later, end a working friendship; and a friend that really
  ended it refuses at connect, which shows as `retrying`, so an
  automatic end would rarely meet the case it is for. That side is
  alerted as unreachable (`friend_unreachable`), since a laptop away
  for a month is not a friend gone. *Precedent:* Syncthing's
  "device wants to connect" prompt and the remover's unanswered device
  (syncthing#4086) leave the decision with the user; lendisk adds the
  alert that names the likely cause and the command, as `pool_too_small`
  and `below_copies` name theirs, and keeps PEER-5's rule that a
  friendship ends by a person's `peer remove` or the friend's call.
  *Holding and purging.* The lender side keeps the former friend's lent
  buckets, tombs and manifest, refused to it, so read-only, until
  `purge_after`; seven days before, `replica_purge_soon` is raised.
  Then the farewell deletes its lent buckets from the store
  (`gateway.Gateway.PurgeFormer`) and its manifest
  (`manifest.Store.Delete`), and `state.DB.ForgetFormerFriend` drops
  every row of it: placement, `place_use`, `place`, lent buckets,
  tombstones, uploads, usage and the former friend row
  (`TestAFormerFriendsReplicaIsKeptForTheWindowThenPurged`). Purges run
  on a goroutine of their own, one friend at a time, so deleting a
  large replica on a slow disk never delays revoking a removal made
  meanwhile (`TestABlockedPurgeDoesNotHoldUpARevocation`). Before each
  bucket the purge checks that the peer is still a former friend due
  for it and not a friend again (`state.DB.PurgeDue`); one that
  redeemed a new invite meanwhile keeps its remaining buckets, its
  manifest and every row (`state.ErrNotFormer`;
  `TestAPurgeStopsWhenTheFormerFriendIsBefriendedAgain`). A redeem
  landing inside one bucket's deletion can still lose that bucket's
  old objects: a window as long as one directory removal, at the end
  of a 30-day window, which nothing closes yet. On the
  borrower side "holding" is only that the placement rows stay, not
  counted (§3.4), so a renewed friendship finds them. Re-placement
  (§3.4 REPL-13, #26) places the copies it held elsewhere at once, and
  once they are acknowledged marks its acknowledged rows `dropping`. A
  new friendship with the same node within the window deletes the
  `former_friend` row, so nothing is purged; its rows still
  acknowledged count again, its `dropping` rows send it the deletes of
  the copies moved away, and its `dropped` rows are not queued again
  (#25 review).
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
  `backup`, `check --read-data`, `restore`), Kopia (`repository create
  s3` from the lines `bucket add` prints, `repository
  validate-provider`, two snapshots, `snapshot verify
  --verify-files-percent=100`, `snapshot restore`, `maintenance run
  --full`; `kopia_test.go`), the AWS CLI, rclone and Duplicacy
  (`minio://` storage: `init -e`, two backups, `check -files`,
  `restore`; `generic_test.go`) against it. CI installs Kopia and
  Duplicacy from pinned GitHub releases checked against a pinned
  SHA-256 (Kopia's `checksums.txt`; GitHub's asset digest for
  Duplicacy, which publishes none), restic and rclone from apt; each
  test skips when its tool is missing. Checked 2026-11-08 with restic
  0.19.1, Kopia 0.23.1, rclone 1.76.0 and Duplicacy 3.3.0. A bucket
  credential cannot create buckets, so rclone needs `no_check_bucket =
  true`, which the generic profile's lines say. For a kopia bucket
  `bucket add` and `bucket show` print `kopia repository create s3
  --endpoint=<host:port> --disable-tls --bucket=<bucket>
  --access-key=… --secret-access-key=…` (the same flags work for
  `repository connect s3`); Kopia signs for `us-east-1` without a
  `--region`, which is versitygw's.
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
  (`move`, `--track-renames`) and its in-place modtime update (a
  same-key `CopyObject`, used whenever only a file's modtime changed)
  fail against a lendisk bucket, so the generic profile's `bucket add`
  and `bucket show` lines tell rclone users to pass `--disable
  Copy,Move` (download and re-upload instead) and
  `--no-update-modtime` (#7). Measured with rclone 1.76.0 (#7):
  `--backup-dir` and `--suffix` cannot be made to work. rclone
  allows them only on a remote with a server-side `Copy` or `Move`
  and, with `--disable Copy,Move`, stops before touching the bucket
  ("can't use --backup-dir on a remote which doesn't support
  server-side move or copy", exit 7); without the flags each server-side
  copy gets the 501 and rclone neither overwrites nor deletes
  anything. Either way the bucket is unchanged, never half-synced
  (`TestRcloneBackupDirRefusedWithoutHarm`), and the lines say these
  two flags cannot be used. Whether to capture copies for generic
  buckets after all, so that `--backup-dir` works, is #77.
  *Measured (EP-2 AC):* `TestCaptureOverhead` in `internal/gateway`
  (opt-in, `LENDISK_OVERHEAD_MB`; `LENDISK_OVERHEAD_TOOL=restic|kopia`,
  default each installed tool as a subtest) times `restic backup` or
  `kopia snapshot create` (Kopia's own maintenance turned off, so it
  stays out of the timed run) of random data
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
  it at that size.
  *Kopia (#7),* Kopia 0.23.1, same VM, 2026-11-08, with restic in the
  same session for comparison: 512 MiB to ext4, seven runs each,
  1.996 s with capture and 2.036 s without (−2.0%; restic +2.8%); a
  five-run set gave +15.4% (2.11 s, 1.83 s) with two pairs stalled by
  writeback at 4.5–5.3 s, and a 1 GiB set was disk-bound
  (3.8 s to 18.5 s per run, −35%), so neither says anything. On tmpfs,
  256 MiB, capture costs Kopia more than noise: seven runs +5.4%
  (890 ms, 844 ms), nine runs +11.1% (942 ms, 848 ms), about 50–95 ms
  on a run of 27 queue rows; restic on tmpfs showed none (−1.7%). The
  guess, not profiled, is a cost per request rather than per byte
  (Kopia makes many small requests: index, session and format blobs,
  and lookups of absent keys, which the read-through answers from the
  database), which would shrink in proportion on a 10 GB snapshot.
  G-3 must measure Kopia at 10 GB on the gate machine before EP-2's
  AC counts as met for Kopia.
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
  this bucket has no offsite copy left. Add a second friend, or run
  `lendisk bucket set <name> --copies 2`." (`bucket set` prints it
  too, §3.4 REPL-13, #26), and `status` prints "! one copy: losing that friend's disk
  loses the offsite copy; 2 is recommended" under the bucket (`buckets[].single_copy` in
  `status --json`). `bucket add` also says when the pool has fewer
  places than `copies` (OPS-2), as does the `pool_too_small` alert
  (§3.8).
- **EP-8** `bucket add --profile generic` is a usage error (exit 2)
  unless `--i-understand-no-encryption` is given, and nothing is
  created; the flag means nothing for restic or kopia. A generic
  `bucket add` prints "Warning: this bucket is not encrypted by its
  tool. Every friend that holds a copy can read it: it is outside
  lendisk's trust guarantee." `status` prints "! not encrypted:
  outside the trust guarantee; friends can read its objects" under
  every bucket whose profile is not `encrypted` (a generic one, or one
  whose profile has no file), and `status --json` has
  `buckets[].outside_trust`. The generic lines end with the rclone
  advice of EP-2 above: `--disable Copy,Move --no-update-modtime`, why,
  and that `--backup-dir` and `--suffix` cannot be used.
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
  (§3.1 ID-5). A sixth, `rewritable` (v0.11.3, #74), names the few
  keys the tool rewrites in place in normal use; a rewritable key
  keeps its class. A flag, `names_snapshots` (#20), says a snapshot
  key's last segment is the snapshot's ID as the tool shows it, so
  `status` names the snapshot that is offsite (§3.4 REPL-11): `true`
  for `restic`; off for `kopia`, whose snapshot keys are index blobs,
  and `generic`, which has none. A bucket whose
  profile has no file classifies every key as data and none as
  rewritable. The compiled-in
  files are `restic`, `kopia` and `generic`. `kopia` (#7) is EP-7's
  list as written: `local_only = ["_log*"]`, `snapshot = ["n*",
  "xn*"]`, `metadata = ["kopia.repository", "kopia.blobcfg",
  "kopia.maintenance", "n*", "x*", "s*"]`, `data = ["p*", "q*"]`,
  `encrypted = true`, no `hash_named` key, since Kopia names blobs
  by an HMAC of their content or at random, and `rewritable =
  ["kopia.repository", "kopia.blobcfg", "kopia.maintenance"]`;
  `restic` has `rewritable = ["config"]`; `generic` has none. The patterns are as broad
  as EP-7 states them (`s*` would take any key starting with `s`);
  Kopia writes none outside these prefixes but `_log*` and the three
  format blobs. A Kopia 0.23.1 repository after two snapshots holds
  all four classes (`TestKopiaEndToEnd`). The
  decorator classifies a key at capture: a local-only key (restic
  `locks/*`, Kopia `_log*`) gets no queue row at all; the others get a
  queue row with their class, and placement rows as §3.4 says.
  The decorator records at capture whether the bucket's profile names
  the key rewritable (`queue.rewritable`, migration 20), beside its
  class, so a profile changed later does not change what was captured.
  *Rewritable keys (#74).* Kopia 0.23.1 writes `kopia.maintenance`
  during the quick maintenance it runs itself after a snapshot when one
  is due, and several times in every later maintenance run, quick or
  full, with new contents and no delete; it rewrites `kopia.repository`
  and `kopia.blobcfg` on a password change or `set-parameters`. restic
  rewrites `config` in `restic migrate upgrade_repo_v2`. Under
  v0.11.2 each was a changed acknowledged key the friend refused (412),
  raising `place_refused` for good. Now each such write is sent as a
  rewrite (§3.4), stored by the friend, whose replaced version becomes
  a tombstone (§3.5 LEND-4, §3.6 RET-1): no refusal, no alert, and
  `status` shows no refused object. `TestReplicateKopiaToFriend` runs a
  full and a quick maintenance and finds B holding exactly A's
  objects, each replaced `kopia.maintenance` in a tomb at B, and the
  tombs purged once B's clock is past the window;
  `TestResticMigrateReplicatesTheNewConfig` upgrades a version 1
  restic repository and finds the new `config` at B and the old one in
  a tomb. A changed key that is not rewritable is not replicated
  (EP-11, #22): held back at capture with the `key_changed` alert
  (§3.4).
  *Kopia format 1 (COMPAT-1, #79).* Kopia 0.23.1 still creates format
  1 (`kopia repository create … --format-version=1`; versions in
  `repo/format/format_provider.go:42-44`). What only format 1 writes,
  read from its source: `repo/format/content_format.go:29-39` gives
  formats 2 and 3 the epoch manager and format 1 none, and
  `repo/content/committed_read_manager.go:295-298` picks the legacy
  `indexblob.ManagerV0` exactly when the epoch manager is off. Only
  ManagerV0 writes the prefixes of
  `repo/content/indexblob/index_blob_manager_v0.go:23-29`: plain index
  blobs `n*` (`WriteIndexBlobs`, :216), the compaction log `m*`
  (`registerCompaction`, :192) and the cleanup blobs `l*`
  (`delayCleanupBlobs`, :401). Formats 2 and 3 index through
  `internal/epoch/epoch_manager.go:206-212` (`xn`, `xs`, `xr`, `xe`,
  `xw`), so a plain `n*` is format 1's too, with one exception: the
  upgrade (`set-parameters --upgrade`,
  `cli/command_repository_set_parameters.go:134-138`, or `repository
  upgrade`'s commit, `repo/format/upgrade_lock.go:111`) writes the
  legacy-index poison blob
  `n00000000000000000000000000000000-repository_unreadable_by_this_kopia_version_upgrade_required`
  (`upgrade_lock.go:19`) beside its rewrite of `kopia.repository`, so that a
  format-1 client can no longer open the repository. The upgrade
  converts the active `n*` indexes into the first epoch (`xn0_*`,
  `index_blob_manager_v1.go:165`) and deletes no `n*`, `m*` or `l*`
  blob; nothing in later maintenance does either, so they stay in the
  bucket (and at friends) after an upgrade, unread. A format-1
  repository writes `n*` blobs at `create` and with every snapshot,
  `m*` and `l*` only when maintenance compacts indexes.
  Two profile lists say this as data: `format1_keys = ["n*", "m*",
  "l*"]` and `upgraded_keys` = the poison blob's name and `x*`, checked first
  (`profile.Profile.Format`); only `kopia` has them. They change no
  class (`n*` stays a snapshot key, `m*` and `l*` data, so these reach
  `copies` friends only: why format 1 is unsupported), and no write is
  refused for them. After the store accepts a captured put or completed
  upload, the decorator (`noteFormat`) marks the bucket on a
  `format1_keys` key, once (`buckets.format1_at`, `format1_key`,
  migration 27; `state.DB.MarkFormat1`, on the gateway's clock), and
  records the upgrade on an `upgraded_keys` key (`buckets.upgraded_at`,
  `MarkUpgraded`). The bucket holds a format-1 repository while
  `format1_at` is set and `upgraded_at` is not (`state.Bucket.Format1`);
  a format-1 key captured after the upgrade marks nothing, which keeps
  an import of an upgraded repository, walking its leftover `n*`
  blobs, from marking it. So the warning ends at the upgrade itself,
  not after a quiet period or on a `kopia.repository` rewrite (which a
  format-1 `set-parameters` also does, and whose format version is
  encrypted), and when the bucket is deleted with its row. `x*` is in
  `upgraded_keys` because the poison blob is only Kopia 0.12's: 0.9 to
  0.11 upgraded without it (#79 review), and Kopia only logs a failed
  poison write. Format 1 never writes the epoch manager's `x*`, so any of
  them means format 2 or later, whatever the order an import brings the
  blobs in. Once `upgraded_at` is set nothing marks the bucket again, so
  a bucket emptied by hand and reused for a new format-1 repository, or
  an experimental `kopia repository upgrade rollback`, is not warned
  about; deleting and re-adding the bucket resets it. `status`
  prints "! Kopia repository in format version 1, which lendisk does
  not support" under the bucket with the first such blob, its time and
  `kopia repository set-parameters --upgrade` (Kopia 0.9 or later);
  `status --json` adds `buckets[].kopia_format1` (`since`, `key`,
  `fix`), absent otherwise; the `kopia_format1` alert (§3.8) says the
  same. `bucket add`'s Kopia lines say Kopia 0.9 or later (repository
  format 2 or later). `TestKopiaFormat1RepositoryIsWarnedUntilUpgraded`
  creates a format-1 repository with real Kopia 0.23.1 through
  lendisk, takes two snapshots (every key captured in its class), finds
  the status lines and the alert, upgrades, snapshots and verifies, and
  finds both gone.
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
    store. Its row is marked held locally (`queue.local`,
    `state.MarkLocal`), so losing it from the store afterwards is a
    delete to the store reconciler (EP-14), as it is not before.
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
  - A miss happens after `recover` (ID-6); nothing else removes an
    object from the local store (EP-9). VER-5 replaces a rotten object
    in place with the read-through's verified fetch (`fetchAny`), so its
    key is never missing (§3.6). Only what a tool reads is fetched; no download runs in the
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
- **EP-14** The store reconciler (`gateway/reconcile.go`, #8) runs in
  the gateway once it serves, in the full role only (a lender-only node
  has no buckets, PEER-9; its store marker is written by the first pass
  after a switch to full): a pass at start, one every
  `store.reconcile_every` (default 24h), and between them, every minute
  (`ReconcileSettings.Poll`), a pass of each bucket the user allowed
  deletes for (below). Passes run one at a time, over the node's own
  buckets (rows in `buckets`) only: `lent-*` buckets, and store
  directories no bucket row names, are never looked at; keys under
  `.lendisk/` and `.sgwtmp/` are skipped (posix lists neither).
  - *Paging.* A pass lists a bucket with the posix backend's
    `ListObjectsV2` under the decorator (so neither the read-through
    nor the index merge of §3.3 EP-10 is in the way), 1,000 keys a page
    (`MaxListPage`), and walks the index beside it
    (`state.IndexedKeys`: each key whose latest done row is a put, in
    byte order, 1,000 at a time) over the range the store's page covers,
    up to its largest key. Neither side is ever held whole. The store's
    order need not match the index's: whatever the walk finds is checked
    again under the key's lock before anything is written.
  - *Writes.* An object the index does not hold is a write that reached
    the store outside the endpoint: under the key's lock (`keyLocks`,
    as a capture takes it), if no row of the key is pending and the store
    still has it, the reconciler writes a pending put row in the key's
    class with its profile's `rewritable` flag (`queue.reconciled` = 1,
    migration 33), reads the object once to hash it (`hashStored`), and
    finishes the row with `FinishPut`, so EP-11's held-back changed keys,
    rewritable keys, placement and the format-1 mark (`noteFormat`)
    apply exactly as to a capture. Local-only keys get no row. A crash
    between the row and its finish leaves a pending row with no token,
    which startup's `ResolvePending` settles as landed (the object
    carries no other write's token) and hashes.
  - *Held locally* (`queue.local`, migration 33, unix seconds): set on a
    done put when this node knew its object to be in its own store:
    `FinishPut` (a capture stored it, also startup's settling; an
    identical re-upload marks the row it repeats), the read-through
    after storing a verified fetch (`state.MarkLocal`, for the version
    it fetched), and a pass that lists an indexed key's object
    (`MarkLocalSeqs`). Recovery's rows (§3.1 ID-5) are never marked: a
    recovered node's store is empty by design and fills only through
    the read-through. A captured delete ends the key's index entry, so
    the mark goes with it; a recovery verdict that replaces a row leaves
    the new row unmarked until the next pass sees the object.
    Rows of an earlier release are unmarked until a pass lists their
    objects, so a pass right after the upgrade only marks.
  - *Deletes.* An indexed key whose object the store lacks is a delete
    only if it is held locally; a recovered key not yet read is not
    (`TestRecoveredNodeDeletesNothingItNeverHeld`). A first walk counts
    such keys; if the pass may delete them (below), a second walk
    enqueues each, under the key's lock, only if no row of the key is
    pending, its latest done row is still the one the walk saw, the
    store's `HEAD` says `NoSuchKey` (not `NoSuchBucket`), and nothing is
    at the key's path (`Lstat`: a directory where the object was, or an
    object whose attributes were lost, answers `NoSuchKey` too, and is
    logged and left; `TestSomethingAtThePathIsNoDelete`): a delete row
    with `reconciled` = 1, finished by `FinishDelete`, which places it
    at every place holding the key (§3.4). Lenders keep it as a
    tombstone for the retention window (RET-1), so even a wrong delete
    is recoverable for that long, but a whole bucket's would be the
    user's whole backup; hence the guards.
  - *Guards against mass deletion.* Researched 2026-11-09: Syncthing
    takes a file missing from a folder as deleted only if its index had
    it, and only while the folder's marker `.stfolder` exists, which
    keeps an unmounted disk from deleting everything at every device
    ("folder marker missing" stops the folder until the user recreates
    it); Unison's `confirmbigdeletes` (default on) asks before
    propagating the deletion of a whole replica; rsync's `--max-delete`
    caps the deletes of one run. lendisk does all three:
    - the store marker: the first pass writes
      `<store>/.lendisk/store-marker` (0600, its directory 0700) and
      records it in the node table (`store_marker`); from then on a
      pass that finds the file missing reconciles nothing at all, writes
      included (another directory in the store's place would otherwise
      be enqueued whole), records the path (`store_marker_missing`,
      `state.NodeMarkerMissing`) and raises `store_marker_missing`
      (§3.8). The fix, after checking the mount, is `touch` on the path;
      the next pass clears the alert
      (`TestMissingStoreMarkerStopsReconciling`);
    - a bucket whose directory is gone (`NoSuchBucket`) is skipped,
      logged as a warning;
    - the delete limit (`store.reconcile_delete_limit`, a percentage,
      default 10, 1 to 100), cumulative (decided 2026-11-09, #8 review):
      the deletes a pass would enqueue count together with the
      reconciler's own deletes of the bucket in its window, those
      enqueued after the last used allowance (`reconcile.window_seq`,
      `state.StartDeleteWindow`) and within `DeleteWindow` (7 days),
      whichever is later (`state.RecentDeletes`), against that share of
      the objects held then (held now plus those deletes). rsync's
      `--max-delete` caps one run because a person starts each run; the
      reconciler runs unattended, so a store losing under the limit
      per pass would otherwise be emptied over a few passes or restarts
      (`TestDeleteLimitIsCumulativeAcrossPasses`: 1 of 40 lost per pass
      is deleted four times and held the fifth). A pass over the limit
      enqueues none of them. It records the hold (table `reconcile`, migration 33: `held`, of
      `held_of`, since `held_at`; `state.FinishReconcile`), logs an error
      and raises `reconcile_held` for the bucket. `lendisk bucket
      reconcile <bucket> --allow-deletes N` (`state.AllowDeletes`)
      refuses an N below the held count, and otherwise records it in
      `reconcile.allowed`, tied to the hold it was given for
      (`allowed_for` = its `held_at`, `allowed_held` = its count;
      `ReconcileState.AllowanceValid`). The next pass, within the poll,
      uses it only while that hold stands: a pass that held another
      count since voids it, and so does any pass that does not finish
      (the bucket's directory gone, a listing or database error, a
      stop), which drops it, so it never covers another set of deletes
      (`TestAllowanceIsTiedToItsHold`). Used, it lets the pass delete
      what it finds if no more than N are missing (else it holds again
      with the new count), starts a new delete window, and is cleared
      unless the user set another meanwhile. A pass that finds fewer
      missing (objects put back) ends the hold. 100 never holds
      (`TestMassDeleteHeldUntilAllowed`,
      `TestEmptiedBucketHeldUntilTheUserAllowsDeletes`);
    - the capped second walk: the first walk may take hours (it hashes
      every dropped file), so what goes missing between the walks was
      never counted. The second walk enqueues at most as many deletes as
      the first counted (which the limit or the allowance admitted, so
      never more than N); every further held key it finds missing is
      held back as above, with `reconcile_held`
      (`TestDeletesBetweenTheWalksAreHeld`: 1 of 20 missing at the first
      walk, all at the second, gives 1 delete and 19 held;
      `TestAllowanceNeverDeletesMoreThanAllowed`).
  - *Captures in flight.* A key with a pending row is the capture's (in
    flight while versitygw writes it, or stranded for `ResolveStranded`)
    and is left alone, for writes and deletes
    (`TestKeysWithACaptureInFlightAreLeftAlone`); the key's lock also
    orders the pass after a running capture or read-through of the key.
  - *Counted.* `status` shows under each bucket "reconciled outside the
    endpoint: N writes, M deletes" (done rows with `reconciled` = 1,
    ever), and a held hold with the command; `status --json` adds
    `buckets[].reconciled` (`writes`, `deletes`, `held_deletes`, and
    while held `held_of`, `held_since`, `allow`; `last_pass`) and
    `store_marker_missing`. ACs: `TestStoreChangesWhileStoppedAreReconciledAtStart`
    (`internal/cli`), `TestDroppedFilesEnqueuedAsWritesInTheirClass`,
    `TestMissingObjectEnqueuedAsDeleteOnlyIfHeldLocally` and
    `TestPassPagesThroughListingAndIndex` (`internal/gateway`, pages of
    3 keys).
  - *Edits in place (#88's second item, #31).* A pass that lists a held
    key whose file's identity (`queue.stamp`: inode, size, modification
    time; §3.6 VER-5) is recorded stats it; another identity means the
    file was edited or replaced since this node verified it, so it is
    hashed again, paced by `store.bitrot_read_limit` (`Pacer.Scrub`) and
    holding no lock, then, under the key's lock with the row and the
    file still the ones read, other bytes are enqueued as a put
    (`reconciled` = 1, `Decorator.captureChanged`, EP-11 applying as to
    a capture) and the same bytes (a copy back) record the new identity
    (`TestReconcilerRehashesObjectsChangedInPlace`). A chmod or chown
    changes no identity and hashes nothing; a restore of the store with
    `cp -a` (new inodes) costs one pass that hashes everything. A key
    with no identity recorded is the sampler's to verify. No clock is
    read, so a file whose times are ahead of the node's clock is hashed
    once (`TestAFileAheadOfTheClockIsHashedOnce`). #88's first item
    (files still being copied) stays open.
  - *Rule for later code:* anything that removes an own bucket's object
    from the store without a captured delete (post-v1 evict) must hold
    the key's lock and clear `queue.local` first, or the next pass
    enqueues the loss as a delete. VER-5's repair removes nothing: it
    replaces a rotten object in place (§3.6).
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
    (below) less the metadata reserve (below). A friend that lacks any
    of the bucket's live metadata (a friend added later, until
    re-placement queues it there, §3.4 REPL-13, #26) is not a candidate,
    so its FIFO sends the metadata before data. A friend
    that is `removed`, whose last answer was `507` (condition `quota`,
    below), or that is gone past `replace_after` is not a candidate;
  - a delete first cancels the key's `queued` puts that no sender has
    taken (*Deletes*, below, #21), then goes to every friend whose latest remaining row of the
    key is a put, in any state (REPL-2): every place that holds or may
    hold it, and one that refused it, which may hold older bytes;
  - a healing re-upload of a data key (*Healing*, below) goes where the
    row it heals was placed, except where that row was `dropped` or
    `abandoned`, topped up to `copies` by the choice.

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
  lender's own count (the quota answer, #24) corrects the borrower's count, not
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
  *Reachability* is the place's condition, which replication and the
  reachability probe (below, #65) keep: a friend is unreachable once
  it is `retrying` or `removed` and has not answered `ok` for
  `replication.reroute_after` (1 hour; a shorter failure is a blip),
  and gone once, in addition, `replication.replace_after` (7 days) has
  passed since its last answer (`last_ok`, or the failure's start if
  it never answered). Both count from the last answer, a probe's
  included: a friend full for a week that answers probes is neither
  unreachable nor gone after one missed probe (#65 review). A friend
  answering `503 SlowDown` is `busy`, an answer like `ok`, for a
  while: an honest one lasts only while another write is in flight
  there. *Decision (#92 review):* a friend `busy` and nothing else for
  `reroute_after` on end (its `since` older) counts as unreachable
  (`poolPlace.busyForLong`): its unsent data goes elsewhere and it no
  longer holds deletes, so a broken or hostile friend cannot hold
  copies back for ever by answering `SlowDown`. It is never gone,
  since it keeps answering (`last_ok` moves on). Past
  `alerts.unreachable_after` (24 h) busy it raises
  `friend_unreachable`, its message saying it answers busy
  (`TestABusyFriendIsAwayOnlyOnceBusyForRerouteAfter`,
  `TestAFriendBusyPastThresholdRaisesAlert`). An
  ok answer ends the stretch. An
  unreachable friend with room is still chosen
  when the reachable ones are not enough; a gone one is not.
  *Away is a run of failing answers, not one condition (#65, from the
  #21 review).* `place.since` restarts at every change of condition,
  so a friend alternating between network errors and `507`s was never
  `retrying` or `quota` for an hour on end, and never counted as away:
  it held the delete and metadata barriers and its unsent data for
  ever. `place.failing_since` (migration 32) is the start of the run
  of failing answers (`retrying`, `quota`, `removed`): kept as the
  condition changes among them, and through `store` (this node's
  failure, not the friend's answer), cleared by `ok`
  (`state.SetPlaceCondition`; `ResetPlaceCondition` and `ResumePlace`
  clear it too). Unreachable, full for long (*Reroute*, below) and
  gone count from it, the current condition still saying which: a
  friend `retrying` now after an hour of `507`s and network errors is
  unreachable, one answering `507` now is full for long
  (`TestAwaySurvivesAlternatingNetworkErrorsAnd507s`). NULL reads as
  `since`: rows from before the migration, and a previous release's
  writes, which do not keep it. *Decision:* a column rather than
  "no `last_ok` for the hour", because the probe advances `last_ok` of
  a full friend that answers it (it is reachable), which must still
  become full for long.
  *The reachability probe (OPS-2, D6, #65).* Replication alone learns
  a friend's reachability only when something is sent there, so an
  idle friend (nothing queued, paused backups) could be gone for weeks
  unjudged. Each sender therefore probes its place when it has nothing
  to send (its FIFO empty, its head held at the barrier, or the
  schedule closed) and nothing reached the place for
  `Worker.ProbeEvery` (`replica.DefaultProbeEvery`, 15 minutes) since
  its last send or probe, and at once when the sender starts idle,
  which is what the alert service's first pass a minute after start
  reads. The probe is `replica.Prober`: for a friend `Friend.Probe`, one
  `ListBuckets` (`GET /`, NET-7's list, which the lender answers from
  its mapping table, PEER-8) over the sender's own client, so the
  tunnel's one client per friend (#52, #69) carries it, signed like any
  request, bounded by the stall timeout; a refusal of this node is
  `ErrRemoved` (`TestProbeAsksForTheBucketListThroughTheTunnel`). It runs on the sender's
  goroutine, so never beside one of its sends, and carries no object,
  so it is not held to the schedule (REPL-7) or the rate limits. Its
  outcome is recorded like a send's (`TestAnIdleFriendIsProbedAtMostEveryInterval`,
  `TestNoProbeWhileASendIsInFlight`, `TestAFriendWaitingAtTheBarrierIsProbed`):
  an answer is a sign of life, not an acknowledgement
  (`state.ProbeAnswered`, `TestAProbeAnswerRefreshesLastOKButKeepsFullAndStore`):
  `last_ok` advances, `retrying` and
  `removed` become `ok`, while `quota` stands (the probe stores
  nothing, and its run of failing answers goes on) and so does
  `store`; a refusal of this node is `removed` (with `refused_at`, so
  #83's `friend_refuses` keeps judging an idle friend); any other
  failure is `retrying`, whether or not anything is queued, except
  that a `store` wait stands (`TestAFailedProbeLeavesAWaitOnTheStore`).
  *Decision: the quota call is not a probe.* It reaches the control
  API, not the S3 surface replication needs, hourly, from another
  package; keeping reachability with the sender keeps one writer of
  each place's condition, and a probe costs one small request per idle
  friend each 15 minutes. A friend whose client cannot even be built
  (`Worker.Connect` fails: a malformed node ID, a secret that cannot
  be derived) has no sender; the worker records it `retrying` with the
  reason, once per run, and builds it again each pass, so it is judged
  unreachable and alerted like a friend that does not answer rather
  than counting as answering for ever
  (`TestAFriendWhoseClientCannotBeBuiltIsRetrying`).
  *The borrower's count* (`place_use`, schema 12) is the sum over a
  place's `queued` and `acked` rows of `placement.size`: a put's bytes
  plus `state.ObjectOverhead` (4 KiB, the lender's per-object overhead,
  LEND-1), or 0 when the place's latest row of the key acknowledged the
  same hash (a healing re-upload, which the lender stores nothing for);
  a delete minus what the place acknowledged of the key since its last
  delete there (a put a delete cancels leaves the count with its row).
  SQLite triggers on `placement` keep it as rows are
  inserted, acknowledged, refused, re-queued, cancelled and moved, so
  placing an object reads one row per friend. A put acknowledged after
  a delete of its key was placed at the place (a send in flight, #21)
  landed there, and the lender counts it and then its tomb until the
  purge: it keeps its size, and `AckPlacement` adds that size to the
  first delete after it at the place, as if the put had been
  acknowledged before the delete was placed, so the delete frees it
  through `place_freed` like any other (below). *#85:*
  #21 acknowledged such a put with size 0, so the borrower counted
  neither the object nor its tomb and stayed short of the lender, one
  object and overhead per late put, until the next answer, which
  `TestResticPruneTombstonesAtEveryFriend` caught under load as two
  overheads (`TestALateAcknowledgedPutCountsUntilTheLenderPurgesIt`,
  `TestAPutAcknowledgedAfterItsDeleteIsFreed`). Before a place's first
  answer its deletes free at once (below), so this count frees a
  deleted object a retention window early there, erring towards more
  room until the `507` below or the `quota` answer corrects it.
  *Correction from the lender's answers:* a place
  whose last answer was `507` counts as full, gets no new data and has
  its unsent data re-routed or dropped (below), until it acknowledges a
  row again or the friendship's quota rises (`state.DB.ResumePlace`,
  §3.2, #24). Each backup run queues metadata at every friend, so a
  full friend learns of freed room within a `PauseRetry` (an hour) of
  the next run.
  *Reconciled from the lender (#24, for #68).* The rows alone drift
  from what the lender holds: a send that landed but whose answer was
  lost, whose row is then re-routed, cancelled as superseded or dropped
  (an `abandoned` row, #21, is not counted either, though a delete
  still follows it); a lender's real per-object overhead and its
  retention of deleted objects (RET-1); a refused row's place that
  still holds older bytes of the key; partial multipart uploads (#23).
  So each answer to the quota call (§3.2: at least hourly, and at once
  after a `507` through `Worker.Full`) replaces the place's
  `place_use` with the lender's own count of this node there
  (`lent`, its `lent_usage`), plus the bytes of the place's `queued`
  rows that are puts (`state.DB.QuotaAnswered`, one transaction); the
  triggers move it from there. An orphan copy the borrower does not
  know of is so counted, not deleted. A delete stays counted until an
  answer stops counting it (the lender purged the tomb): what a
  delete's negative size takes off `place_use` after the place's last
  answer is added up in `place_freed` (migration 26, triggers on
  `placement` for rows of negative size) and counted back wherever the
  use is read (`PlaceUse`, D6's pool), until the next answer. The
  answer sets `place_freed` to what the place's `queued` and `acked`
  deletes take off `place_use`, and `place_use` to the lender's count
  plus the queued puts less that, so the sum is the lender's word and
  each delete then nets to nothing: one acknowledged, or sized anew by
  a late put, after the answer moves bytes between the two and never
  adds them (#85: the answer set `place_freed` to 0, and the triggers'
  floor at 0 then counted such a delete back on top of the lender's
  count, which already held it). A place gets its `place_freed` row from its first answer; before
  that its deletes free at once, as they did. A send in flight across the answer may be
  counted twice (less room) or, if acknowledged between the lender's
  count and the transaction, missed
  (`TestQuotaAnswerReconcilesThePlacesUse`). So such an answer does
  not stand for the hour: when the place had a row taken and not yet
  acknowledged as the call was sent, or one taken since or still in
  flight at the answer (`state.DB.PlaceSending`, to the second), the
  exchanger marks the friend unsettled and calls it again at the first
  pass that finds the place's FIFO empty (`state.DB.PlaceIdle`): one
  more call per busy spell, and the count settles on the lender's word
  once the sends stop (#85,
  `TestAnAnswerThatCrossedASendIsSettledOnceThePlaceIsIdle`). A place
  whose FIFO never empties (a friend at quota) waits for the hourly
  call. A failed re-routing batch is retried row by row (#68, below).
  *What each side counts (#85).* Between answers the borrower's count
  of a place (`PlaceUse`: `place_use` plus `place_freed`) follows the
  lender's rule (§3.5 LEND-1: an object's bytes plus 4 KiB until its
  tomb is purged) row by row. S is a row's object plus
  `ObjectOverhead`; "answer" is the place's next quota answer, which
  replaces both sides' difference with the lender's count.

  | Event at the place | Lender counts | Borrower counts |
  |---|---|---|
  | Put queued, not sent | 0 | S (queued) |
  | Put sent, its answer not back | S once it lands | S (queued) |
  | Put acknowledged | S | S |
  | Put of the bytes the place's latest row acknowledged (a healing re-upload) | nothing more (accepted unstored) | 0 |
  | Rewrite of an acknowledged version (#74) | both, the old as a tomb, until its purge | both rows' S; the old freed by the answer after the purge |
  | Change held back (EP-11, #22): placed nowhere | the version it holds | that version's acknowledged row |
  | Put refused (507, 412, LEND-4; or the node's store, #50) | nothing new | 0 (out of the count); a 412's other bytes only from the answer |
  | Put dropped (never sent, #19, #25) | 0 | 0 |
  | Put abandoned (taken, never acknowledged, #21) | S if a send landed, else 0 | 0 until the answer |
  | Unstarted put cancelled by a delete | 0 | 0 (row removed) |
  | Delete queued or acknowledged | the object, then its tomb, until the purge | S: −S in `place_use`, +S in `place_freed` |
  | Put acknowledged after its delete was placed (#85) | S, then its tomb, until the purge | S: the put keeps S, its delete takes −S more, +S in `place_freed` |
  | Delete refused | the object, live | S: the delete leaves both counts |
  | Copy marked `dropping`, then `drained` (#26) | the copy, then its tomb, until the purge | S: out of `place_use`, +S in `place_freed` (`place_freed_dropping`) |
  | Tomb undeleted (RET-3, #32) | unchanged: the tomb, live again | 2S until the answer: the restored put's acknowledged S, and the S its delete left in `place_freed` |
  | A `drained` copy placed there again (re-placement) | the tomb and the new copy | the new S queued, plus the S still in `place_freed` |
  | The bucket at the place | 4 KiB | 0 until an answer counts it |
  | A quota answer | its count, `lent` | `lent` plus the place's queued puts, split so that `place_freed` holds its deletes |
  | A delete queued across an answer, acknowledged after it | its tomb (in `lent`) | unchanged: the delete nets to nothing |
  | Before the place's first answer | as above | deletes and drops free at once (no `place_freed` row) |

  Only an abandoned put, the bucket's overhead, a refusal's other
  bytes and a send in flight across an answer (above) leave the two
  apart; the next answer settles each, and an answer that crossed a
  send is followed by another as soon as the place's FIFO empties.
- **REPL-1 to REPL-3** The queue is a SQLite table with one FIFO
  sequence per place, ordered by the bucket's write sequence. The
  worker uploads one object at a time per place in sequence order,
  several places in parallel. A delete (REPL-2) is a row in the FIFO of
  every place whose `placement` row for the key is `acked` or
  `queued`; a `queued` put that has not started is simply cancelled
  (*Deletes*, below).
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
  so the index outlives it, `state` (`queued`, `acked`, `refused`,
  from #19 `dropped`, from #21 `abandoned`, and from #26 `dropping` and
  `drained`, re-placement's copy being deleted and deleted, REPL-13
  below, with `replaces` and `replaced_at`, schema 28),
  and on acknowledgement `sha256` and `acked_at`: acked put rows are
  the index of acknowledged hashes (REPL-10, `state.AckedHash`).
  `started_at` (schema 21) is set when the place's sender takes the row
  to send it (`state.DB.StartPlacement`) and never cleared; migration
  21 sets it to 0 on every row queued when it runs, as an older lendisk
  may have sent them. `abandoned` is a taken put that will not be sent
  again and was never acknowledged (*Deletes*, below).
  `refused` (with `detail`) is a refusal that would repeat; the row
  leaves the FIFO. Its `cause` (schema 9) is NULL when the place
  refused (a `BadDigest` for bytes that match, a rewrite it keeps
  refusing), `immutable` when it answered `412` to a key that is not
  rewritable (with `held`, schema 23: *EP-11*, below, #22), `store` when the node's own store could not
  supply the object as captured (`state.RefuseObject`, #50), and
  `oversize` when the only reason was that the object is larger than
  one PUT carries (#61): such a refusal takes the row out of every
  place's FIFO at once and counts against the bucket
  (`state.RefusedObjects`, both causes), not any place.
  *Healing (#57):* a refusal is healed by writing the same bytes again.
  `FinishPut` drops an identical re-upload as a retry (EP-3) only while
  the key's last done row is refused nowhere (an `immutable` refusal
  does not count: the place would refuse the same bytes again, #22);
  when it is refused at
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
  refused only as too large to send (cause `oversize`) are put back
  in their places' FIFOs by `state.RequeueOversize`, which the worker
  calls once at start (`Worker.start`, #23) with the largest object it
  sends (`replica.MaxObjectSize`, 5 TiB): since #23 an object above one
  PUT goes in parts, so every row refused for its size before is
  re-queued once, and only one above 5 TiB stays refused. Rows refused
  before #61 told the causes apart (cause `store`, every object above
  5 GiB, refused for its size before its file was looked at) were given
  cause `oversize` by migration 31, an append-only data migration, so
  `RequeueOversize` matches the cause alone and never re-queues, and
  re-alerts at each start, a genuine store refusal of a large object
  (`TestOldStoreRefusalsAboveOnePutBecomeOversize`,
  `TestOversizeRefusalsCanBeRequeued`, `TestOversizeRowsAreRequeuedAtStart`).
  A row a later write of the key replaced stays as it is. A re-upload
  also heals them.
  Table `place` keeps each place's last answer:
  `condition` (`ok`, `retrying`, `quota`, `removed`, `busy` when it
  answered `503 SlowDown` (#92), and `store` while
  it waits because this node's store cannot be read), `detail`, `since`,
  `failing_since` (the start of its run of failing answers, #65) and
  `last_ok`, D6's last-reachable time: its last acknowledgement,
  answered probe or busy answer. `busy` is not failing: it sets
  `last_ok` and clears `failing_since` as `ok` does, and is away only
  once it lasts `reroute_after` (above); `status` shows it as "busy (it asks
  to send again shortly)", `status --json` as `state: "busy"` (a new
  value, no new field). A sender whose FIFO is empty,
  including at daemon start, sets `store` back to `ok` without touching
  `last_ok` (`state.ResetPlaceCondition`): the rows it waited on went
  for another reason than its answer, such as a refusal for the store.
  `retrying`, `quota` and `removed` are the place's own last answer and
  stand until it answers again, at its next row or probe (#61, #65). A friend
  added after a row was placed gets no copy of it until re-placement
  (REPL-13 below, #26), which sends it the bucket's metadata first.
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
  A data put before `m` whose queue row is still `pending` holds it too
  (#20 review), whatever wrote it: a pending row has no placement yet,
  and one kind is answered before it is done (a completed multipart
  upload, hashed after its answer, §3.3, #49), so the tool may write the
  index naming it first. A row stranded pending holds the bucket's
  metadata until `ResolveStranded` (each minute) or the next start
  settles it (`TestAPendingDataUploadHoldsTheMetadataBarrier`). One may
  never settle: a completion whose object's upload ID cannot be read
  (`errUploadUnreadable`, its xattrs lost when the store was copied
  without them, #62). It still holds the metadata, which keeps the
  index consistent, and since a held metadata row heads its place's
  FIFO, replication of every bucket to every friend waits behind it. So
  the decorator marks it in `lendisk.db` (`queue.unmatchable`, schema 22,
  `state.DB.MarkUnmatchable`, read only while the row is pending): `status`
  counts it as stuck ("cannot be matched to the store: restore the
  store's xattrs or rewrite the key"), `status --wait` exits 4, and the
  `upload_unmatchable` alert names it (§3.8;
  `TestAnUnmatchablePendingUploadIsStuckNotBeingWritten`,
  `TestUnreadableUploadIDLeavesTheRowPendingAndReported`,
  `TestStatusWaitStopsOnAnUnmatchableUpload`).
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
  bytes. A row a sender took before (a failed send that may have
  landed) leaves its friend an `abandoned` row, so a later delete of
  the key still goes there (*Deletes*, below;
  `TestRerouteKeepsATakenRowsPlaceForItsDelete`). A row its sender has taken and not yet recorded an outcome for
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
  milliseconds). A batch that fails is rolled back and retried one row
  at a time, and the pass goes on, returning the failed rows' errors
  with what the others did, so one bad row holds only itself, never
  the other 63 of its batch (#68,
  `TestOneBadRowDoesNotHoldItsReroutingBatch`). A busy or locked
  database (`SQLITE_BUSY`, `SQLITE_LOCKED`) is not a bad row: the pass
  stops at the first such error, since each row would wait out
  `busy_timeout` (30 s) in turn under the worker's lock, and the rest
  wait for the next pass (`TestABusyDatabaseStopsTheReroutingPass`).
  The worker logs what a pass moved even when it also failed.
  With no reachable friend with room the row waits at
  its friend, and the lag shows in `status`. *Decision (#19,
  review):* the row is dropped instead of waiting (state `dropped`:
  out of the FIFO, not counted in `place_use`) when its friend is gone
  past `replace_after`, or answers `507` with no `ok` for
  `replication.reroute_after` (`failing_since`, #65) and the row holds a metadata barrier
  (`Held`'s rule): a full friend would refuse it, and while queued it
  would hold every place's metadata and the full friend's later
  deletes for as long as the friend stays full. A shorter `507` is
  not enough, since the lender's count and its floor (LEND-2) may
  pass, and a dropped copy stays dropped
  until re-placement; a row that holds no barrier waits for the friend
  to accept it. The object is then below `copies`, which
  `status` shows and the `copies_dropped` alert raises (§3.8);
  re-placement (#26) places the copy when there is room. A dropped row
  still counts as the place's latest row of the key, so a later delete
  is sent there too; it is not a target for a healing re-upload.
  `dropped` is a copy never sent; it is not REPL-13's `dropping`, a
  copy the place holds that re-placement is deleting.
  Data deletes have the mirror barrier: they wait for the metadata
  before them (*Deletes*, below, #21).
  Restic and Kopia write packs, then indexes, then the snapshot or
  manifest, so the barrier costs one round of acknowledgements per
  backup run, not per object.
- **REPL-2, deletes (#21).** The decorator captures a `DeleteObject`
  (and each key of a `DeleteObjects`) as a queue row `op = delete`;
  `FinishDelete` places it in the transaction that completes it
  (`placeTx`):
  - *Cancel what never left.* Each `queued` put of the key with
    `started_at` NULL, at any place, is deleted from `placement`
    (freeing its count) and gets no delete: nothing of it reached the
    place. A sender takes its head row with `StartPlacement`, an
    `UPDATE … WHERE state = 'queued'`, after the metadata barrier lets
    it go and before it reads the store; a row a delete removed first
    affects no row, and the sender moves on without sending it. SQLite
    serialises the two writes, so a row is either cancelled or taken,
    never both. A taken row stays taken, through a failed send, a
    retry and a restart: a send whose answer was lost may have landed,
    so the delete must follow it (`TestDeleteFollowsAPutInFlightAndPrecedesARewrite`).
    The claim also outlives the row's leaving the FIFO unacknowledged:
    a taken row cancelled as superseded (`CancelPlacement`) or
    re-routed elsewhere (`Reroute`) becomes `abandoned`, not deleted,
    and stays the place's row of the key, so a later delete still goes
    there even when the write that superseded it went to another place
    or was itself cancelled untaken
    (`TestDeleteReachesAPlaceWhoseTakenPutWasSuperseded`,
    `TestALostAnswerStillGetsItsDelete`). The sender checks
    `Superseded` before it takes a row, so a row never sent is removed,
    not abandoned. An `abandoned` row is out of the FIFO and out of the
    count, and is never acknowledged.
  - *Even what metadata waits for.* The cancel applies when metadata of
    the bucket was captured between the put and the delete too: the
    store no longer holds the put's bytes, so it could never be sent
    (its sender would find it `Superseded` and cancel it). The barrier
    then no longer waits for it, so that metadata can go offsite naming
    a pack no friend got, until the prune's new index and the
    metadata's own delete follow it; locally those bytes are gone
    already. And RET-3's `undelete` can restore only what reached a
    lender: a pack deleted before it was sent, a backlog's packs pruned
    during an outage among them, is lost to it. Keeping the local bytes
    until every place has them is a follow-up the coordinator files.
  - *Then follow what remains.* The delete is queued at each friend
    whose latest remaining row of the key is a put (taken, acked,
    refused, dropped or abandoned), at its sequence, so it is behind every
    earlier write of the key there and ahead of every later one; a
    place whose latest row is a delete already gets no second one. A
    put taken but superseded by the delete is abandoned at its next
    try (`Superseded`), and the delete is sent after it; a key the
    friend lacks counts as deleted.
  - *Ordering across places: the delete barrier.* Per-place FIFO is not
    enough. At copies 1 with friends B and C, a prune's new index, the
    deletes of the old snapshot and index, and the pack deletes reach C
    while B is still behind the new index: C deletes packs only it held
    while B still holds, live, the old snapshot and index naming them,
    so B and C together hold a snapshot whose packs are nowhere (REPL-1
    AC). So the barrier has a mirror: a data delete at sequence `d` is
    sent to any place only once no metadata or snapshot row of its
    bucket before `d`, put or delete, is `queued` at any place
    (`state.DB.Held`, which the sender checks for every delete at the
    head of its FIFO, polling every second while it holds;
    `TestPackDeleteWaitsForTheMetadataBeforeIt`,
    `TestDataDeleteWaitsForEarlierMetadataAtFriendsThatAnswer`). A
    prune's pack deletes thus wait until every friend holds the new
    index and has deleted the old metadata. It cannot deadlock: a data
    put is never held, a metadata row waits only for data puts before
    it, and a data delete only for metadata before it, so the queued
    row with the lowest sequence is always free to go.
    *Decision (#21 review): a friend that is away does not hold
    deletes.* Metadata queued at a friend unreachable for
    `replication.reroute_after`, or full (`507`) for that long, is not
    waited for (both counted from `failing_since`, so a friend
    alternating between the two is away too, #65), as `Reroute` does
    not wait for it either: otherwise one
    friend away for days would hold every other friend's deletes, and,
    since FIFOs are strict, every row behind them, new backups
    included. The cost is the AC while it is away: its replica keeps
    the old snapshot and index, while the packs they name may be
    deleted elsewhere; once it returns its FIFO sends it the new index
    and the deletes, in order. A friend that answers, however slowly,
    holds them. Metadata and snapshot deletes are held like metadata
    puts.
  - *Bookkeeping.* A delete's row counts minus what the place
    acknowledged of the key since its last delete there, from the
    moment it is queued; its acknowledgement changes nothing more. The
    key's latest done write is then a delete, so it leaves
    `below_copies` and `LiveKeys`, and the place's queue shows the row
    until it is acknowledged. The lender keeps the tomb counted until
    its purge (§3.6), which the borrower's count does not follow; that
    is #68's reconcile (*Follow-up*, above).
  - *Recovery.* A lender lists a key held only as a tomb with
    `tombstoned_at`, and recovery records nothing for it (§3.1 ID-5),
    so a key tombstoned at every place is not recovered, served or
    listed (`TestKeyTombstonedAtEveryPlaceIsNotRecovered`); `restic
    forget --prune` and Kopia's full maintenance end to end:
    `TestResticPruneTombstonesAtEveryFriend`, `TestReplicateKopiaToFriend`.
  *Decision (#21): cancel only what never left, and follow the rest.*
  Replicators differ here. rclone `sync` deletes on the destination
  after its transfers by default (`--delete-after`), so a delete never
  precedes the copies that replace what it removes. S3 Replication
  replicates delete markers only when asked
  (`DeleteMarkerReplication`) and never a delete of a version, so a
  destination keeps what a source loses. Syncthing sends a delete as a
  new version of the file, ordered after the writes it follows. lendisk
  takes from each: per place the delete is ordered after the writes, as
  Syncthing's versions are; across places after the metadata, as
  rclone's deletes follow its transfers; and the lender keeps what it
  held, as S3's destination does, in a tomb for the window. A put the
  place never saw has nothing to keep, so it is cancelled, which a
  sender's claim makes safe against a send in flight.
- **REPL-13, re-placement (#26).** `state.DB.Replace`
  (`internal/state/replace.go`), run by the replication worker
  (`replica.Worker.replace`) whenever the pool's fingerprint changes
  (`state.DB.PoolFingerprint`: each friend with an address, its quota,
  whether it refuses this node, is gone past `replace_after`, counts
  more than its quota or is paused for quota (its last answer a 507:
  no room until it acknowledges again or the quota rises,
  `ResumePlace`), each bucket's `copies`, and each replica marked
  unverified (#27); read every five
  polls, so a friend added, removed (`peer remove` or its remove call,
  §3.2), gone, back, paused for quota or resumed, or a quota or
  `copies` changed starts a pass within seconds, whichever process
  wrote the change), and hourly
  (`Worker.ReplaceEvery`) for what no change announces: a refusal, a
  copy `Reroute` dropped, a deficit from capture. A pass looks at each
  bucket's *live* objects only (the key's latest done write is a put,
  not a change EP-11 held back, and not refused for the store): the
  bytes of a new copy come from the local store, read by the place's
  sender as for any row, so an object deleted or rewritten since is
  never re-placed and no lender ever forwards (PEER-11). After
  `recover` (§3.1 ID-5) a bucket is left alone until every friend's
  list of it is read and settled: a copy that looks missing may be one
  a friend not yet listed holds, and a row placed meanwhile would stand
  in the way of the acknowledged row its list records
  (`TestReplacementWaitsForRecoveryLists`,
  `TestRecoverFromPhraseAndOneLendersAddress`). A recovered node's
  store starts empty, so the sender puts an object it holds no file for
  there first through the read-through (`replica.Worker.Fill`, which
  `serve` sets to `gateway.Gateway.Fill`: fetched from a friend that
  acknowledged it and verified against the recorded hash, §3.3 EP-10),
  and then sends it as usual; a failed fetch waits as an unreadable
  store does, and only an object no friend is recorded holding is the
  store's local error (`TestARecoveredNodeReplacesFromTheReadThrough`:
  recover, then `peer add` and `peer remove`, with no refusal). This
  heals an object that vanished from the store outside lendisk too, as
  a read of it would. Overflow targets (#38) are places like friends
  in a pass: an overflow target is never drained for being over
  `max_bytes` (its copies move back to friends only by drain-back, §3.11, #40), but one gone past `replace_after` is. A failed
  verification (#27) is one: see *Unverified replicas* below.
  - *Metadata first.* Each live metadata object gets a row at every
    friend with an address whose row of it is not `queued`, `acked`,
    `refused` or `dropping` (a friend added later, one befriended again
    whose unsent rows #25 dropped): inserted, or a `dropped`/`abandoned`
    row queued again, at the object's own sequence, so the friend's
    FIFO sends the bucket's existing metadata ahead of any newer data
    (REPL-1, REPL-12). Until then the friend is no candidate for data:
    D6's choice (`choose`), `Reroute` and the pass skip a friend that
    lacks any of the bucket's live metadata (`unsentMetadata`, now
    counting rows of the live version that hold, refuse or delete it),
    so no data reaches it, and so counts there, before the metadata
    (`TestANewFriendReceivesTheExistingMetadataFirst`). The pass places
    each bucket's metadata before its data, so a new friend is a
    candidate for data in the same pass.
  - *Missing copies.* A data object's *good* copies are its `queued`
    and `acked` rows at friends that are not being drained of it. A
    friend is drained of everything when it refuses this node
    (condition `removed`) or is gone past `replace_after`; of some
    objects when it holds more live bytes than its quota (PEER-6: its
    `queued` and `acked` puts of live objects, not the tombs it keeps,
    which no move frees), oldest acknowledged data first until what it
    keeps fits the quota (`overQuota`); a former friend (no friendship
    row, #25) is a drained place too. With fewer good copies than
    `copies`, the missing ones go where D6 would place them at capture:
    reachable friends first, by largest free fraction, with room for
    the object less what the pass already placed there (the room model
    of `Reroute`, read once per pass), never a friend with a row of the
    object that holds, refused or is deleting it, never one being
    drained of it. A friend whose row was `dropped`, `abandoned` or
    `drained` may be chosen again; that row is queued again (a taken
    row stays taken, REPL-2). Each new copy names, in
    `placement.replaces` (schema 28), a drained copy it replaces;
    `placement.replaced_at` marks the row as re-placement's.
    `Reroute` carries both when it moves the row.
  - *Dropping, only after the replacement.* An acknowledged copy at a
    drained place is marked `dropping` when its replacement is
    acknowledged, in the transaction that records the acknowledgement
    (`AckPlacement`), or by a pass that finds the object at `copies`
    acknowledged good copies. So the object never has fewer copies than
    it had, and a gone friend that returns does not count the copies
    that were moved away: the watermark's `holds` reads `dropping` and
    `drained` rows as the place's latest row of the key, and neither is
    an acknowledged put (`TestAReturningFriendsMovedCopiesDoNotCount`).
    Only the place's latest row of the key is marked, so a later
    acknowledged write there is never deleted.
    A `dropping` row is in its place's FIFO as a delete of the key at
    the row's sequence (`NextPlacement` takes the lower of the queued
    and dropping heads; index `placement_dropping`): any later write of
    the key there comes after it. The sender takes, sends and
    acknowledges it like any delete, which leaves the row `drained`:
    the lender keeps the bytes as a tomb for the window (RET-1), and a
    later write of the key there names that version (`ReplacedHash`,
    `AckedPutHash`; `AckedHash`, the read-through's source and EP-11's
    `ackedOther` read `drained` as deleted). A data delete is held by
    the barrier only for metadata before it; a `dropping` row is a put's
    row, so `Held` never holds it: its object stays at `copies` places,
    so no N − c + 1 places lose a pack (REPL-1 AC). Its bytes leave
    `place_use` when it is marked, and trigger `place_freed_dropping`
    counts them back until the place's next quota answer, as a delete's
    (#24): the lender keeps the tomb counted.
    A friend gone past `replace_after` is no longer sent anything new,
    but its sender keeps trying its FIFO: when it returns, it receives
    the deletes of its moved copies and keeps them as tombstones
    (`TestAFriendBackAfterReplaceAfterTombstonesItsMovedCopies`, real
    daemons on A's injected clock). A removed friend has no sender; its
    `dropping` rows are deletes it receives only if befriended again.
  - *Surplus.* With more good copies than `copies` (lowered, EP-6; or a
    gone friend back while its replacement waits), unsent ones no
    sender holds are cancelled first, then acknowledged ones are marked
    `dropping` while more than `copies` stay acknowledged: at an
    unreachable friend first, as the others answer, then the fullest
    place (smallest free fraction)
    (`TestASurplusCopyAtAnUnreachableFriendGoesFirst`). A row a sender holds is left
    alone and the worker looks again at its next re-routing tick
    (`TestASurplusCopyBeingSentIsNotCancelled`). Held means taken
    (`started_at`, through a failed send and its retries) or in the
    worker's sending set: such a copy may land, and its acknowledgement
    changes no fingerprint, so the pass counts every surplus copy it
    leaves for that reason (`ReplaceStats.Skipped`) whether or not it
    had anything else to do for the object, and the worker runs again
    at each tick until none is left
    (`TestAPassLeavingATakenSurplusCopyAsksToLookAgain`). The same goes
    for a held row at an unverified friend, drained once acknowledged
    (`TestAPassLeavingATakenCopyAtAnUnverifiedFriendAsksToLookAgain`).
    While such a row waits on an unreachable friend, that is a pass per
    tick (a minute).
  - *Removed friends (#25 review).* A friend removed has no friendship
    row, so its copies are not good copies and are replaced; once the
    replacements are acknowledged its acknowledged rows are `dropping`.
    If it is befriended again, its `dropped` data rows are not queued
    again: those objects were placed elsewhere, and a data copy goes
    there only if D6's choice picks it for an object still short; its
    `dropping` rows send the deletes; live metadata it lacks is placed
    there as at every friend (REPL-12)
    (`TestARemovedFriendsDroppedRowsAreNotRequeuedWhenBefriendedAgain`).
  - *Unverified replicas (VER-1, #27).* A friend whose replica of a
    bucket failed a challenge (`replica_check.unverified_at`, schema
    29, §3.6) is drained of that bucket's data copies, as a removed
    friend is of all of them, and of that bucket alone: its
    acknowledged copies are not good copies, their replacements are
    placed by D6's choice and name it in `placement.replaces`, and its
    copies are `dropping` once the replacements are acknowledged, so
    the object never has fewer copies than it had. Its `queued` rows of
    the bucket that no sender has taken are cancelled and placed
    elsewhere. It is no candidate for the bucket's data: not in the
    pass's `choose`, not in D6's choice at capture (`DB.choose` reads
    `unverifiedAt`), not a `Reroute` target for the bucket. Its
    metadata rows stay, and new metadata still goes there (REPL-12):
    every friend holds the bucket's metadata. The fingerprint carries
    each unverified mark, so a failed challenge starts a pass within
    seconds (`TestAFailedVerificationMovesTheReplicasCopies`,
    `TestUnsentCopiesAtAnUnverifiedFriendGoElsewhere`). A passing
    `lendisk verify --full` clears the mark, and the friend is a
    candidate again.
  - *Batched and bounded.* A pass reads each bucket's live data objects
    in pages of 2048 (no transaction), keeps those with something to
    do, and decides about each again in write transactions of at most
    64 objects (`replaceBatch`) from its rows as they then are, with
    prepared statements (`preparedTx`, §3.1), under the bucket's lock,
    which the placer shares (`FinishPut` and `FinishDelete` take it
    before their transaction, `DB.lockBucket`), and the worker's
    sending lock, so neither decides about an object while the other
    does, and no capture waits behind a pass for long. A batch that
    fails is rolled back and the pass goes on, returning the errors;
    what is below copies is recorded all the same, and the worker runs
    the pass again at its next re-routing tick rather than an hour later
    (`TestBelowCopiesIsRecordedWhenABatchFails`,
    `TestAFailedPassIsRetriedAtTheNextTick`).
    `TestReplaceOfManyObjectsIsFast` (3000 objects to re-place in a
    bucket with 1200 metadata objects) bounds a pass at 4 s without
    `-race` and, under it, at 8 times a pass over a quarter of the
    objects.
  - *Below copies (OPS-2).* Each pass records per bucket since when it
    has found live data objects it could not bring to `copies`
    (`below_copies`, schema 28: the start is kept while some stay
    below, the row goes when none are). The `below_copies` alert
    (§3.8) fires once that is longer than `replace_after`. `status`
    shows re-placement in progress under the bucket ("re-placement: N
    copies (B bytes) placed after the pool changed wait to be sent; M
    copies moved away wait to be deleted; objects below c copies with
    nowhere to place them since …"; `buckets[].replacing` with
    `queued`, `queued_bytes`, `dropping` and `below_since`), and each
    place's copies to delete (`places[].dropping`); a re-placed row's
    lag counts from `replaced_at`. Additive, schema 1. A removed
    friend's moved copies are left out of the bucket's count (#26
    review): it has no sender, and keeps them until its purge; its
    place still lists them.
  - *`copies` changes (EP-6).* `lendisk bucket set <name> --copies N`
    (`state.DB.SetCopies`, no daemon needed) changes it; the daemon's
    next pass adds or removes copies
    (`TestChangingCopiesAddsAndRemovesCopies`,
    `TestTheWorkerAddsAndRemovesCopiesWhenCopiesChange`,
    `TestBucketSetChangesCopies`).
  - *Scenario 13* runs end to end with four daemons and restic
    (`TestScenarioFriendLeaves`, at 24 MB per friendship raised to 32
    MB); its `status` line naming the numbers that would cover the
    deficit is LEND-7's protectable size (#37).
  *Decision (#26): move a copy only after its replacement is
  acknowledged, and trim the old place by an ordinary delete.*
  Comparable systems differ in where the bytes come from and when the
  old copy goes. Ceph's backfill copies a placement group to the new
  OSDs of its acting set from a current replica, and the old OSD keeps
  serving and is cleaned up only once the PG is active+clean on the new
  set (`osd_max_backfills` bounds it); lendisk takes the order, but the
  bytes come from the owner's store, since PEER-11 forbids a lender
  forwarding and the owner always holds them (EP-9). Tahoe-LAFS's
  repairer downloads and re-uploads only the shares missing on the
  servers it can reach, never deleting a share; its leases expire
  instead. lendisk deletes the moved copy explicitly, as a lender's
  quota counts it until its tomb is purged. Syncthing sends a removal
  as a new version ordered after the writes before it; a `dropping`
  row is that, in the place's FIFO at the object's sequence. The pass
  runs on a fingerprint of the pool rather than on events from every
  path that can change it (CLI commands in another process, the quota
  call, the passage of `replace_after`), as Ceph re-peers on each new
  OSD map epoch, and hourly besides.
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
  cancels the request (a friend switched off mid-transfer). *Decision
  (#89):* such an abandoned send first resets its connection
  (`replica.abandoning`: the request's connection, from an
  `httptrace` `GotConn`, is the tunnel's, and `transport`'s
  `tunnelConn.Reset` closes it with linger 0, so gVisor sends a RST
  and discards what it had not sent). A plain cancel closes it with a
  FIN queued behind the rest of the body the tunnel still holds, which
  a busy friend went on reading at its own pace, holding its
  reservation for the write and answering this node's retries over it
  until its stall bound (2 minutes) ended the body. With the reset the
  friend reads only what had reached it, then fails
  (`TestAnAbandonedSendResetsItsConnection`,
  `TestARetryAfterAnAbandonedSendSucceeds`). The connection is
  forgotten once the transport puts it back in its pool (the trace's
  `PutIdleConn`) or the request has returned, so a stall noticed on
  the last slow read never resets a connection another request may
  use (#92, `TestAnAbandonResetsOnlyTheRequestsOwnConnection`). A
  path that is down
  carries the reset no better than the FIN; the friend's stall bound
  ends that body, and its 503s (§3.5 LEND-2) keep this node retrying
  meanwhile rather than paused for quota. `gonet.TCPConn` exposes no
  linger, so the endpoint is reached by reflection, as tailcat's own
  `closeProxyConn` does; if gonet changes, the reset degrades to a
  plain close, logged as a warning with the friend's node ID (#92,
  `TestAResetThatFallsBackToACloseIsLogged`).
- **Operations.** `Put` streams the object from the store into
  `PutObject`; an object larger than the part size (or than one PUT,
  5 GiB) goes into the low-level multipart calls (`replica.Multipart`:
  `NewMultipartUpload`, `PutObjectPart`, `ListObjectParts`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`;
  `internal/replica/multipart.go`), with `x-amz-meta-lendisk-sha256`
  from the queue row, and resumes at the next part (REPL-6, below). An
  object no larger than the part size is one `PutObject`, sent again
  whole when interrupted. The request checksum is SHA-256 (S3-3, Q-Y.2): a single
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
  "paused for quota" (LEND-2), while its `503 SlowDown` (fits, but not
  beside the writes in flight, §3.5 LEND-2; or this node at its cap of
  requests in flight there, §3.7) becomes "busy" (`replica.ErrBusy`):
  retried with backoff like a network error, but recorded as the
  place's condition `busy`, counted as retry reason `busy`, and logged
  at info, not as a failure (*decision, #92:* the friend answered, so
  a friend briefly busy behind another borrower's writes is neither
  unreachable nor rerouted nor alerted as `friend_unreachable`; one
  busy for `reroute_after` on end is, see §3.4 Reachability;
  `TestABusyFriendIsRetriedWithoutFailing`). Only `SlowDown` is busy:
  the lender's other 503s (`ServiceUnavailable`: a gateway still
  starting, relationships it cannot read) say it is failing and stay
  `retrying` (`TestFriendRefusalsAreDistinct`). A probe answered
  `SlowDown` is a sign of life like any answer
  (`TestABusyProbeAnswerIsASignOfLife`); a `403` that says this node is not a
  friend becomes "removed" (PEER-5, SEC-15): the tunnel middleware's
  `AccessDenied` whose message is `state.NoRelationship`, or
  versitygw's `InvalidAccessKeyId` (its IAM no longer resolves the
  node-ID key). Other `403`s are retried like any other error and
  reported as what they are: `RequestTimeTooSkewed` says the two
  clocks differ by more than 15 minutes and to enable NTP, and
  `SignatureDoesNotMatch` and the rest name their S3 code (#53). A
  friend that removed this node normally refuses the tunnel
  connection at admission (SEC-10) before any `403`; it tells this
  node with the remove call (§3.2 PEER-5, #25), which ends the
  friendship here too, so the place becomes a former friend. Until that
  call arrives (this node offline, the friend's daemon down) the place
  shows as `retrying` with a dial error, not `removed`; `removed`
  appears only on a connection opened before the removal, which the
  middleware still refuses request by request (SEC-11), or from a
  lender that admits the key but has no friendship for it. A place
  that keeps answering `removed` raises `friend_refuses` (§3.2 PEER-5,
  §3.8, #83). A `412` to a put of a
  rewritable key is handled as *Rewrites* says, below; any other `412`
  is refused at that place as a changed key, with the hash a `HEAD`
  reports, and raises `key_changed` and `place_refused` (*EP-11*,
  below, #22); anything
  else is a retry with backoff. A `BadDigest` (also
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
  `place_refused`, `key_changed` or `objects_not_replicated` alert (§3.8, #13). A paused (`quota`) or
  `removed` place tries its head row again after an hour (LEND-2);
  other errors back off from 1 s, doubling to 5 minutes, with the place
  `retrying`.
- **Rewrites (#74, EP-7, LEND-4).** A put whose queue row says the key
  is rewritable (`queue.rewritable`, set at capture from the bucket's
  profile, §3.3 EP-7; `state.Placement.Rewritable`) is sent to a place
  that acknowledged another version of the key as a rewrite of that
  version (`state.AckedPutHash`: the last put the place acknowledged,
  deleted since or not, since a deleted version is in the lender's tomb
  and a rewrite naming it is stored beside it, §3.5): `Object.Replaces`, which `Friend.Put` sends
  as the user metadata `x-amz-meta-lendisk-replaces` with the
  replaced version's SHA-256 (`replica.MetaReplaces`), signed like
  every header. A first put, a put to a place that acknowledged the
  same bytes, and a put of a key that is not rewritable carry no mark,
  except, for any key, a put after the place acknowledged a delete of
  it, which names the version in its tomb (*EP-11*, below, #22). If the place refuses the rewrite all the same (`412`), it holds
  a version the index does not show it acknowledged: a rewrite stored
  there whose answer was lost before a later capture superseded and
  cancelled its row, or, after `recover`, another friend's version
  (§3.1 ID-5). The worker then asks the place what it holds
  (`Replica.Head`: `Friend.Head` is a `StatObject` reading
  `x-amz-meta-lendisk-sha256`; the lender admits `HeadObject`) and, if
  that is another version, sends the rewrite once more marked with its
  hash; a place already holding the row's bytes is an
  acknowledgement. Only the key's latest capture is ever sent (a
  superseded row is cancelled first), so asking does not let an older
  version replace a newer one; and a friend that lies in its answer
  gains nothing, since the mark only lets it store what this node
  sends. If the place says it holds nothing live at the key (a deleted
  object's tomb, or the `HEAD` fell inside the lender's own rewrite of
  it), or holds exactly the version the refused rewrite named, that may
  pass: the row waits with the network backoff, the place shows
  `retrying`, and nothing is refused or alerted
  (`TestRefusedRewriteWithNoNewsFromTheHeadIsRetried`), but only for
  an hour (`Worker.RewriteRetry`, `replica.DefaultRewriteRetry`; kept
  in memory, so a restart starts the hour again). It need not pass: a
  tomb of a version other than the one named stands until its purge,
  and a lender too old to read the mark refuses every rewrite. Past the
  hour the row is refused for good at that place, `place_refused`, its
  log line and alert saying the friend keeps refusing a rewrite of a
  rewritable key, so the place's strict FIFO moves on instead of
  stalling for the whole window
  (`TestRewriteRefusedPastTheRetryWindowIsRefused`); a later write of
  the key heals it as any refusal is healed. A rewrite
  refused again after the retry with the place's hash is a refusal like
  any other `412` (`place_refused`). *Decision (#23): a rewrite goes in
  parts like any put.* Rewritable keys are a few KiB, but a put marked
  after a delete (*EP-11*, below) may be any key of any size, so a
  marked object above the part size is a multipart upload carrying the
  mark at `CreateMultipartUpload`; the lender checks it there and again
  at completion (§3.5 LEND-4). The mark an upload was begun with is
  kept with it (`transfer.replaces`); a resumed upload whose row now
  needs another mark is aborted and begun again. Keeping rewrites to
  one PUT would have refused a deleted-and-rewritten object above 5 GiB
  for good, with no cause `RequeueOversize` could tell apart. Precedent:
  S3's conditional writes apply `If-Match` and `If-None-Match` to
  `CompleteMultipartUpload` as to `PutObject` (2024), checked when the
  object is published, not when the upload begins
  (`TestALargeRewriteIsMarkedAtItsCreate`).
  *Choice of mark (#74).* S3's conditional writes put `If-Match` with
  the current version's ETag on a `PutObject` to replace only that
  version; S3 Object Lock with versioning keeps the replaced version
  until its retain-until date. lendisk binds the same way, to the
  version's SHA-256 rather than its ETag: the hash is what LEND-4
  already identifies a version by, what the borrower keeps for every
  acknowledged row, and what the lender stores with every object; the
  ETag is the lender's store's own (MD5 on posix), which the borrower
  never learns. `If-Match` carrying a hash would misuse the header, and
  posix would evaluate it against its ETag. A user-metadata header
  reaches `Lending` in `PutObjectInput.Metadata`, which strips it
  before the store; the gate admits it as part of an ordinary
  `PutObject` (§3.9 SEC-9). A stale rewrite (an old request delayed or
  retried after a newer one landed) names a hash the key no longer
  holds and is refused.
  *What the mark is not.* The mark is a consistency check between an
  honest borrower and its lender, against delayed, duplicated or
  reordered requests; it is not a security control against the
  borrower. A borrower knows the hash of every version it stored, so a
  hostile or compromised one can mark a rewrite of any key it likes,
  and the lender, which does not know the borrower's profile, stores
  it. What protects the data is the tomb: every version a rewrite
  replaces is kept, counted and unreachable by the borrower until its
  window passes, exactly as a deleted object is (RET-1, RET-2, §3.6;
  `rewriteAttacks` in `TestHostilePeer`).
  A lender's `412` to a rewritable key that the retry could not place
  stays a `place_refused` refusal: EP-11 exempts rewritable keys, so it
  is not an immutability violation but a friend refusing what it should
  take.
- **EP-11, the immutability policy (#22).** `internal/state/immutable.go`.
  - *At capture.* `FinishPut` checks, in the transaction that completes
    the row, a put whose `queue.rewritable` is not set: if any friend's
    latest acknowledged row of the key before it, since the key's last
    done delete, is a put of another SHA-256 (`state.ackedOther`, read
    through `queue_key` and `placement_seq`), the row is done (the
    object is in the store and the client got its answer: the local
    store is the user's) but `placeTx` is not called, and
    `queue.immutable` (migration 23, partial index `queue_immutable`)
    records the acknowledged hash. A
    friendship that ended no longer counts as a replica. An identical
    re-upload is still dropped first (EP-3), so it costs nothing and
    alerts nothing; that includes re-sending the changed bytes again.
    After the commit `state.DB.KeyChanged` is called; `serve` logs the
    change at error level with `alert=true` and calls the alert
    service's `Check`. Tests: `TestAChangedAcknowledgedKeyGetsNoPlacement`,
    `TestAChangedKeyIsStoredButNotPlaced` (through the gateway).
  - *At a lender.* A `412` to a put of a key that is not rewritable
    means the friend holds the key with another hash the index does not
    show: a send whose answer was lost and whose row a later write then
    superseded, or, after `recover`, a version listed by no friend the
    node's own row knows. The worker asks the place what it holds
    (`Replica.Head`): the row's own bytes are an acknowledgement; a
    failed `HEAD` is a network error, retried with the backoff (the
    put is sent again first) so the hash can be named; otherwise the
    row is refused at that place with cause `immutable` and
    `placement.held` the hash it reported (`''` when it holds nothing
    live; `state.DB.RefuseChanged`). That is the same condition and the
    same alert, `key_changed`, but the hash is the friend's word, not an
    acknowledgement, and a broken or hostile friend may answer `412` to
    every key. *Decision (#22 review):* so `ChangedKeys` lists such a
    friend in `ReportedBy` (`reported_by` in `status --json`), the
    alert and `status` say "friend B reports holding other bytes, <hash>
    (as reported by B)" and that a friend refusing every key is at
    fault, and the refusal still counts as the friend's own:
    `RefusedAtPlaces` counts cause `immutable` with cause NULL, so
    `place_refused` names the friend too
    (`TestAFriendRefusingEveryKeyIsNamed`). `RefusedObjects` counts only
    the store's causes. Other places keep the row: the borrower's index
    holds no acknowledged version there, and a send that has landed
    cannot be undone. An identical re-upload does not heal such a
    refusal (the place would refuse again): `FinishPut` treats it as a
    retry, and a healing re-upload for another refusal skips a place
    that refused the healed row as immutable
    (`TestALendersImmutabilityRefusalIsReportedAsAChangedKey`,
    `TestALendersImmutabilityRefusalIsAChangedKey`).
  - *Decision: acknowledged at one place, queued at another.* The key
    counts as acknowledged as soon as one friend acknowledged it, so the
    change is held back everywhere. The old version's rows still queued
    elsewhere stay queued until their sender reaches them, then are
    cancelled as `Superseded` (or `abandoned`, if taken), as for any
    later write: the store no longer holds the old bytes, and fetching
    them back from the friend that has them is not worth a code path
    for a violation. Those places get neither version; the key is below
    `copies` and stuck (below). A key queued everywhere and acknowledged
    nowhere is no violation: the new version replaces the old in the
    FIFOs as before, and a send of the old one that landed with its
    answer lost surfaces as the lender's `412`
    (`TestAChangeOfAKeyAcknowledgedAtOnePlaceIsHeldBack`).
  - *Decision: how it clears.* `state.DB.ChangedKeys` reports a changed
    write (held back, or refused `immutable` somewhere) only while it is
    its key's latest done write, so the condition ends, and
    `key_changed` clears, when the key is written back to the
    acknowledged bytes or deleted. A write back heals the row the change
    replaced: `FinishPut` and `placeTx` pass over rows marked
    `immutable` (never placed) when they look for the row a re-upload
    heals, so places holding those bytes are acked at once and nothing
    is sent. After a delete, which every holder receives, a write is a
    new object, placed as usual; each lender keeps the old version as a
    tombstone for the retention window (RET-1). That is how a user keeps
    the new bytes: delete the key and write it again. A lender refuses
    other bytes over a tomb unless the put is marked as replacing it
    (`putOverTomb`, LEND-4), so the worker marks the put of *any* key,
    rewritable or not, with `x-amz-meta-lendisk-replaces` when the
    place's latest acknowledged row of the key is a delete: the hash of
    the last put before it that the place acknowledged, or refused as
    `immutable` with the hash it reported (`state.DB.ReplacedHash`; #74's
    lender takes a marked put of an absent key whose newest tomb holds
    the named hash; after the tomb's purge, a marked put of a key with
    no tomb is stored as a plain put)
    (`TestReplacedHashNamesTheVersionADeleteTombed`,
    `TestAPutAfterADeleteNamesTheTombedVersion`, and against two real
    daemons `TestDeleteAndWriteAgainReplacesTheTombedVersion`: the new
    bytes are stored, the old version stays a tomb, no `key_changed`).
    The same holds for a friend whose `412` named another version: once
    it acknowledged the delete, the key written again is marked with the
    hash it reported. There is no
    acknowledge command. Precedent: ZFS lists files with permanent
    errors in `zpool status -v` for as long as the damaged data is what
    the pool holds; the list goes once the file is deleted or restored
    and a scrub has run, and `zpool clear` resets counters, not the
    data. `restic check` stores nothing and reports the damage on every
    run until `restic repair` fixes the repository. Both tie the report
    to the data, as lendisk's alerts are conditions, never events
    (§3.8). A dismissed alert would leave the key unreplicated and the
    watermark stuck behind it with nothing saying why
    (`TestAChangedKeyClearsWhenRevertedOrDeleted`,
    `TestAChangedKeyRaisesKeyChangedUntilWrittenBackOrDeleted`).
  - *The watermark (#20).* A changed write is a put no place holds, so
    no `w` at or above it is valid while it is the key's latest write;
    it is stuck with reason `immutable` (`state.Shortfall.Immutable`,
    `buckets[].offsite.stuck.immutable`), and `status --wait` exits 4.
    The bucket is never claimed offsite with the new bytes.
  - *`status`.* Under each bucket, "! N keys written with other bytes
    than a friend acknowledged or holds; the local write stands but is
    not replicated (EP-11):", then each key with its acknowledged and
    new hash, or "friend B reports holding other bytes, <hash> (as
    reported by B)" for a `412` (10 shown), and what to do; `status
    --json` adds `buckets[].changed` (`key`, `seq`, `captured_at`,
    `acknowledged_sha256`, `sha256`, `places`: each friend holding
    another version and its hash, `reported_by`: those whose hash is
    their word). Additive, schema 1
    (`TestStatusShowsAChangedAcknowledgedKey`).
  - *Rewritable keys (#74)* are exempt and replicate as rewrites, with
    no alert: `TestARewritableKeyIsPlacedAsARewrite`,
    `TestRewritableKeyIsSentAsARewrite`, and end to end
    `TestResticMigrateReplicatesTheNewConfig` and
    `TestReplicateKopiaToFriend`, which check that no `key_changed`
    alert and no changed key appear.
  - *Decision: after `recover` (§3.1 ID-5).* The placement rebuilt from
    the friends' lists is acknowledged with their hashes, so the
    recovered node's first write of such a key with other bytes is a
    changed key. That is right: the friends hold that version, would
    refuse the new bytes anyway (LEND-4), and a restic or Kopia key that
    is not rewritable is named by its content or written once, so other
    bytes under its name mean a second repository in the bucket, a
    forgery, or corruption. A key the verdict left in conflict has no
    acknowledged row, so the node's write is placed and each friend
    holding another version answers `412`, which is the same alert.
    Rewritable keys (restic's `config`, Kopia's `kopia.*`) stay exempt,
    as their recovered rows are marked rewritable. The same bytes are a
    silent re-upload (`TestAChangedWriteOfARecoveredKeyIsHeldBack`).
- **Local errors (#50, #57).** The worker tells the store's failures apart
  from the network's and the place's. A row whose object the store
  definitely cannot supply as captured is a *local error*: the file is
  gone (`ENOENT`, or `ENOTDIR` for a path through a file), its size
  differs from the capture, it is a directory or special file, its key
  names no file under the bucket, it is larger than the worker sends
  (5 TiB, `replica.MaxObjectSize`, in parts since #23; 5 GiB,
  `replica.MaxPutSize`, to a place that takes no parts; refused with
  cause `oversize`, *Healing* above), or
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
  second when the FIFO is empty; it takes the row (`StartPlacement`,
  *Deletes* above) after the barrier lets it go, and a row a delete
  cancelled meanwhile is not sent. A put whose key a later completed
  capture replaced or deleted (`state.Superseded`) is cancelled, since
  the store no longer holds its bytes; deletes are always sent, and a
  key or bucket the friend lacks counts as deleted. A put reads
  `<store>/<bucket>/<key>` from the `posix` store as it sends it; what
  it cannot read as captured is a local error, above (#48, #50). On
  acknowledgement the row becomes `acked` with its hash and the place
  `ok`. Nothing is acknowledged before the place answers, so a restart
  of either side sends the head row again: whole for an object up to
  the part size, from its next part above it (REPL-6); the friend
  stores the same bytes under the same key, and nothing is sent twice
  that was acknowledged. `status` (and `places` in `status --json`) shows per
  place its condition, queued objects and bytes, lag (age of the oldest
  queued row) and the rows it refused; per bucket (`buckets[].refused`)
  the objects refused for a local error; refusals a later write healed
  are not counted (*Healing*, above).
- **REPL-6 (#23)** An object larger than `replication.part_size` (16
  MiB; 5 MiB to 5 GiB) goes in parts (`Worker.putParts`). Each upload
  belongs to a placement row, since every place has its own: table
  `transfer` (migration 31) keeps, per place and queue row, the upload
  ID, the part size it was begun with and the mark it carries (#74),
  and `transfer_part` the number, ETag, SHA-256 and size of each part
  the place acknowledged, written as each part completes
  (`state.DB.BeginTransfer`, `AddTransferPart`). After a restart or an
  interruption the worker lists the upload's parts at the place
  (`ListObjectParts`), keeps the recorded parts it lists with the same
  ETag, and sends the rest, each read from its offset in the store (an
  `io.SectionReader` over the store's file, through the same stall
  guard as a whole object); it logs "resuming an upload at its next
  part" with `parts_kept`. A part the place holds but the row missed
  (killed between the answer and the write) is sent again; a
  `NoSuchUpload` (aborted, or abandoned by the lender after a week,
  §3.5) starts the object over, as does a row that now needs another
  mark. A part size giving more than 10000 parts (`replica.MaxParts`)
  grows to the next whole MiB that fits; the largest object sent is 5
  TiB. Before each part the worker checks that a later write has not
  superseded the row. Completion is answered by the place's whole-object
  check (LEND-4); a refusal there, a superseded row and any other
  outcome that takes the row out of the FIFO abort the upload at the
  place (best effort) and drop its record (`Worker.forget`); records
  of rows that left their FIFO otherwise (re-routed, refused by another
  sender) are dropped at start and on each re-routing tick
  (`state.DB.StaleTransfers`), their uploads left for the lender to
  abandon. Objects up to the part size are one PUT and restart whole.
  *Decision:* the record is per placement row, not on the queue row
  the requirement's wording suggests, because each place's upload has
  its own ID and parts. Against a real lender with `SIGKILL` inside part
  4 of 6 (`TestALargeObjectResumesAtTheNextPartAfterSIGKILL`); in
  the worker, `TestALargeObjectIsSentInPartsAndRetriedFromTheFailedPart`,
  `TestARestartContinuesAtTheNextPart`,
  `TestAnUploadGoneAtThePlaceStartsOver`, `TestASmallObjectRestartsWhole`,
  `TestAnObjectAboveOnePutGoesInParts`.
- **REPL-7 (#23)** `internal/pace` owns pacing: one `pace.Pacer` per
  daemon, shared by the worker, the read-through's fetcher and the
  tunnel bridge. Limits are token buckets (`golang.org/x/time/rate`,
  64 KiB burst, each read cut to it, reserved at the pacer's clock and
  slept by its injected sleep): `replication.upload_limit` and
  `download_limit` for all friends together, and per friend
  `[[replication.peer_limit]]` (`peer` by node ID, `upload`,
  `download`), in bytes a second. Uploads wrap every body the worker
  sends (single PUTs and each part, `Pacer.Send`); downloads wrap every
  read-through fetch (`Fetcher.Pace`, `Pacer.Fetch`;
  `TestFetchesArePacedByTheDownloadLimits`).
  *Decision: challenges are not downloads.* A daily or full
  verification (VER-1, §3.6) moves 32 bytes per object over the
  tunnel; the reads happen at the lender, which paces them per friend
  already (§3.7 *Pacing*). Counting them would slow nothing on the link.
  *Pace by concurrency (#76):* a lender ends a request body that
  arrives below its floor rate (8 KiB/s over 10 minutes, §3.5 LEND-2).
  So no limit may be below `pace.MinRate`, 16 KiB/s, twice the floor
  (config refuses it, `TestRatesBelowTheFloorAreRefused`), and a
  global upload limit admits only as many bodies at once as it keeps
  at `MinRate` each (`Pacer.Slot`, at least one; each body takes a slot
  for its length), so a body is never trickled below the floor; a
  per-friend limit applies to one body at a time, as each place sends
  one object at a time (`TestUploadSlotsKeepEveryBodyAboveTheFloor`,
  `TestAGlobalLimitPacesByConcurrency`).
  The schedule (`replication.schedule`, local-time windows such as
  "01:00-07:00", comma-separated, one ending before it starts running
  past midnight; empty is always) is checked before a row is taken and
  before each part, so a closing window stops a transfer at the end of
  the part in flight, which is part size over rate in time (16 s for 16
  MiB at 1 MiB/s); the row stays taken with its parts recorded and
  continues at its next part when a window opens
  (`TestAClosingWindowStopsAtTheEndOfThePart`). Deletes wait with the
  puts; re-placement's moves are FIFO rows and so obey both (REPL-13).
  The read-through ignores the schedule: a restore waits for no window.
  The bit-rot sample's disk reads have a bucket of their own
  (`store.bitrot_read_limit`, `Pacer.Scrub`, §3.6 VER-5), reloaded with
  the rest; the schedule does not apply to them.
  *Decision: changes apply without a restart, from the file.* The
  daemon never writes its config (OPS-6), so it re-reads it: it looks
  at the file's modification time, size and inode every 10 s and
  reloads on a change, and reloads at once on SIGHUP (the systemd
  example's `ExecReload`); the flags and environment it started with
  still apply on top (`cli.reloader`). The pacing settings
  (`part_size`, `schedule`, the limits, LEND-5's) apply from each
  transfer's next read (`Pacer.Set`, `SetLimitAt`, Q-Y.4); a file that
  no longer loads keeps the settings in force, with an error logged;
  a change to any other setting is logged as waiting for the next
  start (`TestPacingChangesApplyWithoutARestart`). Precedent: rclone
  changes `--bwlimit` at run time through its rc API
  (`core/bwlimit`) and a timetable, restic's `--limit-upload` is fixed
  for the run, Syncthing applies most settings from its config live and
  marks the rest "requires restart", and nginx, sshd and Prometheus
  reload on SIGHUP. lendisk has no settings API (the CLI talks to the
  daemon through `lendisk.db`), so the file, watched and on SIGHUP,
  serves both the editor in a container and `systemctl reload`.
- **REPL-8, seeding from a disk (#36, package `seed`).** Precedent:
  restic, Kopia (`repository sync-to filesystem`), rclone and Syncthing
  all seed with plain files in the tool's own layout, carried and copied
  into the far side, which then compares hashes and sends only what
  differs; none needs a special container. lendisk does the same, with
  one manifest beside the files.
  - *Export* (`lendisk export <bucket> --for <friend> <dir>`, on the
    borrower, daemon running or not): each key whose latest done write
    is a put with a `queued` or `acked` placement row at that friend
    (`state.SeedObjects`): its share of the data and every metadata
    object (REPL-12). The bytes are read from the store directory
    (`<store>/<bucket>/<key>`) and hashed on the way; a key whose copy
    no longer matches its capture hash (a write under way) is left out,
    counted, for the network. Layout: one file per key at its key path,
    or for a `kopia` bucket Kopia's sharded layout and `.shards`
    (`export.KopiaPath`, as LEND-6's export); a directory marker is
    skipped (no lender stores one). `<dir>` must be new or empty; files
    0600, directories 0700, `O_EXCL` (LEND-6's rules; on a FAT or exFAT
    disk the modes are the filesystem's). *Manifest*
    `.lendisk/seed.json`: `{"seed": {format 1, borrower, lender, bucket
    (sealed name, SEC-16), layout, exported_at, objects: [{key, size,
    sha256}]}, "mac": HMAC-SHA256(pair secret, "lendisk-seed-v1\n" ||
    seed)}`. `.lendisk/` is reserved (SEC-5), so it never collides with
    a key, and restic and Kopia ignore it. The MAC uses the per-pair S3
    secret (NET-7a): import knows the borrower from the manifest and
    accepts it only if it authenticates for this node, so a stranger's
    disk cannot fill a friend's quota, and a disk for another friend is
    refused by name.
  - *Hold.* Export first writes a `seed` row (migration 39: place,
    bucket, `through` = the highest seq exported, counts, outcome). While
    it is pending, `NextPlacement` skips that friend's queued rows of
    the bucket, so the disk's objects are not also sent over the
    network, and writes since queue behind them in order ("queued
    operations continue from where the export ended"). Other buckets and
    other friends are not held: rows a pending seed holds count in
    neither of `Held`'s checks (the metadata barrier's data puts, the
    delete mirror's metadata), as an unreachable place's do not (#21),
    so metadata, snapshots and prunes reach every other friend and the
    watermark advances as they and the seed's confirmations acknowledge
    (decided in #36's review). The seeded friend's replica of the bucket
    lags, as an offline friend's does; its own FIFO keeps the order,
    since it is sent nothing of the bucket until the seed ends. A failed
    export drops its row. A second
    export of the same bucket for the same friend replaces the seed.
    Reroute (#19) still applies to a friend unreachable past
    `reroute_after`.
  - *Import* (`lendisk import <dir>`, on the lender, daemon running):
    reads and authenticates the manifest (at most `seed.MaxManifest`,
    512 MiB, some 2.5 million objects), checks the borrower is a
    friend, then puts each object through the lender's own gateway
    socket (`gw.sock`) with `replica.NewSeeder`: the borrower's access
    key and the pair secret the lender derives too, `x-amz-meta-lendisk-
    sha256` and the checksum header exactly as the worker sends them.
    So quota and floor (`reserve`), hash (`putLent`'s body check),
    existing key (`putAgain`, 412) and key rules are the one network
    path, with no second implementation (decided #36: not an
    in-process `Lending`, whose usage counter and key locks live in the
    daemon). A gateway that refuses before reading the body closes the
    socket; the client's connection discards writes after EPIPE so it
    reads that answer (`seed.answered`). An object refused alone
    (`BadDigest`: damaged on the disk; 412: held with other bytes; a
    file missing or of another size; anything but a regular file) is
    reported and the import goes
    on; quota, floor, a removed friend or a gateway failure stop it.
    Run again, objects already stored answer as `putAgain` and are not
    stored twice. Exit 1 if anything was refused or it stopped.
    Objects go in one PUT each (the worker's multipart path is not
    used), so an object over 5 GiB is not seeded; restic and Kopia
    packs are far smaller. The disk is someone else's filesystem: files
    and the manifest are opened under an `os.Root` of the directory,
    every path component `Lstat`ed and refused if it is a symbolic
    link, then opened `O_NOFOLLOW|O_NONBLOCK` and refused unless a
    regular file (`seed.openPlain`), so no link is followed and no FIFO
    or device blocks or is read; a manifest key that is absolute or has
    `..` refuses the whole manifest.
  - *Confirm* (`seed.Confirmer`, in the borrower's daemon, `full`
    role): every minute each pending seed's friend is listed (§3.7
    `list`; a 404 `NoSuchBucket` is "nothing yet"; failures back off
    from the poll, doubling, to an hour). Each live listed key whose
    hash equals the queue row's capture hash acknowledges that held put
    (`AckPlacement`, with the hash: REPL-10's index); a key listed with
    other bytes, tombstoned, or absent stays queued. The disk is never
    trusted for this: only the friend's list. The seed ends (`done_at`,
    `outcome`) when no held put up to `through` that is still its key's
    latest write is queued (`confirmed`); when it confirmed some and no
    listing has confirmed more for `state.SeedQuietAfter` (1 h:
    `quiet`, an import ended or stopped part way); when nothing was
    confirmed within `state.SeedExpiry` (30 days: `expired`, a disk
    lost or never imported); or when the friend is no longer one
    (`removed`). Then the FIFO sends what is left, in seq order.
  - `status` shows each pending seed with its age and when its hold
    ends at the latest if nothing more is confirmed (text, and
    `expires_at` in `--json`'s `seeds`, which lists every seed);
    `status --wait` that times out names each pending seed of the
    bucket and the objects it holds.
  - *Tests:* `TestSeedHoldsRowsUntilTheFriendsListConfirmsThem`,
    `TestASeedHoldsOnlyTheSeededFriend` (`internal/state`);
    `TestImportFollowsNoLinkAndOpensNoDevice`, `TestExportWritesExactlyTheKeysPlacedAtTheFriend`,
    `TestExportOfAKopiaBucketIsInKopiasLayout`,
    `TestOnlyRowsTheListShowsWithTheirHashAreAcknowledged`,
    `TestSeedEnds` (`internal/seed`);
    `TestScenarioSeedAFriendFromADisk` (quota stop and rerun, a damaged
    object, a held key, confirmation, no re-upload by the worker's
    metrics) and `TestSeedExportAndImportRefusals` (`internal/cli`).
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
- **REPL-11, the watermark (#20).** The bucket's watermark is the
  highest sequence `w` up to which every object has `copies`
  acknowledged placements; it is the offsite-complete point (REPL-4).
  Nothing stores it: `state.DB.Offsite` computes it from `queue` and
  `placement` whenever `status` asks (`internal/state/offsite.go`), so
  it advances as each acknowledgement is recorded, needs no migration
  and cannot drift from the rows. Per-place lag (the oldest `queued`
  row's age) is kept beside it (`places[]`) for `status` and OPS-2.
  How each row counts, against a candidate `w`
  (`TestWatermarkAdvancesAsAcknowledgementsArrive`,
  `TestWatermarkFollowsRewritesAndDeletes`):
  - *a put* counts while `copies` places still hold its bytes: the
    place's latest `acked`, `abandoned`, `dropping` or `drained` row of
    the key is an `acked` put of the same SHA-256. An acknowledgement
    that a later acknowledged delete or rewrite at that place ended no
    longer counts; an older acknowledgement of the same bytes (a
    healing re-upload's) does. An `abandoned` row in between counts
    against, since its send may have landed other bytes; so does a copy
    re-placement moved away (`dropping`, then `drained`, #26), from the
    moment its replacement is acknowledged, even while the delete has
    not reached the place: a gone friend that returns does not count
    the copies moved away (from the #20 review). Metadata, placed at
    every friend, needs `copies` of them like data.
  - *superseded, deleted and rewritten puts:* a put that fails the above
    is still no obstacle once a later done write of its key, rewrite or
    delete, is at or below `w`: as of `w` the bucket holds that write,
    not this one. This is `current` evaluated as of `w`, not as of now:
    a pack the prune deletes after `w` still counts at `w`, so while a
    repack's new pack is unsent and the old one is already deleted at a
    friend, the snapshots before both are not claimed offsite (the old
    pack is no longer held, the new one not yet).
  - *a delete* needs nothing: it adds no bytes to be offsite.
  - *`queued`, `refused`, `dropped` and `abandoned` rows* are not
    acknowledgements. *Decision (#20, the #57 review's finding): a
    refusal holds the watermark honestly.* One friend refusing
    everything (`BadDigest` for bytes that match, a `412`) does not
    stall others' metadata (the barrier ignores refusals, above) and is
    never counted as a copy, so the watermark stops before the first
    object only it was to hold, and stays there until a later write of
    the key or re-placement (#26, which places missing copies by D6's
    choice, excluding holders) brings that object to `copies`: it is
    not pinned forever, but never claims what is not offsite. Metadata
    refused by one friend still reaches `copies` at the others. The
    same holds for a `dropped` copy and for the pool's lack of room.
    `Offsite` reports such objects as *stuck* (their key's latest write,
    held and queued at fewer than `copies` places) with the reason
    (refused, dropped, gone, unplaced; `immutable` for a changed key,
    EP-11 above, #22), so `status` says why the snapshot is
    not offsite and `status --wait` stops instead of waiting silently
    (`TestAFriendRefusingEverythingLeavesTheWatermarkHonest`,
    `TestDroppedAndUnplacedCopiesAreStuckQueuedOnesWait`,
    `TestStatusWaitStopsWhenAFriendRefusesEverything`). A copy still
    `queued` at an unreachable friend is waiting, not stuck: `Reroute`
    may move it. *Decision (#37 flake): nothing is stuck while a
    re-placement pass is due.* A pass that goes through every bucket
    records the pool it answered (its `PoolFingerprint`, table
    `replace_pass`, schema 38), even when a bucket's batch failed
    (#90); a pass cut short by its context records nothing. A failed
    bucket counts as answered: otherwise a bucket whose batch keeps
    failing would leave its short objects (and, with one record per
    pass, every bucket's) waiting forever, `status --wait` with them,
    where stuck says a person is needed; the worker retries a failed
    pass at its next re-routing tick, so a passing failure is stuck for
    about a minute
    (`TestAPassThatFailsOnABucketStillLetsShortObjectsBeStuck`,
    `TestACancelledPassLeavesThePassDue`). While
    the pool differs (a quota raised, a friend added, back or resumed
    from a pause for quota, before the worker's next look) or no pass
    has run, a short put is waiting for the pass, not stuck, whatever
    its reason; only a changed key (`immutable`) and an unmatchable
    pending put stay stuck, as no pass places them. Otherwise
    `status --wait` run right after `peer quota` on both sides exited 4
    for an object the pass seconds later placed
    (`TestAnObjectWithoutRoomWaitsForThePassAQuotaRaiseIsDue`,
    `TestAPauseForQuotaEndingMakesAPassDue`). With the daemon stopped
    no pass runs, and such objects wait with it.
  - *a pending put* (#20 review), any data or metadata put whose queue
    row is not done yet, has no placement at all; a completed multipart
    upload is even answered before its row is hashed and done (§3.3,
    #49), so the tool may write the index and snapshot naming the pack
    first. No `w` at or above a pending put is valid; it counts as
    waiting (`pending`, bytes not yet known) and holds the metadata
    barrier too (above). It is stuck only when marked unmatchable
    (above, reason `unmatchable`). A row stranded pending holds the
    watermark until it is settled
    (`TestAPendingUploadBelowTheSnapshotHoldsTheWatermark`,
    `TestStatusWaitWaitsForAMultipartPack`).
  - *only the pool holds copies* (#20 review). An acknowledgement
    counts only at a friend with an address that does not refuse this
    node (`removed`) and is not gone past `replace_after`: the places
    `state.DB.PoolSize` counts. *Decision:* a gone friend is no holder.
    REPL-12 no longer counts it as room and REPL-13 re-places its copies
    (#26), and an offsite claim should not rest on a friend silent for a
    week; it may well still hold the bytes, and counts again as soon as
    it answers. Its copies are stuck (reason `gone`) until re-placement
    or its return. A friend merely unreachable (past `reroute_after`,
    within `replace_after`) still counts: scenario 2's friend away for
    three days holds its copies throughout
    (`TestARemovedOrGoneFriendHoldsNoCopy`, `TestHoldsRules`).
    *Decision (#25, the #20 review's checkbox): a friend this node
    removed, or that removed it, holds nothing from the commit.* Its
    friendship row is gone, so it is no place at all: its
    acknowledgements no longer count (stuck reason `gone`, "held by a
    friend removed or gone"), `BelowCopies` counts only rows at
    friends, so what it held shows as below copies, and its unsent rows
    leave its FIFO in the removing transaction (a put a sender took
    becomes `abandoned`, every other queued row `dropped`), so none
    holds the metadata barrier. It may well keep the bytes for the
    retention window, but this node can no longer reach them, so an
    offsite claim must not rest on them. So it stops counting at the
    removal, before its purge can happen, which is the #20 review's
    checkbox met early. Re-placement (§3.4 REPL-13, #26) restores
    `copies` at once; until then `status` says copies at an ended
    friendship no longer count
    (`TestARemovedFriendHoldsNoCopyAndNothingWaitsForIt`,
    `TestARemovedFriendsCopiesArePlacedByTheChoiceExcludingHolders`,
    `TestRemovingAFriendReplacesItsCopies`).
  - *local-only keys* are never queued, so never counted.

  `w` is valid when every put at or below it passes; the watermark is
  the highest valid `w` up to the bucket's latest done write. Each
  failing put `r` rules out `[r, next)`, `next` its key's next done
  write (none for the key's latest), so one pass over the failing puts
  in descending order finds it. The query reads every done put of the
  bucket once, with one indexed look-up per place (`placement_key`),
  counted once in a materialized CTE; a put's next write and queued
  rows are read only when it is short. The snapshot look-ups name
  `class IN ('metadata', 'snapshot')` so the partial index
  `queue_metadata` serves them, and the latest-write look-ups walk the
  queue backwards by sequence (`+q.bucket`) instead of sorting the
  bucket. About 6 µs per object at three friends on the development
  machine (140 ms for 23 000, was 175 ms; 1.35 s for 5 600 under
  `-race`, was 1.6 s; 1.6 s for 221 000 in the #20 review), so some
  6 s at a million objects there and several times that on a Pi.
  `TestOffsiteOfALargeBucketIsFast` bounds it at 2 s without `-race`
  and, everywhere, at 8 times a yardstick query (one `placement_key`
  look-up per put and place) timed beside it, so a loaded CI runner
  under `-race` (9.6 s on a7d636f) does not fail it but a query that
  grows worse than linear does; it measures about 2.5 times. `status` reads it once per bucket and
  recounts what waits for the latest snapshot with `Offsite.For`, which
  reads nothing (`TestForRecountsLikeAFreshRead`); `status --wait`
  pauses ten times as long as its last read took, never less than 2 s
  (`TestStatusWaitBacksOffOnASlowRead`). *Next step (#20 review): a
  stored, incremental watermark* (per bucket, advanced as
  acknowledgements, deletes and settled pending rows arrive, recomputed
  only from the lowest sequence a change can affect), so neither cost
  grows with the repository.

  *What `status` names (REPL-4, REPL-11).* Under each bucket, and in
  `buckets[].offsite` of `status --json` (`complete`, `watermark`,
  `latest`, `snapshot` and `latest_snapshot` with `key`, `id`, `seq`,
  `captured_at` and `offsite`, `through`, `before`, and `waiting` and
  `stuck` counts with `pending` and the reasons; additive, schema 1).
  Capture times are whole seconds and sequences follow them, so a time
  claim is strictly before a second (#20 review): the second of the
  first write that is not offsite may hold offsite writes too, and the
  second of an offsite index may hold another index that is not. Both
  claims assume sequence order follows `created_at`: a clock stepped
  back (no NTP, a manual set) can capture a later write with an earlier
  second, and the claim then overclaims for it:
  - *restic:* the latest snapshot key still live, `snapshots/<id>`, by
    restic's short ID (its first 8 characters, as `restic snapshots`
    prints): "snapshot 1a2b3c4d is offsite at 2 copies"; while the next
    is on its way, "snapshot … is not offsite at 2 copies yet: N
    objects, B bytes still to reach 2 places" with the stuck count and
    reasons, and the earlier one that is offsite. The profile says its
    keys name snapshots (`names_snapshots = true`, EP-7's profile data).
  - *Kopia:* a snapshot key is an index blob (`xn0_<hash>-s<session>-c1`
    in 0.23: one per session, each snapshot's session writing a `p`
    data pack, a `q` pack holding the manifest and that index, and
    maintenance writing its own), and Kopia's snapshot IDs (`k…`
    manifest IDs) are inside encrypted `q` packs. *Decision (#20):*
    `status` names the time, not a snapshot: "every snapshot finished
    before 2026-11-08 10:00:00 UTC is offsite at 2 copies (through
    xn0_…)", the capture second of the latest snapshot key at or below
    the watermark, deleted since or not (compaction deletes `xn`
    blobs). Kopia writes a snapshot's manifest and then flushes the
    index naming it, so a snapshot finished before that second has its
    manifest in a `q` pack named by an index at or below the watermark;
    the residual case is two sessions whose flushes cross. The user
    compares the time with the snapshots' *end* times (`kopia snapshot
    list --json`, `endTime`), not the start times `kopia snapshot
    list` prints.
  - *generic:* no snapshot keys: "every write captured before <time>
    is offsite at 2 copies", the capture second of the first write above
    the watermark, and what later writes still wait for; with nothing
    above it, "every write up to <time>", the last fully placed one.

  How other tools report completeness, which this follows: restic
  prints the ID of the snapshot it saved, and `restic snapshots` the
  short IDs; Kopia's `snapshot list` shows start times and IDs, and
  `maintenance info` times; `rclone check` counts the objects that
  differ or are missing rather than a bare yes or no. lendisk names the
  snapshot (or the time) and counts what is missing, with why.
- **REPL-5, `status --wait <bucket>` (#20).** Waits until the latest
  snapshot key captured before the wait began (for a bucket with none,
  its latest write) is at or below the watermark, prints the `status`
  phrase for it and exits 0; a snapshot captured during the wait is not
  waited for. Exit codes, in `status --help`: 1 an error (no such
  bucket, the daemon not running), 2 a usage error (with `--json`, a
  negative `--timeout`), 3 timed out, 4 it cannot be offsite as things
  stand (stuck objects, above), with the reason on stderr. `--timeout`
  defaults to 24 hours (a nightly script's next run), 0 waiting without
  a limit. *Decision (#20): it polls `lendisk.db`, not the daemon.*
  The watermark is computed from the rows, which the daemon writes in
  WAL mode, so a reader never blocks it and needs no control API call;
  it reads every 2 s, or ten times as long as a read took (above). A
  pending row below the snapshot holds it (above), so a multipart pack
  still being hashed is waited for (`TestStatusWaitWaitsForAMultipartPack`).
  It does need the daemon to make progress: it
  tells one is running by connecting to the gateway's socket
  (`<data>/run/gw.sock`), and with none at the start it exits 1 at
  once, saying to start `lendisk serve`; one that stops during the wait
  (a restart) is noted once on stderr and waited for within the
  timeout. The clock and the pause between polls are the `app`'s
  (`clock`, `sleep`), so tests run the timeout on injected time
  (`TestStatusWaitTimesOut`). Scenario 1 and 2 run end to end with real
  daemons and restic (`TestScenarioFirstBackupIsOffsiteAtTwoCopies`,
  `TestScenarioFriendOfflineForThreeDays`, the latter on A's injected
  clock: `serve` now gives the replication worker the `app`'s clock,
  `replica.Worker.Now`, as it already gave `lendisk.db`'s).

### 3.5 Lending and quotas

The lender's policy on a borrower's requests lives in the decorator's
outer layer, `gateway.Lending` (`internal/gateway/quota.go`), which
knows the borrower from the request's account; it sits above the
capture decorator, so nothing stands between that and posix (#62).
Every `Lending` method versitygw calls first detaches the request's
context (`detach`, `context.WithoutCancel`). versitygw passes the
`*fasthttp.RequestCtx`, whose `Done` is the server's shutdown channel
and whose values fasthttp recycles when the handler returns; each
`context.WithCancel` below it (every `database/sql` query makes one)
starts a goroutine that reads those values at shutdown, after the
handler may have returned, which the race detector caught in CI. The
detached context keeps the values (the account, `bucket-owner`) for
the handler's own reads and is never cancelled; nothing below
`Lending` sees the fasthttp context
(`TestARequestsContextIsNotWatchedPastItsHandler`,
`TestEveryLendingMethodDetachesTheRequestContext`).

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
  `peer quota`, `invite` and `peer add` refuse a value that would
  exceed it (LEND-8, §3.2); unset, it is the store's filesystem size less
  the free-space floor (*decision, #24 review*, §3.2). The
  borrower side keeps its own count of what it has stored at each
  place, for D6's free fraction (`place_use`, §3.4), and corrects it
  from the lender's answers: a `507`, which makes the place full until
  it acknowledges again or the quota rises, and the `quota` answer,
  which replaces the count with the lender's own (§3.4).
- **PEER-6, SEC-15 (lowering a quota, #24)** `reserve` reads the
  friendship on every write, so a lowered number is in force for the
  next request on any connection. A write admitted before and still
  streaming is checked again: `Lending.Requota(peer)` (through
  `Gateway.Requota`, which the daemon calls when the quota exchanger
  sees the quota fall, §3.2) counts up a per-borrower generation, and
  each of that borrower's bodies (`watch`, on `PutObject`, rewrites
  and `UploadPart`) re-reads the friendship at its next read once the
  generation moved: if what the borrower stores plus what all its
  writes in flight were admitted for exceeds the quota (#24 review:
  several writes that each fit alone must not land together), the read fails with the `507`,
  so the store never publishes it, and the count is unchanged. A body
  that has ended is published as admitted. With `peer quota` run in
  another process the exchanger's 10 s poll is the delay, so a running
  transfer is refused within about 12 s
  (`TestLoweringAQuotaEndsARunningTransfer`, the SEC-15 AC for a
  lowering; `TestLoweredQuotaEndsAPutInFlight`). Nothing stored is
  deleted. Connections are not cut: unlike a removal, the friend stays
  a friend and every later request is answered under the new quota.
- **LEND-2** `Lending` refuses a borrower's `PutObject`,
  `CreateMultipartUpload`, `UploadPart` and `CreateBucket` of a new
  bucket with S3 error code `QuotaExceeded`, HTTP 507, when the counter
  plus the request (its `Content-Length`, plus the overhead for a new
  object, bucket or upload) would exceed quota, or when `statfs` on
  the store less the request is below the floor
  (`store.free_space_floor`, default 5%). A request that fits on its
  own but not beside the writes in flight (the counter plus what the
  borrower's admitted writes in flight may add plus the request over
  quota, or `statfs` less what every borrower's writes in flight may
  add less the request below the floor) is answered `SlowDown`, HTTP
  503 (`errInFlight`), which the borrower retries with backoff:
  *decision (#89),* the writes in flight may yet fail (a send the
  borrower abandoned holds its reservation until its body ends), so
  a 507, which pauses the borrower for quota for an hour, would be
  wrong for them (`TestAWriteThatFitsButForWritesInFlightIsRetried`).
  Once they have stored, the retry is answered 507 if it no longer
  fits. The check reads the counter under the same lock that admits the
  write. A completion is checked against the floor alone, for the size
  of its parts: posix assembles the object in a staging file while the
  parts still exist, so it needs that much again for a while; the
  parts are already counted against quota. The same rule splits its
  answer (#92): `507` when the copy does not fit above the floor
  alone, `503 SlowDown` when it fits only without the writes in flight
  (`reserveFloor`, `TestACompletionThatFitsButForWritesInFlightIsRetried`). A create of a bucket the
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
  one stall period. A body must also keep a floor rate
  (`Bridge.FloorRate`, 8 KiB/s, averaged over each `FloorWindow` of
  10 minutes from its start, #76): the first read after a window that
  brought less fails, every later read on the connection fails at
  once (so net/http does not drain the rest at the trickle's pace),
  and the answer is the same `RequestTimeout`, logged at warning
  level ("the friend's upload was slower than the floor rate"), so a
  path that never finishes an upload shows. 8 KiB/s is 64 kbit/s,
  so a borrower keeping all 16 of its request slots (*Per-friend
  caps*, §3.9) above it needs 1 Mbit/s of uplink; the window averages
  out a brief stop. A trickling body thus holds its request slot for
  at most Content-Length ÷ the floor rate plus one window
  (`TestABodyTricklingBelowTheFloorRateIsEnded`). The rate is measured
  as the bridge reads, so a lender's store that reads slower than the
  floor ends its own requests too; such a store is failing anyway.
  An honest borrower whose lender's store stops reading for a stall
  period gets `RequestTimeout` and retries. Nothing reads the body but
  the store. A body longer
  than its `Content-Length` ends the write in `Lending` with
  `InvalidRequest`.
  *LEND-5 (#23):* `[[lending.borrower_limit]]` (`peer` by node ID,
  `receive`, `send`, bytes a second, at least `pace.MinRate`) limits
  what one borrower sends this node (its puts and parts) and what this
  node answers it, on the tunnel only (`Bridge.Pace`: `Pacer.LendIn`
  around the bounded body, `Pacer.LendOut` around the answer). The
  node's own replication limits do not apply to lending. The time the
  lender's own limit holds a body back moves its floor window and its
  stall deadline on (`boundedBody.excuse`), so a lender's pacing never
  ends a borrower's body as slow, however many of its bodies share the
  limit (`TestALenderLimitsOneBorrowerWithoutEndingItsBodies`). It
  changes without a restart, like REPL-7's.
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
  `PutObject`, `CreateMultipartUpload` and again at completion, unless
  the put or the upload is a rewrite marked with the stored hash (*Rewrites*,
  below). With
  the same hash the put is accepted without storing anything: its body
  is hashed and discarded, so a body that only claims the hash gets
  `BadDigest` and the stored copy is never touched; a completion with
  the same hash aborts the upload and answers with the stored object's
  ETag. A put takes no key lock while its body streams, since a body
  that stalls (a borrower killed mid-transfer) would hold up its own
  retry; a put of an absent key takes the key's lock once its body has
  ended and matched its hash, before the publication lock, and holds it
  until the store has answered, so it cannot publish inside a
  rewrite's gap (*Rewrites*, below; #74 review). This relies on one
  store feature: a put of an absent key, and a completion, is stored
  with `If-None-Match: *`, which posix evaluates under its publish lock
  together with the publication, so of two racing writes of one key
  the second is refused (or accepted unstored if it carries the same
  bytes: posix also checks `If-None-Match` before reading the body, so
  on a `412` the put reads `HEAD`'s hash and, if it is the declared
  one, hashes and discards the rest of the body as for a stored key;
  #32). Multipart create and completion and deletes hold a per-key
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
  *Rewrites (v0.11.3, #74; `gateway/rewrite.go`).* A borrower's
  `PutObject` carrying `x-amz-meta-lendisk-replaces` (§3.4 *Rewrites*;
  64 hex digits, else `InvalidRequest`), to a key whose stored object
  has exactly that hash and another than the put's, is a rewrite. The
  mark is taken out of the metadata whatever happens, so it is never
  stored. *In parts (#23):* a `CreateMultipartUpload` carrying the mark
  is admitted to a key holding exactly that version, or holding nothing
  but tombs the newest of which has it, and the mark is kept with the
  upload (`lent_upload.replaces`, migration 31); at completion, under
  the key's lock, a key that still holds the marked version has it
  moved into a tomb (`Lending.tombForRewrite`, the row pending and
  marked `rewrite`, as for a PUT) and the upload is completed without
  condition and verified whole; a mismatch or a failed completion
  moves the version back (`revive`). A key that changed meanwhile is
  `412`; a key with only tombs, the newest the marked one, takes the
  object beside them (`TestMarkedMultipartRewriteKeepsTheReplacedVersionAsATomb`,
  `TestMarkedMultipartRewriteRefusedWhenStaleOrWrong`,
  `TestMarkedMultipartOfADeletedKeyKeepsItsTomb`). A lender older than
  #23 drops the mark at the create and refuses the upload `412`, which
  the borrower handles as any refused rewrite. A rewrite is admitted against quota like a new
  object (its size plus the overhead), since both versions count until
  the old one is purged (LEND-1). Its body streams to the store as any
  put's; once it has read its declared length (or ended) and matched
  its declared hash, and before the store has those last bytes, the
  rewrite takes the key's lock and the borrower's publication lock
  (held to the end) and heads the key again: if it holds the put's
  bytes already (a retry whose answer was lost) the put is accepted
  unstored; if it no longer holds the marked version (deleted, or
  replaced by a racing rewrite, while the body streamed), `412`.
  Otherwise the replaced version becomes a tombstone exactly as a
  delete makes one (§3.6 RET-1), the row marked `rewrite` (migration
  20) and left pending, and then the store publishes the new version,
  unconditionally, because posix checks any condition once before the
  body too, while the key still holds the old version. Nothing else can
  publish at the key in between: every write that publishes there
  holds the key's lock to do so (deletes, revivals, purges, multipart
  creates and completions, and plain puts from the end of their
  verified body until the store answers), and the rewrite holds it
  from the move to its publication. A plain put whose body ends in
  that gap waits, publishes after the rewrite under `If-None-Match: *`
  and is refused `412` (or accepted for the same bytes);
  `TestPlainPutCannotPublishInARewritesGap`. Once
  published, the row is kept and the new bytes are counted. If the
  store does not publish (its checksum check, a full disk), the
  replaced version is moved back (`revive`), and the borrower retries.
  *A deleted key written anew (#74, second review).* A `PutObject` to a
  key that holds no object, marked with the hash of its newest tomb
  (the version the borrower deleted), is stored as a new object beside
  the tombs, which stay for their windows: this is a rewritable key
  the tool deleted and wrote again with other bytes (a Kopia repository
  made anew), and without it the lender would refuse every version
  until the purge. A mark naming an older tomb is stale and refused,
  as is any unmarked write of other bytes
  (`TestRewriteOfADeletedKeyKeepsItsTomb`). A put decides between these
  paths on arrival, so once its body is verified and it holds the key's
  lock it reads the key's tombs again: if the newest is no longer the
  one it saw (another write published the key and a delete tombed that
  version while the body streamed), it is refused `412`
  (`TestStaleRewriteOfADeletedKeyRefused`). A borrower's put of a key
  whose multipart upload is completing waits for the completion's lock
  and is then refused, or answered with the stored ETag for the same
  bytes (`TestPutRacingACompletionWaitsForIt`); a put takes the key's
  lock as soon as it has read its declared length, even from a store
  that never reads the end of the body
  (`TestPutTakesTheKeyLockWhenTheStoreStopsAtTheLength`).
  The lender does not know the borrower's profile and needs not: a
  borrower may mark any key, and whatever it marks, each replaced
  version is kept for the window (RET-2,
  `TestUnmarkedOrStaleRewriteRefused`,
  `TestMarkedRewriteKeepsTheReplacedVersionAsATomb`,
  `TestRewriteRacingDeleteLosesNothing`,
  `TestRewriteOverQuotaRefused`).
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
  <path> [--kopia-layout]` writes the tool's own layout, excluding `.lendisk/`, naming
  the bucket as the store does since the lender does not know its real
  name (REPL-8, SEC-16). *Realisation (#35, package `export`):*
  - *No daemon* (decided #35). The export walks
    `<store>/lent-…/` directly, as posix lays it out (§3.10 COMPAT-4),
    with no posix backend, database or lock: it works with lendisk
    stopped, which is when a friend asks for it, and also with the
    daemon running, since posix publishes an object by an atomic link
    or rename; then an object tombed while the walk passes is counted
    as gone and left out, and a write after the walk passed is not in
    it. It needs only the config, for the store path.
  - *What is left out:* `.lendisk/` at any depth (posix excludes it so,
    and the tombs are `.lendisk/tomb/`), `.sgwtmp/` and `.vgwlocks/` at
    the bucket's top (`gateway.ReservedDirs`; deeper, those names are a
    borrower's keys), empty directories, anything but regular files,
    and xattrs. A name not beginning `lent-` is a usage error (exit 2);
    a missing bucket's error lists the store's `lent-` buckets.
  - *Layouts.* Plain (restic, generic): one file per key at its key
    path, which is restic's repository layout. `--kopia-layout`: each
    flat blob ID at `export.KopiaPath`, the reverse of `repoimport`'s
    mapping (§3.10 COMPAT-3): an ID of up to 20 characters is
    `<id>.f`, a longer one `<id[0]>/<id[1:4]>/<id[4:]>.f`, and `.shards`
    is written as Kopia 0.23.1 writes it in a new repository,
    `{"default":[1,3],"maxNonShardedLength":20}`, so Kopia reads the
    sharding rather than guessing it; a key with a `/` is no Kopia blob
    and fails the export, saying to drop the flag. The lender does not
    know the bucket's tool, so the user says it; a bucket holding
    `kopia.repository` exported plain is exported, with a note on
    stderr to add the flag.
  - *Safety.* The destination must be new or an empty directory (an
    export never merges or overwrites: the friend combines exports);
    directories are made 0700 and files 0600 with `O_EXCL`. A failure
    part way says to remove the directory and run again.
  - *Tests:* `TestExportWritesEachKeyAtItsPathWithoutHousekeeping`,
    `TestExportRefusals`, `TestKopiaLayoutIsTheReverseOfImport`,
    `TestKopiaLayoutOfAResticBucketRefused`,
    `TestKopiaOpensAnExportAsAFilesystemRepository` (Kopia 0.23.1
    connects, verifies and restores; `internal/export`);
    `TestExportRefusesWhatIsNoLentBucket`, `TestScenarioZeroLockIn`
    (scenario 7, §3.10 COMPAT-4) and
    `TestKopiaExportsFromTwoLendersFormARepository` (`internal/cli`).
- **LEND-7** `init` suggests `lending.max_total` = `c × S` plus the
  retention margin of §4. The control API `quota` answer carries the
  quota, both sides' use and the answering node's repository size
  (`state.DB.RepositorySize`, the live bytes of its own buckets: each
  key's latest done write, if a put), rounded up to a whole GiB
  (`control.RoundSize`; *decision, #24*: up, so a small repository is
  never reported as none, and to the GiB, so a friend learns no
  closer). The request carries the caller's the same way.
  *Realised (#37), package `capacity`.* Pure arithmetic on a
  `capacity.Plan`, which `capacity.Read` takes from `lendisk.db`; nothing
  is stored but the low-use marks below.
  - *The repository* (`state.DB.RepositoryUse`) is counted as places
    count it, each live object plus its 4 KiB overhead: the data part
    and the metadata part `m` (classes `metadata` and `snapshot`, held at
    every friend, §3.4), and `c`, the largest `copies` of a bucket holding
    live data (`replication.copies` with none). *Decision:* one `c` for
    the node, the largest, rather than a per-bucket sum: conservative,
    and exact for the usual single bucket. `status` shows the local copy
    beside it (`RepositorySize`, the live bytes in the store).
  - *Protectable size* (`Plan.Protectable`): the largest `S` with
    Σ min(D, quotaⱼ − m) + Σ min(D, overflow `max_bytes` − m) ≥ c × D over the
    friendships with an address, each term floored at 0, `m` scaled with
    `S` at the repository's present metadata share (1% while it holds no
    data) and `D = S − m`; the feasible set is an interval from 0, so a
    bisection finds its end. The fair bound (`Plan.FairProtectable`)
    puts each friend's reported repository size in place of its quota.
    Overflow targets are a parameter (`Plan.Overflow`, each target's
    `max_bytes`, read by `capacity.Read` since #38). An overflow target
    holds at most one copy of each object, as a friend does, so its term
    is capped at D too (#38; it was not, which overstated the
    protectable size once `max_bytes` passed D). `status` lists them
    (`capacity.overflow`, §3.11).
  - *Guidance* (`Plan.Cover`). While the repository is above 90% of the
    protectable size (`capacity.AlertPercent`), the numbers aim at
    `S ÷ 0.9` (`Plan.Target`), so that setting them clears the alert
    and covers any deficit with that margin. *Decision (#37): the
    shortfall is shared by water-filling.* The friendships with the
    least room are raised to one common level, the lowest that is
    enough, and only those below it are named; with two friends at two
    copies both are named (the LEND-8 AC), and a friend that already
    has room is not. Each number is the smallest quota whose room under
    placement's rule (`capacity.PlacementRoom`: quota less `m` less the
    metadata reserve of §3.4, the larger of 3 × `m` and 1% of the quota)
    reaches the level, rounded up to a whole MiB (a whole GiB from
    10 GiB), so the named number lets placement put the copies there,
    not only the §4 formula. With fewer friends than `copies` and not
    enough overflow no number covers it (`coverable: false`): status
    and the alert say to add a friend or an overflow target.
  - *Under half used for 30 days.* Migration 37 adds
    `friendship.low_there_since` and `low_here_since`: each quota call,
    either way (`QuotaReported`, `QuotaAnswered`, `state.markLowUse`),
    marks since when this node's use there (`place_use` with what its
    deletes will free, as `PlaceUse` counts it) or the friend's use here
    (`lent_usage`) has been below half the quota, keeps an earlier mark,
    and clears it at half or more or a zero quota. The call is hourly
    (§3.2), so a mark is right to the hour; a friendship not called for
    a while keeps its mark. *Decision:* a "since" mark rather than daily
    samples: one condition needs one timestamp. `status` flags a side
    whose mark is `cli.LowUseFor` (30 days) old on its clock, saying the
    two could lower the number; a lender-only node's own side is not
    flagged, since it uses nothing by design (PEER-9).
  - *`init`* asks, after the phrase, for the expected repository size
    and `copies` (`--repository-size` and `--copies` answer without
    asking; Enter or the end of input skips, so a script piping the
    phrase is asked nothing more), prints `c × S` plus 20%
    (`capacity.SuggestMaxTotal`), the disk the node then needs and the
    rule of thumb, and writes `lending.max_total` (and `copies`, when not
    the default) into the config it creates; with a config already
    there it says what to set. A lender-only node is not asked.
  - *`status`* (`capacity` in `status --json`: `repository_size`,
    `metadata`, `local_copy`, `copies`, `protectable`,
    `fair_protectable`, `alerting`, `deficit`, `coverable`, `friends[]`
    with `peer`, `quota`, `use_there`, `use_here`, `repository_size`,
    `cover`, `low_there_since`, `low_here_since`; `overflow[]`; `lent`,
    `lent_used`, `received`, `received_unused`) prints one "Capacity:"
    block; on a lender-only node a "Lending only:" line with what it
    lends, what its friends use, and the quota received, unused until it
    has a bucket (PEER-9). The `protectable_size` alert is §3.8.
  - *Tests:* `internal/capacity` (§4's examples and scenario 1's figure,
    the fair bound, overflow, too few friends, the AC's numbers),
    `TestRepositoryUseSplitsDataFromMetadata`,
    `TestQuotaCallsMarkSinceWhenUseIsUnderHalf`,
    `TestStatusShowsCapacityAndTheNumbersThatCoverADeficit`,
    `TestFriendshipsUnderHalfUsedFor30DaysAreFlagged`,
    `TestALenderOnlyNodesStatusShowsLentSpaceAndUnusedQuota`,
    `TestInitSuggestsWhatToLend`,
    `TestQuotaACNamesTheNumbersAndCopiesResumeAtTheRaisedFriend` (the
    LEND-8 AC with three daemons: raising A–B alone resumes A's copies
    at B through the quota exchange and re-placement, #24 and #26, with
    nothing else done), `TestScenarioQuota` (scenario 6), and scenario
    1's protectable size in `TestScenarioFirstBackupIsOffsiteAtTwoCopies`.

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
  *Rewrites (#74):* the version a borrower's rewrite replaces becomes a
  tombstone the same way, renamed into its tomb with its own window
  from the rewrite, and marked `rewrite` (migration 20; §3.5 LEND-4
  *Rewrites*). Both versions count against quota until the old one is
  purged. A live key the borrower rewrote is listed with
  `replaced`: the hashes of the versions its rewrites replaced whose
  tombs are kept, newest first, at most 1,000
  (`gateway.MaxReplaced`, `state.ReplacedVersions`), which recovery
  reads (§3.1 ID-5). A delete's tomb is never listed as replaced. In
  the moment between a rewrite's move and its publication, the key is
  listed as the version it replaces, live, from the pending rewrite
  row, so a recovery list read then does not record it as deleted
  (`TestKeyInARewritesGapListsAsItsOldVersion`). The list reads the
  store before the tombstones, so for any key whose newest tombstone is
  a rewrite's and which the store's listing lacked, it heads the store
  again: a rewrite that published in between is listed live with its
  new version; one still pending as the version it replaces; only a
  key the store still lacks after a kept rewrite row is listed
  tombstoned. A key listed from a rewrite row carries its replaced
  versions too (`TestListSeesARewriteFinishedBetweenItsReads`).
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
  only its own bytes back until it is purged, except a rewrite marked
  with the newest tomb's hash, stored beside the tombs (§3.5
  *Rewrites*, #74). A put that found the key absent before an undelete
  or a re-put revived it, and its tombstones gone after, publishes
  under `If-None-Match: *` and is refused by the store; the revived
  object of its own hash answers it with the stored ETag, so the
  borrower sees no conflict (EP-11), and other bytes stay `412`
  (`TestSameBytesPutAfterARevivalFoundTheKeyAbsentSucceeds`, #32).
  *Crash safety:* the row is written, pending, before the rename and
  marked kept after it; a revival marks it pending before the link.
  At every start, before anything is served and before the reconcile,
  `settleTombs` settles each pending row from the store: no tomb at its
  path (the rename never happened, or the revival finished) drops the
  row, the object live where it was; a tomb beside a live object of
  the same hash (a revival linked back) unlinks the tomb and drops the
  row; a tomb alone, or beside other bytes, is kept. No order of steps
  leaves bytes without either a key or a kept row. A rewrite writes its
  row, pending, before the replaced version's rename and keeps it only
  after the new version is published; `settleTombs` moves a pending
  rewrite tomb whose key holds nothing back to its key (the new version
  was never published), and keeps one beside other bytes (it was)
  (`TestRewriteCrashOrderAndUndo`). *Accepted race:* a
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
  (`purgeAttacks`). Since #74 the list includes a rewrite marked with
  the tomb's hash, of the key (stored beside the tomb, which is kept:
  §3.5 *Rewrites*) and of the tomb's own path (refused); the check after
  each attack is that the tomb is kept with its bytes, since a rewrite
  and a later delete may add tombs beside it. `TestHostilePeer` also
  has the borrower rewrite a live key three times and then try stale,
  false, malformed and absent marks and a marked multipart upload
  (`rewriteAttacks`): every replaced version is a tomb holding its
  bytes and nothing else changes. Completing the upload begun before the delete
  while it is still open revives the tombstoned object, since it
  declared that object's hash, and stores none of the parts' bytes:
  nothing is lost, and a later delete starts a new window
  (`TestCompletingAnOldUploadRevivesTheObject`).
- **RET-3** `undelete` for a friend is the control API `undelete`
  (bucket, since; §3.7 NET-7, #32): the lender moves each tomb copy
  deleted since that time back to its key and drops the row; `list`
  shows beforehand what that would restore. For an overflow target that versions, the
  borrower does not remove the delete marker, since S3-5's credential
  cannot remove versions: it copies the last version before the marker
  onto the same key (`CopyObject` with the source version id), which
  makes it current again and leaves the marker to the lifecycle rule.
  Elsewhere it cancels the borrower's own client-side tombstones
  (S3-4). Both are realised in #39 (§3.11 *Retention and undelete*);
  the friends' side is #32's:
  - *The command.* `lendisk undelete <bucket> [--since <time>]
    [--rewrites] [--yes]` (`internal/cli/undelete.go`) asks the daemon
    through `lendisk.db` (table `undelete_request`, schema 34:
    `state.DB.RequestUndelete`), as `verify` does (VER-3): only the
    daemon dials with the node's keys (§3.7, #69). `--since` is an RFC
    3339 time, a date (local midnight) or how long ago (`36h`, `7d`);
    without it, every tomb in the window. The daemon's runner
    (`internal/undelete`, polled every second) first reads every
    friend's `list` of the bucket and writes a preview: the
    tombstoned keys whose `tombstoned_at` is at or after since, each
    once however many friends hold it, their bytes, the snapshot keys
    among them (named, up to 50) and the count per friend. The command
    prints it and asks; no, or an interrupt, drops the request
    (`CancelUndelete`), and nothing was asked of any friend. Yes
    (`ConfirmUndelete`) has the runner ask every friend with an
    address, page by page, and record what each restored; the command
    prints per friend what came back and what was left, and exits 1
    if a friend was not reached (undelete is safe to repeat). Every
    friend is asked, not only those placement names: after an
    attacker's prune placement no longer says who holds the bucket's
    tombs; a friend holding none answers `NoSuchBucket` or restores
    nothing.
  - *The lender (`gateway.Gateway.Undelete`, `undelete.go`).* For the
    caller's own bucket (`lent_bucket`; another sealed name is
    `NoSuchBucket`, PEER-8), up to a page of keys after `after` with a
    kept tombstone deleted at or after since (`state.UndeleteKeys`),
    each under its key's lock and the borrower's publication lock:
    *which version* — the key's newest delete tomb since then: the
    version it held when it was last deleted. With `rewrites`, a
    rewrite's tomb (#74; multipart rewrites' too, #23) counts as well,
    and the key comes back as it was at since: its oldest tomb, of
    either kind, deleted at or after since. *Never over a live
    object* — a key live with other bytes is skipped and reported with
    both hashes (`conflict`), one live with the chosen bytes is
    reported `live`; its tombs stay for their window, so no version is
    lost (RET-1). To roll a live rewritable key back, delete it first
    and undelete with `--rewrites`. *The revival* is the re-put's
    (`revive`): the row pending, the tomb hard-linked back (never over
    an object), the tomb unlinked, the row dropped, so `settleTombs`
    settles a crash midway as for a re-put
    (`TestAnUndeleteCrashIsSettledAtStart`); the key's lock, held from
    the choice of tomb to the end, keeps a purge due meanwhile from
    taking the tomb in between (`TestUndeleteHoldsTheKeyAgainstThePurge`,
    `TestUndeleteRacingAPutOfTheKey`). *Quota:* a tomb counts
    until purged (LEND-1), so a revival moves its count from tomb to
    live unchanged (`TestUndeleteRestoresTheTombsDeletedSince`).
    *Never a revival unreported (#32 review).* A key that cannot be
    revived now (its directory blocked by an object written since, the
    disk) is skipped as `failed` with the reason, its tomb kept, and the
    page goes on (`TestUndeleteReportsAKeyItCannotReviveAndGoesOn`).
    Each key's steps run on a context detached from the request, so a
    cancel (the borrower's call timing out, its daemon stopping) cannot
    cut a revival midway; it ends the page after the key in hand, with
    `next` at the last key handled
    (`TestUndeleteCancelledMidPageReportsWhatItDid`). The control call
    answers what a page did even if it ends in an error
    (`TestAnUndeletePageThatFailsLateAnswersWhatItDid`). An answer
    still lost on the way is healed by the borrower's adoption, below.
  - *The borrower (`state.DB.RecordRestored`).* What a friend restored
    is checked as recovery checks a list (key, size, hash, a
    hash-named key's name; rejected entries are logged and counted),
    classified by the bucket's profile (local-only keys are not
    recorded), and joins the index in one transaction per page: a key
    whose latest done write is a delete, or that has none (a recovered
    node's key every friend listed tombstoned) and that the preview the
    user confirmed listed (table `undelete_key`, schema 35), gains a done put of
    the version, created now, with an acknowledged placement row at
    that friend (size plus `ObjectOverhead`); a second friend
    restoring the same version adds its acknowledgement (a `queued`,
    `refused`, `dropped` or `abandoned` row of that version there
    becomes `acked`; one re-placement is `dropping` keeps its course).
    The local store is not refilled: the row's `queue.local` is NULL,
    so the read-through fetches each object when it is first read
    (ID-6, EP-10) and the store reconciler does not take its absence
    for a delete (§3.3 EP-14). The offsite watermark counts the rows as
    any acknowledged put (REPL-11): restored at `copies` friends, the
    bucket is offsite again with no byte sent; at fewer, re-placement
    (#26) fills the missing copies through the read-through
    (`TestRestoredObjectsJoinTheIndex`). *Decision: the local version
    wins (EP-11).* A key whose latest done write is a put of another
    version (this node wrote it again since, or holds a change EP-11
    held back) keeps it: the local store is the user's, and its version
    is the newer; the friend's restored copy is reported (`kept`) and
    not counted. A key with a write in flight (a pending row) is left
    to that write. *Decision (#32 review): kept and busy keys are
    reported, not counted.* The friend holds the restored version live,
    which is not this node's version (kept) or not yet known to be
    (busy); counting it as held would place a version the index does
    not hold. The command names each, with both hashes for a kept key;
    the node's own write reaches that friend as any write over a version
    it holds (EP-11, LEND-4). The borrower's count of the friend is high by the
    restored bytes until its next quota answer (§3.4 *What each side
    counts*). *A friend cannot add keys:* a key this node has no
    history of that the confirmed preview did not list is recorded
    nowhere and reported `unknown` (`TestAFriendCannotAddKeysByUndelete`).
    *What a friend restored is never lost to the index:* a page that
    fails to record is recorded again (five tries, a second apart)
    before the next page is asked for; and after every run the runner
    reads each friend's list again and adopts its live keys whose
    latest done write here is a delete that friend acknowledged, or
    that have no history here and were in the confirmed preview
    (`state.DB.AdoptRestored`): the keys of an answer lost on the way,
    or of an earlier run that failed, join the index as restored there
    (`TestOrphansOfALostAnswerAreAdopted`,
    `TestTheRunnerTrustsOnlyThePreviewAndHealsLostAnswers`). A delete
    not yet acknowledged there is the friend's lag, not a revival, and
    is left alone.
  - *Scenario 4* (`TestScenarioRansomwareIsUndone`, real daemons,
    restic): A at two copies over B and C; the attacker's `restic
    forget --prune` of both snapshots replicates as tombs at both; A
    is wiped and recovered from B; restic sees no snapshot; an
    undelete declined changes nothing; one accepted brings every
    object back at both friends, `restic snapshots` lists both,
    `restic check --read-data` passes through the read-through, the
    latest snapshot restores exactly, and `status --wait` says the
    bucket is offsite at two copies. *Scenario 9*
    (`TestScenarioKopiaRestoresAfterMaintenanceAndRecovery`): a Kopia
    bucket offsite at two copies; full maintenance after a snapshot
    delete tombs its deleted packs and indexes at both friends; after
    recovery the latest snapshot restores through the read-through;
    an undelete then brings back every deleted blob (the rewrites'
    tombs of `kopia.maintenance` stay: not asked) and Kopia still
    verifies and restores.
  - *Precedent (#32).* S3 versioning undeletes by removing the delete
    marker, which makes the newest version before it current again;
    older versions stay as they were. Backblaze B2 has no unhide call:
    a hidden file is unhidden by deleting its hide marker
    (`b2_delete_file_version`), the same way, and a file uploaded again
    after the hide is simply the newest version. restic's advice after a
    ransomware prune is to restore from a copy the attacker could not
    reach and keep `forget --prune` away from the backup credentials
    (append-only rest-server, object lock); it has no undelete of its
    own, since a pruned pack is gone. So: the newest delete tomb, as
    the delete marker's removal (the version before the prune); never
    over a live object, as S3 never removes a version that is current;
    a rewrite's replaced version only when asked, as S3 keeps
    noncurrent versions out of the way until a copy names one; and the
    friend's tomb-keeping is the copy the attacker could not reach.
- **VER-1** The challenge is a control API call (#27; §3.7 NET-7):
  the borrower sends the bucket's sealed name, an object key, a byte
  range and a fresh 32-byte nonce; the lender reads the range through
  `Lending` and returns HMAC-SHA256 keyed with the nonce over the bytes
  (`control.ChallengeMAC`); the borrower computes the same over its
  local copy. Package `verify` is the borrower's side; `verify.Verifier`
  runs in `serve` (full role), reading `lendisk.db` and the store and
  dialling through the tunnel.
  - *What is challenged.* Per friend and bucket (a *replica*), the
    objects the friend acknowledged whose latest write is that very row,
    so the store holds the same bytes, and that are not empty
    (`state.DB.ChallengeableCount`, `ChallengeableAt`): data and
    metadata alike. Daily, from the replica's last answer
    (`replica_check.checked_at`, schema 29; `state.DB.DueReplicas`,
    looked at every 5 s; a replica with rows of the bucket `queued` or
    `dropping` there waits until its FIFO is empty, so a backup in
    progress is not slowed by its challenge, but only until it is more
    than a day (`state.Overdue`) past due, counted from its last answer
    or, never answered, from its first acknowledgement: then it is
    challenged anyway, so a FIFO that never empties, at a friend at
    quota or behind a slow uplink, does not starve it,
    `TestAStarvedReplicaIsChallengedOnceOverdue`), up to
    `verify.DefaultSamples` (8) distinct
    objects at random, each with a random range of at most
    `verify.DefaultRange` (64 KiB) and its own nonce. Randomness
    (`Verifier.Random`) and the clock (`Verifier.Now`) are injected.
    VER-1 names one object; eight cost at most 512 KiB of reads per
    replica a day, and catch a friend that lost half of them with
    probability 1 − 2⁻⁸ each day, which scenario 5's "within a day"
    needs (one object gives a coin toss). A replica holding eight or
    fewer is challenged whole every day.
  - *Verdicts.* Every answer right: verified (`verified_at`). A failed
    proof is the lender's own word that it lacks the bytes (`404` with
    code `NoSuchKey` or `NoSuchBucket`, `416` with `InvalidRange`:
    `control.FailedProof`), or an HMAC that differs from the local
    copy's while the local copy still hashes to its capture hash. It
    marks the replica unverified (`unverified_at`, `detail`), logs at
    error with `alert=true`, calls `Alerted` (the alert service's
    `Check`), and changes the pool's fingerprint, so re-placement
    drains that friend's copies of the bucket (§3.4 *Unverified
    replicas*). Anything else decides nothing: a network error, a
    timeout, a `503`, a `5xx`, a `404` without a code (a lendisk
    without the call): recorded in `replica_check.error` and retried an
    hour later (`DefaultRetry`), never a failure
    (`TestATransientErrorIsNoFailedProof`). An object whose local copy
    is gone (a recovered node before the read-through filled it, EP-10)
    or no longer matches its capture hash (VER-5's business; the HMAC
    cannot tell whose bytes changed) is skipped; a check that skipped
    every object is recorded as an error ("nothing to check")
    (`TestTheLocalCopysOwnFaultsAreNotTheFriends`). The friend is asked
    first and the local copy read after its answer, after any wait for
    its pacing. Before a failure is recorded, the object's row is read
    again (`state.DB.StillChallengeable`): it must still be `acked` at
    that place and its key's latest done write. A delete, a rewrite
    (#74's gap) or re-placement's drop (`dropping`, then the delete
    sent) that reached the friend while the challenge was in flight
    explains a `404`, a `416` or other bytes, so the object is skipped,
    not failed (`TestAnObjectThatChangesMidChallengeIsSkipped`, #27
    review: a full check runs for hours beside backups). An unverified
    replica is not challenged daily any more; `lendisk verify --full`
    passing clears the mark (`RecordVerified` with full), a ranged pass
    does not. A replica's record ends with the friendship
    (`RemoveFriendship`) and with the bucket (`DeleteBucket`), so a
    friend befriended again starts unchecked, not unverified
    (`TestReplicaRecordsEndWithTheFriendshipAndTheBucket`).
  - *Decision: one failed proof drains the replica (#27).* Precedent:
    Tahoe-LAFS's verifier (`tahoe check --verify`) downloads and checks
    every share, and its repairer regenerates only the shares found
    missing or corrupt, judging no server; Ceph's deep scrub compares
    replicas, marks the placement group inconsistent and `pg repair`
    rewrites the bad copy from a good one, an OSD with repeated errors
    being taken out by the operator; Storj's satellite audits random
    stripes, counts only definite failures against a node's audit
    reputation (offline and unknown errors go to separate scores and
    containment re-asks the same piece), and on disqualification treats
    every piece the node held as lost and repairs from the others.
    lendisk's challenge samples a few objects of a replica whose other
    objects no one is checking, as Storj's audits do, not every share
    as Tahoe's verifier does; repairing the one object found missing
    would leave the rest of the replica trusted on a proof it just
    failed. So, like Storj's disqualification, the whole replica is
    distrusted and every copy of it re-placed, but at once, since a
    pool of a few friends has no reputation to average over and the
    copies stay where they are until their replacements are
    acknowledged (REPL-13), so acting at once costs only transfer.
    Like Storj, only a definite answer counts. Unlike it, the scope is
    the one bucket's replica: the friend's other replicas answer their
    own challenges, and the friendship stays.
- **VER-3** `lendisk verify <bucket> --peer <id> [--full]`
  (`internal/cli/verify.go`) asks the daemon through `lendisk.db`
  (table `verify_request`, schema 29: `state.DB.RequestVerify`), polls
  the outcome (`VerifyResult`) every 500 ms, and prints it: exit 0
  verified, 1 no answer, nothing to check, the daemon not running, an
  unknown bucket or a stranger, 2 usage, 3 timed out (`--timeout`, 24 h
  by default), 4 failed. It does not dial the friend itself: one
  tunnel client per dial key at a relay (§3.7, #69), so a second
  process dialling with the daemon's key would stall both. The daemon
  runs requests ahead of the daily challenges. While it waits, the
  command checks every poll that the daemon still runs (exit 1 when it
  stopped), and on an interrupt, a timeout or a stopped daemon takes
  its request back with a context of its own
  (`TestVerifyStopsWaitingForAGoneDaemon`). A ranged check of a replica
  marked unverified prints that it passed and stays unverified until
  `--full` passes. Requests older than a week are dropped at the next
  one (`state.VerifyKept`). `--full` sends a
  whole-object challenge for every object the friend acknowledged
  (`Whole`, no range; `ChallengeablePage` in pages of 256): the lender
  streams the object into the HMAC and answers 32 bytes, the borrower
  computes the same from its copy, and no object's bytes cross the
  tunnel. A `503` from the lender's pacing (§3.7) is waited out during
  a full check, as long as `Retry-After` says, up to 5 minutes plus
  what the largest object read so far takes at `verify.DefaultWholeRate`
  (1 MiB/s, a quarter of the lender's default): a 5 GiB object's debt
  is waited out, a friend asking for more is no answer
  (`TestAFullCheckWaitsOutTheFriendsPaceForLargeObjects`, scaled down
  to 1000 B/s). The daily check counts a `503` as no answer
  (`TestVerifyOnDemandAndStatus`, `TestAFullCheckCoversEveryObject`).
  The daemon runs one check at a time, so a long full check holds up
  the daily challenges until it ends; accepted, as a full check is the
  user's explicit request and most replicas wait a few hours at most.
- **VER-4** `status` lists under each bucket its replicas (every
  friend with an address holding an acknowledged object of it, or with
  a record): "verified at <friend> <time>", "not verified yet at
  <friend>" (with the last challenge's error, if one got no answer),
  or "! UNVERIFIED at <friend> since <time>: <what failed>"; `status
  --json` adds `buckets[].replicas[]` with `peer`, `last_verified`,
  `last_full`, `unverified`, `unverified_since`, `detail` and `error`.
  `peer list` shows it per friend (§3.2 PEER-4).
- *Scenario 5* (`TestScenarioBadLender`, real daemons, restic, A's
  injected clock): scenario 1's circle, every replica verified; B
  deletes half the packs it holds of A's in its store; with A's clock
  a day on, A raises `replica_unverified` for B's replica and every
  data object ends at both C and D, the snapshot offsite at two copies.
- *Withheld proofs (SEC-7, #27 review).* SEC-7 has VER-1 detect a
  friend that loses *or withholds* a replica. One that answers every
  challenge with an error, never a definite one, is never failed, and a
  replica whose challenge never runs proves nothing either. So a
  replica holding acknowledged objects that has not passed a challenge
  for `alert.DefaultUncheckedAfter` (3 days), counted from its last
  pass or, never passed, from its first acknowledgement still held
  (`ReplicaCheck.LastPassOrHeld`), raises `replica_unchecked` (§3.8).
  It drains nothing: the friend may be right and the network wrong, and
  REPL-13's unreachable threshold covers a friend that does not answer
  at all. It says the replica's state is unknown, and what the last
  challenge got (`TestAReplicaNeverVerifiedIsAlerted`: a friend
  answering `500`, and one never challenged since its last pass).
- **VER-2** Samples are whole-object `GetObject` calls by the replica
  client, hashed by lendisk and compared with the index; the method
  for overflow targets (S3-6), and for the evicted part of a bucket if
  evict mode arrives post-v1. Realised for overflow targets in #39
  (§3.11 *Verification*).
- **VER-5** The bit-rot sampler (`gateway/bitrot.go`, #31) runs in the
  gateway once it serves, in both roles, and hashes a share of the
  store against the index, on both sides; neither backend has a scrub
  of its own.
  - *How much.* `store.bitrot_sample`, a percentage of the stored bytes
    a week (default 0.1, VER-5's least, refused below it; at most 100).
    Every 10 minutes (`gateway.DefaultSampleEvery`) a step owes that
    share of `state.DB.StoredBytes` (the own objects held locally plus
    every friend's `lent_usage`, overhead and tombs included, so the
    share errs high) for the time passed on the gateway's clock since
    the last step, never more than the store holds, and hashes objects
    until it has paid; the last object may run over, and an empty one
    counts a byte. The last step and what it owed are kept with the
    sweep, so a restart neither forgives the time it was down nor
    repeats what it paid: a daemon restarted more often than it steps
    still samples, and one down for a week owes that week at its first
    step (#31 review; the coverage test restarts with 1500 bytes paid
    ahead and checks exactly what is owed). A clock set back restarts the count.
  - *Paced.* Every read goes through `pace.Pacer.Scrub`, a token bucket
    of its own (`store.bitrot_read_limit`, default 8M a second, at least
    16K, empty for none; reloaded without a restart like the other
    limits, §3.4 REPL-7), apart from every network limit: on a Pi the
    sample must not compete with backups for a USB disk
    (`TestSampleReadsArePaced`, `TestBitrotReadsHoldToTheirLimit`).
  - *Which objects: a sweep, not a draw.* The step goes on from where
    the last one stopped, in bucket name then the store's listing
    order, over the own buckets and every friend's `lent-*` bucket,
    wrapping after the last; its place (table `bitrot_sweep`, schema
    36: the last object checked, the cycle's start, objects and bytes
    checked in it, cycles done, the last step and what it owed;
    `state.DB.Sweep`) survives a restart.
    So the next object is always the one checked longest ago (or never),
    every object is checked once a cycle, and a step stops when it comes
    back past where it began rather than check anything twice
    (`TestSampleCoversTheStoreInTurnAtTheConfiguredRate`). The injected
    randomness (`SampleSettings.Random`) only picks the bucket the first
    cycle starts at; the clock is the gateway's. *Decision (#31).* A
    random draw finds rot at the same rate on average but leaves some
    objects unchecked for many cycles; a sweep bounds the time to
    detection by a cycle. Precedent: ZFS's scrub walks the whole pool
    and resumes where it stopped; Ceph deep-scrubs each placement group
    in turn, the longest unscrubbed first, randomising only when;
    Garage's block scrub walks every block with a saved checkpoint,
    paced by its `scrub-tranquility`, every 25 to 35 days at random.
    Listing order is posix's: where it differs from byte order (a key
    beside a directory of a longer name) an object may be visited a
    cycle late, never lost to the sweep.
  - *Rot or an edit: the file's identity, never a clock (#31 review).*
    `queue.stamp` (schema 36) is the identity of the file whose bytes
    this node wrote or hashed to the row's hash: inode, size and
    modification time in nanoseconds (`fileStamp`). It is recorded by
    `state.DB.MarkLocal` when the capture stores a put (still holding
    the key's lock), when a completed upload, a settled row or a
    reconciled write is hashed and the file did not change while it was
    read (`Decorator.hashStamped`), when the read-through stores a
    verified fetch, and when the sampler or the reconciler hash a file
    to the row's hash. An identical re-upload clears it (another file)
    until its capture records it; a key marked held from a listing
    (`MarkLocalSeqs`), a recovered or undeleted row and a row of an
    earlier release have none: *unverified*. Then:
    - *the same inode and modification time, other bytes:* rot (writing
      a file moves its modification time, replacing it changes its
      inode, rot changes neither), also when the size differs: a file
      that lost its tail without a write (fsck) is damage, not an edit
      (`sameFile`; `TestATruncatedFileIsRotNotAnEdit`: a truncated
      rewritable restic `config` is repaired, not enqueued, and the
      reconciler leaves it to the sampler);
    - *another modification time or inode:* an edit outside the
      endpoint, the user's write (#88);
    - *no identity recorded (unverified):* the right bytes record the
      identity; other bytes are taken for the user's write, never for
      rot, with one exception. *Decision (#31 second review):* an
      unverified file of a key its profile names rewritable (restic's
      `config`, Kopia's `kopia.repository`, `kopia.blobcfg` and
      `kopia.maintenance`) whose bytes differ from the version a friend
      holds is not captured, since EP-11 does not hold a rewritable
      key's change back and the bytes would replicate over the friend's
      copy; it is recorded and alerted as rot ("an unverified file
      differed from the version a friend holds") and replaced by the
      friend's verified copy. The realistic case is a damaged store
      copied back onto a recovered node; a user who meant the bytes
      writes them again through the endpoint
      (`TestAnUnverifiedRewritableKeyIsRepairedNotCaptured`). Any other
      key's unverified bytes are captured: EP-11 holds the change back,
      with `key_changed`, wherever a friend still holds another version,
      so nothing replicates over it;
    - *a change of mode, owner or xattrs alone* (a chmod, a tagging or
      retention call) moves the ctime only and is no edit: rot under it
      is still rot (`TestRotUnderAMetadataChangeIsStillRot`; for a
      rewritable key the rotten bytes would otherwise replicate over the
      friends' good copy).
    No clock is read, so an edit right after its capture is an edit
    (`TestAnEditRightAfterItsCaptureIsAnEdit`) and a file whose times
    are ahead of the node's clock (fake-hwclock, a network filesystem's
    server) is hashed once, not at every pass
    (`TestAFileAheadOfTheClockIsHashedOnce`). *Accepted:* a tool that
    writes other bytes of the same size into the same file and sets its
    modification time back (`rsync --inplace -t`, `touch -d`) makes an
    edit look like rot, and its object is replaced by the version the
    index holds; so does an edit of the same size within the
    filesystem's timestamp granularity of the capture (a few
    milliseconds on ext4). Any inode change counts as an edit: a store
    moved with `cp -a`, rsync, `btrfs send`, or served by a FUSE
    filesystem without stable inodes re-stamps nothing and costs a paced
    re-hash of every object at the next reconcile pass (the same bytes
    record the new identity). Bytes never verified are not judged rot
    (but for the rewritable exception above), so on an upgraded node, or
    after recovery, rot found before a first verifying read is taken for
    an edit, which EP-11 holds back for a key a friend acknowledged
    other bytes of; a key no friend holds other bytes of has nothing to
    protect (`TestBytesNeverVerifiedAreNeverRot`).
  - *An own object* is checked if its key's latest done write is a put
    held locally (`state.OwnVersion`) with no write in flight and not
    held back by EP-11 (its bytes are the user's, replicated nowhere),
    and not local-only; its bytes must hash to the queue row's size and
    SHA-256 (`TestHeldBackAndBusyKeysAreSkipped`). The right bytes record
    the file's identity if it was not recorded. Other bytes are read a
    second time, holding no lock, and must read the same from the same
    file (a misread, or a file being written, decides nothing); then,
    holding the key's lock, the row and the file's identity must still
    be the ones read (`TestTheSampleHoldsNoLockWhileItHashes`: a write of
    the key goes through during the second read;
    `TestAWriteBeforeTheSamplersLockWins` and
    `TestAWriteBeforeTheReconcilersLockWins`: a write landing between
    the read and the lock is neither rot nor captured as what was read):
    - *An edit* (or bytes never verified) is enqueued as a put
      (`reconciled` = 1, `Decorator.captureChanged`), which EP-11 judges
      as any write (`TestAnObjectChangedInPlaceIsAWriteNotRot`).
    - *Rot* is logged at error with `alert=true`, recorded (table
      `bitrot`, schema 36: bucket, key, the row, its identity, both
      hashes, when found; `state.DB.RecordRot`) and alerted (`bitrot`,
      §3.8). If a friend acknowledged that very version (`state.Source`),
      the sampler, still holding the key's lock, fetches it as the
      read-through does (`Decorator.fetchAny`: most recently reachable
      friend first, verified as it streams) *over* the rotten copy:
      posix links the new object only once it is complete and verified,
      so the rotten copy stays until a verified one takes its place. The
      key is never missing, so the store reconciler sees nothing and
      `queue.local` is never cleared. Nothing is enqueued and no friend is
      asked to change anything
      (`TestRottenLocalObjectIsAlertedRemovedAndRefetched`). If no friend
      holding it answers, or one serves other bytes, the rotten copy
      stays, the record says so (`refetch`), each later step tries again,
      and reconcile passes enqueue nothing
      (`TestARepairNeverLeavesTheKeyMissing`). With no friend holding the
      version (not replicated yet, or held back) the rotten object is
      kept, as it is all there is, and logged as such
      (`TestRotNoFriendHoldsIsKeptAndAlerted`). *Decision (#31 review):
      replace, not delete then fetch:* a restic pack with one rotten
      blob still holds every other blob, and a store that loses a key
      even for a moment needs EP-14's rule.
    - *Follow-up.* Each step first goes over the records: one a friend
      holds is fetched again (one kept is taken up once a friend holds
      its version, after recovery or an undelete); a key written,
      deleted or stored again since, or whose file was replaced, is over
      and the sweep judges what is there; a repaired record is dropped
      `state.RotShown` (7 days) after its repair.
  - *A friend's object* is hashed against the hash it was stored with
    (`x-amz-meta-lendisk-sha256` in its xattrs, LEND-4); a mismatch is
    read a second time, holding no lock, and then, holding `Lending`'s
    key lock (a put, rewrite or delete of the key holds it until the
    store answers), the hash it was stored with and the file's identity
    must still be the ones read; then it is logged with
    `alert=true`, recorded with the friend and its sealed bucket, and
    alerted (`lent_bitrot`, §3.8). The object is left as it is
    (`TestRottenLentObjectIsAlertedOnly`); the record ends when the
    friend deletes or replaces it. Tombs are not sampled: a rotten tomb
    an undelete brings back fails the borrower's verification as any
    rotten object does.
  - *Decision (#31): the lender does not tell the friend.* It cannot
    repair a friend's data and must not change it (LEND-4: it holds
    the borrower's bytes and their hash, nothing more). The friend learns it without a new call: its
    daily challenge (VER-1, §3.6) samples eight objects of the replica,
    so a rotten one fails a proof within about n/8 days for a replica of
    n objects, at once with `lendisk verify --full`, and one failed
    proof re-places the whole replica; meanwhile the read-through and
    re-placement verify every byte they read from it and try the next
    holder (§3.3 EP-10). A notice would be a new peer-protocol call and
    the lender's unverifiable word, and would add no repair the
    challenge does not start. The lender's own user gets the alert,
    which names the friend and the key: friends can talk. Precedent:
    Storj's storage nodes do not report their own corrupted pieces; the
    satellite's audits find them. A notice may come later if the
    challenge's latency proves too long in the field.
  - *Status.* `status --json` adds `bitrot` (`cycle_started`, `cycles`,
    `objects`, `bytes`, `last_checked`, and `rotten[]`: `bucket` or
    `friend` and `sealed`, `key`, `sha256`, `found`, `found_at`,
    `state` repaired, refetching, kept or lent, `repaired_at`,
    `detail`), and `status` prints the cycle and a line per rotten
    object. *AC:* `TestBitrotIsFoundOnBothSidesAndRepairedAtTheBorrower`
    (`internal/cli`, real daemons): a byte flipped at A and another in
    B's copy of A's bucket, each keeping its file's identity; a week on
    both clocks, A alerts and replaces its object with a verified copy
    from B, enqueueing nothing, and B alerts and keeps A's object as it
    is.

### 3.7 Network

- **NET-1** Every peer connection is a WireGuard tunnel via tailcat; the
  lender's `AllowClient` hook admits known peer keys and pending invite
  keys and nothing else (SEC-10).
- **NET-2 to NET-4** magicsock hole punching, without port mapping in
  v1 (Q-R), and DERP fallback; `PathInfo` is built on `Client.DiscoPing`,
  whose result names an endpoint only when the pong came over a direct
  path, and reports direct or relayed for `status`. `Client.Ping`
  always measures the DERP path and is not used for this. *Built
  (#42):* tailcat admits a client by its meow ping, which travels over
  the relay only, so a relay both nodes reach is needed once per client
  even when the path then goes direct; once direct, the tunnel no longer
  needs it (`TestNATOneSymmetricGoesDirectAndOutlivesTheRelay`), and with
  no relay reachable at all no tunnel comes up, explicit address or not
  (`TestNoReachableRelayNoTunnel`). Tailscale is the same without its
  control plane. *The path keeper* (`cli.keepPaths`) disco-pings every
  friend the daemon has a client for: every 30 s while relayed, since
  each disco ping (with tailcat's TSMP nudge) also tries for a direct
  path, which is NET-4's retry, and every 5 minutes while direct, and
  stores the answer in `friendship` (`path`, `path_relay`,
  `path_endpoint`, `path_at`, migration 43). A friend not dialled is not
  probed, since the probe would build a client. `status` shows it per
  friend (`places[].path`, `path_relay`, `path_endpoint`,
  `path_checked_at`; text `, direct` or `, relayed via <relay>`, and a
  `Paths:` list on a lender-only node), `peer list` in REACH and
  `path_relay`, `path_endpoint`, `path_checked_at`. tailcat strips
  region codes from addresses, so the relay is named by its servers
  (host:port), not a code. The quota call still records the path it
  saw, as before. *The NAT test (NET-2 AC):* tailcat builds magicsock
  with no packet-listener hook (`wgengine.Config` has none, and
  tailcat sets `ListenPort: 0`), so Tailscale's in-memory natlab cannot
  be put under it, and natlab's vnet needs QEMU VMs; network namespaces
  need privileges CI does not give. So the NATs are simulated at what
  each node learns: with `TS_DEBUG_OMIT_LOCAL_ADDRS` a node advertises
  only its STUN answers, and a node "behind a symmetric NAT" learns them
  from `transporttest.Symmetric`, a STUN server that answers every
  request with a fresh 127.0.0.1 port nothing listens on, as a symmetric
  NAT's mapping for the STUN server does not reach the node from anyone
  else. A node behind an endpoint-independent NAT learns its real
  socket. One symmetric side goes direct (the symmetric side's pings
  reach the other's STUN address, which answers the real source), both
  symmetric stay relayed (`TestNATBothSymmetricFallBackToTheRelay`).
  The gap: no packet filtering is simulated (a peer that learnt the
  real port another way gets through), nor mapping timeouts,
  hairpinning or port preservation; G-1 measures the real thing.
- **NET-3** A friend's explicit addresses (`lendisk peer address
  <friend> <ip:port>…`, `--clear`; `friendship.explicit_addresses`) are
  given to the friend's tailcat client as its server's endpoints, as a
  control plane's network map would: magicsock disco-pings them at
  once, beside the call-me-maybe endpoints, and the first that answers
  carries the tunnel (`TestExplicitAddressIsDialledDirectly`, both sides
  symmetric). tailcat exposes neither this nor a fixed UDP port, so
  `transport` reaches its backend's magicsock by reflection, each field
  checked for its type; a tailcat whose internals changed logs a
  warning and the friend is reached as before (`engine.go`). The
  addresses are read at each dial, so a running daemon uses them from
  its next one. A port forward needs a fixed port: `tunnel.port` binds
  the node's engine to it (default 0, any port; Tailscale's 41641 is a
  good choice). Addresses are IP literals: a dynamic DNS name would be
  resolved at each dial, which is left for later.
- **NET-5** Relays are DERP servers. The node's relay set
  (`transport.RelaySet`) is the project's DERP map, shipped in the
  binary (`transport/projectrelays.json`, empty while the project runs
  no relay), unless `relays.map_url` (a DERP map URL, Tailscale's JSON
  format) or `[[relays.list]]` (self-hosted relays, `host`, `port`,
  `stun_port`, `cert`) is set: either replaces it, both are used
  together, the list's regions numbered from 900 after the map's.
  `relays.tailscale_bootstrap` (default true) adds the hosted map
  tailcat publishes (Tailscale's relays) as a last resort: its regions
  never take an ID of the node's own, and the address's region is
  picked from them only when none of the own regions answers a latency
  probe (`RelaySet.Pick`). Set false, the node depends on no third
  party's relay. Fetched maps are kept in `lendisk.db` (`node`
  `relays.map:<url>`, `transport.StoreMapCache`), so a node restarted
  while a map's host is down still knows its set. Precedent: Syncthing
  ships a default relay pool URL that any list replaces; Tailscale's
  clients carry an embedded DERP map as a fallback to the control
  plane's; Headscale lets the operator replace or extend the map by
  URL or file. `lendisk relay` is derper's relay part in lendisk's
  binary (`internal/relay`): `derpserver` at `/derp` with derper's probe
  and `generate_204` handlers, a STUN server, HTTP/2 off, over TLS that
  is `self-signed` (made once, kept in `<data_dir>/relay` at 0600, and
  pinned by nodes with `cert = "sha256-raw:<hex>"`, tlsdial's own pin,
  so an IP-only VPS needs no CA), `letsencrypt` (autocert, TLS-ALPN on
  :443) or `files`. Settings are `[relay]` (`hostname`, `listen`
  `:443`, `stun_port` 3478, `tls`, `cert`, `key`); it logs the
  `relays.list` entry. It forwards for any key, as derper does without
  `--verify-clients`, which needs a tailscaled. No mesh: a second relay
  is a second list entry. `TestFriendsThroughASelfHostedRelayOnly`
  befriends two nodes whose only relay it is.
- **NET-5a** With `relays.accept_peer_hints = false` (the default) a
  friend's address must name a relay of the set: `Tailcat.Dial`
  refuses one that does not, before a client is built or the relay
  dialled, with `transport.ErrRelayNotAllowed` naming the relay and the
  two ways to allow it; `peer add` refuses an invite the same way. An
  address shares a relay with the set when one of its nodes is a node
  of a set region (relay identity, below). An address naming a hosted
  region by ID only is resolved by tailcat against Tailscale's map, so
  it is a peer hint. The set's map is read at most every 5 minutes, a
  failure remembered for a minute, and a relay found outside it is
  checked once more against a fresh read before it is refused. Since
  tailcat's client reaches the server only through the server's own
  region, a refused relay means no tunnel to that friend: hints decide
  whether the friend can be reached at all, and the error says so.
  With hints accepted, a set that cannot be read does not stop a dial.
  The daemon, `peer add`, `peer forward` and `recover` all dial under
  the policy.
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
  `list`, `remove`) is plain HTTP under `/_lendisk/` on the same tunnel port,
  authenticated by the tunnel peer key and checked per request against
  `lendisk.db` (SEC-11). `manifest` (`PUT` and `GET`, the caller's own
  sealed manifest, §3.1 ID-4) is served (#28). `quota` is served
  (#24): `POST /_lendisk/quota` with `{"number", "use",
  "repository_size"}` (the caller's number for the friendship, its
  count of what it stores at the callee, and its repository size in
  whole GiB), answered with `{"number", "quota", "use", "lent",
  "repository_size"}` after the callee applied the caller's number
  (§3.2); open to the `peer` role only, keyed by the resolved caller; a
  number outside 1 to 2^62, a negative or oversized use, or a size not
  in whole GiB is `400`, an invite key `403`. *Rate limit (#24 review,
  LEND-5):* a friend's call is applied at most once per
  `Server.QuotaEvery` (`control.DefaultQuotaEvery`, 10 s); one sooner
  is answered `503` with `Retry-After: 1` and changes nothing. Each
  applied call writes `lendisk.db` and may change the quota, its
  alerts and the writes in flight, so a hostile friend toggling its
  number does so at most every 10 s; an honest one calls hourly or
  once per change and retries after its backoff. The answered
  repository size is a scan of the queue and is reused for a minute
  (`control.SizeCache`). The caller checks the
  answer the same way (`control.QuotaResponse.Check`) and reads the
  callee's protocol from its header. `challenge` is served (#27, §3.6
  VER-1): `POST /_lendisk/challenge` with `{"bucket": <sealed>,
  "key", "nonce", "offset", "length"}` or `"whole": true` and no
  range, answered `{"hmac", "size"}` (HMAC-SHA256 keyed with the
  nonce over the bytes read, and how many). Open to the `peer` role
  only and keyed by the resolved caller, like `list`; the read goes
  through `Lending` in the caller's account (`gateway.LentIndex.Read`,
  `Server.Held`), so the sealed name maps to the caller's own bucket
  alone (PEER-8) and a tomb or any key under a reserved prefix is
  unreachable (SEC-5, RET-2). No such bucket is `404` with code
  `NoSuchBucket`, no such key `404` with `NoSuchKey`, a range starting
  past the end `416` with `InvalidRange`: the answers the borrower
  counts as a failed proof. A nonce outside 16 to 64 bytes, a key a
  borrower may not name, a range not 1 to `control.MaxChallengeRange`
  (1 MiB) bytes, or a whole challenge with a range is `400`.
  *Pacing (LEND-5, #27):* a friend may already read any of its bytes
  by `GetObject`, paying the bandwidth; a challenge returns 32 bytes,
  so what bounds it is the lender's reads. Per friend, one challenge
  reads at a time, and two token buckets of bytes admit a call only
  while its own is not in debt: ranged challenges (`ChallengeBurst`,
  8 MiB, refilled at `ChallengeRate`, 256 KiB/s) and whole-object ones
  (`ChallengeWholeBurst`, 32 MiB, `ChallengeWholeRate`, 4 MiB/s;
  `control.Default…`). The call then takes what it read, at least
  `ChallengeCallCost` (4 KiB). So a whole-object challenge may
  overdraw once and the friend waits for the debt; a call refused is
  answered `503` with `Retry-After` (seconds to refill) and reads
  nothing, other friends unaffected. A full check's debt never holds up
  a daily challenge, which reads at most 512 KiB a replica and stays
  inside the ranged burst (`TestChallengesArePacedPerFriend`).
  *Decision (#27 review):* the bound is a rate, under 4.3 MiB/s of
  reads per friend for 32-byte answers, rather than a daily budget
  proportional to what the friend stores: a full check of a 100 GB
  replica then takes about 7 hours and needs no budget arithmetic, and
  a hostile friend gets a sustained read that a disk serves beside
  backups, below what its own `GetObject`s could ask over a fast link.
  `control.CallError.RetryAfter` carries the header. `list` is served (#29):
  `GET /_lendisk/list?bucket=<sealed>&after=<key>&limit=<n>` pages the
  lender's index for that borrower's bucket: key, size, SHA-256, if
  tombstoned, when (`tombstoned_at`, §3.6), and for a live key the
  borrower rewrote, the hashes of the versions it replaced
  (`replaced`, newest first, omitted when none; #74), as
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
  needs a request per object. `undelete` is served (#32, §3.6 RET-3):
  `POST /_lendisk/undelete` with `{"bucket": <sealed>, "since":
  <unix seconds, 0 for every tomb in the window>, "rewrites", "after",
  "limit"}`, answered `{"restored": [{"key", "size", "sha256",
  "tombstoned_at"}], "skipped": [{"key", "sha256", "live",
  "reason"}], "next"}`: at most a page (`gateway.MaxListPage`) of the
  keys with tombs since then after `after`, each restored or skipped
  (`conflict`, `live`, or `failed` with a `detail`). Open to the `peer` role only and keyed by the
  resolved caller, like `list`; another's or an unknown sealed name is
  `404` with `NoSuchBucket`; a negative since, a page over 1000 or no
  bucket `400`; a gateway still starting `503` with `Retry-After: 5`.
  *Rate limit (LEND-5, #32):* per friend one undelete runs at a time
  (a second is `503`), and a token bucket of keys looked at
  (`Server.UndeleteBurst`, 2,000, refilled at `UndeleteRate`, 100 a
  second; `control.Default…`) admits a call only while not in debt;
  the call then takes the keys it restored or skipped, at least
  `UndeleteCallCost` (10), so a page may overdraw once and the friend
  waits, `503` with `Retry-After`; other friends are unaffected
  (`TestUndeleteIsPacedPerFriend`). The borrower's runner waits out a
  `503` as long as it asks. 6,000 keys, a 100 GB restic repository's
  packs, take under a minute; a hostile friend makes the lender walk
  at most 100 tombstone keys a second, each a link and an unlink it
  could cause anyway by a re-put of the same bytes. The borrower replicates to a friend with
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
  of the address's region that answers, and does not report which:
  derphttp's `dialRegion` tries the nodes in order and takes the first
  whose TCP connect succeeds within 1.5 s, before any TLS. So `relayOf`
  names every DERP server the address could reach: each node that is
  not STUN-only, by host name, by each IP it gives and by each IP its
  host name resolves to (so two DNS names, or a name and its IP, for
  one server are one relay), with its DERP port (0 is the scheme's: 443
  over HTTPS, 3340 over plain HTTP with `TS_DEBUG_USE_DERP_HTTP`, as
  derphttp dials); and the region of the node's relay set (NET-5) that
  holds any of those servers. A meshed region's nodes forward to each
  other, so a second client of the key on another of its nodes stalls
  the first as on one server: two addresses with disjoint nodes of one
  region of the set are one relay. tailcat strips region IDs, codes and
  node names from an address, so the region is known only from the
  set; a dialler with none (a test's) knows relays by their servers.
  The names are resolved at the dial, within 2 s, and an address's
  relay is reused for 10 minutes (#60 review, #42). An address naming
  a hosted region by ID only gives that ID. Two addresses are one relay
  when those sets share a server or a region, whatever the node order,
  so an address that keeps a
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
  takes that lock. `Listen` likewise releases it while tailcat starts,
  listens and, on an error, closes the server, marking the transport
  as starting meanwhile; a `Close` that ran meanwhile closes what
  `Listen` started (#60 review).
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
  *Built (#34, `cli.runPeerForward`):* `lendisk peer forward <node ID>
  <loopback IP:port>`; any address that is not a loopback IP literal is
  a usage error (SEC-8). It dials with `transport.NewFriendDialer`, one
  tailcat dial per accepted connection, spliced byte for byte
  (`cli.splice`); it adds, removes and rewrites nothing, so the
  friend's gate, bridge and `Lending` see the tool's own requests. The
  buckets listed are those of the node's own map with an acknowledged
  placement at that friend (`state.DB.HeldAt`). Per bucket it prints
  the bucket, its sealed name and object count, then
  `rclone copy ":s3,provider=Other,env_auth=true,endpoint='http://<addr>',list_version=2:<sealed>" <bucket>`
  (rclone reads the pasted `AWS_*` variables; `list_version=2`, since
  the gate refuses v1 listings) and, for a restic bucket,
  `restic --no-lock -o s3.region=us-east-1 -r s3:http://<addr>/<sealed> snapshots`
  (`--no-lock`, since a lock is a write the friend refuses; the region,
  since without one minio-go first asks `GetBucketLocation`, which the
  gate refuses, and only then falls back to us-east-1). A backup tool
  cannot write through the forward: `Lending` refuses a borrower's put
  without `x-amz-meta-lendisk-sha256` (LEND-4), so `restic prune`
  through it fails at its lock and changes nothing at the friend, while
  a delete a client does send is a tombstone (RET-1). Run at enough friends into
  one directory named for the bucket, the copies are the repository
  (P-5); the lender's `.lendisk/` never appears in a borrower's
  listing. restic 0.19.1 and rclone 1.76 send only shapes the gate
  admits through it (`TestPeerForwardRefusesPruneAndOffList`,
  `TestScenario10RestoreStraightFromFriends`, a byte-for-byte tap in
  front of the forward). *One dialling process (#69):* the forward
  dials with the node's dial key for that friend, as the daemon does,
  and two tailcat clients of one key at one relay stall each other. So
  `serve` and `peer forward` both take `<config>/locks/dial.lock`
  (flock, dropped by the kernel when the process ends), and whichever
  comes second is refused, naming the other. Restoring straight from
  friends thus means stopping `serve` meanwhile, as `recover` already
  asks. Precedent: Tailscale's `tailscale nc` and Syncthing's CLI ask
  the running daemon instead; lendisk has no daemon IPC beyond
  `lendisk.db`, and a forward through the daemon would be a second
  socket SEC-8 does not name.
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
  `Tailcat` also takes the relay policy, explicit addresses and UDP
  port (`SetRelays`, `SetEndpoints`, `SetPort`; NET-3, NET-5a), set
  before use. `transport.NewDialer` is a dial-only transport with any key and no
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
    when `last_ok` is unset (it never answered). Since the
    reachability probe (§3.4, #65), a friend with nothing queued is
    judged too: while this node runs, its last answer is at most a probe
    interval (15 minutes) old, so a friend that went off at t0 is
    retrying from its next probe and alerted at about t0+24h, whether or
    not anything waits for it
    (`TestAnIdleFriendThatGoesAwayIsAlertedAtThreshold`,
    `TestAnIdleFriendIsProbedAndAlertedOnlyOnceAway`); one answering
    its probes never is. *Decision (#65): the hour stays.* The probe
    keeps `last_ok` fresh only while this node runs: a node switched
    off for longer than `unreachable_after` starts with every friend's
    last answer that old, and a one-minute blip at its first probe
    would alert at the first pass. The hour asks for four failed probes
    in a row (or an hour of failed sends) first, and costs an idle
    friend's alert nothing but that hour;
  - `store_unreadable`, no subject: a place waits with condition
    `store` (§3.4 *Local errors*);
  - `objects_not_replicated`, subject the bucket: objects refused
    because the store could not supply them (VER-5, #50), until written
    again;
  - `place_refused`, subject the place: rows it refused for good (a
    `BadDigest` for bytes that match, #57; a rewrite of a rewritable key
    it keeps refusing, #74; a `412` saying it holds other bytes, which
    also raises `key_changed`, #22), until written again;
  - `key_changed`, subject the bucket: keys written with other bytes
    than a friend acknowledged that the profile does not name
    rewritable, held back at capture or refused by a lender's `412`
    (`state.DB.ChangedKeys`, §3.4 *EP-11*, #22), naming the first key,
    both hashes and how many more; for a `412`, it names the friend and
    gives its hash as that friend's report, not an acknowledgement.
    Raised at once (`state.DB.KeyChanged` and the worker call `Check`),
    cleared when each such key is written back to the acknowledged
    bytes or deleted;
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
    below `copies` and raises `below_copies` past the threshold;
  - `below_copies`, subject the bucket: live data objects have been
    below `copies` for longer than `replication.replace_after` (7
    days, REPL-13's threshold; `alert.Service.BelowAfter`), as the
    re-placement pass records them (`state.DB.BelowCopiesSince`, §3.4
    REPL-13, #26): re-placement had nowhere to put the missing copies,
    so the message says to add a friend, agree a higher quota or lower
    `copies`. Per bucket, not per object: it says since when the bucket
    has had some below, continuously. Cleared at the first pass that
    finds none (`TestObjectsBelowCopiesPastTheThresholdAreAlerted`).
    Precedent: Ceph's `PG_DEGRADED` health warning becomes "stuck" only
    past `mon_pg_stuck_threshold`;
  - `replica_unverified`, subject `<friend>/<bucket>`: the friend's
    replica of the bucket failed a challenge (VER-1, §3.6, #27; a
    failed verification in OPS-2's list), naming what failed, that its
    copies are being placed at other friends, and that `lendisk verify
    <bucket> --peer <friend> --full` passing clears it. Raised at once
    (the verifier calls `Check`); held while `replica_check.unverified_at`
    is set; not raised for a former friend
    (`TestAFailedVerificationIsAlerted`). A challenge that got no answer
    raises nothing;
  - `replica_unchecked`, subject `<friend>/<bucket>`: the replica holds
    acknowledged objects and has not passed a challenge for 3 days
    (`alert.Service.UncheckedAfter`; §3.6 *Withheld proofs*), with the
    last challenge's error if it got none; drains nothing, cleared by
    the next pass;
  - `friend_removed`, subject the former friend: it ended the
    friendship (its remove call, §3.2 PEER-5, #25); raised at once,
    cleared at the purge or when the two are friends again. This
    node's own `peer remove` raises nothing: its output says the same;
  - `friend_refuses`, subject the friend: it has refused this node as
    no friend (place `removed`) over at least an hour, the last time
    within 2 hours (§3.2 PEER-5, #83), so it has probably ended the
    friendship unheard; the message says to run `lendisk peer remove`
    if it did. Cleared when it answers otherwise or stops being asked;
  - `replica_purge_soon`, subject the former friend: an ended
    friendship's replicas are purged within 7 days
    (`alert.PurgeWarning`), naming the date and that a new invite
    before then keeps both; cleared at the purge
    (`TestEndingAFriendshipIsAlertedAndSoIsItsPurge`);
  - `upload_unmatchable`, subject the bucket: completed uploads whose
    pending rows cannot be matched to the store (`queue.unmatchable`,
    §3.4 REPL-1, #20), each holding the bucket's metadata and so every
    friend's queue; raised once, cleared when the rows settle
    (`TestAnUnmatchableUploadIsAlertedUntilItSettles`).
  - `quota_high`, subject the friend: this node's use there
    (`place_use`, reconciled from the friend's count) is above 90%
    of the quota (`alert.QuotaHighPercent`, OPS-2, #24);
  - `quota_exhausted`, subject the friend: the use is at or above the
    quota (a lowering below use, PEER-6) or the friend's last answer
    was `507`; it says no new copies go there and nothing is deleted.
    The worker calls `Alerted` on a `507`;
  - `lent_over_quota`, subject the friend: it stores more here
    (`lent_usage`) than the quota, after a lowering: no new objects
    are accepted from it and nothing is deleted. With the friend's
    `quota_exhausted` this is PEER-6's "notifies both", as is `peer
    quota`'s own output (`TestQuotaUseRaisesAlerts`);
  - `kopia_format1`, subject the bucket: it holds a Kopia repository in
    format version 1 (`state.Bucket.Format1`, §3.3 EP-7, COMPAT-1,
    #79), which lendisk does not support; the message names the first
    format-1 blob, says writes are accepted and replicated as before,
    and gives `kopia repository set-parameters --upgrade`. Raised once
    per bucket, cleared when the bucket captures the upgrade's poison
    blob or is deleted (`TestKopiaFormat1BucketIsAlertedUntilUpgraded`);
  - `reconcile_held`, subject the bucket: the store reconciler found
    more than `store.reconcile_delete_limit` percent of the objects this
    node held missing from the bucket's directory and enqueued none of
    them as deletes (§3.3 EP-14, #8); the message gives the count, of
    how many, since when, and `lendisk bucket reconcile <bucket>
    --allow-deletes <count>` (`alert.ReconcileHeldMessage`). Cleared by
    the first pass that holds nothing back;
  - `store_marker_missing`, no subject: the store marker is gone, so the
    store may be unmounted or replaced and nothing is reconciled (§3.3
    EP-14); the message names the path and says to check the mount and
    `touch` it. Cleared by the first pass that finds it
    (`TestReconcileHoldAndMissingMarkerAreAlerted`);
  - `bitrot`, subject the bucket: the bit-rot sampler found own objects
    whose bytes no longer hash to the version captured (§3.6 VER-5,
    #31); the message names the first key and both hashes, how many were
    fetched again, are waiting for a friend, or are kept as no friend
    holds them, and says to check the disk (`alert.BitrotMessage`).
    Held while one is not repaired and for `state.RotShown` (7 days)
    after the last repair, so a failing disk is told even when every
    object came back;
  - `lent_bitrot`, subject the friend: objects that friend stores here
    no longer hash to the hash they were stored with; the message says
    this node cannot repair them and the friend's challenges find them,
    and to check the disk (`alert.LentBitrotMessage`). Held while the
    objects are (`TestBitrotIsAlertedOnBothSides`);
  - `recovery_sheet_stale`, no subject: the recovery sheet last printed
    no longer matches the node's state (§3.1 ID-7, #33); the message
    says when it was printed, what changed (a friend's address, a
    friend, overflow target or bucket added or removed) and to run
    `lendisk recovery-sheet` again, which clears it at the next pass
    (`TestFriendsAddressChangeAlertsToRegenerateTheSheet`);
  - `protectable_size`, no subject: the repository, as places count
    it, is above 90% of the protectable size (`capacity.Plan.Alerting`,
    §3.5 LEND-7, #37). The message gives both sizes and `copies`, says
    copies beyond it are paused for quota when it is past it, and names
    per friendship the quota that would cover it, "friend <ID>: 68G (now
    50G)", for both sides to set with `peer quota`
    (`alert.ProtectableMessage`); with fewer friends than `copies` it
    says to add a friend or an overflow target instead. `Service.Copies`
    is `replication.copies`, the `c` of a node with no bucket
    (`TestRepositoryNearTheProtectableSizeNamesANumberPerFriendship`,
    `TestTooFewFriendsToProtectSaysSo`).
  The four quota alerts (`quota_high`, `quota_exhausted`,
  `lent_over_quota`, `protectable_size`) have hysteresis (#24 review;
  the last since #37, as a repository near the line crosses it with
  each backup and prune): one stays raised
  until its condition has been false for `alert.Service.QuotaHold`
  (`DefaultQuotaHold`, 10 minutes), so a friend flipping it with every
  quota call gets one delivery per window, not one per call
  (`TestToggledNumbersRaiseAQuotaAlertOnce`); `quota_high` and
  `quota_exhausted` of one friend are one condition at two levels, the
  current one replacing the other at once.
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
- **OPS-4, metrics (#43).** Package `metrics`; the daemon serves
  `GET /metrics` (`HEAD` too; any other path 404, other methods 405) in
  the Prometheus text exposition format 0.0.4 on `metrics.listen`, a
  loopback IP literal and port such as `127.0.0.1:9101`. *Decision:
  off by default* (empty): a default port would be a second loopback
  listener on every node and would collide on a host running two nodes
  or an exporter there, and the lender role could not tell a default
  from a setting. `Config.Validate` refuses a non-loopback address, a
  host name (`localhost` too: a name can resolve elsewhere) and any
  value in the lender role (PEER-9); `serve` checks the bound address
  is loopback again (`listenLoopback`, SEC-8). `status` and `status
  --json` (`endpoint.metrics`, additive) show the URL.
  *Decision: no dependency.* `prometheus/client_golang` (v1.25.0,
  pure Go) was measured on this binary: +3.1 MB amd64 (61.1 to 64.2 MB)
  and +3.0 MB armv7 (57.1 to 60.0 MB), about 5% each, and seven modules
  more (`client_model`, `common`, `procfs`, `protobuf`, …), for a
  format that is line-based text and stable since 2014. lendisk writes
  counters and gauges only, so `metrics.Write` (~150 lines) writes them
  and refuses what Prometheus could not read (bad names, a counter not
  ending in `_total` or negative, a series twice); `metrics.Parse`
  reads the format back strictly and the tests check every scrape with
  it. The format's only maintenance is OpenMetrics, which Prometheus
  negotiates and does not require.
  - *Counters* (`metrics.Counters`, in memory, from zero at each start;
    `rate()` handles the reset) are moved by the replication worker
    (`replica.Worker.Metrics`): `lendisk_replication_sent_bytes_total`
    `{place}`, the object bytes the replica client read from the store
    for the place, every attempt and part (counted after pacing, so a
    retried part counts again); `lendisk_replication_objects_total`
    `{place,op}`, rows acknowledged, `op` put or delete; and
    `lendisk_replication_retries_total{place,reason}`, failed sends that
    wait to be retried, `reason` network, store, quota, removed or busy.
  - *Gauges* are read from `lendisk.db` at each scrape by the functions
    `status` uses (`cli.metricsFamilies`), so the two never disagree:
    per place (status's Places) `lendisk_place_queue_objects`,
    `_queue_bytes`, `_lag_seconds`, `_refused_objects`,
    `_dropping_copies`, `_last_ok_timestamp_seconds`, and
    `lendisk_place_state{place,state}`, 1 for the current condition of
    ok, retrying, quota, removed, store, busy, none; per friend (§3.5 LEND-7,
    `capacity`) `lendisk_friend_quota_bytes`, `_used_there_bytes`,
    `_used_here_bytes`, with `lendisk_repository_bytes` and
    `lendisk_protectable_bytes`; `lendisk_alerts{kind}`, pending alerts
    for every kind in `alert.Kinds()`, 0 when none, so a rule can match
    a kind before it first fires; per bucket `lendisk_bucket_copies`,
    `_captured_objects`, `_captured_bytes`, `_refused_objects`,
    `_below_copies_objects`, `_below_copies_bytes`,
    `_offsite_complete`, `_offsite_behind_seconds` (the age of the
    first write not offsite, the offsite watermark's lag, 0 when
    complete) and `_offsite_through_timestamp_seconds`; the bit-rot
    sample (§3.6 VER-5) `lendisk_bitrot_findings{state}` (kept,
    refetching, repaired, lent, as status shows them),
    `lendisk_bitrot_cycles_total` and
    `lendisk_bitrot_last_checked_timestamp_seconds`; and
    `lendisk_info{version}`.
  - *Cardinality:* labels are places and friends (PEER-7: 20), buckets,
    alert kinds and fixed states, never object keys. A scrape costs
    what `status` costs; nothing is cached.
  - Not started in the lender-only profile (PEER-9), which refuses the
    setting. *Tests:* `TestWriteParsesBackAsPrometheusText`,
    `TestWriteRefusesWhatPrometheusCannotRead`,
    `TestParseRefusesInvalidText`, `TestServerServesMetricsOnly`,
    `TestCountersMoveWhenReplicationSends` (replica),
    `TestMetricsListenLoopbackOnly`, `TestLenderOnlyRefusesMetricsSettings`
    (config), `TestMetricsEndpointBindsLoopbackOnly` (serial: it reads
    the process's sockets) and `TestMetricsCountReplication` (cli).
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
  relays are `[relays]` (`map_url`, `[[relays.list]]`,
  `tailscale_bootstrap`, `accept_peer_hints`; NET-5, NET-5a) and
  `lendisk relay`'s `[relay]`, the address's bootstrap region `[tunnel]`
  (`region`, SEC-14, and `port`, NET-3), alerts `[alerts]` (`command`, `webhook`,
  `unreachable_after`) and metrics `[metrics]` (`listen`, OPS-4). There are no
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
  The challenge's attacks (#27) are a malformed body, B's own
  tombstoned object (`404 NoSuchKey`: a tomb is no live byte), C's
  live object by C's sealed name (`404 NoSuchBucket`), the tomb's path
  and a range over `control.MaxChallengeRange` (`400`), and a `GET`
  (`405`); its pacing is `TestChallengesArePacedPerFriend`'s. The
  undelete's (#32) are an empty body and a page over 1000 keys
  (`400`), C's bucket by C's sealed name (`404 NoSuchBucket`), B's
  own since 2100 (`200`, nothing restored, so B's tombstone stays for
  the RET-2 phase), and a `GET` (`405`); its pacing is
  `TestUndeleteIsPacedPerFriend`'s, an invite key's `403`
  `TestUndeleteCallRestoresTheCallersOwnTombs`'s.
  With friendships A–B and A–C, a hostile B, holding every credential
  its node holds, sends A all of them from its node ID and from its
  dial key: everything outside NET-7's list is refused and A's store is
  unchanged; the admitted operations reach nothing but B's own
  buckets; each answer on the dial key equals the answer on the node
  ID; a request signed with the dial key, C's credential or A's node ID
  as the access key is refused; B's own key respelled with whitespace (`respellings`), which versitygw still reads as B, is refused everything outside the list; RET-2's attack list leaves B's
  tombstone intact; the redeem abuses of #69 are refused; after
  `peer remove` B, both keys are refused everything on their open
  connections, the remove call and the manifest included, and once the
  daemon's farewell pass has cut them neither key connects. An unknown
  identity is refused the connection with zero peers, two, one (after
  B's removal) and zero again (after `peer remove` C), and B at C (not
  its friend) is refused the connection. B's own remove call is left
  out of the friend-phase list (`friendlyControl`): sending it ends the
  friendship, which is its right. The `admitall` build
  (`TestMiddlewareAloneRefusesStranger`) sends the whole list from an
  unknown identity whose connection is admitted, and the middleware
  refuses every request (SEC-11).
- **Per-friend caps.** fasthttp's `MaxConnections` and `MaxRequests` at
  `gw.sock` are shared by every listener, so the tunnel bounds each
  friend before it gets there (`bridge.Caps`, #15): at most 32 open
  connections per friend (`DefaultConns`), counted on the friend the
  connection's key resolves to in `lendisk.db` at accept (`Caps.Of`,
  `RolesIn`), so its node ID and its dial key share one budget, and a
  connection over that is closed unread; a key with no relationship
  counts on its own and is refused at its first request anyway; and at
  most 16 requests in flight per friend,
  counted on the resolved node ID (`DefaultRequests`), one over that
  answered 503 `SlowDown` with `Retry-After: 1` (a JSON 503 on the
  control path), which S3 clients and the replica worker retry. The
  borrower's replica client moves one object per bucket at a time over
  a couple of connections (§3.4), well inside both. Loopback and LAN
  listeners are not counted. The caps are constants, not settings;
  rate limits per borrower are settings (LEND-5, §3.5).
  A request holds its slot until its answer is written, so neither
  direction may hang on the friend (#76): a request body is bounded by
  the stall bound and the floor rate (§3.5 LEND-2), and each write to
  the connection gets a deadline `Caps.WriteStall` ahead (2 minutes,
  the borrower's own stall watchdog), set by the tunnel connection
  (`peerConn.Write`), so a friend that stops reading an answer loses
  the connection, and the slot, once one chunk's write has waited that
  long (`TestAFriendThatStopsReadingAnAnswerLosesItsSlot`). The
  hijacked connections of the removed answer and the remove call
  (§3.2) only read after their answer, so the deadline leaves their
  linger alone. The accept loop only accepts: each connection's key
  (`PeerOf`) and friend (`Caps.Of`, a database read of up to 5 s) are
  resolved on a goroutine of their own, which counts it under the cap
  and hands it to `Accept`, so a slow database holds up that
  connection, not every tunnel accept
  (`TestASlowCapLookupDoesNotStallOtherAccepts`). At most 64
  connections are being admitted at once across every key
  (`maxAdmits`, #76 review): the accept loop takes a slot before it
  starts one and waits when none is free, so a burst of connections
  from a friend or an invite holder waits in the listener's backlog,
  and holds at most 64 lookups, each with its own SQLite connection,
  not one per connection (`TestAdmitsInFlightAreBounded`). The
  database pool itself is left unbounded: a cap there would make any
  path that reads through the pool while it holds a transaction wait on
  itself, and nothing audits the daemon for such paths yet.
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
  tailcat; loopback metrics when `metrics.listen` is set (OPS-4), never in
  the lender-only profile; the
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
  | Admitted | `ListBuckets` (own only), `CreateBucket`, `HeadBucket`, `ListObjectsV2`, `PutObject` (a rewrite too, marked with the user metadata `x-amz-meta-lendisk-replaces`, which `Lending` checks, not the gate: §3.5 LEND-4, #74; `TestGateAdmitsTheRewriteMark`), `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `GetObject` (ranged), `HeadObject`, `DeleteObject` (no version id) | the same thirteen |
  | Refused at the gate | copies (`CopyObject`, `UploadPartCopy`), `DeleteObjects`, browser-form `PostObject`, `RenameObject`, `RestoreObject`, `SelectObjectContent`; list v1, `ListObjectVersions`, `ListMultipartUploads`, `GetObjectAttributes`, `GetBucketLocation`, torrent; ACLs, policy, ownership controls; bucket and object tagging; object-lock configuration, retention, legal hold; versioning; CORS and website; lifecycle, analytics, encryption, intelligent tiering, inventory, metrics, replication, public access block, logging, request payment, notification, accelerate; annotations; `DeleteBucket`; CORS preflight (`OPTIONS`); versitygw's error routes | every other method that an S3 action names |
  | Internal | none | `GetBucketAcl`, `GetBucketPolicy` (access check on every request), `GetBucketCors` (when a request carries `Origin`), `GetBucketVersioning`, `GetObjectLockConfiguration`, `GetObjectRetention`, `GetObjectLegalHold` (object-lock checks on writes and deletes), and `String`, `Shutdown`, `NormalizeObjectKey`, `StoresDotSegmentKeys`, `ValidateUploadID` |
  | Unreachable | none | `ChangeBucketOwner`, `ListBucketsAndOwners`: the admin API only, whose every route is a `PATCH` the gate refuses (SEC-8) |

  Every class but Admitted is decided at the gate, before versitygw
  routes or authenticates and before the backend is touched. `Lending`
  then refuses every Refused and Unreachable backend method a second
  time, keyed on the account versitygw authenticated rather than the
  gate's unverified reading of the key, as its first step
  (`TestLendingRefusesEveryRefusedMethodToABorrower` calls each one
  directly as a borrower, with no store and no database behind it). The table is re-reviewed on every
  versitygw upgrade, together with the replication-hole review (Q-H),
  and the two tests above fail when an upgrade adds an action or a
  method. The control API is not in the S3 table: the middleware
  admits only the `peer` role to it (redeem aside, SEC-13), and each
  call checks the role again and answers for `Caller.Peer`. The one
  call that reaches the store's objects, `challenge` (#27), does so
  through `Lending.GetObject` in the caller's account, the admitted
  `GetObject` entry of `gateway.BackendMethods`, so it reaches exactly
  what the caller's own ranged `GetObject` would, and answers an HMAC
  instead of the bytes.
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
  never admissions or errors, each for 3 seconds
  (`transport.DeniedFor`), empties itself at 4096 keys, and
  `Admission.Clear` empties it after a commit. The expiry is for
  changes a one-shot command commits from another process (`peer add`
  writing the invited side's row), which cannot reach the daemon's
  cache. It is well under a tailcat client's 10-second try to be
  admitted (a meow resent every second), because the inviter's daemon
  calls the invited node as soon as it has stored the friendship,
  usually before `peer add` has stored its side: the refusal is
  forgotten and the key admitted within that same try. At 30 seconds
  the first calls failed, and a remove call made then waited out the
  farewell's minute of backoff
  (`TestAFriendRemovedJustAfterBefriendingIsTold`). tailcat keeps a once-admitted key's tunnel, so a used invite
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
  access key without verifying anything, exactly as versitygw reads it
  (`gateway.ReadCredentials`, §3.7 NET-7): from the `Authorization`
  header, split at its first space with every whitespace character
  removed from each part that holds a space, then `Credential=` as the
  first of three comma-separated pairs; or from a presigned URL's
  `X-Amz-Credential`. It refuses the request unless that key equals the
  peer key; versitygw then verifies the signature for that access key.
  A request with no credential is refused, and so is one whose
  credential cannot be read that way (two `Authorization` headers,
  any scheme but `AWS4-HMAC-SHA256`, SigV2's `AWS key:sig` included,
  a malformed header or `X-Amz-Credential`, an `AWSAccessKeyId`) and
  one whose header and query name two different keys
  (`bridge.AccessKeyOf`, `Bridge.TunnelS3Handler`). Since the reading
  is versitygw's, `Credential =<id>` or an ID with a space or tab
  inside is that ID here too: it passes this check only on the
  connection of that ID, and the gate then applies the borrower list to
  it. A friend dials
  with its dial key (ID-1), which the middleware resolves to the
  friend's node ID before anything acts on it; "the peer key" here is
  that node ID, so the access key and the per-pair secret stay keyed
  by node ID and a request signed with the dial key is refused. The
  control API takes the borrower from the context, never from a
  parameter. `redeem` is the one call whose connection is not the
  friend's own key; the friend's key comes from its address and is
  accepted only with the key-agreement proof of §3.2 (PEER-2).
  `bridge.ServeTunnel` does this: a listener wrapper resolves
  `Transport.PeerOf` at accept and closes a connection without a key
  unread, and `ConnContext` puts the key in the request context.
  *A friend's credential is served on the tunnel only (#15).* The
  loopback and LAN handler (`Bridge.LoopbackHandler`) refuses with
  `AccessDenied`, before the gateway sees it, any request whose
  credential, read as versitygw reads it, is a node ID, or whose
  whitespace-free spelling of any `Credential=`, SigV2 key or
  `X-Amz-Credential` is one, and any request whose credential it
  cannot read (`gateway.ReadCredentials`;
  `TestLoopbackRefusesFriendCredential`,
  `TestListenersReadSpacedCredentials`). Behind it, the gate applies
  the borrower list to the same reading and `Lending` refuses every
  method outside it to a borrower's authenticated account, so a
  reading that missed a spelling would still reach no refused method
  (§3.9 SEC-9). Off the tunnel,
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
  friend's node ID and its dial key, in that order. *Realised for
  ending a friendship (#25):* the commit is `peer remove` (a one-shot
  process) or a friend's remove call; the daemon's farewell
  (`internal/cli/ending.go`) applies it on its next pass, at most 10 s
  later or at once after a remove call: `Admission.Clear`, then
  `bridge.Conns.Cut` for the node ID and the dial key, then
  `Transport.Disconnect` for each, then `Cut` once more for a
  connection opened inside the still-standing tunnel between the two
  (its requests were refused anyway). `bridge.Conns` tracks each
  accepted tunnel connection under the key it authenticated with;
  `ServeTunnel` gives each a connection context that `Cut` cancels with
  the cause `bridge.ErrCut`, which ends the reverse proxy's request to
  `gw.sock`, and sets its read deadline in the past (kept there, so
  neither the body bound nor net/http's post-handler drain waits on the
  client). A request whose answer had not started gets the removed
  answer (403 `AccessDenied`, `state.NoRelationship`, `Connection:
  close`, with its length) and the connection is then hijacked and
  closed once the client has the answer (below); an idle connection is
  closed at once; a busy one that does not end within `Conns.Grace`
  (2 s), or the linger below when it was hijacked, is closed anyway.
  The cut sets the read deadline under the connection's lock, which a
  hijack and every deadline set take too, so a hijack comes wholly
  before the cut or after it and its handler's deadline is never
  overwritten (`TestCutDoesNotOverwriteAHijackersDeadline`). It marks
  the connection cut in the same step and cancels the context only
  after, so a proxied body read can fail on the deadline before the
  context says why; the handler asks the connection's mark first, and
  such a request still gets the removed answer, not the stalled-body
  one, which would have closed the connection at once (CI on 3426550,
  `TestARequestWhoseReadsTheCutEndedIsSeenAsCut`). A connection
  its handler hijacked (`http.StateHijacked`, seen by `ServeTunnel`'s
  ConnState) keeps its read deadline: the remove call reads it until
  its caller closes it after the answer (§3.2 PEER-5, #84), and `Cut`
  waits for the handler to close it, until the grace after the cut or
  `Conns.Linger` after the hijack, whichever is later. The removed
  answer is delivered the same way (#84, CI on 2b1f67e: B's transfer
  timed out with no answer): `writeCut` writes it with `Connection:
  close`, hijacks the connection and reads it until the client closes
  it, for `Conns.Linger` (5 s) at the most, and only then closes it, so
  neither a close with unread body bytes (a reset) nor the disconnect
  after `Cut` discards an answer still in the tunnel's send buffer.
  `Cut` waits for such a connection for the grace from the cut or
  `Linger` from the hijack, whichever ends later. A client still
  sending its body keeps sending until its write fails, so it gets the
  answer when the connection closes after `Linger`
  (`TestCutClosesOnlyOnceTheClientHasTheRemovedAnswer`)
  (`TestCutCancelsInFlightRequestsAndClosesTheKeysConnections`; over
  the real tunnel, spike #3's order, `TestCutThenDisconnect`;
  `TestFarewellRevokesInOrderTellsAndPurges`). The SEC-15 AC,
  `TestRemovingAFriendEndsItsRunningTransfer`: B's 2 MiB PUT trickling
  on its dial key is answered 403 "no relationship" about 8 s after `peer
  remove`, B's next request on its node-ID connection is refused at
  once, nothing is stored, and neither key connects after. tailcat's disconnect alone resets nothing and cuts only the
  server-to-client direction: the client's packets still arrive on
  open connections for some seconds, and a connection closed after
  the disconnect never tells the client (Q-Q). So lendisk closes its
  own connections first, and SEC-11's per-request check refuses
  anything that arrives meanwhile. *Realised for a lowered quota
  (#24):* no connection is cut; the next request is checked against
  the new quota, and a write in flight at its body's next read once
  the daemon calls `Gateway.Requota` (§3.5). Later requests are answered
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
- **SEC-2** The list of what a lender can observe is documented for
  users in `docs/store-layout.md` (*What a friend can see of your
  backups*, #35), with the note that bucket names are not on it and
  that a generic bucket is readable.

### 3.10 Compatibility

- **COMPAT-1** Kopia repositories in format version 2 or later: a
  `kopia` bucket that captures a blob only format 1 writes is marked,
  shown in `status` and alerted (`kopia_format1`), never refused
  (§3.3 EP-7, *Kopia format 1*).
- **COMPAT-2** lendisk is one static Go binary for amd64, arm64 and
  32-bit ARM, so the package exists for every v1 platform.
  The container image recommends `network_mode: host` (D2). versitygw
  also builds on macOS, BSD and Windows, which keeps post-v1 ports
  open.
- **COMPAT-3** `lendisk bucket import <name> <path> --profile
  restic|kopia [--copies N] [--resume]` (#9) creates the bucket as
  `bucket add` does (row, credential, directory through the posix
  backend) and puts every object of the repository at `<path>` through
  the capture decorator (§3.3 EP-2, EP-3), in the command's own process:
  `Decorator.PutObject` over the posix backend with the state database,
  streaming each file from disk, so its SHA-256 is taken as it passes
  and nothing is read whole into memory or read twice. Each object gets
  its done queue row with hash, size, class and `rewritable` flag, is
  marked held locally (`queue.local`), gets its placements and the
  format-1 mark (`noteFormat`) exactly as a tool's upload would, and has
  no acknowledged placement, so it replicates like a fresh backup.
  - *Why the decorator in process, not the S3 endpoint or the
    reconciler* (decided 2026-11-09). Through the endpoint the command
    would need the daemon running and its address (a test daemon's port
    is chosen at serve) and would sign every request with the bucket's
    credential, for the same decorator call at the end. Dropping the
    files into the bucket directory for the store reconciler (EP-14)
    would capture them in the same class with `queue.reconciled` set,
    but only at the next pass (start, or 24 h later), reading every
    object a second time to hash it, and counted in `status` as
    changes outside the endpoint, which they are not. In process, the
    import needs no daemon, like `bucket add`; a running daemon's
    replication worker sees the rows at its next poll (1 s) and starts
    sending while the import continues, and its reconciler leaves a key
    alone while its row is pending and finds it indexed after. Nothing
    else writes the new bucket meanwhile: its credential is printed
    only at the end. A pending row left by a killed import is settled
    by the daemon's next start (`ResolvePending`).
  - *Layouts* (`internal/repoimport`). restic: the path must have
    `config` at its top; every file's key is its path, since restic's
    default layout (restic 0.19.1 `internal/backend/layout`) is used by
    its local and S3 backends alike: `config`, `data/<2 hex>/<id>`,
    `keys`, `index`, `snapshots`, `locks` flat, every object named by
    64 hex digits. Anything else is skipped: restic's local temporary
    files `<name>-tmp-<random>`, rest-server's
    `<id>.rest-server-temp<random>`, stray files. rest-server (0.14.0,
    `repo/repo.go` `getObjectPath`) stores each repository in that
    same layout at `<data>/<repo>`, or with `--private-repos` at
    `<data>/<user>` and `<data>/<user>/<repo>` (`MaxFolderDepth` 2);
    the user gives the repository's directory. A path that is
    rest-server's data directory (no `config`, repositories below it
    to that depth) is refused naming each repository, since each is a
    bucket of its own. Kopia: the path must hold
    `kopia.repository.f`. Kopia's filesystem storage (0.23.1
    `repo/blob/sharded`) splits a blob ID longer than `.shards`'
    `maxNonShardedLength` (20) into directories by the shard sizes
    (`{"default":[1,3]}` in a repository 0.23.1 creates, so
    `p8bf4fa…` is `p/8bf/4fa….f`; `[3,3]` where `.shards` predates
    it, which 0.23.1 also writes for `--format-version=1`; per-prefix
    `overrides` possible) and appends `.f`; its S3 storage uses the
    blob ID as the key. Whatever the sizes, the key is every directory
    name and the file name joined with `.f` removed, which is how
    Kopia's own `ListBlobs` reads the files back
    (`getBlobIDFromFileName`); a file without `.f` (`.shards`, a
    temporary `<path>.tmp.<hex>`) is not a blob.
  - *Left out:* local-only keys (restic `locks/*`, a dead process's
    stale lock that would block restic at the bucket; Kopia `_log*`),
    counted in the output; they would get no queue row anyway.
  - *Kopia format 1 (COMPAT-1, #79).* A first walk counts the objects
    and bytes and asks the profile what each key says of the format
    (`profile.Profile.Format`): a `format1_keys` key with no
    `upgraded_keys` key anywhere in the repository refuses the import
    before the bucket exists, saying it is format version 1, naming the
    key and `kopia repository set-parameters --upgrade`. An upgraded
    repository (its leftover `n*` blobs beside the poison blob or `x*`)
    is imported and not marked, as §3.3 EP-7 says.
  - *Progress* on stderr: the object count and size first, then a line
    each time another whole percent of the bytes is in, and at the end;
    stdout gets the summary, the copies notes of `bucket add`, and the
    tool lines with `restic snapshots` and `kopia repository connect
    s3` in place of `restic init` and `kopia repository create s3`
    (`bucket show` still prints the creating lines).
  - *Stopped partway* (an I/O error, a full disk, a kill): the bucket
    keeps what was imported, and the error says so and to run the same
    command with `--resume`, which takes the existing bucket (same
    profile; `--copies` is refused with it) and skips each key whose
    latest done row is a put of the file's size. A file put again would
    in any case be dropped by `FinishPut` as an identical re-upload.
  - *Tests:* `TestImportedResticRepositoryListsItsSnapshots` (a real
    restic repository at rest-server's `<data>/<user>`, the data
    directory refused, then `restic snapshots` and `restic check
    --read-data` through the endpoint),
    `TestImportedKopiaFilesystemRepositoryOpensThroughS3` (a real
    sharded Kopia repository; `repository connect s3` from the printed
    lines, `snapshot list` and a byte-for-byte restore), both checking
    every object queued with its hash and none acknowledged;
    `TestImportOfKopiaFormat1RepositoryRefused`,
    `TestImportResumesIntoItsBucket` (`internal/cli`); the layouts in
    `internal/repoimport`.
- **COMPAT-4** The documented layout on `posix` is
  `<store>/<bucket>/<key>` as plain files with versitygw's xattrs and
  its housekeeping directories, `.sgwtmp/` in each bucket (temporary
  files, `multipart/` parts) and `.vgwlocks/<bucket hash>/<shard>` at
  the store's top (publish locks; found on a real store, #35); borrowers'
  buckets under `lent-<id16>-` and their sealed names (SEC-16), each
  with its `.lendisk/tomb/` directory (§3.6: one file per tombstone at
  `.lendisk/tomb/<key>/<unix nanoseconds>`, ignorable by a direct
  reader); the friends' sealed
  manifests as `.lendisk/manifests/<node ID>` and the store marker
  `.lendisk/store-marker` at the store's top (§3.1 ID-4, §3.3 EP-14);
  `lendisk.db` and its tables; and the export layout
  (one file per key at its key path; Kopia's sharded form with
  `--kopia-layout`, §3.5 LEND-6). Every object a friend stores here
  carries `x-amz-meta-lendisk-sha256` (`user.metadata` xattr), so the
  lender's hash index can be rebuilt from the store; an own object
  carries the capture token `lendisk-capture` instead (§3.3), its hash
  being in the queue. *User documentation (#35):* `docs/store-layout.md`
  (every directory, file and table; the export layout; SEC-2's list;
  LEND-7's rule of thumb from §4) and `docs/restore-without-lendisk.md`
  (P-5's procedure), linked from the README.
  - *The engine alone.* versitygw 1.10.0, the version in `go.mod`
    (`go install github.com/versity/versitygw/cmd/versitygw@v1.10.0`):
    `versitygw --access <a> --secret <s> --port 127.0.0.1:7070 --readonly
    posix --exclude-dirs .lendisk <store>`. The root account reads every
    bucket; `--readonly` keeps the store unchanged, so restic needs
    `--no-lock` and Kopia connects `--readonly`; `--exclude-dirs
    .lendisk` hides tombs and manifests as lendisk's own `OpenBackend`
    does. A friend's replica is copied out with any S3 client (rclone
    in the guide) and combined with the recovery sheet's commands (§3.1
    ID-7). Flat Kopia keys copied this way are laid out for Kopia's
    filesystem storage by a bash loop in the guide doing what
    `export.KopiaPath` does (checked by hand against Kopia 0.23.1).
  - *Scenario 7 as a test* (`TestScenarioZeroLockIn`, on scenario 1's
    four nodes): every daemon killed and D's store deleted; versitygw's
    gateway and posix backend run embedded in the test
    (`engineAlone`: `embedgw.RunVersityGW` with the binary's defaults,
    read-only, `.lendisk` excluded, plus `AbsolutePaths` so posix does
    not change the test process's directory; one at a time, the test
    holding `serving`) on A's store for `restic --no-lock snapshots`,
    then on B's and on C's for `rclone copy` of A's bucket under the
    name A's printed sheet gives; the sheet's combine command and
    `restic check --read-data` pass. Then the sheet's printed steps end
    to end: its export command on B and C, its combine command, `restic
    check --read-data` and its restore line, byte for byte.
    `TestKopiaExportsFromTwoLendersFormARepository` does the same for
    Kopia at one copy over two friends (each replica partial): A's own
    copy through the engine alone to `kopia repository connect s3
    --readonly`, and two `--kopia-layout` exports combined into a
    filesystem repository that verifies and restores.

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
re-placement worker drains it back to friends once they have room
for the repository (§3.4; *Hysteresis* and *Drain-back* below).
`status` shows what it holds, the objects for which it is one
of the `copies`, and the estimated cost (S3-7).

- **S3-1** `overflow add s3 <name> --endpoint … --bucket … --prefix …
  --max-bytes …` writes a row in `lendisk.db` (state, not
  configuration, OPS-6) and reads the credential from the environment
  (SEC-3), testing it before the target is accepted; the credential is carried in the encrypted
  manifest for recovery, and the client is built from all of these at
  start. Each of the node's buckets is a sub-prefix
  `<prefix>/<sealed name>/` (SEC-16), with keys below it as the tool
  wrote them (S3-2).
  *Realised (#38).* Table `overflow` (migration 40): name, endpoint
  URL, region (empty: minio-go asks the provider), bucket, prefix
  (default `lendisk`), `max_bytes`, `env_prefix`, retention, price per
  GB-month, and what `overflow add` found (`versioning`: enabled,
  suspended or off; `lifecycle`: present, created, absent or
  unsupported; `version_removal`: allowed, denied or unknown). Names
  are 1 to 32 of `[a-z0-9-]`, starting with a letter
  (`overflow.CheckName`). The target's place name in `placement`,
  `place`, `place_use` and every per-place table is `overflow:<name>`
  (`state.OverflowPlace`), which no node ID can be. Package
  `internal/overflow` owns the name and the credential:
  `<env_prefix>ACCESS_KEY` and `<env_prefix>SECRET_KEY`, the prefix
  `LENDISK_OVERFLOW_<NAME>_` (upper case, `-` as `_`) unless `--env-prefix`
  names another; `overflow.Credential` prints, logs and marshals as
  `[credential redacted]`, and errors name variables, never values.
  The client is `replica.NewOverflow`: the one replica `Client` (D12)
  with the provider's host, TLS by the URL's scheme, bucket lookup
  auto (path style except at AWS), keys at
  `replica.OverflowKey(prefix, sealed, key)`, and its own error
  classification (`classifyOverflow`): a checksum mismatch is
  `ErrRejected`, `SlowDown` or 503 `ErrBusy`, a 403 the provider
  refusing the credential, retried and reported, never `ErrRemoved`
  or `ErrQuota`. Its probe lists one key under the prefix (S3-5's
  credential may do that; a friend's probe lists buckets).
  `overflow add` checks before it writes the row
  (`Client.CheckOverflow`): it lists under the prefix, puts a test
  object `<prefix>/lendisk-check-<random>` with the SHA-256 checksum,
  reads it back, reads versioning (anything but enabled is warned of),
  removes the test object's own version to learn whether the
  credential can remove versions (allowed is warned of, with S3-5's
  recommendation; denied leaves the test object as a non-current
  version for the lifecycle rule), deletes it, and reads the lifecycle
  rules: with no enabled rule expiring non-current versions under the
  prefix it offers one, and creates it, added to the bucket's rules
  as `lendisk-<name>` with the retention in whole days, when given
  `--lifecycle`. Any failure of the credential leaves no row. Every error the
  overflow client returns has the credential's keys taken out of its
  message (`replica.redacted`; Garage's 403 names the access key), so
  `place.detail`, the log, alerts and webhooks never carry it. `serve`
  refuses to start while any target's variables are unset
  (`checkOverflowCredentials`, before anything starts); a target added
  while the daemon runs is built at the worker's next friendship read,
  and one whose variables that daemon lacks is retrying with the
  reason, alerted as `overflow_unreachable`. *Decision (#38):* the
  credential comes from those two variables only. restic and Kopia
  walk minio-go's chain (static keys, `AWS_*` variables, shared files,
  instance roles), and rclone does too with `env_auth`; a chain here
  could pick up a credential meant for another program or bucket, so
  lendisk names the variables per target and reads nothing else.
- **S3-3** Uploads carry a SHA-256 request checksum, the object's on a
  single PUT and each part's on multipart (§3.4), plus lendisk's
  SHA-256 as metadata. Garage verifies both (Q-Y.2); a provider that
  refuses them is set per target to send none, and the sample
  downloads of VER-2 carry the check there. Large objects use
  multipart with the resume of §3.4.
  *Realised (#38):* the same calls as a friend's (`Client.Put`,
  `Create`/`PutPart`/`Complete`): `x-amz-checksum-sha256` with
  `UNSIGNED-PAYLOAD` on a PUT, `x-amz-checksum-algorithm: SHA256` at
  create and each part's SHA-256 in an unsigned trailer, and the parts'
  checksums at complete; versitygw and Garage both refuse a body that
  does not match (`TestOverflowPutsUnderThePrefixWithAVerifiedChecksum`,
  `TestScenarioOverflowAtAnExternalProvider`). *Decision:* SHA-256, not
  Content-MD5. restic and Kopia send Content-MD5 (minio-go's
  `SendContentMd5`) and rclone sends Content-MD5 per part with the
  whole object's MD5 as metadata; lendisk already has each object's
  SHA-256 from capture (EP-3), takes each part's as it streams, and
  sends the same checksum to friends, so one path covers both. The
  per-target switch to send none is not built: no provider tried
  refuses it.
- **Placement** (#38, D6 step 3, `state/overflow.go`). `poolPlaces`
  returns the targets after the friends, with `max_bytes` as quota.
  `choose` (capture) and the re-placement pass's choose take friends
  first by D6's rule, at most `copies − 1` of them while placement is
  split (D6 step 3, `overflowSplit`, for buckets with `copies` ≥ 2;
  cached at most 30 s since it reads the whole repository, and
  recomputed at once when the friendships, their numbers, the targets
  or a bucket's copies change, read as one cheap `splitKey` query, so a
  change made by another process counts at the next capture; `status`
  shows it as `capacity.overflow_split`), and for the copies still missing a target
  that answers (one failing is no candidate: its queued copies would
  hold the metadata barrier everywhere), by most free bytes: `max_bytes` less use, less the bucket's metadata
  it lacks, so a target never passes `max_bytes` (S3-7). A target
  chosen for a data copy is first given every live metadata object of
  the bucket it lacks (`placeMetadata`), in the same transaction and
  ahead of the data in its FIFO, at their own sequences, so the
  barrier still sends each only after the data written before it. A
  friend lacking some of the bucket's metadata (added since the last
  pass) is room the same way at capture, its metadata queued with the
  copy, so no copy goes to a target for want of it (#38 review). A
  re-placement batch reads the target's `place_use` again in its
  transaction before it places a copy there, so a capture between the
  pass's scan and its batch cannot take it past `max_bytes`.
  *Split re-placement:* a split bucket's pass runs two rounds over its
  data (`dataRound`): the first places missing copies only, the second
  also moves copies, so a move never takes a target's room from an
  object with fewer copies (#38 review). In the second, an object
  whose copies are all at friends gets a copy at a target in place of
  the fullest friend's (that copy is counted out as drained, and the
  new copy records which it replaces), which is deleted there once the
  target acknowledges and only while the object keeps `copies`
  acknowledged copies at intact places (`keepReplaced`: not removed,
  gone or unverified; otherwise the next pass drops it when
  `decide`'s guard allows); the
  friend's room comes back when it purges the tomb after its retention
  window (RET-1), and the objects that had no friend copy get one then
  (`TestSplitPlacementGivesEveryObjectOneFriendAndTheTarget`,
  `TestScenarioOverflow`).
  From then on each metadata put of the bucket goes there too
  (`overflowHolding`: a target with a queued or acknowledged data put
  of the bucket), and each delete of a key it holds or was sent. A
  target counts in `PoolSize`, the pool fingerprint (adding one runs a
  re-placement pass, which places copies that were below `copies`),
  `BelowCopies`, `Replacing` and the offsite holders (#20 review). It
  is never a target of re-routing; as a source it is a friend's like.
  A delete, or a drain, goes to a versioned target at once (a delete
  marker); one for a target without versioning is held as a
  client-side tombstone (#39, *Retention and undelete* below).
- **Hysteresis** (#38 review; realised #40). The split decision flips
  as the repository or the pool changes, and a move in flight is
  cancelled as surplus when it flips back; that is acceptable here,
  since a move only adds a copy before it deletes one. Drain-back,
  which moves copies from targets to friends, does not run on the same
  threshold, or the two would move the same copies back and forth: it
  runs only while the friends hold `copies` copies (`FriendsHold`, with
  their quotas now) of the *largest the repository has been in the
  last 30 days* (`state.DrainWindow`; `overflowDrain`), and never while
  placement is split. The repository is sampled at every look the
  split takes (at most every 30 s, at capture and at each pass) into
  table `repository_peak` (migration 42: per UTC day, the day's
  largest data bytes, metadata bytes and copies, each the day's
  maximum on its own, so `copies` is the largest any bucket had in the
  window; only days after the window's start and up to today are read).
  A clock run ahead and back cannot block drain-back or wipe the
  history (#40 review): a day after today is folded into today's, so
  it ages out with the window, and days past the window are deleted
  except the newest 31, whatever the clock says
  (`TestTheRepositoryPeakSurvivesAClockThatJumps`). After the upgrade
  to migration 42 the table starts empty, so drain-back may run at once
  on the repository as it is, with no window behind it. So the split turns on at once when the repository
  outgrows the friends (REPL-12), and turns off as soon as they hold it
  again, but the copies the split moved stay at the target until the
  friends hold everything the repository was in the last 30 days.
  *Decision (#40): a window in time, not a margin in size.* A size
  margin (drain back only at, say, 110% of `copies` × the repository)
  fails scenario 12 itself: with F at 400 GB the friends hold
  4 × 96 + 376 = 760 GB against 2 × 376 = 752 GB, 101%, and the target
  must empty. A margin small enough for that (1%) is smaller than one
  prune: restic's `forget --prune` routinely frees several percent,
  and the next backups grow it back, which would move every split copy
  back to friends and out again each cycle. What swings a repository
  is that shrink and regrow, so the hysteresis is the regrow's horizon:
  a prune frees no target copy until the repository has stayed small
  for the window, and a growth back to where it was never re-triggers
  a move, since the copies are still at the target. A friend added or a
  quota raised counts at once, since the window is over the
  repository, not the pool. Precedent: Kubernetes' horizontal
  autoscaler scales down only to the largest recommendation of its
  stabilization window (`behavior.scaleDown.stabilizationWindowSeconds`)
  and up at once, for the same flapping. 30 days covers a monthly
  prune schedule, and costs at most a month of the target's fee for
  copies friends could hold. What remains is a repository that grows
  steadily across the threshold after a drain-back (scenario 12's F
  leaves 1% to spare): the split then moves one copy of each object to
  the target again, once, as REPL-12 asks
  (`TestTheSplitAndDrainBackNeverMoveACopyBackAndForth`: grown past the
  threshold, pruned below it and grown again four times, no copy
  leaves the target, and after the window the copies move back and
  stay). A drain-back copy already sent when the split returns lands
  all the same, and the split then moves that object's copy to the
  target once more: one move back, not a cycle.
- **Drain-back** (#40, REPL-13, scenario 12). The re-placement pass
  (§3.4) of a bucket not split, while drain-back runs, takes a last
  round (`dataRound`, after the round that places missing copies, so a
  drain never takes a friend's room from an object below copies): an
  object whose copies are all acknowledged, `copies` of them at intact
  places, one or more at a target, gets a friend copy in place of each
  target copy, by D6's choice among friends only (`choose`,
  `friendsOnly`), in sequence order, which is oldest first; the row
  records the target it replaces (`placement.replaces`). The target's
  copy is marked dropping when that row is acknowledged
  (`AckPlacement`), and only while the object keeps `copies`
  acknowledged copies at intact places without it (`keepReplaced`,
  #38 review; `TestADrainedTargetCopyStaysWhileTheObjectNeedsIt`);
  otherwise a later pass drops it when it may. While drain-back runs,
  a target copy a friend copy replaces is counted out; while it does
  not, that friend copy is a surplus and is cancelled while unsent
  (`TestAnUnsentDrainBackCopyIsCancelledWhenDrainBackTurnsOff`), as a
  split's move is. A surplus copy goes from an overflow target before
  any friend unless placement is split. *Lowest priority:* a queued
  row replacing a target copy (`state.drainBack`, `Placement.DrainBack`)
  comes after every other row of its friend's FIFO, newer captures and
  dropping rows included (`NextPlacement`), and holds no metadata
  barrier (`Held`; `waiting` counts it as holding none), since its
  object keeps `copies` acknowledged copies until it lands and a
  barrier it held would stop the FIFO it waits at the end of
  (`TestDrainBackComesLastInTheFriendsFIFOAndHoldsNoBarrier`). Sends
  and the target's deletes go through the senders, so only inside the
  schedule (REPL-7;
  `TestDrainBackSendsOnlyInsideTheScheduleAndAfterNewerCopies`). Once
  a target holds, has queued and is deleting no copy of a live data
  object of a bucket, its copies of the bucket's metadata are dropped
  too (`dropIdleMetadata`, end of each bucket's pass: acknowledged ones
  marked dropping, unsent ones cancelled), so the target holds nothing
  and `status` shows it empty
  (`TestDrainBackEmptiesTheTargetOnceAFriendJoins`,
  `TestScenarioOverflowDrainsBackToANewFriend`). Every delete at the
  target, drained data and metadata alike, goes the
  retention-preserving way (*Retention and undelete* below). A
  metadata row still dropping when the target is chosen again for a
  data copy is not queued again with it; the pass after its delete
  lands puts it back, ahead of later data in the FIFO. Whether a
  target holds data of a bucket is one condition (`holdsLiveData`: a
  queued, acknowledged or dropping copy of a live data object) for
  sending it the bucket's metadata (`overflowHolding`) and for dropping
  it there, so an old put row of a key deleted since never makes a pass
  place what the next drops (#40 review,
  `TestAnIdleTargetsMetadataIsNotPlacedAndDroppedOnEveryPass`). A copy
  placed again at a place clears the `replaces` of the object's other
  rows naming that place, so a friend copy that once drained the
  target never counts out the target's copy the split placed there
  later (#40 review, `TestAStaleDrainBackPointerDoesNotCountATargetCopyOut`).
- **Alerts and status** (#38, S3-7). `overflow_receiving` while a
  target holds or has queued a live data copy (it costs money);
  `overflow_high` at 90% of `max_bytes` and `overflow_full` at 100%,
  each replacing the other and held like the quota alerts;
  `overflow_unreachable` after the same thresholds as a friend's.
  `status` lists each target in `places` (kind `overflow`, its name as
  `place`), and in `capacity.overflow`: `max_bytes`, use, percent, the
  live data objects and bytes it is one of the copies of, endpoint,
  bucket, prefix, `env_prefix`, what `overflow add` found, and the
  monthly cost estimate (use in GB of 10⁹ bytes times
  `--price-per-gb`, two decimals, no currency) when a price was given;
  never the credential. Since #39 also `retention` (`provider` or
  `client-side`) and, for a client-side target holding deletes,
  `tombstones`, `tombstone_bytes` and `tombstone_next_due`; the text
  line labels such a target "(retention: client-side; N deletes held,
  size)", and `overflow add` warns that its retention is client-side
  and guards against mistakes, not a compromised node. The replicas at
  a target are listed with the friends' (VER-4) and alerted the same
  way (`replica_unverified`, `replica_unchecked`).
- **Testing** (#38). Unit and CLI tests run versitygw's posix backend
  in the test process as the provider (`overflow/overflowtest`; once
  per process, so a CLI test holds the gateway with `serving` and runs
  its nodes as daemon processes). CI's `overflow` job runs scenario
  12's first half (`TestScenarioOverflowAtAnExternalProvider`) against
  a Garage container (`.github/garage.sh`, the image pinned at
  v2.5.0); the test skips without its `LENDISK_TEST_OVERFLOW_*`
  variables. #39 adds `TestScenarioOverflowRetentionAtAnUnversionedProvider`
  to that job, and an `overflow-versioned` job against RustFS
  (`.github/rustfs.sh`, `rustfs/rustfs:1.0.3`, a bucket with
  versioning made through the AWS CLI; `LENDISK_TEST_OVERFLOW_VERSIONED_*`)
  running `TestAVersionedProviderUndeletesByCopyingTheVersionBeforeTheMarker`
  and `TestScenarioOverflowRetentionAtAVersionedProvider`. Locally the
  same scenarios run against versitygw with versioning on and off
  (`TestScenarioOverflowRetentionVersioned`, `…Unversioned`);
  `overflowtest` sets versitygw's `CopyObjectThreshold` to 5 GiB, since
  its default of 0 refuses every copy.
- **S3-4** Buckets without versioning get lendisk-side tombstones: the
  delete is recorded locally and issued after the window; `status`
  labels the target "retention: client-side". Drain-back deletes go
  the same way.
  *Retention and undelete, realised (#39).* A target is client-side
  when `overflow add` found versioning anything but enabled
  (`state.Overflow.ClientSide`; *suspended* counts, as a delete there
  removes the null version). Versioning is read only at `overflow
  add`: enabling it later changes nothing until the target is added
  again (no command re-reads it yet).
  - *Versioned target.* The worker sends the delete (a drain's too) as
    a plain `DeleteObject`: a delete marker, left to the lifecycle
    rule. `undelete` lists the bucket's versions under
    `<prefix>/<sealed name>/` (`Client.ListDeleted`), gathered by key,
    since a provider need not list a key's versions together (RustFS
    1.0.3 lists every delete marker of a page before its versions);
    each key's versions are ordered newest first (at equal times the
    current one, `IsLatest`, then the provider's list order, which S3
    defines as newest first). A key whose newest is a delete marker
    made at or after `since` comes back as the version just before it,
    read once with a HEAD for its `x-amz-meta-lendisk-sha256`; when
    that is another marker the key was deleted already, and when there
    is none the lifecycle rule purged it: nothing comes back (#39
    review). Right before the copy, a key with a put queued or in
    flight at the target (`state.PutInFlight`) is skipped and reported
    kept, so an old version is never copied over a newer one.
    `Client.Restore`
    copies that version onto the key (`CopyObject` with the source
    version id, keeping its metadata; `ComposeObject` with the hash
    as metadata above 5 GiB), which makes it current and leaves the
    marker. A copy is made before it is recorded (`RecordRestored`, as
    a friend's page); one whose record is lost is adopted after the
    run as a friend's is (`Client.ListLive`, `state.AdoptCandidates`,
    then a HEAD of each candidate only).
  - *Unversioned target.* The worker's `send` holds a delete for such
    a target (`state.HoldOverflowDelete`) instead of sending it, and
    acknowledges the row as for a sent delete: table `overflow_tomb`
    (migration 41: place, the placement row held, bucket, key, the
    version's hash where the target acknowledged one, its size with
    the overhead, drained, deleted and due times, claim time). The
    window is the target's `--retention`, from when the delete would
    have been sent. The tomb's bytes stay in the target's use
    (triggers on the table), so `max_bytes` and the cost estimate
    count what the provider still holds. The target's own sender, at
    most once a poll and only while the schedule is open, claims the
    tombs due (`DueOverflowTombs`) and sends each delete, then drops
    the tomb; a failed delete gives back its claim and the rest's. A
    tomb whose key the target holds again (an acknowledged put of the
    key there at or after the tomb's row: written again, or an
    undelete's record) is dropped unsent (`DropSupersededTombs`), so
    a held delete never removes a newer copy. Tombs live in
    `lendisk.db`: a restart neither forgets nor hastens one
    (`TestAClientSideTombstoneIsDueOnlyAfterTheWindowAndSurvivesARestart`,
    `TestAnUnversionedTargetGetsItsDeleteOnlyAfterTheWindow`).
    `undelete` first drops superseded tombs, then takes each key whose
    newest tomb is a delete (not a drain) made at or after `since` and
    still inside its window, and claims every tomb of that key, drained
    and due ones included (`ClaimOverflowTombs`). The newest tomb says
    which version the target holds: a HEAD checks the target still
    holds it, and it is recorded (`RecordRestored`); the tombs that
    record supersedes are dropped and the rest given back, to be sent
    when due. A claim holds the key, not one tomb: the worker sends no
    held delete of a key with a live claim on any of its tombs, and an
    undelete claims no key the worker holds, in one statement each, so
    the two never meet on a key. An undelete renews its claims at each
    page (`RenewOverflowClaims`); a claim older than an hour died with
    its process and is free again (#39 review:
    `TestUndeleteJudgesAKeyByItsNewestHeldDelete`,
    `TestAnUndeleteClaimsEveryTombOfTheKey`,
    `TestUndeleteClaimsADrainedCopysHeldDeleteToo`).
  - *Drained copies.* A copy re-placement drained or moved away is no
    delete of the user's: undelete leaves it at both kinds of target
    (`state.PlaceDeleted`: the target's last row of the key that holds
    or held it must be a delete; a drain's tomb is marked drained),
    whatever order the places are asked in. A friend that restores the
    key first makes the target's record an acknowledgement of the same
    version (`TestUndeleteLeavesACopyDrainedFromAVersionedTarget`).
  - *`--rewrites`* applies to friends only: a target holds the
    versions a rewrite replaced as non-current versions or not at all,
    and none is brought back.
  - *Decision (#39), from precedent.* restic has no notion of provider
    versioning: a prune's `DeleteObject` leaves delete markers, and its
    documentation leaves recovery to the provider's tools and advises
    object lock or an append-only credential. rclone deletes leave
    markers too (`--s3-versions` and `--s3-version-at` list and read
    old versions; B2's default hides rather than deletes), and getting
    a version back is a copy of it (`rclone copy --s3-version-at`),
    not a marker's removal; its client-side retention for providers
    without versioning is `--backup-dir`, which moves each deleted or
    overwritten file aside instead of deleting it. Kopia reads a
    versioned bucket as of a time (`--point-in-time`: the newest
    version before it of each blob) and relies on object lock and the
    lifecycle rule for retention. So lendisk copies the newest version
    before the marker (Kopia's point-in-time choice, rclone's
    mechanism), which needs no version-removal permission (S3-5), and
    holds deletes rather than moving objects aside: a move is a copy
    and a delete at the provider, doubling the bytes for the window,
    where a held delete costs a row.
- **S3-5** Drain-back has to delete at the target, so the recommended
  credential may create delete markers (`s3:DeleteObject` without a
  version id) and read earlier versions (`s3:GetObjectVersion`, for
  undelete, §3.6) but not remove versions (`s3:DeleteObjectVersion`),
  and the lifecycle rule purges non-current versions after the window.
  `overflow add` tests both permissions and reports them. Where a
  provider cannot separate the two (Q-AC), the choice is the user's:
  with no delete permission at all, drained copies stay at the target
  as dead weight until the user removes them, and `status` counts them.
  *Realised (#40).* The overflow client returns a 403 `AccessDenied`
  to a delete as `replica.ErrDeleteDenied`. The worker then keeps the
  copy as a tomb marked denied (`state.DenyOverflowDelete`;
  `overflow_tomb.denied_at`, migration 42), acknowledges the row as
  sent, and goes on with the target's FIFO, which a delete retried for
  ever would stop; a client-side tomb whose held delete is refused is
  marked the same way. A denied tomb stays in the target's use and is
  due again after a day (`state.DeniedRetry`), so a permission granted
  later empties the target; a deleted user object kept so is kept as
  well. `status` shows them per target as `delete_denied` and
  `delete_denied_bytes`, outside `tombstones`, and the text line says
  "N copies kept, size: its credential may not delete them (tried
  again daily)"
  (`TestDrainedCopiesTheTargetMayNotDeleteStayAndAreCounted`,
  `TestARefusedHeldDeleteIsKeptCountedAndTriedAgainDaily`,
  `TestAnOverflowDeleteRefusedForPermissionIsDeleteDenied`,
  `TestStatusCountsCopiesTheTargetMayNotDelete`).
- **S3-6** Stored checksums are read with `StatObject` (the ETag,
  where it is an MD5 of a single-part object) and compared where the
  type matches; sample downloads (VER-2) are the check everywhere.
  *Verification, realised (#39).* A target's replicas are due daily
  with the friends' (`DueReplicas` and `ReplicaChecks` read friends and
  targets alike), and `verify.Verifier.Check` sends a target's to
  `checkTarget`: up to `DefaultSamples` random acknowledged objects of
  the replica are read with a HEAD in checksum mode
  (`Client.Stat`). The object must be there with its size; its
  `x-amz-meta-lendisk-sha256`, where present, must be the index's;
  the provider's stored `x-amz-checksum-sha256` of the whole object,
  where it returns one (single-part objects sent with it, S3-3; a
  composite multipart checksum is not compared), must be the index's;
  failing that, an ETag of 32 hex digits without a part count
  (`ObjectStat.MD5ETag`) must be the MD5 of the local copy, read and
  checked against its capture hash first (gone or changed: skipped, as
  for a challenge). Any other ETag is not compared, and neither
  checksum is for a multipart object or a composite checksum: a false
  mismatch would drain the whole replica
  (`TestAMultipartObjectsChecksumsAreNotCompared`). The weekly sample
  (VER-2, `sampleStep`) owes `replication.overflow_sample` percent
  (default 0.1, above 0 up to 100) of the bytes each target holds a
  week, stepped every 10 minutes like the bit-rot sweep, its step time
  and debt kept in table `overflow_sample` (migration 41), and
  downloads random acknowledged objects whole through `Pace.Fetch`
  (REPL-7's global `download_limit`; `peer_limit` names friends only,
  so a target has no limit of its own) until it has paid, hashing each
  against the index. A replica already marked unverified is not
  sampled, and a ranged pass of one leaves its last-verified time as it
  was (`RecordVerified`; friends' too), so it never looks freshly
  verified until a full check clears the mark
  (`TestAnUnverifiedTargetIsNotSampledNorRefreshed`). `lendisk verify <bucket> --peer overflow:<name>
  [--full]` runs the daily check, or with `--full` downloads every
  object of the replica (VER-3). Outcomes are a friend's: a missing
  object, other bytes or a mismatched checksum, read again with
  `StillChallengeable` first, marks the replica unverified, which
  alerts (`replica_unverified`) and drains its copies (REPL-13); an
  error decides nothing. *Decision:* the daily stored-checksum check
  is added to VER-2's sample because 0.1% a week downloads a small
  target's objects months apart, which `replica_unchecked` (3 days)
  would report as unknown; eight HEADs a day per replica cost
  nothing at any provider (`TestATargetIsCheckedDailyByTheProvidersStoredChecksums`,
  `TestTheSampleDownloadsAShareOfTheTargetsBytesAWeek`,
  `TestASampleThatDoesNotMatchTheIndexAlerts`,
  `TestAFullCheckOfATargetDownloadsEveryObject`).

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

    Σⱼ min(D, qⱼ − m) + Σₖ min(D, Oₖ − m) ≥ c × D

and the protectable size (LEND-7) is the largest `S` that satisfies it
(each term floored at 0: a friend too small for the metadata holds no
data, it does not take room from the others; §3.5 *Realised (#37)*).
Below, `m` is left out where the example has margin to spare, and
counted where a fit is close. Placement leaves a little more free at
each friend than `m`: the metadata reserve of §3.4, about `3m` more. The
formula does not count it; the alert at 90% of the protectable size
comes first, and the numbers `status` names to cover a shortfall do
count it, so setting them is enough for placement too. A friend
 has no use for more of your disk than its own repository
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
   GB. An overflow target at `max_bytes` 450 GB adds min(376, 446): 384 +
   376 = 760 ≥ 2 × 376, so every data object can have one copy at a
   friend and the other at a friend or the target (D6's split placement,
   Q-AD). At about $6 per TB-month that is
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
  else, and no supported tool sends them; ACLs fail there anyway
  (`BucketOwnerEnforced`), and tags set by a tool live on the local copy
  only. Object lock is the exception: `posix`'s
  `PutObjectLockConfiguration` turns lock on for an existing bucket
  even with versioning off, after which retention and legal hold would
  bind objects against lendisk's own deletes. So the gate refuses, for
  every account, a `PUT` with `?object-lock`, `?retention` or
  `?legal-hold` and any request with an `X-Amz-Object-Lock-*` header,
  as it refuses `CreateBucket` asking for lock; lock is never on
  (`TestObjectLockRefusedOnOwnBuckets`). `RestoreObject` and `SelectObjectContent` are refused for
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
    gate refuses that header. (#15 review: `PutObjectLockConfiguration`
    turns lock on for an existing bucket too, with versioning off, so the
    gate refuses it, the retention and legal-hold writes and every
    `X-Amz-Object-Lock-*` header, for every account;
    `TestObjectLockRefusedOnOwnBuckets`.)
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
     as none of them (since #92 a `503 SlowDown` is `ErrBusy`), through a real tailcat tunnel with minio-go
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
- **Q-AD (resolved, #38)** D6's choice and scenario 12. Friends of
  equal quotas fill at the same rate, each data object going to
  `copies` of them while they have room, so once they are full every
  later object finds no friend with room and gets a single copy, at the
  overflow target. *Decision (the user, REQUIREMENTS v0.11.5):* while
  the capacity plan says the friends cannot hold `copies` copies of the
  repository, each data object goes to `copies − 1` friends and one
  overflow target, at capture and in re-placement (D6 step 3, §3.11).
  An object already at `copies` friends moves one copy to the target;
  the friend's room returns after its retention window.
- **Q-AC (gate, open)** Drain-back deletes at overflow targets (§3.11,
  S3-5). Check per tier-1 provider (B2, Wasabi, Hetzner, R2) whether a
  credential can allow delete markers but not version removal, and
  whether the lifecycle rule then purges the drained copies after the
  window. Where it cannot, the user chooses between a credential that
  can remove versions and drained copies left in place. Check also that
  undelete's server-side copy of an earlier version onto the same key
  (§3.6) works with that credential at each provider. *#39:* the copy
  works at RustFS 1.0.3 and versitygw with the root credential (CI's
  `overflow-versioned` job); the per-provider credential checks stay
  open.
