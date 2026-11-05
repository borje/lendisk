# Spike: pin tailcat, prove fail-closed admission (issue #3)

Throwaway. Settles DESIGN.md Q-Q and Q-R, 2026-11-05.

Pinned: `github.com/tailscale/tailcat v0.7.1-0.20261105151022-e00cdde6ff78`
(`main` at e00cdde, 2026-11-05), which pulls in
`tailscale.com v1.105.0-pre.0.20261013225543-fb79793d99f5`. Needs Go
1.27.2 (`GOTOOLCHAIN=auto` fetches it). Dependencies are pinned by go.sum; `go mod vendor` vendors them locally.

## Run

    go test -v .        # two nodes in one process, local DERP, ~25 s
    TCSPIKE_LONG=1 go test -v -run InboundDuration .   # ~5 min
    ./demo.sh local                 # two processes, local DERP+STUN on 127.0.0.1
    ./demo.sh real                  # two processes over tailcat.dev's DERP relays

`go build -tags portmapper` links Tailscale's port mapper (Q-R).

`tcspike` subcommands: `derp` (local relay), `init` (key plus a saved
region), `serve`, `dial`, `pubkey`, `decode`. `serve` admits the keys
listed in `<state>/allowed`, re-read on every call and rejecting on any
error; `-deny-all` rejects everything; `-nil-hook` shows the guard.

Both modes were run: the tests and `demo.sh local` against a relay on
127.0.0.1, `demo.sh real` against region 303 (fra) of tailcat.dev's map.

## Answers

- **API.** As DESIGN.md says: `Server.AllowClient func(key.NodePublic)
  bool`, `KeySet` (`Add`, `Remove`, `Contains`), `Server.PeerKey(net.Addr)
  (key.NodePublic, bool)` and `Server.DisconnectClient(key.NodePublic) bool`
  are on `main` (commits aa8950c 2026-09-25 and 7d80847 2026-09-26),
  in no tag. v0.7.0 has `AllowedClients []key.NodePublic` (empty admits
  all) and `AddAllowedClient`, no remove, no peer key. On `main`
  `AllowedClients` is gone, not deprecated.
- **Fail closed.** A deny-all hook is asked about once a second while
  the client retries; the client never gets the meow acknowledgement,
  never becomes a WireGuard peer, and its dial times out. The listener
  accepts no connection, so no application byte arrives
  (`TestDenyAllRefusesBeforeAnyByte`, `demo.sh`). A nil hook makes
  `StartServer` refuse to start (`ErrNilHook`).
- **Address.** With `DisablePresharedKey` and a saved `Region`, the
  address is identical across restarts. It decodes to the node public
  key, the disco public key (an HMAC of the node private key, so fixed
  and public) and the region's nodes (host name, IPv4, IPv6, ports).
  No pre-shared key, nothing secret. With tailcat's defaults the
  address carries a new pre-shared key on every start. Setting only
  `RegionID` is not enough: the server then embeds the region's nodes
  as fetched from the DERP map at that start, so the address changes
  when the map does. lendisk must save the whole `tailcfg.DERPRegion`.
- **Peer key.** `PeerKey(conn.RemoteAddr())` on an accepted connection
  returns the client's key (`TestPeerKeyOfAcceptedConn`, `demo.sh`); a
  stranger's address returns ok=false.
- **Disconnect.** `DisconnectClient` resets nothing: neither end sees
  EOF or RST, and closing the server's end afterwards does not reach
  the client (`TestDisconnectDoesNotResetConns`). Closing first and
  then disconnecting gives the client EOF at once
  (`TestCloseThenDisconnect`), the order SEC-15 uses. Worse than the
  tailcat doc says ("stops routing its traffic in either direction"):
  only the server-to-client direction stops. The WireGuard session
  stays in the engine (`Status` still lists the peer), so the client's
  packets are still decrypted and delivered, to open TCP and UDP
  connections and as new UDP flows, for about 8 s after
  the call (`TestDisconnectInboundStillDelivered`,
  `TestDisconnectInboundDuration`). `PeerKey` returns ok=false for
  them. A new TCP connection does not complete. lendisk is safe only
  because it serves no UDP, closes a connection with no peer key
  before reading (SEC-12), checks the relationship on every request
  (SEC-11) and closes the key's connections itself (SEC-15).
- **Q-R, port mapping.** Not run. tailcat links no port mapper:
  magicsock creates one only when `portmappertype.HookNewPortMapper`
  is set (`tailscale.com/wgengine/magicsock/magicsock.go:681-697`),
  and only `tailscale.com/feature/portmapper` sets it
  (`feature/portmapper/portmapper.go:17-22`). Nothing in tailcat
  imports that package or `feature/condregister/portmapper` (tsnet
  does, tailcat does not use tsnet); `go list -deps` shows only
  `net/portmapper/portmappertype`. tailcat's own test comment says the
  library and released binary omit it (`tailcat_test.go:46-49` at e00cdde), and
  release builds also pass `ts_omit_portmapper` (`build-tags.txt`).
  At run time netcheck logs `portmap=?` (never probed). Built with
  `-tags portmapper`, which adds `import _
  "tailscale.com/feature/portmapper"`, it logs `portmap=` (UPnP, PMP and
  PCP probed, none found on this LAN): tailcat sets no
  `DisablePortMapper`, so one blank import in lendisk turns it on,
  for every engine in the process.
- **Q-E.** tailcat's two-minute idle timeout applies only to incoming
  UDP flows (`DefaultUDPIdleTimeout`, wrapped in `newIdlePacketConn`).
  lendisk's tunnel port is TCP, which tailcat gives no idle timeout.
- **Q-S.** Confirmed in source: each `Client` builds its own event bus,
  netmon, userspace engine and netstack (`Client.initLocked`). With the
  port mapper linked, each would also run its own.
