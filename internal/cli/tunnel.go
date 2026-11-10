package cli

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// relays is the node's relay set (NET-5): where the tunnel region is
// chosen from, and the relays it dials friends through (NET-5a). Fetched
// maps are kept in st, if given. Tests set app.regions to a local relay.
func (a *app) relays(cfg *config.Loaded, st transport.NodeStore) transport.RegionSource {
	if a.regions != nil {
		return a.regions
	}
	set := &transport.RelaySet{MapURL: cfg.Relays.MapURL, Bootstrap: cfg.Relays.Bootstrap()}
	if st != nil {
		set.Cache = transport.StoreMapCache{Store: st}
	}
	for _, e := range cfg.Relays.List {
		set.List = append(set.List, transport.RelayServer{Host: e.Host, Port: e.Port, STUNPort: e.STUNPort, Cert: e.Cert})
	}
	return set
}

// dialPolicy makes tc dial friends only through the node's relays,
// unless relays.accept_peer_hints (NET-5a), and dial a friend's
// explicit addresses directly (NET-3).
func (a *app) dialPolicy(tc *transport.Tailcat, cfg *config.Loaded, db *state.DB, log *slog.Logger) {
	tc.SetRelays(a.relays(cfg, db), cfg.Relays.AcceptPeerHints.On())
	tc.SetEndpoints(func(peer identity.NodeID) []netip.AddrPort {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		f, err := db.GetFriendship(ctx, peer.String())
		if err != nil {
			return nil
		}
		var eps []netip.AddrPort
		for _, s := range f.ExplicitAddresses {
			if ap, err := netip.ParseAddrPort(s); err == nil {
				eps = append(eps, ap)
			} else {
				log.Warn("ignoring a friend's explicit address that is not ip:port", "peer", f.Peer, "address", s)
			}
		}
		return eps
	})
}

// tunnel is the running peer transport.
type tunnel struct {
	transport.Transport
	// Admission is the admission function; call Clear after changing a
	// friendship or an invite (SEC-15).
	Admission *transport.Admission
	// S3 is the tunnel's S3 handler, set once the gateway serves; until
	// then S3 on the tunnel answers 503.
	S3 *bridge.Late
	// Conns are the tunnel's open connections by key, which ending a
	// friendship cuts (SEC-15).
	Conns *bridge.Conns
	// Manifests are the friends' manifests this node keeps (ID-4).
	Manifests *manifest.Store
	// Undeleter answers the undelete call (RET-3) once Set to the
	// gateway; until then it asks the friend to retry.
	Undeleter *lateUndeleter
}

// startTunnel chooses the region if none is saved yet, starts the
// transport with admission over db and serves the tunnel port until ctx
// ends: the bridge's tunnel middleware in front of the control API
// (§3.7) and, once tun.S3 is set, S3 for friends. changed, if set, is
// called after the control API commits a friendship (the manifest
// pusher's Kick, ID-4); ended, if set, after a friend's remove call
// ended one (the farewell's Kick, #25); reported, if set, after a
// friend's quota call applied its number (the quota exchanger, #24).
func (a *app) startTunnel(ctx context.Context, cfg *config.Loaded, id *identity.Identity, db *state.DB, log *slog.Logger,
	changed, ended func(), reported func(peer string)) (*tunnel, error) {
	region, chosen, err := transport.EnsureRegion(ctx, db, cfg.Tunnel.Region, a.relays(cfg, db))
	if err != nil {
		return nil, err
	}
	if chosen {
		log.Info("tunnel region chosen and saved; the address follows it", "region", region.RegionCode, "region_id", int(region.RegionID))
	}
	tc, err := transport.NewTailcat(id, region, log)
	if err != nil {
		return nil, err
	}
	a.dialPolicy(tc, cfg, db, log)
	tc.SetPort(uint16(cfg.Tunnel.Port.Int()))
	adm := transport.NewAdmission(func(ctx context.Context, key identity.NodeID) (bool, error) {
		_, r, err := db.TunnelPeer(ctx, key.String(), a.now())
		return r != state.RoleNone, err
	}, log)
	admit := adm.Admit
	if admitAll {
		log.Warn("TEST BUILD (admitall): the tunnel admits every key; only the middleware refuses")
		admit = func(identity.NodeID) bool { return true }
	}
	tc.Admit(admit)
	ln, err := tc.Listen(ctx)
	if err != nil {
		tc.Close()
		return nil, err
	}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	// Friends' manifests live in the store, not lendisk.db (ID-4); a Put
	// that crashed may have left a temporary file.
	manifests := &manifest.Store{Dir: manifest.Dir(cfg.StorePath())}
	if err := manifests.Sweep(a.now()); err != nil {
		log.Warn("sweeping leftover manifest files failed", "dir", manifests.Dir, "err", err)
	}
	// The list and challenge calls read what this node holds for each
	// friend from the store (NET-7 list, VER-1): a second posix view of
	// it, read only, the challenge through Lending.
	be, err := gateway.OpenBackend(cfg.StorePath())
	if err != nil {
		tc.Close()
		return nil, err
	}
	go func() {
		<-ctx.Done()
		be.Shutdown()
	}()
	lent := &gateway.LentIndex{Store: be, DB: db}
	undeleter := &lateUndeleter{}
	ctl := &control.Server{DB: db, Self: id, Log: log, Lent: lent, Held: lent, Undeleter: undeleter,
		Changed: func() {
			adm.Clear()
			if changed != nil {
				changed()
			}
		},
		Ended: ended, Reported: reported, Manifests: manifests}
	if a.tuneControl != nil {
		a.tuneControl(ctl)
	}
	s3 := &bridge.Late{}
	caps := &bridge.Caps{Of: bridge.RolesIn(db, a.now)}
	conns := &bridge.Conns{}
	go bridge.ServeTunnel(ctx, ln, tc.PeerOf, caps, conns, bridge.TunnelHandler(bridge.RolesIn(db, a.now), ctl, s3, caps, log), log)
	go purgeInvites(ctx, db, adm, a.now, log)
	go (&pathKeeper{Tr: tc, DB: db, Now: a.now, Log: log}).run(ctx)
	log.Info("tunnel listening", "address", tc.Address(), "port", transport.Port, "region", region.RegionCode)
	return &tunnel{Transport: tc, Admission: adm, S3: s3, Conns: conns, Manifests: manifests, Undeleter: undeleter}, nil
}

// lateUndeleter is the control API's undelete (RET-3) through the
// gateway, which serve makes after the tunnel listens: until Set, the
// call is answered as a node still starting.
type lateUndeleter struct {
	gw atomic.Pointer[gateway.Gateway]
}

// Set makes g answer the undelete call.
func (u *lateUndeleter) Set(g *gateway.Gateway) { u.gw.Store(g) }

func (u *lateUndeleter) Undelete(ctx context.Context, borrower identity.NodeID, sealed string, since time.Time,
	rewrites bool, after string, limit int) (gateway.Undeleted, error) {
	g := u.gw.Load()
	if g == nil {
		return gateway.Undeleted{}, gateway.ErrNotServing
	}
	return g.Undelete(ctx, borrower, sealed, since, rewrites, after, limit)
}

// purgeInvites deletes expired invites at start and every minute after
// (SEC-3, SEC-6), and clears admission when it deleted any (SEC-15).
func purgeInvites(ctx context.Context, db *state.DB, adm *transport.Admission, now func() time.Time, log *slog.Logger) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		n, err := db.PurgeInvites(ctx, now())
		if err != nil && ctx.Err() == nil {
			log.Warn("purging expired invites failed", "err", err)
		}
		if n > 0 {
			adm.Clear()
			log.Info("purged expired invites", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// chooseRegion saves a tunnel region at init. Failing is not fatal:
// serve chooses one when the relays are reachable.
func (a *app) chooseRegion(ctx context.Context, cfg *config.Loaded, db *state.DB, log *slog.Logger) {
	if _, _, err := transport.EnsureRegion(ctx, db, cfg.Tunnel.Region, a.relays(cfg, db)); err != nil {
		log.Warn("no tunnel region yet, so no address; `lendisk serve` chooses one when the relays are reachable", "err", err)
	}
}

// nodeAddress is the node's address, computed from the key and the
// region saved in lendisk.db, without the daemon. It is empty when no
// region is saved yet or the database cannot be read.
func nodeAddress(ctx context.Context, cfg *config.Loaded, id *identity.Identity, log *slog.Logger) string {
	if _, err := os.Stat(filepath.Join(cfg.DataDir, state.FileName)); errors.Is(err, fs.ErrNotExist) {
		return ""
	}
	db, err := state.Open(ctx, cfg.DataDir, log)
	if err != nil {
		log.Warn("cannot read the state database for the address", "err", err)
		return ""
	}
	defer db.Close()
	region, err := transport.SavedRegion(ctx, db)
	if err != nil {
		log.Warn("cannot read the saved tunnel region", "err", err)
		return ""
	}
	if region == nil {
		return ""
	}
	return transport.AddressOf(id, region)
}
