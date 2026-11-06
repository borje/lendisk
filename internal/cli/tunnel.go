package cli

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// relays is where the tunnel region is chosen from; tests set
// app.regions to a local relay.
func (a *app) relays() transport.RegionSource {
	if a.regions == nil {
		return transport.PublicRelays
	}
	return a.regions
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
}

// startTunnel chooses the region if none is saved yet, starts the
// transport with admission over db and serves the tunnel port until ctx
// ends: the bridge's tunnel middleware in front of the control API
// (§3.7) and, once tun.S3 is set, S3 for friends.
func (a *app) startTunnel(ctx context.Context, cfg *config.Loaded, id *identity.Identity, db *state.DB, log *slog.Logger) (*tunnel, error) {
	region, chosen, err := transport.EnsureRegion(ctx, db, cfg.Tunnel.Region, a.relays())
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
	adm := transport.NewAdmission(func(ctx context.Context, peer identity.NodeID) (bool, error) {
		r, err := db.PeerRole(ctx, peer.String(), a.now())
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
	roles := func(ctx context.Context, peer identity.NodeID) (state.Role, error) {
		return db.PeerRole(ctx, peer.String(), a.now())
	}
	ctl := &control.Server{DB: db, Self: id, Changed: adm.Clear, Log: log}
	s3 := &bridge.Late{}
	go bridge.ServeTunnel(ctx, ln, tc.PeerOf, bridge.TunnelHandler(roles, ctl, s3, log), log)
	go purgeInvites(ctx, db, adm, a.now, log)
	log.Info("tunnel listening", "address", tc.Address(), "port", transport.Port, "region", region.RegionCode)
	return &tunnel{Transport: tc, Admission: adm, S3: s3}, nil
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
	if _, _, err := transport.EnsureRegion(ctx, db, cfg.Tunnel.Region, a.relays()); err != nil {
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
