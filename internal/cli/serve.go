package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/quota"
	"github.com/borje/lendisk/internal/recovery"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/sheet"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/undelete"
	"github.com/borje/lendisk/internal/verify"
	"github.com/borje/lendisk/internal/version"
)

func (a *app) serveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the lendisk daemon",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// The daemon logs to stdout (OPS-5).
			cfg, log, err := a.load(a.out)
			if err != nil {
				return err
			}
			id, err := identity.Load(cfg.ConfigDir())
			if err != nil {
				return err
			}
			nodeID := id.NodeID().String()
			if cfg.Alerts.Command != "" {
				// Refused now, not at the first alert (OPS-3).
				if err := alert.CheckCommand(cfg.Alerts.Command); err != nil {
					return err
				}
			}

			ctx := a.ctx
			if ctx == nil {
				var stop context.CancelFunc
				ctx, stop = signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
				defer stop()
			}
			db, err := state.Open(ctx, cfg.DataDir, log)
			if err != nil {
				return err
			}
			defer db.Close()
			db.Clock = a.clock
			db.RerouteAfter, db.ReplaceAfter = cfg.RerouteAfter(), cfg.ReplaceAfter()
			if prev, ok, err := db.GetNode(ctx, "id"); err != nil {
				return err
			} else if ok && prev != nodeID {
				return fmt.Errorf("%s belongs to node %s, but the identity key is %s", db.Path, prev, nodeID)
			} else if !ok {
				if err := db.SetNode(ctx, "id", nodeID); err != nil {
					return err
				}
			}
			floor, _ := config.ParseFloor(cfg.Store.FreeSpaceFloor)
			if err := a.checker().Check(cfg.StorePath(), floor); err != nil {
				return err
			}
			profiles, err := profile.Load(filepath.Join(cfg.ConfigDir(), "profiles"))
			if err != nil {
				return err
			}
			// The node manifest (ID-4), pushed to every friend; in both
			// profiles, since a lender-only node is rebuilt from it too.
			push := &manifest.Pusher{DB: db, Self: id, Now: a.clock, Random: a.random, Log: log}
			// Ending friendships (PEER-5, SEC-15, #25): revoked, told and
			// purged by the farewell, kicked by a friend's remove call.
			fw := &farewell{DB: db, Log: log, Now: a.clock}
			// The quota call (LEND-8, #24): tells each friend this node's
			// number, kicked by a friend's own call.
			xch := &quota.Exchanger{DB: db, Now: a.clock, Log: log}
			tun, err := a.startTunnel(ctx, cfg, id, db, log, func() { push.Kick(); xch.Kick() }, fw.Kick, func(string) { xch.Kick() })
			if err != nil {
				return err
			}
			defer tun.Close()
			push.Dial = tun.Dial
			xch.Dial = tun.Dial
			xch.Path = func(ctx context.Context, peer string) string {
				pid, err := identity.ParseNodeID(peer)
				if err != nil {
					return ""
				}
				pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				p, err := tun.PathInfo(pctx, pid)
				switch {
				case err != nil:
					return ""
				case p.Direct:
					return "direct"
				}
				return "relayed"
			}
			if a.tuneManifest != nil {
				a.tuneManifest(push)
			}
			schema, _, _ := db.Versions(ctx)
			log.Info("lendisk started", "version", version.Version, "node_id", nodeID,
				"role", cfg.Role, "config", cfg.Path, "data_dir", cfg.DataDir, "schema", schema)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			errc := make(chan error, 12) // gateway, LAN, loopback, replication, verification, undelete, farewell, alerts, quota, manifest, recovery, reload
			// Pacing (REPL-6, REPL-7, LEND-5): one pacer for every transfer
			// over the tunnel, changed without a restart by the reloader.
			pacer := pace.New(a.clock)
			pacer.Set(paceSettings(&cfg.Config))
			// The read-through (EP-10, ID-6) fetches what the store lacks
			// from the friends holding it, over the tunnel.
			fetch := &replica.Fetcher{DB: db, Self: id, Dial: tun.Dial, Pace: pacer}
			if a.tuneFetch != nil {
				a.tuneFetch(fetch)
			}
			// Alerts (OPS-2, OPS-3) read state and deliver on goroutines
			// of their own: nothing else waits for them. Made before the
			// gateway, whose captures may hold back a changed key (EP-11).
			alerts := &alert.Service{DB: db, Log: log, Node: nodeID, Command: cfg.Alerts.Command, Webhook: cfg.Alerts.Webhook,
				UnreachableAfter: cfg.UnreachableAfter(), BelowAfter: cfg.ReplaceAfter(), Now: a.clock,
				// The printed recovery sheet against the node's state (ID-7).
				Sheet: func(ctx context.Context) (time.Time, []string, error) { return sheet.Stale(ctx, db, id) }}
			if a.tuneAlerts != nil {
				a.tuneAlerts(alerts)
			}
			db.KeyChanged = func(c state.ChangedKey) {
				log.Error("immutability alert: a key was written with other bytes than a friend acknowledged; "+
					"the local write stands but is not replicated", "alert", true, "bucket", c.Bucket, "key", c.Key,
					"seq", c.Seq, "acknowledged_sha256", c.Acknowledged, "sha256", c.SHA256)
				alerts.Check()
			}
			// The store reconciler (EP-14): at start, every
			// store.reconcile_every, and for deletes the user allowed.
			rec := &gateway.ReconcileSettings{Every: cfg.ReconcileEvery(), DeleteLimit: cfg.Store.ReconcileDeleteLimit.Int(), Pace: pacer.Scrub,
				Alerted: alerts.Check}
			if a.tuneReconcile != nil {
				a.tuneReconcile(rec)
			}
			// The bit-rot sample (VER-5): a share of the store each week,
			// own objects and friends' alike, its reads paced.
			sample := &gateway.SampleSettings{Percent: cfg.BitrotSample(), Pace: pacer.Scrub, Random: a.random,
				Alerted: alerts.Check}
			if a.tuneSample != nil {
				a.tuneSample(sample)
			}
			gw := &gateway.Gateway{StoreDir: cfg.StorePath(), Socket: gateway.SocketPath(cfg.DataDir), DB: db, Self: id, Log: log,
				Profiles: profiles, AfterStore: a.afterStore, BeforeHash: a.beforeHash, Clock: a.clock, Floor: &floor, Statfs: a.checker().Statfs,
				Fetch: fetch, Reconcile: rec, Sample: sample}
			tun.Undeleter.Set(gw) // the undelete call (RET-3)
			ready := make(chan struct{})
			go func() { errc <- gw.Run(ctx, ready) }()
			select {
			case <-ready:
			case err := <-errc:
				if err == nil {
					log.Info("lendisk stopped")
				}
				return err
			}

			running := 1
			var w *replica.Worker
			br := bridge.New(gw.Socket, log)
			br.Pace = pacer // LEND-5
			if a.tuneBridge != nil {
				a.tuneBridge(br)
			}
			tun.S3.Set(br.TunnelS3Handler()) // S3 for friends (NET-7, SEC-12)
			// LAN listeners (EP-17); config load refuses them for a lender.
			if len(cfg.Endpoint.Listen) > 0 {
				lan := &bridge.LAN{Entries: cfg.Endpoint.Listen, ConfigDir: cfg.ConfigDir(), Handler: br.LANHandler(), Log: log,
					Clock: a.clock, Random: a.random}
				lanReady := make(chan struct{})
				running++
				go func() { errc <- lan.Run(ctx, lanReady) }()
				select {
				case <-lanReady:
				case err := <-errc:
					cancel()
					<-errc // the gateway
					return err
				}
			}
			// The lender-only profile has nothing on loopback (PEER-9)
			// and no buckets to replicate.
			if cfg.Role == "full" {
				ln, err := net.Listen("tcp", cfg.Endpoint.Loopback)
				if err != nil {
					// Stop what already runs before the deferred
					// db.Close, which must not race the gateway (#53).
					cancel()
					for i := 0; i < running; i++ {
						<-errc
					}
					return fmt.Errorf("S3 endpoint: %w", err)
				}
				log.Info("S3 endpoint listening", "address", ln.Addr().String())
				if a.serving != nil {
					a.serving(ln.Addr().String())
				}
				running++
				go func() { errc <- bridge.Serve(ctx, ln, br.LoopbackHandler(), log) }()

				// Replication to friends (#18) never blocks the endpoint
				// (REPL-9): it reads the store and lendisk.db only.
				w = &replica.Worker{DB: db, StoreDir: cfg.StorePath(), Self: id, Dial: tun.Dial, Log: log,
					Alerted: alerts.Check, Now: a.clock, Full: xch.Due, Fill: gw.Fill, Pace: pacer}
				if a.tuneReplication != nil {
					a.tuneReplication(w)
				}
				running++
				go func() { w.Run(ctx); errc <- nil }()

				// Verification (VER-1, VER-3, #27): the daily challenge of
				// each friend's replica, and the checks `lendisk verify`
				// asks for through lendisk.db. A failure marks the replica
				// unverified, which the worker's next pass re-places.
				ver := &verify.Verifier{DB: db, StoreDir: cfg.StorePath(), Self: id, Dial: tun.Dial, Now: a.clock, Random: a.random,
					Alerted: alerts.Check, Log: log}
				if a.tuneVerify != nil {
					a.tuneVerify(ver)
				}
				running++
				go func() { ver.Run(ctx); errc <- nil }()

				// Undelete (RET-3, #32): the previews and undeletes
				// `lendisk undelete` asks for through lendisk.db, run here
				// so only the daemon dials with this node's keys.
				und := &undelete.Runner{DB: db, Self: id, Dial: tun.Dial, Profiles: profiles, Now: a.clock, Log: log}
				if a.tuneUndelete != nil {
					a.tuneUndelete(und)
				}
				running++
				go func() { und.Run(ctx); errc <- nil }()
			}
			fw.Clear, fw.Cut, fw.Disconnect = tun.Admission.Clear, tun.Conns.Cut, tun.Disconnect
			fw.Tell = func(ctx context.Context, f state.FormerFriend) error {
				peer, err := identity.ParseNodeID(f.Peer)
				if err != nil {
					return err
				}
				hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true,
					DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return tun.Dial(ctx, peer, f.Address) }}}
				defer hc.CloseIdleConnections()
				return (&control.Client{HTTP: hc}).Remove(ctx)
			}
			fw.Purge = func(ctx context.Context, peer string) error {
				pid, err := identity.ParseNodeID(peer)
				if err != nil {
					return err
				}
				if err := gw.PurgeFormer(ctx, peer); err != nil {
					return err
				}
				return tun.Manifests.Delete(pid)
			}
			fw.Alerted = alerts.Check
			if a.tuneEnding != nil {
				a.tuneEnding(fw)
			}
			running++
			go func() { fw.Run(ctx); errc <- nil }()
			running++
			go func() { alerts.Run(ctx); errc <- nil }()
			// A changed quota applies at once (PEER-6, SEC-15, LEND-8): a
			// lowering to the friend's writes in flight here, a raising to
			// this node's copies paused for quota there.
			xch.Changed = func(peer string, before, after int64) {
				if after < before || before < 0 { // lowered, or first seen
					gw.Requota(peer)
				}
				if after > before && before >= 0 {
					if err := db.ResumePlace(ctx, peer, a.now()); err != nil && ctx.Err() == nil {
						log.Error("quota: resuming the friend's copies failed", "peer", peer, "err", err)
					}
					if w != nil {
						w.Resume(peer)
					}
				}
				alerts.Check()
			}
			if a.tuneQuota != nil {
				a.tuneQuota(xch)
			}
			running++
			go func() { xch.Run(ctx); errc <- nil }()
			running++
			go func() { push.Run(ctx); errc <- nil }()
			// A recovered node lists each place it could not reach at
			// recovery when it answers again (ID-5); a node with nothing
			// left to list only waits.
			lister := &recovery.Lister{DB: db, Self: id, Dial: tun.Dial, Profiles: profiles, Now: a.clock, Log: log}
			if a.tuneRecovery != nil {
				a.tuneRecovery(lister)
			}
			running++
			go func() { lister.Run(ctx); errc <- nil }()
			// Pacing changes apply without a restart: the file is looked
			// at every 10 s, and at once on SIGHUP (§3.4 REPL-7).
			rl := &reloader{Load: a.flags.Load, Path: cfg.Path, Pace: pacer, Log: log, Every: a.reloadEvery, started: cfg.Config}
			if a.ctx == nil {
				hup := make(chan os.Signal, 1)
				signal.Notify(hup, syscall.SIGHUP)
				defer signal.Stop(hup)
				rl.Hup = hup
			}
			running++
			go func() { rl.Run(ctx); errc <- nil }()

			var first error
			for i := 0; i < running; i++ {
				err := <-errc
				if first == nil && err != nil && ctx.Err() == nil {
					first = err
				}
				cancel() // one part stopping stops the daemon
			}
			log.Info("lendisk stopped")
			return first
		},
	}
}
