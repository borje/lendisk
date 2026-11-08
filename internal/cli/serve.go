package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/recovery"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
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
			tun, err := a.startTunnel(ctx, cfg, id, db, log, push.Kick)
			if err != nil {
				return err
			}
			defer tun.Close()
			push.Dial = tun.Dial
			if a.tuneManifest != nil {
				a.tuneManifest(push)
			}
			schema, _, _ := db.Versions(ctx)
			log.Info("lendisk started", "version", version.Version, "node_id", nodeID,
				"role", cfg.Role, "config", cfg.Path, "data_dir", cfg.DataDir, "schema", schema)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			errc := make(chan error, 7) // gateway, LAN, loopback, replication, alerts, manifest, recovery
			// The read-through (EP-10, ID-6) fetches what the store lacks
			// from the friends holding it, over the tunnel.
			fetch := &replica.Fetcher{DB: db, Self: id, Dial: tun.Dial}
			if a.tuneFetch != nil {
				a.tuneFetch(fetch)
			}
			// Alerts (OPS-2, OPS-3) read state and deliver on goroutines
			// of their own: nothing else waits for them. Made before the
			// gateway, whose captures may hold back a changed key (EP-11).
			alerts := &alert.Service{DB: db, Log: log, Node: nodeID, Command: cfg.Alerts.Command, Webhook: cfg.Alerts.Webhook,
				UnreachableAfter: cfg.UnreachableAfter(), Now: a.clock}
			if a.tuneAlerts != nil {
				a.tuneAlerts(alerts)
			}
			db.KeyChanged = func(c state.ChangedKey) {
				log.Error("immutability alert: a key was written with other bytes than a friend acknowledged; "+
					"the local write stands but is not replicated", "alert", true, "bucket", c.Bucket, "key", c.Key,
					"seq", c.Seq, "acknowledged_sha256", c.Acknowledged, "sha256", c.SHA256)
				alerts.Check()
			}
			gw := &gateway.Gateway{StoreDir: cfg.StorePath(), Socket: gateway.SocketPath(cfg.DataDir), DB: db, Self: id, Log: log,
				Profiles: profiles, AfterStore: a.afterStore, BeforeHash: a.beforeHash, Clock: a.clock, Floor: &floor, Statfs: a.checker().Statfs,
				Fetch: fetch}
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
			br := bridge.New(gw.Socket, log)
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
				w := &replica.Worker{DB: db, StoreDir: cfg.StorePath(), Self: id, Dial: tun.Dial, Log: log,
					Alerted: alerts.Check, Now: a.clock}
				if a.tuneReplication != nil {
					a.tuneReplication(w)
				}
				running++
				go func() { w.Run(ctx); errc <- nil }()
			}
			running++
			go func() { alerts.Run(ctx); errc <- nil }()
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
