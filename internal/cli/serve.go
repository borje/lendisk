package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
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
			tun, err := a.startTunnel(ctx, cfg, id, db, log)
			if err != nil {
				return err
			}
			defer tun.Close()
			schema, _, _ := db.Versions(ctx)
			log.Info("lendisk started", "version", version.Version, "node_id", nodeID,
				"role", cfg.Role, "config", cfg.Path, "data_dir", cfg.DataDir, "schema", schema)

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			errc := make(chan error, 3)
			gw := &gateway.Gateway{StoreDir: cfg.StorePath(), Socket: gateway.SocketPath(cfg.DataDir), DB: db, Log: log}
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
			// LAN listeners (EP-17); config load refuses them for a lender.
			if len(cfg.Endpoint.Listen) > 0 {
				lan := &bridge.LAN{Entries: cfg.Endpoint.Listen, ConfigDir: cfg.ConfigDir(), Handler: br.LANHandler(), Log: log}
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
			// The lender-only profile has nothing on loopback (PEER-9).
			if cfg.Role == "full" {
				ln, err := net.Listen("tcp", cfg.Endpoint.Loopback)
				if err != nil {
					return fmt.Errorf("S3 endpoint: %w", err)
				}
				log.Info("S3 endpoint listening", "address", ln.Addr().String())
				if a.serving != nil {
					a.serving(ln.Addr().String())
				}
				running++
				go func() { errc <- bridge.Serve(ctx, ln, br.LoopbackHandler(), log) }()
			}

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
