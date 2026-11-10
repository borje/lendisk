package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/relay"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// relayCmd is `lendisk relay` (NET-5): a self-hosted relay on a VPS.
func (a *app) relayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "relay",
		Short: "Run a relay for friends who cannot connect directly",
		Long: `relay runs a DERP relay and a STUN server, as Tailscale's derper does,
on a host friends' nodes can reach, such as a small VPS (NET-5). It needs
no lendisk identity and stores nothing but its certificate. It logs one
relays.list entry; every node that should use it adds that entry to its
[[relays.list]] (or LENDISK_RELAYS_LIST), and nodes that list it as their
only relay depend on no relay run by anyone else.

Set relay.hostname to the DNS name or IP address nodes reach this host
by. With relay.tls = "self-signed" (the default) the certificate is made
once, kept in <data_dir>/relay, and pinned by the entry's cert field;
with "letsencrypt", relay.hostname must be a DNS name pointing here and
relay.listen on port 443. The relay forwards packets it cannot read, but
it sees the IP addresses of the nodes it relays and how much they send.

Without relay.allow it relays for any node that finds it, as derper
does. Set relay.allow to the node IDs of your group, which
` + "`lendisk relay keys`" + ` prints on any friend's node, and it forwards only
packets to or from one of them; relay.rate_limit caps what each
connected key may send. A node it drops packets for is logged by node
ID, so a friend missing from the list shows in the log.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, log, err := a.load(a.out) // a daemon: logs on stdout
			if err != nil {
				return err
			}
			allow, err := cfg.Relay.AllowList()
			if err != nil {
				return err // Load validated it
			}
			c := relay.Config{Hostname: cfg.Relay.Hostname, Listen: cfg.Relay.Listen, STUNPort: cfg.Relay.STUNPort.Int(),
				TLS: cfg.Relay.TLS, CertFile: cfg.Relay.Cert, KeyFile: cfg.Relay.Key, Dir: filepath.Join(cfg.DataDir, "relay"),
				Allow: allow, RateLimit: cfg.Relay.RateLimitBytes(), Log: log}
			if err := c.Check(); err != nil {
				return usageError{err}
			}
			if err := ensureDir(cfg.DataDir); err != nil {
				return err
			}
			ctx := a.ctx
			if ctx == nil {
				var stop context.CancelFunc
				ctx, stop = signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
				defer stop()
			}
			return relay.Run(ctx, c, func(e relay.Entry) {
				if a.serving != nil {
					a.serving(e.String())
				}
			})
		},
	}
	cmd.AddCommand(a.relayKeysCmd())
	return cmd
}

// relayKeysCmd is `lendisk relay keys` (#99): what a relay's
// relay.allow needs for this node and its friends.
func (a *app) relayKeysCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "keys",
		Short: "Print the node IDs a relay's relay.allow needs for this node and its friends",
		Long: `relay keys prints this node's ID and those of its friends, one per line,
for the relay.allow setting of a relay your group runs with ` + "`lendisk relay`" + `.
Paste them into the relay's lendisk.toml, as one string with a node ID
per line, and restart the relay:

  [relay]
  allow = """
  <the lines relay keys printed>
  """

A relay with relay.allow forwards only packets to or from a listed node.
Every node whose address names the relay must be listed; a friend that
only dials such a node needs no entry, nor do the other keys a node
dials with. Run it on each friend's node whose friends use the relay,
and merge the lists.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, db, err := a.openState(cmd.Context())
			if err != nil {
				return err
			}
			defer db.Close()
			id, err := identity.Load(cfg.ConfigDir())
			if err != nil {
				return err
			}
			fs, err := db.Friendships(cmd.Context())
			if err != nil {
				return err
			}
			fmt.Fprintln(a.out, id.NodeID())
			for _, f := range fs {
				fmt.Fprintln(a.out, f.Peer)
			}
			return nil
		},
	}
}

// peerAddressCmd is `lendisk peer address` (NET-3).
func (a *app) peerAddressCmd() *cobra.Command {
	var clear bool
	cmd := &cobra.Command{
		Use:   "address <friend> [host:port ...]",
		Short: "Give a friend's static address, dialled directly",
		Long: `peer address sets the UDP addresses a friend's node is reached at
directly (NET-3): its static public IP, or a router's port forward to
the port the friend sets as tunnel.port, as ip:port, or as name:port
with a DNS name, such as a dynamic DNS name, which is looked up again
at each dial. They replace the
ones given before; --clear removes them. Each dial of the friend tries
them at once, beside hole punching and the relay, and the first that
answers carries the tunnel; a running daemon uses them from its next
dial. The relay is still needed once per connection to start it, so an
explicit address does not replace a relay.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runPeerAddress(cmd.Context(), args[0], args[1:], clear)
		},
	}
	cmd.Flags().BoolVar(&clear, "clear", false, "remove the friend's explicit addresses")
	return cmd
}

func (a *app) runPeerAddress(ctx context.Context, s string, addrs []string, clear bool) error {
	peer, err := identity.ParseNodeID(strings.TrimSpace(s))
	if err != nil {
		return usageError{fmt.Errorf("%q is not a node ID: %w; `lendisk peer list` shows your friends", s, err)}
	}
	switch {
	case clear && len(addrs) > 0:
		return usageError{errors.New("give addresses or --clear, not both")}
	case !clear && len(addrs) == 0:
		return usageError{errors.New("give one or more ip:port addresses, or --clear to remove them")}
	}
	var keep []string
	for _, s := range addrs {
		k, err := transport.ParseExplicitAddress(s)
		if err != nil {
			return usageError{err}
		}
		keep = append(keep, k)
	}
	_, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.SetExplicitAddresses(ctx, peer.String(), keep); errors.Is(err, state.ErrNotFound) {
		return fmt.Errorf("%s is not a friend of this node; `lendisk peer list` shows your friends", peer)
	} else if err != nil {
		return err
	}
	if clear {
		fmt.Fprintf(a.out, "%s has no explicit address now; it is reached by hole punching and the relay.\n", peer)
	} else {
		fmt.Fprintf(a.out, "%s is dialled directly at %s, beside hole punching and the relay.\n", peer, strings.Join(keep, ", "))
	}
	return nil
}

// The path keeper's timings (NET-4): a relayed friend is disco-pinged
// every pathRelayedEvery, each ping also trying for a direct path; a
// direct one every pathDirectEvery, so status stays true.
const (
	pathTick         = 10 * time.Second
	pathRelayedEvery = 30 * time.Second
	pathDirectEvery  = 5 * time.Minute
	pathProbeTimeout = 5 * time.Second
)

// pathKeeper probes the path to every friend this node has dialled and
// records it in lendisk.db for status and peer list (NET-4): direct or
// relayed, reported by a disco ping, whose answer names an endpoint only
// when it came over a direct path. A friend not dialled is not probed:
// the probe would build a client for it.
type pathKeeper struct {
	Tr  transport.Transport
	DB  *state.DB
	Now func() time.Time
	Log *slog.Logger
	// Tick, RelayedEvery and DirectEvery are pathTick,
	// pathRelayedEvery and pathDirectEvery when zero; tests set them.
	Tick, RelayedEvery, DirectEvery time.Duration
}

func (k *pathKeeper) run(ctx context.Context) {
	tick := cmp.Or(k.Tick, pathTick)
	relayedEvery, directEvery := cmp.Or(k.RelayedEvery, pathRelayedEvery), cmp.Or(k.DirectEvery, pathDirectEvery)
	type seen struct {
		at   time.Time // monotonic: when it was last probed
		path string
	}
	last := map[string]seen{}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		fs, err := k.DB.Friendships(ctx)
		if err != nil && ctx.Err() == nil {
			k.Log.Warn("reading friendships to probe their paths failed", "err", err)
		}
		for _, f := range fs {
			if ctx.Err() != nil {
				return
			}
			s, ok := last[f.Peer]
			every := relayedEvery
			if s.path == "direct" {
				every = directEvery
			}
			if ok && time.Since(s.at) < every {
				continue
			}
			id, err := identity.ParseNodeID(f.Peer)
			if err != nil {
				continue
			}
			pctx, cancel := context.WithTimeout(ctx, pathProbeTimeout)
			p, err := k.Tr.PathInfo(pctx, id)
			cancel()
			if err != nil {
				// Not dialled yet, or no answer: the last record stands,
				// and the friend is probed again after a relayed one's
				// wait, not at every tick (#42 review L4).
				last[f.Peer] = seen{at: time.Now(), path: ""}
				continue
			}
			path := "relayed"
			if p.Direct {
				path = "direct"
			}
			if s.path != path {
				k.Log.Info("path to friend", "peer", f.Peer, "path", path, "relay", p.Relay, "endpoint", p.Endpoint, "latency", p.Latency.Round(time.Millisecond))
			}
			last[f.Peer] = seen{at: time.Now(), path: path}
			if err := k.DB.RecordPath(ctx, f.Peer, p.Direct, p.Relay, p.Endpoint, k.Now()); err != nil && ctx.Err() == nil {
				k.Log.Warn("recording a friend's path failed", "peer", f.Peer, "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
