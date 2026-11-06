package cli

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/invite"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

func (a *app) now() time.Time {
	if a.clock == nil {
		return time.Now()
	}
	return a.clock()
}

func (a *app) rng() io.Reader {
	if a.random == nil {
		return rand.Reader
	}
	return a.random
}

func (a *app) redeemTimeout() time.Duration {
	if a.dialTimeout == 0 {
		return time.Minute
	}
	return a.dialTimeout
}

func (a *app) inviteCmd() *cobra.Command {
	var quota, retention, expires string
	cmd := &cobra.Command{
		Use:   "invite --quota <size>",
		Short: "Print a single-use invite for a friend",
		Long: `invite prints an invite string for one friend (PEER-1). It carries this
node's address, the space you lend them (--quota), how long you keep what
they delete (--retention) and a single-use secret, valid for --expires.
Your friend runs ` + "`lendisk peer add <invite>`" + ` while this node runs
` + "`lendisk serve`" + `. Treat the string as a secret until it is used.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runInvite(cmd.Context(), quota, retention, expires)
		},
	}
	cmd.Flags().StringVar(&quota, "quota", "", "space you lend this friend, such as 100G (required)")
	cmd.Flags().StringVar(&retention, "retention", "30d", "how long you keep what they delete")
	cmd.Flags().StringVar(&expires, "expires", "7d", "how long the invite stays valid")
	return cmd
}

func (a *app) runInvite(ctx context.Context, quota, retention, expires string) error {
	number, err := parseNumber(quota)
	if err != nil {
		return usageError{fmt.Errorf("--quota: %w", err)}
	}
	ret, err := parseDays(retention)
	if err != nil {
		return usageError{fmt.Errorf("--retention: %w", err)}
	}
	exp, err := parseDays(expires)
	if err != nil {
		return usageError{fmt.Errorf("--expires: %w", err)}
	}
	cfg, _, db, id, err := a.openNode(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	addr, err := a.ownAddress(ctx, cfg, db, id)
	if err != nil {
		return err
	}
	now := a.now()
	if _, err := db.PurgeInvites(ctx, now); err != nil {
		return err
	}
	inv, err := invite.New(a.rng(), addr, number, ret)
	if err != nil {
		return err
	}
	key, err := inv.Key()
	if err != nil {
		return err
	}
	until := now.Add(exp)
	if err := db.AddInvite(ctx, state.Invite{Key: key.NodeID().String(), Number: number, Retention: inv.Retention, ExpiresAt: until}); err != nil {
		return err
	}
	fmt.Fprintln(a.out, inv.String())
	fmt.Fprintf(a.errOut, "Single-use invite, valid until %s. Your friend runs `lendisk peer add <invite>`\n"+
		"while `lendisk serve` runs here. Treat it as a secret until it is used.\n", until.Format(time.RFC3339))
	return nil
}

func (a *app) peerCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "peer", Short: "Manage friendships"}
	var retention string
	add := &cobra.Command{
		Use:   "add <invite>",
		Short: "Redeem a friend's invite and become friends",
		Long: `peer add shows what the inviting friend lends you and asks for your own
number, what you lend them; press Enter for the same. The quota of the
friendship, each way, is the smaller of the two (LEND-8).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runPeerAdd(cmd.Context(), args[0], retention)
		},
	}
	add.Flags().StringVar(&retention, "retention", "30d", "how long you keep what they delete")
	var asJSON bool
	list := &cobra.Command{
		Use:   "list",
		Short: "List friends",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runPeerList(cmd.Context(), asJSON)
		},
	}
	list.Flags().BoolVar(&asJSON, "json", false, "print JSON")
	cmd.AddCommand(add, list)
	return cmd
}

func (a *app) runPeerAdd(ctx context.Context, s, retention string) error {
	inv, err := invite.Parse(s)
	if err != nil {
		return usageError{err}
	}
	ret, err := parseDays(retention)
	if err != nil {
		return usageError{fmt.Errorf("--retention: %w", err)}
	}
	them, err := transport.KeyOf(inv.Address)
	if err != nil {
		return usageError{fmt.Errorf("the invite's address is damaged: %w", err)}
	}
	cfg, log, db, id, err := a.openNode(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	if them == id.NodeID() {
		return errors.New("this invite is your own; give it to a friend")
	}
	if _, err := db.GetFriendship(ctx, them.String()); err == nil {
		return fmt.Errorf("already friends with %s", them)
	} else if !errors.Is(err, state.ErrNotFound) {
		return err
	}
	addr, err := a.ownAddress(ctx, cfg, db, id)
	if err != nil {
		return err
	}

	ik, err := inv.Key()
	if err != nil {
		return err
	}
	ikID := ik.NodeID().String()

	fmt.Fprintf(a.out, "Invite from node %s.\n", them)
	fmt.Fprintf(a.out, "They lend you %s and keep what you delete for %s.\n", formatSize(inv.Number), formatDays(inv.Retention))
	// A redeem with them started before, from any invite, means they
	// may hold the friendship already (#56).
	earlier, err := db.PendingRedeemsWith(ctx, them.String())
	if err != nil {
		return err
	}
	for _, p := range earlier {
		if p.InviteKey != ikID {
			fmt.Fprintf(a.out, "An unfinished redeem with this node, started %s from another invite (invite key %s), is settled by this one.\n",
				p.StartedAt.UTC().Format(time.RFC3339), p.InviteKey)
		}
	}
	// A redeem started before repeats its terms (#51): the inviting
	// node may have stored them already.
	number := inv.Number
	pending, err := db.PendingRedeemOf(ctx, ikID)
	switch {
	case err == nil:
		number, ret = pending.Number, pending.Retention
		fmt.Fprintf(a.out, "Resuming the redeem started %s: you lend them %s and keep what they delete for %s.\n",
			pending.StartedAt.UTC().Format(time.RFC3339), formatSize(number), formatDays(ret))
	case errors.Is(err, state.ErrNotFound):
		fmt.Fprintf(a.out, "What you lend them [%s]: ", formatSize(inv.Number))
		line, err := a.readLine()
		if err != nil {
			return fmt.Errorf("no number given; nothing was done: %w", err)
		}
		if line != "" {
			if number, err = parseNumber(line); err != nil {
				return usageError{err}
			}
		}
		err = db.StartRedeem(ctx, state.PendingRedeem{InviteKey: ikID, Peer: them.String(),
			Number: number, Retention: ret, StartedAt: a.now()})
		if errors.Is(err, state.ErrExists) {
			return errors.New("a redeem of this invite is already in progress (another `lendisk peer add` started it while this one asked); let it finish, then check `lendisk peer list`")
		} else if err != nil {
			return err
		}
	default:
		return err
	}

	req := control.RedeemRequest{Address: addr, Number: number, RetentionSeconds: int64(ret / time.Second)}
	resp, timedOut, err := a.redeem(ctx, log, ik, id, them, inv.Address, req)
	var ce *control.CallError
	switch {
	case err != nil && len(earlier) > 0 && (timedOut || errors.As(err, &ce) && ce.Status < http.StatusInternalServerError):
		// An earlier redeem with them may have been stored there while
		// this invite can no longer be redeemed, its key no longer
		// admitted once it expired (#56). Its record is kept: the redeem
		// of a new invite settles it.
		why := "it did not answer"
		if ce != nil {
			why = "it refused: " + ce.Message
		}
		return fmt.Errorf("this invite has expired or node %s cannot be reached (%s). An earlier redeem with that node, started %s, did not finish, "+
			"so it may already count you as a friend. Ask them for a new invite and run `lendisk peer add` with it: that settles this one. If they were only offline and the invite is still valid, running the same `lendisk peer add` again also works",
			them, why, earlier[0].StartedAt.UTC().Format(time.RFC3339))
	case errors.As(err, &ce):
		if ce.Status < http.StatusInternalServerError {
			// Refused for good: a re-run starts afresh.
			if err := db.AbandonRedeem(ctx, ikID); err != nil {
				log.Warn("dropping the refused redeem failed", "err", err)
			}
		}
		return fmt.Errorf("node %s refused the invite: %s", them, ce.Message)
	case err != nil && timedOut:
		return fmt.Errorf("cannot reach node %s: it is offline or not running `lendisk serve`, or the invite was already used or has expired; run the same `lendisk peer add` again to retry", them)
	case err != nil:
		return fmt.Errorf("redeeming the invite at node %s: %w; run the same `lendisk peer add` again to finish", them, err)
	}
	if a.beforeFriendship != nil {
		if err := a.beforeFriendship(); err != nil {
			return err
		}
	}
	f := state.Friendship{
		Peer: them.String(), Address: inv.Address,
		OwnNumber: number, PeerNumber: resp.Number,
		OwnRetention: ret, PeerRetention: time.Duration(resp.RetentionSeconds) * time.Second,
		CreatedAt: a.now(),
	}
	if err := db.FinishRedeem(ctx, ikID, f); err != nil {
		return fmt.Errorf("node %s stored the friendship but this node could not: %w; run the same `lendisk peer add` again to finish", them, err)
	}
	if resp.Number != inv.Number || f.PeerRetention != inv.Retention {
		fmt.Fprintf(a.out, "They already held this friendship: they lend you %s and keep what you delete for %s.\n",
			formatSize(resp.Number), formatDays(f.PeerRetention))
	}
	fmt.Fprintf(a.out, "Now friends with %s. Quota: %s each way.\n", them, formatSize(f.Quota()))
	return nil
}

// redeem calls redeem at them, dialling with the invite key; the proof
// by self binds that key. timedOut reports that the call ran out of
// time.
func (a *app) redeem(ctx context.Context, log *slog.Logger, key, self *identity.Identity, them identity.NodeID,
	address string, req control.RedeemRequest) (resp control.RedeemResponse, timedOut bool, err error) {
	dl, err := transport.NewDialer(key, log)
	if err != nil {
		return resp, false, err
	}
	defer dl.Close()
	if req.Proof, err = control.RedeemProof(self, them, key.NodeID()); err != nil {
		return resp, false, err
	}
	cctx, cancel := context.WithTimeout(ctx, a.redeemTimeout())
	defer cancel()
	var rt http.RoundTripper = &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dl.Dial(ctx, them, address)
		},
	}
	if a.redeemTransport != nil {
		rt = a.redeemTransport(rt)
	}
	cl := &control.Client{HTTP: &http.Client{Transport: rt}}
	resp, err = cl.Redeem(cctx, req)
	return resp, err != nil && cctx.Err() != nil, err
}

// PeerJSON is one friend in `peer list --json`.
type PeerJSON struct {
	ID                   string    `json:"id"`
	Address              string    `json:"address"`
	Quota                int64     `json:"quota"`
	OwnNumber            int64     `json:"own_number"`
	PeerNumber           int64     `json:"peer_number"`
	OwnRetentionSeconds  int64     `json:"own_retention_seconds"`
	PeerRetentionSeconds int64     `json:"peer_retention_seconds"`
	Since                time.Time `json:"since"`
}

func (a *app) runPeerList(ctx context.Context, asJSON bool) error {
	_, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	fs, err := db.Friendships(ctx)
	if err != nil {
		return err
	}
	if asJSON {
		out := []PeerJSON{}
		for _, f := range fs {
			out = append(out, PeerJSON{ID: f.Peer, Address: f.Address, Quota: f.Quota(),
				OwnNumber: f.OwnNumber, PeerNumber: f.PeerNumber,
				OwnRetentionSeconds:  int64(f.OwnRetention / time.Second),
				PeerRetentionSeconds: int64(f.PeerRetention / time.Second),
				Since:                f.CreatedAt.UTC()})
		}
		enc := json.NewEncoder(a.out)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	if len(fs) == 0 {
		fmt.Fprintln(a.out, "No friends yet; `lendisk invite --quota 100G` makes an invite.")
		return nil
	}
	tw := tabwriter.NewWriter(a.out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "FRIEND\tQUOTA\tYOU LEND\tTHEY LEND\tYOU KEEP\tTHEY KEEP\tSINCE")
	for _, f := range fs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", f.Peer, formatSize(f.Quota()),
			formatSize(f.OwnNumber), formatSize(f.PeerNumber),
			formatDays(f.OwnRetention), formatDays(f.PeerRetention), f.CreatedAt.UTC().Format(time.DateOnly))
	}
	return tw.Flush()
}

// openNode is openState plus the identity.
func (a *app) openNode(ctx context.Context) (*config.Loaded, *slog.Logger, *state.DB, *identity.Identity, error) {
	cfg, log, err := a.load(a.errOut)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	id, err := identity.Load(cfg.ConfigDir())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	db, err := state.Open(ctx, cfg.DataDir, log)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return cfg, log, db, id, nil
}

// ownAddress is this node's address, choosing and saving the region if
// init could not.
func (a *app) ownAddress(ctx context.Context, cfg *config.Loaded, db *state.DB, id *identity.Identity) (string, error) {
	region, _, err := transport.EnsureRegion(ctx, db, cfg.Tunnel.Region, a.relays())
	if err != nil {
		return "", fmt.Errorf("this node has no address yet, since no relay is reachable; try again online: %w", err)
	}
	return transport.AddressOf(id, region), nil
}

// parseNumber reads a friendship number: a positive size.
func parseNumber(s string) (int64, error) {
	if strings.TrimSpace(s) == "" {
		return 0, errors.New("give a size such as 100G")
	}
	n, err := config.ParseSize(s)
	if err != nil {
		return 0, err
	}
	if n == 0 || n > 1<<62 {
		return 0, fmt.Errorf("%q: want a size above zero, such as 100G", s)
	}
	return int64(n), nil
}

// parseDays reads "30d" or a Go duration such as "36h"; it must be
// positive.
func parseDays(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	var d time.Duration
	if n, ok := strings.CutSuffix(s, "d"); ok {
		v, err := strconv.ParseUint(n, 10, 16)
		if err != nil {
			return 0, fmt.Errorf("%q: want days such as 30d, or hours such as 36h", s)
		}
		d = time.Duration(v) * 24 * time.Hour
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, fmt.Errorf("%q: want days such as 30d, or hours such as 36h", s)
		}
	}
	if d < time.Second {
		return 0, fmt.Errorf("%q: want at least a second", s)
	}
	return d, nil
}

// formatSize writes bytes in the largest unit that divides them, as
// ParseSize reads them.
func formatSize(n int64) string {
	for _, u := range []struct {
		s string
		b int64
	}{{"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}} {
		if n >= u.b && n%u.b == 0 {
			return strconv.FormatInt(n/u.b, 10) + u.s
		}
	}
	return strconv.FormatInt(n, 10)
}

func formatDays(d time.Duration) string {
	if d > 0 && d%(24*time.Hour) == 0 {
		return strconv.FormatInt(int64(d/(24*time.Hour)), 10) + "d"
	}
	return d.String()
}
