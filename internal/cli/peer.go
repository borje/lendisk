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
	"path/filepath"
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
	max, from, err := a.maxTotal(cfg)
	if err != nil {
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
	if err := db.AddInviteWithin(ctx, state.Invite{Key: key.NodeID().String(), Number: number, Retention: inv.Retention, ExpiresAt: until},
		max, now); err != nil {
		return overMaxTotal(err, max, from)
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
	remove := &cobra.Command{
		Use:   "remove <friend>",
		Short: "End a friendship",
		Long: `peer remove ends the friendship with a friend, named by its node ID
(PEER-5). Both of its keys are refused from that moment; a running
daemon closes its open connections within seconds and tells it, and it
ends the friendship on its side too. Its copies of your data no longer
count. What it stores here is kept, refused to it, until the retention
window ends, then deleted; it does the same with what it holds of yours.
Becoming friends again before then, with a new invite, keeps both.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runPeerRemove(cmd.Context(), args[0])
		},
	}
	cmd.AddCommand(add, list, remove, a.peerQuotaCmd())
	return cmd
}

// runPeerRemove ends the friendship with s (PEER-5, #25). It commits the
// removal and returns; the daemon, from its next pass, cuts the friend's
// connections and tells it (farewell).
func (a *app) runPeerRemove(ctx context.Context, s string) error {
	peer, err := identity.ParseNodeID(strings.TrimSpace(s))
	if err != nil {
		return usageError{fmt.Errorf("%q is not a node ID: %w; `lendisk peer list` shows your friends", s, err)}
	}
	_, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	f, err := db.RemoveFriendship(ctx, peer.String(), false, a.now())
	if errors.Is(err, state.ErrNotFound) {
		return fmt.Errorf("%s is not a friend of this node; `lendisk peer list` shows your friends", peer)
	} else if err != nil {
		return err
	}
	below, err := db.BelowCopies(ctx)
	if err != nil {
		return err
	}
	var objects int64
	for _, b := range below {
		objects += b.Objects
	}
	fmt.Fprintf(a.out, "Ended the friendship with %s. Both of its keys are refused from now on;\n"+
		"a running `lendisk serve` closes its open connections within seconds and tells it.\n", peer)
	if objects > 0 {
		fmt.Fprintf(a.out, "Its copies of your data no longer count: %d objects are below their copies until\n"+
			"re-placement (not built yet, #26); `lendisk status` shows them.\n", objects)
	}
	fmt.Fprintf(a.out, "What it stores here is kept, refused to it, until %s, then deleted;\n"+
		"it deletes what it holds of yours then. A new invite before then keeps both.\n",
		f.PurgeAfter.UTC().Format(time.RFC3339))
	return nil
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
	// One peer add per invite at a time: two dialling with the invite
	// key at one relay stall each other (#52, #64).
	release, err := tryLock(cfg.ConfigDir(), "redeem-"+ikID+".lock")
	if errors.Is(err, errLocked) {
		return errors.New(redeemInProgress)
	} else if err != nil {
		return fmt.Errorf("cannot take the redeem lock in %s: %w; run `lendisk peer add` as the user that owns that directory", filepath.Join(cfg.ConfigDir(), "locks"), err)
	}
	defer release()

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
		max, from, err := a.maxTotal(cfg)
		if err != nil {
			return err
		}
		err = db.StartRedeemWithin(ctx, state.PendingRedeem{InviteKey: ikID, Peer: them.String(),
			Number: number, Retention: ret, StartedAt: a.now()}, max, a.now())
		if errors.Is(err, state.ErrExists) {
			return errors.New(redeemInProgress)
		} else if err != nil {
			return overMaxTotal(err, max, from)
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
	theirDial, err := resp.InviterDialKey(id, them, ik.NodeID())
	if err != nil {
		return fmt.Errorf("node %s stored the friendship but its answer is not usable: %w; run the same `lendisk peer add` again to finish", them, err)
	}
	if a.beforeFriendship != nil {
		if err := a.beforeFriendship(); err != nil {
			return err
		}
	}
	f := state.Friendship{
		Peer: them.String(), Address: inv.Address, DialKey: theirDial.String(),
		OwnNumber: number, PeerNumber: resp.Number,
		OwnRetention: ret, PeerRetention: time.Duration(resp.RetentionSeconds) * time.Second,
		CreatedAt: a.now(),
	}
	if err := db.FinishRedeem(ctx, ikID, f); errors.Is(err, state.ErrDialKeyTaken) {
		// A retry would get the same answer: only a hostile or broken
		// inviter answers with another friend's key (#69).
		return fmt.Errorf("node %s answered with a dial key that already names another friend here, so its friendship was not stored: %w; do not retry this invite, and ask the inviter about it", them, err)
	} else if err != nil {
		return fmt.Errorf("node %s stored the friendship but this node could not: %w; run the same `lendisk peer add` again to finish", them, err)
	}
	if resp.Number != inv.Number || f.PeerRetention != inv.Retention {
		fmt.Fprintf(a.out, "They already held this friendship: they lend you %s and keep what you delete for %s.\n",
			formatSize(resp.Number), formatDays(f.PeerRetention))
	}
	fmt.Fprintf(a.out, "Now friends with %s. Quota: %s each way.\n", them, formatSize(f.Quota()))
	return nil
}

// redeemInProgress is peer add's answer when another peer add of the same
// invite is running.
const redeemInProgress = "a redeem of this invite is already in progress (another `lendisk peer add` of it is running); let it finish, then check `lendisk peer list`"

// redeem calls redeem at them, dialling with the invite key; the proof
// by self binds that key and self's dial key (#69). timedOut reports that the call ran out of
// time.
func (a *app) redeem(ctx context.Context, log *slog.Logger, key, self *identity.Identity, them identity.NodeID,
	address string, req control.RedeemRequest) (resp control.RedeemResponse, timedOut bool, err error) {
	dl, err := transport.NewDialer(key, log)
	if err != nil {
		return resp, false, err
	}
	defer dl.Close()
	if err := req.Prove(self, them, key.NodeID()); err != nil {
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

// PeerJSON is one friend in `peer list --json` (PEER-4). Fields are
// added, never renamed or removed.
type PeerJSON struct {
	ID                   string    `json:"id"`
	Address              string    `json:"address"`
	Quota                int64     `json:"quota"`
	OwnNumber            int64     `json:"own_number"`
	PeerNumber           int64     `json:"peer_number"`
	OwnRetentionSeconds  int64     `json:"own_retention_seconds"`
	PeerRetentionSeconds int64     `json:"peer_retention_seconds"`
	Since                time.Time `json:"since"`
	// UseThere is what this node stores at the friend, as it counts it
	// (reconciled from the friend's own count by the quota call, #68);
	// Remaining is the quota less that (LEND-3). UseHere is what the
	// friend stores here, as this node counts it as lender (LEND-1).
	UseThere  int64 `json:"use_there"`
	UseHere   int64 `json:"use_here"`
	Remaining int64 `json:"remaining"`
	// NumberTold is false while this node's number is not yet told to
	// the friend (the quota call, LEND-8).
	NumberTold bool `json:"number_told"`
	// Reachability is direct, relayed or offline, from the last quota
	// call; unknown before the first, online if the path is not known.
	Reachability string `json:"reachability"`
	// LastSeen is when the friend last called or answered (RFC 3339);
	// LastVerified when its replica last passed verification (VER-1,
	// #27: never yet). Both absent when never.
	LastSeen     *time.Time `json:"last_seen,omitempty"`
	LastVerified *time.Time `json:"last_verified,omitempty"`
	// Protocol is the friend's protocol version (NET-6), "" if unknown.
	Protocol string `json:"protocol,omitempty"`
	// RepositorySize is the friend's repository size as it reported it,
	// rounded up to a GiB (LEND-7); absent before its first report.
	RepositorySize *int64 `json:"repository_size,omitempty"`
}

func (a *app) runPeerList(ctx context.Context, asJSON bool) error {
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	fs, err := db.Friendships(ctx)
	if err != nil {
		return err
	}
	use, err := db.PlaceUse(ctx)
	if err != nil {
		return err
	}
	out := []PeerJSON{}
	for _, f := range fs {
		here, _, err := db.LentUsage(ctx, f.Peer)
		if err != nil {
			return err
		}
		there := max(use[f.Peer], 0)
		p := PeerJSON{ID: f.Peer, Address: f.Address, Quota: f.Quota(),
			OwnNumber: f.OwnNumber, PeerNumber: f.PeerNumber,
			OwnRetentionSeconds:  int64(f.OwnRetention / time.Second),
			PeerRetentionSeconds: int64(f.PeerRetention / time.Second),
			Since:                f.CreatedAt.UTC(),
			UseThere:             there, UseHere: here, Remaining: max(f.Quota()-there, 0),
			NumberTold: f.ToldNumber == f.OwnNumber, Reachability: reachability(f), Protocol: f.PeerProtocol}
		if !f.SeenAt.IsZero() {
			seen := f.SeenAt.UTC()
			p.LastSeen = &seen
			size := f.PeerRepositorySize
			p.RepositorySize = &size
		}
		out = append(out, p)
	}
	if asJSON {
		enc := json.NewEncoder(a.out)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}
	if len(fs) == 0 {
		fmt.Fprintln(a.out, "No friends yet; `lendisk invite --quota 100G` makes an invite.")
		return nil
	}
	if l, err := a.lendingStatus(ctx, cfg, db); err == nil {
		fmt.Fprintf(a.out, "This node lends at most %s (%s); its numbers and open invites take %s.\n",
			humanSize(l.MaxTotal), l.From, humanSize(l.Numbers))
	} else {
		fmt.Fprintf(a.out, "This node's lending cap is unknown: %v\n", err)
	}
	tw := tabwriter.NewWriter(a.out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "FRIEND\tQUOTA\tNUMBERS\tYOU USE THERE\tTHEY USE HERE\tREMAINING\tREACH\tLAST SEEN\tVERIFIED\tPROTOCOL\tTHEIR REPO\tYOU KEEP\tTHEY KEEP\tSINCE")
	for _, p := range out {
		numbers := "same"
		if p.OwnNumber != p.PeerNumber {
			numbers = "you " + formatSize(p.OwnNumber) + ", they " + formatSize(p.PeerNumber)
		}
		if !p.NumberTold {
			numbers += " (yours not told yet)"
		}
		seen, repo, proto := "never", "unknown", p.Protocol
		if p.LastSeen != nil {
			seen = p.LastSeen.Format(time.RFC3339)
			repo = humanSize(*p.RepositorySize)
		}
		if proto == "" {
			proto = "unknown"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", p.ID, formatSize(p.Quota), numbers,
			humanSize(p.UseThere), humanSize(p.UseHere), humanSize(p.Remaining), p.Reachability, seen, "never", proto, repo,
			formatDays(time.Duration(p.OwnRetentionSeconds)*time.Second), formatDays(time.Duration(p.PeerRetentionSeconds)*time.Second),
			p.Since.Format(time.DateOnly))
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
