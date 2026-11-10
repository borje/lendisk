package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/store"
)

// Each side's number for a friendship (LEND-8, PEER-6, #24): `peer
// quota` sets this node's; the daemon's quota exchanger (package quota)
// tells the friend and applies what changed.

func (a *app) peerQuotaCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "quota <friend> <size>",
		Short: "Set what you lend a friend",
		Long: `peer quota sets your number for a friendship: what you are willing to
lend that friend, such as 80G (PEER-6, LEND-8). The friendship's quota,
each way, is the smaller of your number and theirs. Lowering takes effect
at once on both sides; raising takes effect once their number allows.
Nothing is deleted when the quota falls below what is stored: new objects
stop, and the node that stored them places them elsewhere. The sum of
your numbers may not exceed lending.max_total. A running ` + "`lendisk serve`" + `
tells the friend within seconds.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runPeerQuota(cmd.Context(), args[0], args[1])
		},
	}
}

func (a *app) runPeerQuota(ctx context.Context, s, size string) error {
	peer, err := identity.ParseNodeID(strings.TrimSpace(s))
	if err != nil {
		return usageError{fmt.Errorf("%q is not a node ID: %w; `lendisk peer list` shows your friends", s, err)}
	}
	n, err := parseNumber(size)
	if err != nil {
		return usageError{err}
	}
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	db.Clock = a.clock
	maxTotal, from, err := a.maxTotal(cfg)
	if err != nil {
		return err
	}
	before, after, err := db.SetOwnNumber(ctx, peer.String(), n, maxTotal)
	switch {
	case errors.Is(err, state.ErrNotFound):
		return fmt.Errorf("%s is not a friend of this node; `lendisk peer list` shows your friends", peer)
	case err != nil:
		return overMaxTotal(err, maxTotal, from)
	}
	fmt.Fprintf(a.out, "Your number for %s is now %s (was %s); theirs is %s.\n", peer, formatSize(n),
		formatSize(before.OwnNumber), formatSize(after.PeerNumber))
	switch q, was := after.Quota(), before.Quota(); {
	case q < was:
		fmt.Fprintf(a.out, "The quota falls from %s to %s each way, at once here and at %s once it is told.\n", formatSize(was), formatSize(q), peer)
	case q > was:
		fmt.Fprintf(a.out, "The quota rises from %s to %s each way, here at once and at %s once it is told.\n", formatSize(was), formatSize(q), peer)
	case n > after.PeerNumber:
		fmt.Fprintf(a.out, "The quota stays %s each way until they raise their number (`lendisk peer quota` on their node).\n", formatSize(q))
	default:
		fmt.Fprintf(a.out, "The quota stays %s each way.\n", formatSize(q))
	}
	use, err := db.PlaceUse(ctx)
	if err != nil {
		return err
	}
	lent, _, err := db.LentUsage(ctx, peer.String())
	if err != nil {
		return err
	}
	q := after.Quota()
	if lent > q {
		fmt.Fprintf(a.out, "They store %s here, more than the quota: no new objects are accepted from them, and nothing is deleted.\n", humanSize(lent))
	}
	if u := use[peer.String()]; u > q {
		fmt.Fprintf(a.out, "You store %s there, more than the quota: no new copies go there, nothing is deleted, and copies go to other friends with room.\n", humanSize(u))
	}
	fmt.Fprintln(a.out, "A running `lendisk serve` tells them within seconds; `lendisk peer list` shows both numbers.")
	return nil
}

// Where the effective cap comes from.
const (
	capSet  = "lending.max_total"
	capDisk = "the store's filesystem less its free-space floor"
)

// maxTotal is the effective cap on the sum of the node's numbers
// (LEND-1) and where it comes from: lending.max_total if set, else what
// the store's filesystem can hold, its size less the free-space floor
// (LEND-2), read with Statfs (#24 review).
func (a *app) maxTotal(cfg *config.Loaded) (int64, string, error) {
	if n, ok := cfg.MaxTotal(); ok {
		return n, capSet, nil
	}
	statfs := a.lendStatfs
	if statfs == nil {
		statfs = a.checker().Statfs
	}
	sp, err := statfs(cfg.StorePath())
	if err != nil {
		return 0, "", fmt.Errorf("cannot read the store's size, which caps what this node lends unless lending.max_total is set: %w; set lending.max_total, or check %s", err, cfg.StorePath())
	}
	floor, _ := config.ParseFloor(cfg.Store.FreeSpaceFloor)
	f := store.FloorBytes(floor, sp.Total)
	if sp.Total <= f {
		return 1, capDisk, nil // nothing to lend; 0 would mean no cap
	}
	return int64(min(sp.Total-f, uint64(control.MaxNumber))), capDisk, nil
}

// StatusLending is the effective cap on what the node lends (LEND-1) in
// `status --json`: the cap, where it comes from (lending.max_total, or
// the store's filesystem less its free-space floor) and what the node's
// numbers and open invites take of it.
type StatusLending struct {
	MaxTotal int64  `json:"max_total"`
	From     string `json:"from"`
	Numbers  int64  `json:"numbers"`
}

// lendingStatus reads the effective cap and what it holds.
func (a *app) lendingStatus(ctx context.Context, cfg *config.Loaded, db *state.DB) (StatusLending, error) {
	max, from, err := a.maxTotal(cfg)
	if err != nil {
		return StatusLending{}, err
	}
	n, err := db.LentNumbers(ctx, a.now(), "")
	return StatusLending{MaxTotal: max, From: from, Numbers: n}, err
}

// overMaxTotal words a refusal past the cap max, which comes from from;
// any other error is returned as it is.
func overMaxTotal(err error, max int64, from string) error {
	var over *state.MaxTotalError
	if !errors.As(err, &over) {
		return err
	}
	how := "raise lending.max_total"
	if from == capDisk {
		how = "set lending.max_total, if the store's filesystem is shared or will grow"
	}
	return fmt.Errorf("refused: this node lends at most %s (%s) and its numbers and open invites take %s, so at most %s more; "+
		"lower a number (`lendisk peer quota`) or %s", formatSize(max), from, formatSize(over.Others),
		formatSize(max-min(over.Others, max)), how)
}

// humanSize writes bytes for a person: B, KiB, MiB, GiB or TiB with one
// decimal.
func humanSize(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	v, i := float64(n), 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// reachability is PEER-4's: direct, relayed or offline, from the quota
// call's last outcome and the path keeper's disco pings (NET-4);
// unknown before the first.
func reachability(f state.Friendship) string {
	switch {
	case f.ExchangeError != "":
		return "offline"
	case f.Path != "":
		return f.Path
	case f.SeenAt.IsZero():
		return "unknown"
	}
	return "online"
}
