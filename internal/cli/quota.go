package cli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
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
	maxTotal, _ := cfg.MaxTotal()
	before, after, err := db.SetOwnNumber(ctx, peer.String(), n, maxTotal)
	var over *state.MaxTotalError
	switch {
	case errors.Is(err, state.ErrNotFound):
		return fmt.Errorf("%s is not a friend of this node; `lendisk peer list` shows your friends", peer)
	case errors.As(err, &over):
		return fmt.Errorf("refused: lending.max_total is %s and your other numbers and open invites take %s, so this friendship can have at most %s; "+
			"lower another number or raise lending.max_total", formatSize(over.Max), formatSize(over.Others), formatSize(max(over.Max-over.Others, 0)))
	case err != nil:
		return err
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

// checkMaxTotal refuses adding a number n at now past lending.max_total
// (LEND-1): an invite's or a redeem's.
func checkMaxTotal(ctx context.Context, cfg *config.Loaded, db *state.DB, now time.Time, n int64) error {
	max, ok := cfg.MaxTotal()
	if !ok {
		return nil
	}
	err := db.CheckMaxTotal(ctx, now, n, max)
	var over *state.MaxTotalError
	if errors.As(err, &over) {
		return fmt.Errorf("refused: lending.max_total is %s and your numbers and open invites take %s, so you can lend at most %s more; "+
			"lower a number (`lendisk peer quota`) or raise lending.max_total", formatSize(over.Max), formatSize(over.Others),
			formatSize(max-min(over.Others, max)))
	}
	return err
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
// call's last outcome; unknown before the first.
func reachability(f state.Friendship) string {
	switch {
	case f.ExchangeError != "":
		return "offline"
	case f.SeenAt.IsZero():
		return "unknown"
	case f.Path == "":
		return "online"
	}
	return f.Path
}
