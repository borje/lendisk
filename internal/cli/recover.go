package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/recovery"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

func (a *app) recoverCmd() *cobra.Command {
	var skip []string
	cmd := &cobra.Command{
		Use:   "recover <lender address>",
		Short: "Rebuild this node on a new machine from the recovery phrase and one friend's address",
		Long: `recover rebuilds a node whose machine is lost (ID-5). It reads the
24-word recovery phrase from standard input, fetches the node's manifest
from the friend at the address given (the tc… string on the recovery
sheet) and from every other friend it names, keeping the newest, and
restores the friendships and buckets. It then asks each friend for the
list of what it holds, to know where every object is. Nothing is
downloaded: the local store starts empty.

Stop the old node first if it still runs: it dials your friends with
the same keys, and two nodes with one identity stall each other.

Listing takes at most a few minutes: a friend not finished by then,
and one named with --skip, is listed by the daemon instead.

Run it on a fresh machine (empty config and data directories), then
start the daemon with ` + "`lendisk serve`" + `.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			skipped := map[string]bool{}
			for _, s := range skip {
				id, err := identity.ParseNodeID(s)
				if err != nil {
					return usageError{fmt.Errorf("--skip %q: not a friend's node ID: %w", s, err)}
				}
				skipped[id.String()] = true
			}
			return a.runRecover(cmd.Context(), args[0], skipped)
		},
	}
	cmd.Flags().StringSliceVar(&skip, "skip", nil, "a friend's node ID not to ask what it holds now; the daemon asks it later (repeatable)")
	return cmd
}

// defaultListBudget is how long recover lists what friends hold before
// it leaves the rest to the daemon.
const defaultListBudget = 3 * time.Minute

func (a *app) runRecover(ctx context.Context, addr string, skip map[string]bool) error {
	lenderID, err := transport.KeyOf(addr)
	if err != nil {
		return usageError{fmt.Errorf("the lender's address: %w; give the tc… string from the recovery sheet", err)}
	}
	cfg, log, err := a.load(a.errOut)
	if err != nil {
		return err
	}
	confDir := cfg.ConfigDir()
	if _, err := identity.Load(confDir); err == nil || !errors.Is(err, identity.ErrNoIdentity) {
		if err == nil {
			return fmt.Errorf("this machine already has a node in %s; recover is for a fresh machine: use empty --config and --data-dir directories", confDir)
		}
		return err
	}
	if err := ensureDir(confDir); err != nil {
		return fmt.Errorf("config directory: %w", err)
	}
	if err := ensureDir(cfg.DataDir); err != nil {
		return fmt.Errorf("data directory: %w", err)
	}
	floor, _ := config.ParseFloor(cfg.Store.FreeSpaceFloor)
	if err := a.checker().Prepare(cfg.StorePath(), floor); err != nil {
		return err
	}

	w := a.out
	fmt.Fprintln(w, "Recovery dials your friends with this node's keys. If the old machine still runs")
	fmt.Fprintln(w, "`lendisk serve`, stop it first: two nodes with one identity stall each other at")
	fmt.Fprintln(w, "the relay, and both would replicate.")
	fmt.Fprint(w, "Is the old node stopped, or its machine gone? [y/N]: ")
	line, err := a.readLine()
	if err != nil {
		return fmt.Errorf("no answer; nothing was done: %w", err)
	}
	if ans := strings.ToLower(line); ans != "y" && ans != "yes" {
		return errors.New("not confirmed; nothing was done. Stop `lendisk serve` on the old machine, then run `lendisk recover` again")
	}
	phrase, err := a.readPhrase()
	if err != nil {
		return err
	}
	id, err := identity.FromPhrase(phrase)
	phrase = ""
	if err != nil {
		return err
	}
	nodeID := id.NodeID().String()
	if lenderID == id.NodeID() {
		return usageError{errors.New("that address is this node's own; give a friend's address")}
	}
	fmt.Fprintf(w, "\nNode ID: %s\n", nodeID)

	db, err := state.Open(ctx, cfg.DataDir, log)
	if err != nil {
		return err
	}
	defer db.Close()
	if prev, ok, err := db.GetNode(ctx, "id"); err != nil {
		return err
	} else if ok && prev != nodeID {
		return fmt.Errorf("%s belongs to node %s, not %s; use another data_dir or move it aside", db.Path, prev, nodeID)
	}
	db.Clock = a.clock

	// One dialler for the whole recovery, closed before anything else
	// can dial with these keys (ID-5).
	dl, err := transport.NewFriendDialer(id, log)
	if err != nil {
		return err
	}
	defer dl.Close()
	fmt.Fprintf(w, "Fetching the manifest from %s and the friends it names…\n", lenderID)
	m, tried, err := recovery.Newest(ctx, addr, func(ctx context.Context, addr string) (*manifest.Manifest, error) {
		fctx, cancel := context.WithTimeout(ctx, a.redeemTimeout())
		defer cancel()
		return manifest.Fetch(fctx, id, addr, dl.Dial)
	})
	printFetched(w, tried, m)
	if err != nil {
		return fmt.Errorf("no friend served this node's manifest; nothing was restored: %w. "+
			"Check that the friend runs `lendisk serve`, or try another friend's address; "+
			"a friend that lost its lendisk.db no longer admits this node until the two of you make a new invite", err)
	}
	if a.afterFetch != nil {
		a.afterFetch()
	}

	if err := db.SetNode(ctx, "id", nodeID); err != nil {
		return err
	}
	restored, err := recovery.Restore(ctx, db, m)
	if err != nil {
		return err
	}
	if len(m.Settings.TunnelRegion) == 0 {
		a.chooseRegion(ctx, cfg, db, log)
	}
	if err := a.restoreBuckets(ctx, cfg.StorePath(), db, m.Buckets); err != nil {
		return err
	}
	profiles, err := profile.Load(filepath.Join(confDir, "profiles"))
	if err != nil {
		return err
	}
	lister := &recovery.Lister{DB: db, Self: id, Dial: dl.Dial, Profiles: profiles, Now: a.clock,
		Timeout: a.redeemTimeout(), Skip: skip, Log: log}
	budget := a.listBudget
	if budget <= 0 {
		budget = defaultListBudget
	}
	fmt.Fprintf(w, "Asking each friend what it holds (for at most %s)…\n", budget)
	results, err := lister.RoundWithin(ctx, budget)
	if err != nil {
		return err
	}
	dl.Close()

	if err := id.Save(confDir); err != nil {
		return err
	}
	if err := db.SetNode(ctx, "initialised_at", a.now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	if !cfg.FileExists {
		if err := writeInitialConfig(cfg); err != nil {
			return err
		}
	}
	log.Info("node recovered", "node_id", nodeID, "manifest_version", m.Seq)
	held := map[string]map[string]int64{}
	for _, b := range m.Buckets {
		if held[b.Name], err = db.HeldAt(ctx, b.Name); err != nil {
			return err
		}
	}
	conflicts, err := db.Conflicts(ctx)
	if err != nil {
		return err
	}
	a.printRecovered(w, m, restored, results, nodeAddress(ctx, cfg, id, log), held, conflicts)
	return nil
}

// restoreBuckets adds the manifest's buckets with new credentials of
// their own (SEC-3: a bucket's credential is not in the manifest) and
// creates them, empty, in the store. A bucket already there, from an
// interrupted recover, is kept.
func (a *app) restoreBuckets(ctx context.Context, storeDir string, db *state.DB, bs []manifest.Bucket) error {
	be, err := gateway.OpenBackend(storeDir)
	if err != nil {
		return err
	}
	for _, mb := range bs {
		access, err := newAccessKey(a.rng())
		if err != nil {
			return err
		}
		secret, err := newSecretKey(a.rng())
		if err != nil {
			return err
		}
		b := state.Bucket{Name: mb.Name, Profile: mb.Profile, Copies: mb.Copies, AccessKey: access, SecretKey: secret,
			CreatedAt: mb.CreatedAt}
		if b.Copies < 1 {
			b.Copies = state.DefaultCopies
		}
		if err := db.AddBucket(ctx, b); errors.Is(err, state.ErrExists) {
			continue
		} else if err != nil {
			return err
		}
		if err := gateway.CreateBucket(ctx, be, b.Name, b.AccessKey); err != nil {
			db.DeleteBucket(ctx, b.Name)
			return fmt.Errorf("creating bucket %s in the store: %w", b.Name, err)
		}
	}
	return nil
}

// printFetched says what each friend served of the manifest, and warns
// when one did not answer: it may hold a newer version than the one
// kept, which this node then continues from below (#29 review).
func printFetched(w io.Writer, tried []recovery.Fetched, m *manifest.Manifest) {
	var silent []string
	for _, f := range tried {
		switch {
		case f.Err != nil:
			fmt.Fprintf(w, "  %s: %v\n", f.Peer, f.Err)
			silent = append(silent, f.Peer.String())
		case m != nil && f.Seq == m.Seq:
			fmt.Fprintf(w, "  %s: version %d (newest)\n", f.Peer, f.Seq)
		default:
			fmt.Fprintf(w, "  %s: version %d, older\n", f.Peer, f.Seq)
		}
	}
	if m != nil && len(silent) > 0 {
		fmt.Fprintf(w, "  ! Warning: %s did not answer and may hold a newer version of the manifest than %d\n"+
			"    (a friend or bucket added since would be missing). If so, stop here and run recover again once they answer.\n",
			strings.Join(silent, ", "), m.Seq)
	}
}

// printRecovered says what recover restored and what to run next.
func (a *app) printRecovered(w io.Writer, m *manifest.Manifest, r recovery.Restored, results []recovery.Result, addr string,
	held map[string]map[string]int64, conflicts []state.RecoveredKey) {
	fmt.Fprintf(w, "\nRecovered from manifest version %d.\n", m.Seq)
	if addr != "" {
		fmt.Fprintf(w, "Address: %s\n", addr)
		if m.Settings.Address != "" && m.Settings.Address != addr {
			fmt.Fprintln(w, "  (the address changed: give your friends the new one)")
		}
	}
	fmt.Fprintf(w, "\nFriends: %d\n", len(r.Friends))
	for _, f := range r.Friends {
		fmt.Fprintf(w, "  %s, quota %s each way\n", f.ID, formatSize(min(f.OwnNumber, f.PeerNumber)))
	}
	for _, id := range r.NoDialKey {
		fmt.Fprintf(w, "  ! %s: the manifest does not carry the key it dials you with, so it cannot reach this node; ask it for a new invite and run `lendisk peer add` with it\n", id)
	}
	fmt.Fprintf(w, "\nBuckets: %d\n", len(m.Buckets))
	for _, b := range m.Buckets {
		fmt.Fprintf(w, "  %s (%s, copies %d)\n", b.Name, b.Profile, b.Copies)
		for _, res := range results {
			if res.Bucket != b.Name {
				continue
			}
			switch {
			case errors.Is(res.Err, recovery.ErrOutOfTime):
				fmt.Fprintf(w, "    at %s: not finished in time; `lendisk serve` lists the rest\n", res.Place)
			case errors.Is(res.Err, recovery.ErrSkipped):
				fmt.Fprintf(w, "    at %s: skipped; `lendisk serve` lists it\n", res.Place)
			case res.Err != nil:
				fmt.Fprintf(w, "    at %s: not listed (%v); `lendisk serve` tries again\n", res.Place, shortErr(res.Err))
			default:
				fmt.Fprintf(w, "    at %s: %d objects", res.Place, held[b.Name][res.Place])
				if d := int64(res.Listed) - held[b.Name][res.Place]; d > 0 {
					fmt.Fprintf(w, ", %d more in a version that does not count", d)
				}
				if res.Rejected > 0 {
					fmt.Fprintf(w, ", %d rejected as not this bucket's (see the log)", res.Rejected)
				}
				if res.Tombstoned > 0 {
					fmt.Fprintf(w, ", %d deleted and kept for retention", res.Tombstoned)
				}
				fmt.Fprintln(w)
			}
		}
	}
	if len(conflicts) > 0 {
		fmt.Fprintf(w, "\n%d objects are in conflict: the friends disagree about them, and none is used until you pick.\n", len(conflicts))
		fmt.Fprintln(w, "`lendisk status` lists them; `lendisk bucket resolve <bucket> <key> <friend>` picks a friend's version.")
	}
	printOverflow(w, m.Overflow)
	fmt.Fprintln(w, "\nThe local store is empty: nothing was downloaded.")
	if len(m.Buckets) > 0 {
		fmt.Fprintln(w, "Each bucket has a new credential for your backup tool: `lendisk bucket show <name>` prints it.")
	}
	fmt.Fprintln(w, "\nNext: start the daemon with")
	fmt.Fprintln(w, "\n  lendisk serve")
}

// shortErr is an error's message cut to one short line.
func shortErr(err error) string {
	s := err.Error()
	if len(s) > 120 {
		s = s[:117] + "..."
	}
	return s
}

// printOverflow shows the overflow targets the manifest carries, with
// their credentials, once (S3-1, SEC-3): they are put in the
// environment by the user and never written to disk.
func printOverflow(w io.Writer, targets []manifest.Overflow) {
	if len(targets) == 0 {
		return
	}
	fmt.Fprintf(w, "\nOverflow targets: %d. Their credentials are shown this once and not written anywhere;\n", len(targets))
	fmt.Fprintln(w, "put each in the environment of `lendisk serve` (SEC-3).")
	for _, o := range targets {
		fmt.Fprintf(w, "  %s: %s, bucket %s", o.Name, o.Endpoint, o.Bucket)
		if o.Prefix != "" {
			fmt.Fprintf(w, ", prefix %s", o.Prefix)
		}
		fmt.Fprintln(w)
		if o.AccessKey != "" || o.SecretKey != "" {
			fmt.Fprintf(w, "    access key: %s\n    secret key: %s\n", o.AccessKey, o.SecretKey)
		}
	}
}
