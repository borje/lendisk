package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// verifyPoll is how often `lendisk verify` reads lendisk.db for the
// daemon's outcome.
const verifyPoll = 500 * time.Millisecond

func (a *app) verifyCmd() *cobra.Command {
	var peer string
	var full bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "verify <bucket> --peer <id>",
		Short: "Challenge a friend's replica of a bucket now",
		Long: `Challenge a friend's replica of a bucket now (VER-3), as the daemon
does daily: random byte ranges of random objects the friend acknowledged,
each with a fresh nonce, answered with an HMAC the friend cannot compute
without the bytes and checked against this node's own copy.

With --full, every object the friend acknowledged is checked whole by the
same HMAC: the friend reads every byte, none crosses the network, and a
replica marked unverified is marked verified again once it passes.

The daemon runs the check, so it must be running; one check runs at a
time, so a long --full check holds up the daily ones. A ranged check of
a replica marked unverified says it passed, and the replica stays
unverified until --full passes. Exit codes: 0 verified or passed; 1 an
error, no answer from the friend, nothing to check, or the daemon stopped;
2 a usage error; 3 timed out; 4 verification failed: the friend does not
hold what it acknowledged, its replica is marked unverified, and its
copies move to other friends.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]
			if peer == "" {
				return usageError{errors.New("name the friend whose replica to check with --peer <node ID>")}
			}
			if _, err := identity.ParseNodeID(peer); err != nil {
				return usageError{fmt.Errorf("--peer %q is not a node ID: %w", peer, err)}
			}
			if timeout < 0 {
				return usageError{fmt.Errorf("--timeout %v is negative; 0 waits without a limit", timeout)}
			}
			// An interrupt ends the wait and takes the request back.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			cfg, db, err := a.openState(ctx)
			if err != nil {
				return err
			}
			defer db.Close()
			bs, err := db.Buckets(ctx)
			if err != nil {
				return err
			}
			found := false
			for _, b := range bs {
				found = found || b.Name == bucket
			}
			if !found {
				return fmt.Errorf("no bucket %q on this node: `lendisk status` lists them", bucket)
			}
			if _, err := db.GetFriendship(ctx, peer); errors.Is(err, state.ErrNotFound) {
				return fmt.Errorf("node %s is not a friend of this node: `lendisk peer list` lists them", peer)
			} else if err != nil {
				return err
			}
			if !daemonRunning(cfg.DataDir) {
				return errors.New("the daemon is not running, and it runs the check: start `lendisk serve`, then verify again")
			}
			id, err := db.RequestVerify(ctx, peer, bucket, full, a.now())
			if err != nil {
				return err
			}
			// Taking the request back must work once ctx has ended (an
			// interrupt), so it gets a context of its own.
			takeBack := func() {
				cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				db.CancelVerify(cctx, id)
			}
			start := a.now()
			for {
				o, done, err := db.VerifyResult(ctx, id)
				if err != nil {
					return err
				}
				if done {
					return a.verifyOutcome(ctx, db, bucket, peer, full, o)
				}
				if timeout > 0 && a.now().Sub(start) >= timeout {
					takeBack()
					return codeError{fmt.Errorf("timed out after %v waiting for the daemon's check of %s at %s", timeout, bucket, peer), 3}
				}
				if err := a.pause(ctx, verifyPoll); err != nil {
					takeBack()
					return err
				}
				if !daemonRunning(cfg.DataDir) {
					takeBack()
					return errors.New("the daemon stopped before it ran the check: start `lendisk serve`, then verify again")
				}
			}
		},
	}
	cmd.Flags().StringVar(&peer, "peer", "", "the friend whose replica to check (its node ID)")
	cmd.Flags().BoolVar(&full, "full", false, "check every object the friend holds, by hash")
	cmd.Flags().DurationVar(&timeout, "timeout", 24*time.Hour, "how long to wait for the daemon's check; 0 waits without a limit")
	return cmd
}

// verifyOutcome prints o, the check of bucket's replica at peer, or
// returns it as an error with its exit code.
func (a *app) verifyOutcome(ctx context.Context, db *state.DB, bucket, peer string, full bool, o state.VerifyOutcome) error {
	skipped := ""
	if o.Skipped > 0 {
		skipped = fmt.Sprintf("; %s skipped, with no local copy as captured to compare with, or changed while challenged",
			plural(o.Skipped, "object", "objects"))
	}
	switch o.Outcome {
	case state.VerifyPassed:
		how := "by random ranges"
		if full {
			how = "whole, by hash"
		}
		checked := plural(o.Checked, "object", "objects")
		if !full {
			// A ranged pass leaves an unverified mark (§3.6): say so.
			cs, err := db.ReplicaChecks(ctx)
			if err != nil {
				return err
			}
			for _, c := range cs {
				if c.Place == peer && c.Bucket == bucket && c.Unverified() {
					fmt.Fprintf(a.out, "%s at %s passed: %s checked %s%s; it stays unverified until `lendisk verify %s --peer %s --full` passes\n",
						bucket, peer, checked, how, skipped, bucket, peer)
					return nil
				}
			}
		}
		fmt.Fprintf(a.out, "%s at %s verified: %s checked %s%s\n", bucket, peer, checked, how, skipped)
		return nil
	case state.VerifyFailed:
		return codeError{fmt.Errorf("verification of %s at %s failed: %s; the replica is marked unverified and its copies move to other friends",
			bucket, peer, o.Detail), 4}
	case state.VerifyNothing:
		return fmt.Errorf("nothing to verify of %s at %s: no object it holds has a local copy to compare with%s", bucket, peer, skipped)
	}
	return fmt.Errorf("the check of %s at %s got no answer, so nothing was decided: %s", bucket, peer, o.Detail)
}
