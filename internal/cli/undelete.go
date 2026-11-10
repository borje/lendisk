package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/undelete"
)

// undeletePoll is how often `lendisk undelete` reads lendisk.db for the
// daemon's preview and result.
const undeletePoll = 500 * time.Millisecond

func (a *app) undeleteCmd() *cobra.Command {
	var since string
	var rewrites, yes bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "undelete <bucket> [--since <time>]",
		Short: "Bring back what was deleted from a bucket at the friends holding it",
		Long: `undelete reverses deletes at the friends (RET-3): every friend keeps what this
node deleted, as a tombstone, for the retention window, and moves it
back to the live set when asked. Run it after an attacker's
` + "`restic forget --prune`" + ` or a mistaken delete, and after ` + "`lendisk recover`" + ` if
the machine was wiped too.

It first shows, from the friends' lists, how many objects and bytes would
come back (deleted at or after --since; every tombstone in the window
without it) and which snapshots, then asks to go on. Nothing is
downloaded: the local store reads each object from a friend the first
time it is read, as after recover.

--since takes a time (2026-11-07T14:00:00Z, or a date, 2026-11-07, at
midnight local time) or how long ago (36h, 7d). A delete counts from when
the friend or overflow target took it, which is when replication sent it:
no delete made after --since is missed, and one made shortly before it
comes back too if a friend was offline and took it later.

Of a key deleted more than once since then, the version it held when it
was last deleted comes back. A key written again since, with other bytes,
is left as it is and reported: no version is lost, the tombstone stays
for its window. A version a rewrite replaced (config, kopia.repository)
comes back only with --rewrites; then a key comes back as it was at
--since, if it is not live now (delete it first to roll it back).

The daemon asks the friends, so it must be running. Exit codes: 0 done;
1 an error, a friend not reached, or the daemon not running (run it
again: undelete is safe to repeat); 2 a usage error; 3 timed out.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bucket := args[0]
			var at time.Time
			if since != "" {
				var err error
				if at, err = parseSince(since, a.now()); err != nil {
					return usageError{fmt.Errorf("--since %w", err)}
				}
			}
			if timeout < 0 {
				return usageError{fmt.Errorf("--timeout %v is negative; 0 waits without a limit", timeout)}
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			cfg, db, err := a.openState(ctx)
			if err != nil {
				return err
			}
			defer db.Close()
			if _, err := db.GetBucket(ctx, bucket); errors.Is(err, state.ErrNotFound) {
				return fmt.Errorf("no bucket %q on this node: `lendisk status` lists them", bucket)
			} else if err != nil {
				return err
			}
			if !daemonRunning(cfg.DataDir) {
				return errors.New("the daemon is not running, and it asks the friends: start `lendisk serve`, then undelete again")
			}
			id, err := db.RequestUndelete(ctx, bucket, at, rewrites, a.now())
			if err != nil {
				return err
			}
			w := &undeleteWait{a: a, db: db, dataDir: cfg.DataDir, id: id, timeout: timeout, start: a.now()}
			fmt.Fprintf(a.out, "Asking the friends what they hold of %s deleted %s…\n", bucket, sinceText(at))
			q, err := w.until(ctx, state.UndeletePreviewed)
			if err != nil {
				return err
			}
			if q.Stage == state.UndeleteFailed {
				return fmt.Errorf("the preview failed: %s", resultError(q.Result))
			}
			var p undelete.Preview
			if err := json.Unmarshal([]byte(q.Preview), &p); err != nil {
				return fmt.Errorf("the daemon's preview is unreadable: %w", err)
			}
			a.printPreview(p, at)
			if p.Keys == 0 && p.Rewritten == 0 {
				w.takeBack()
				fmt.Fprintln(a.out, "Nothing to undelete.")
				return nil
			}
			if !yes {
				fmt.Fprint(a.out, "Bring these back at every friend holding them? [y/N]: ")
				line, err := a.readLine()
				if ans := strings.ToLower(line); err != nil || ans != "y" && ans != "yes" {
					w.takeBack()
					return errors.New("not confirmed; nothing was undeleted")
				}
			}
			if err := db.ConfirmUndelete(ctx, id, a.now()); err != nil {
				return fmt.Errorf("confirming the undelete: %w", err)
			}
			fmt.Fprintln(a.out, "Asking the friends to bring them back…")
			w.confirmed = true
			q, err = w.until(ctx, state.UndeleteDone)
			if err != nil {
				return err
			}
			if q.Stage == state.UndeleteFailed {
				return fmt.Errorf("the undelete failed: %s", resultError(q.Result))
			}
			var res undelete.Result
			if err := json.Unmarshal([]byte(q.Result), &res); err != nil {
				return fmt.Errorf("the daemon's result is unreadable: %w", err)
			}
			return a.printUndeleted(bucket, res)
		},
	}
	cmd.Flags().StringVar(&since, "since", "", "bring back what was deleted at or after this time (2026-11-07T14:00:00Z, 2026-11-07, 36h or 7d ago)")
	cmd.Flags().BoolVar(&rewrites, "rewrites", false, "also bring back versions that rewrites replaced (config, kopia.repository)")
	cmd.Flags().BoolVarP(&yes, "yes", "y", false, "do not ask before bringing them back")
	cmd.Flags().DurationVar(&timeout, "timeout", time.Hour, "how long to wait for the daemon; 0 waits without a limit")
	return cmd
}

// parseSince reads --since: an RFC 3339 time, a date (midnight local
// time), or how long before now (36h, 7d).
func parseSince(s string, now time.Time) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	if t, err := time.ParseInLocation(time.DateOnly, s, time.Local); err == nil {
		return t, nil
	}
	if d, err := parseDays(s); err == nil {
		return now.Add(-d), nil
	}
	return time.Time{}, fmt.Errorf("%q: want a time (2026-11-07T14:00:00Z), a date (2026-11-07) or how long ago (36h, 7d)", s)
}

func sinceText(at time.Time) string {
	if at.IsZero() {
		return "within the retention window"
	}
	return "since " + at.Local().Format(time.RFC3339)
}

func resultError(result string) string {
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal([]byte(result), &e) == nil && e.Error != "" {
		return e.Error
	}
	return "no reason recorded; see the daemon's log"
}

// undeleteWait polls an undelete request until the daemon moves it on.
type undeleteWait struct {
	a         *app
	db        *state.DB
	dataDir   string
	id        int64
	timeout   time.Duration
	start     time.Time
	confirmed bool
}

// takeBack drops the request, if not yet confirmed, with a context of
// its own: it must work once ctx has ended (an interrupt).
func (w *undeleteWait) takeBack() {
	if w.confirmed {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.db.CancelUndelete(ctx, w.id)
}

// until waits for the request to reach stage, or to fail.
func (w *undeleteWait) until(ctx context.Context, stage string) (state.UndeleteRequest, error) {
	for {
		q, err := w.db.GetUndelete(ctx, w.id)
		if err != nil {
			return q, err
		}
		if q.Stage == stage || q.Stage == state.UndeleteFailed {
			return q, nil
		}
		if w.timeout > 0 && w.a.now().Sub(w.start) >= w.timeout {
			w.takeBack()
			return q, codeError{fmt.Errorf("timed out after %v waiting for the daemon", w.timeout), 3}
		}
		if err := w.a.pause(ctx, undeletePoll); err != nil {
			w.takeBack()
			if w.confirmed {
				return q, errors.New("stopped waiting; the daemon goes on with the undelete, and `lendisk status` shows the objects as they come back")
			}
			return q, err
		}
		if !daemonRunning(w.dataDir) {
			w.takeBack()
			return q, errors.New("the daemon stopped before it finished: start `lendisk serve`, then undelete again")
		}
	}
}

func (a *app) printPreview(p undelete.Preview, at time.Time) {
	fmt.Fprintf(a.out, "Deleted %s: %s, %s, would come back.\n", sinceText(at),
		plural(int64(p.Keys), "object", "objects"), humanSize(p.Bytes))
	if p.Snapshots > 0 {
		fmt.Fprintf(a.out, "Among them %s:\n", plural(int64(p.Snapshots), "snapshot", "snapshots"))
		for _, k := range p.Named {
			fmt.Fprintf(a.out, "  %s\n", k)
		}
		if n := p.Snapshots - len(p.Named); n > 0 {
			fmt.Fprintf(a.out, "  … and %d more\n", n)
		}
	}
	if p.Rewrites && p.Rewritten > 0 {
		fmt.Fprintf(a.out, "%s live now %s replaced by rewrites; a version from since then comes back only where the key is not live.\n",
			plural(int64(p.Rewritten), "key", "keys"), map[bool]string{true: "has versions", false: "have versions"}[p.Rewritten == 1])
	}
	for _, f := range p.Friends {
		switch {
		case f.Error != "":
			fmt.Fprintf(a.out, "  %s: not reached (%s)\n", f.Peer, f.Error)
		default:
			fmt.Fprintf(a.out, "  %s: %s, %s\n", f.Peer, plural(int64(f.Keys), "object", "objects"), humanSize(f.Bytes))
		}
	}
}

// printUndeleted prints what each friend brought back; a friend not
// reached, or that failed, makes it an error: run it again.
func (a *app) printUndeleted(bucket string, res undelete.Result) error {
	var failed []string
	failedKeys := false
	for _, f := range res.Friends {
		if f.Error != "" {
			fmt.Fprintf(a.out, "  %s: not finished (%s)\n", f.Peer, f.Error)
			failed = append(failed, f.Peer)
			continue
		}
		fmt.Fprintf(a.out, "  %s: %s back, %s\n", f.Peer, plural(int64(f.Restored), "object", "objects"), humanSize(f.Bytes))
		for _, c := range f.Conflicts {
			fmt.Fprintf(a.out, "    ! %s is live with other bytes (%s); its deleted version (%s) stays a tombstone there\n",
				c.Key, short(c.Live), short(c.SHA256))
		}
		if n := f.ConflictCount - len(f.Conflicts); n > 0 {
			fmt.Fprintf(a.out, "    ! … and %d more keys live with other bytes\n", n)
		}
		for _, k := range f.Kept {
			fmt.Fprintf(a.out, "    ! %s: this node keeps its own version (%s); the friend's (%s) is not counted\n",
				k.Key, short(k.Ours), short(k.SHA256))
		}
		if n := f.KeptCount - len(f.Kept); n > 0 {
			fmt.Fprintf(a.out, "    ! … and %d more keys this node wrote again since\n", n)
		}
		for _, k := range f.BusyKeys {
			fmt.Fprintf(a.out, "    ! %s was being written here: the friend holds its deleted version live again, not counted; this node's write goes to it as any write over a version it holds (EP-11)\n", k)
		}
		if n := f.Busy - len(f.BusyKeys); n > 0 {
			fmt.Fprintf(a.out, "    ! … and %d more keys being written here\n", n)
		}
		for _, k := range f.FailedKeys {
			fmt.Fprintf(a.out, "    ! %s could not be brought back now (%s); its tombstone stays: undelete again later\n", k.Key, k.Detail)
			failedKeys = true
		}
		if n := f.Failed - len(f.FailedKeys); n > 0 {
			fmt.Fprintf(a.out, "    ! … and %d more keys not brought back now\n", n)
			failedKeys = true
		}
		if f.Unknown > 0 {
			fmt.Fprintf(a.out, "    ! %s the friend restored this node never had and the preview did not list: not counted (see the daemon's log)\n",
				plural(int64(f.Unknown), "key", "keys"))
		}
		if f.Adopted > 0 {
			fmt.Fprintf(a.out, "    %s an earlier undelete brought back there, now counted\n", plural(int64(f.Adopted), "object", "objects"))
		}
		if f.Rejected > 0 {
			fmt.Fprintf(a.out, "    ! %s the friend sent cannot be this node's and %s not counted (see the daemon's log)\n",
				plural(int64(f.Rejected), "entry", "entries"), map[bool]string{true: "was", false: "were"}[f.Rejected == 1])
		}
	}
	if failedKeys && len(failed) == 0 {
		return fmt.Errorf("some objects could not be brought back now; run `lendisk undelete %s` again later", bucket)
	}
	if len(failed) > 0 {
		return fmt.Errorf("%s not finished; run `lendisk undelete %s` again when %s reachable",
			plural(int64(len(failed)), "friend", "friends"), bucket, map[bool]string{true: "it is", false: "they are"}[len(failed) == 1])
	}
	fmt.Fprintf(a.out, "Done. Objects are read from the friends as they are read; `lendisk status` shows %s.\n", bucket)
	return nil
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}
