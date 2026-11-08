package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// This file is what `status` says of each bucket's offsite-complete
// state (REPL-4, REPL-11) and `status --wait` (REPL-5), #20. Both read
// state.DB.Offsite: the watermark and what keeps it below a snapshot.

// StatusOffsite is how much of a bucket is offsite at its copies.
type StatusOffsite struct {
	// Complete is true when everything the bucket captured is offsite.
	Complete bool `json:"complete"`
	// Watermark is the write sequence up to which every object is held
	// at copies places (REPL-11); Latest is the bucket's latest write.
	Watermark int64 `json:"watermark"`
	Latest    int64 `json:"latest"`
	// Snapshot is the latest snapshot at or below the watermark: offsite.
	// LatestSnapshot is the latest one written, offsite or not. Absent
	// for a bucket with no snapshot keys (generic) or none yet.
	Snapshot       *StatusSnapshot `json:"snapshot,omitempty"`
	LatestSnapshot *StatusSnapshot `json:"latest_snapshot,omitempty"`
	// Through is the capture time of the last fully placed write (RFC
	// 3339, UTC). Empty when none is.
	Through string `json:"through,omitempty"`
	// Before is the capture time of the first write not offsite (RFC
	// 3339, UTC): every write captured in an earlier second is offsite.
	// Capture times are whole seconds, so that write's second may hold
	// offsite writes too. Empty when everything is offsite.
	Before string `json:"before,omitempty"`
	// Waiting is what keeps the latest snapshot (the latest write, with
	// none) from being offsite: objects short of copies places. Stuck is those with no copy on its way to
	// make up copies, by reason; waiting alone does not bring them.
	Waiting StatusShortfall `json:"waiting"`
	Stuck   StatusShortfall `json:"stuck"`
}

// StatusSnapshot is a snapshot key: for restic a snapshot, whose ID the
// key names; for Kopia an index blob, which marks the time by which the
// snapshots it lists were finished.
type StatusSnapshot struct {
	Key string `json:"key"`
	// ID is the snapshot's ID as the tool shows it, when the profile's
	// snapshot keys name one (restic); empty for Kopia.
	ID         string `json:"id,omitempty"`
	Seq        int64  `json:"seq"`
	CapturedAt string `json:"captured_at"` // RFC 3339, UTC
	Offsite    bool   `json:"offsite"`
}

// StatusShortfall counts objects short of copies, how many are still
// being written to the store (Pending, whose bytes are not counted yet),
// and why for stuck ones: refused by a friend (or unreadable in the
// store), dropped at a full or gone friend, held only by a friend
// removed or gone past replace_after, or no room in the pool.
type StatusShortfall struct {
	Objects  int64 `json:"objects"`
	Bytes    int64 `json:"bytes"`
	Pending  int64 `json:"pending"`
	Refused  int64 `json:"refused"`
	Dropped  int64 `json:"dropped"`
	Gone     int64 `json:"gone"`
	Unplaced int64 `json:"unplaced"`
	// Unmatchable: completed uploads whose rows cannot be matched to the
	// object in the store (#62), which may never settle.
	Unmatchable int64 `json:"unmatchable"`
	// Immutable: keys written with other bytes than a friend
	// acknowledged (EP-11, #22), replicated nowhere they would replace
	// that version until written back or deleted.
	Immutable int64 `json:"immutable"`
}

func shortfall(s state.Shortfall) StatusShortfall {
	return StatusShortfall{Objects: s.Objects, Bytes: s.Bytes, Pending: s.Pending, Refused: s.Refused,
		Dropped: s.Dropped, Gone: s.Gone, Unplaced: s.Unplaced, Unmatchable: s.Unmatchable, Immutable: s.Immutable}
}

// offsiteStatus reads bucket b's offsite state.
func offsiteStatus(ctx context.Context, db *state.DB, b state.Bucket, p *profile.Profile) (StatusOffsite, error) {
	o, err := db.Offsite(ctx, b.Name, 0)
	if err != nil {
		return StatusOffsite{}, err
	}
	out := StatusOffsite{Complete: o.Complete(), Watermark: o.Watermark, Latest: o.Latest}
	if o.Through.Seq > 0 {
		out.Through = utc(o.Through.CapturedAt)
	}
	snap, latest := o.Snapshot, o.LatestSnapshot
	if !p.NamesSnapshots {
		// Kopia's snapshot keys are index blobs that compaction deletes:
		// the latest written marks the time, deleted since or not.
		if snap, err = db.LatestSnapshotWrite(ctx, b.Name, o.Watermark); err != nil {
			return out, err
		}
		if latest, err = db.LatestSnapshotWrite(ctx, b.Name, 0); err != nil {
			return out, err
		}
	}
	out.Snapshot = snapshotOf(snap, p, o.Watermark)
	out.LatestSnapshot = snapshotOf(latest, p, o.Watermark)
	// What waits is counted for the latest snapshot, when there is one:
	// the writes of a backup still running after it are not its. For
	// recounts without reading the database again.
	if latest.Seq > 0 {
		o = o.For(latest.Seq)
	}
	if o.Next.Seq > 0 {
		out.Before = utc(o.Next.CapturedAt)
	}
	out.Waiting, out.Stuck = shortfall(o.Waiting), shortfall(o.Stuck)
	return out, nil
}

func snapshotOf(w state.Write, p *profile.Profile, watermark int64) *StatusSnapshot {
	if w.Seq == 0 {
		return nil
	}
	id, _ := p.SnapshotID(w.Key)
	return &StatusSnapshot{Key: w.Key, ID: id, Seq: w.Seq, CapturedAt: utc(w.CapturedAt), Offsite: w.Seq <= watermark}
}

func utc(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// textTime is an RFC 3339 time as status prints it.
func textTime(rfc string) string {
	t, err := time.Parse(time.RFC3339, rfc)
	if err != nil {
		return rfc
	}
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}

func plural(n int64, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

// snapshotName is how status names a snapshot: restic's short ID.
func snapshotName(s *StatusSnapshot) string {
	if len(s.ID) > 8 {
		return s.ID[:8]
	}
	return s.ID
}

// offsitePhrase says that s is offsite at copies.
func offsitePhrase(s *StatusSnapshot, copies int) string {
	at := plural(int64(copies), "copy", "copies")
	if s.ID != "" {
		return fmt.Sprintf("snapshot %s is offsite at %s", snapshotName(s), at)
	}
	// Strictly before the index blob's capture second: another index
	// captured in that second may be above the watermark.
	return fmt.Sprintf("every snapshot finished before %s is offsite at %s (through %s)", textTime(s.CapturedAt), at, s.Key)
}

// notYetPhrase says that s is not offsite at copies yet.
func notYetPhrase(s *StatusSnapshot, copies int) string {
	at := plural(int64(copies), "copy", "copies")
	if s.ID != "" {
		return fmt.Sprintf("snapshot %s is not offsite at %s yet", snapshotName(s), at)
	}
	return fmt.Sprintf("the snapshot key %s of %s is not offsite at %s yet", s.Key, textTime(s.CapturedAt), at)
}

// shortPhrase says what is short of copies, and what of it is stuck.
func shortPhrase(waiting, stuck StatusShortfall, copies int) string {
	places := plural(int64(copies), "place", "places")
	s := fmt.Sprintf("%d objects, %d bytes still to reach %s", waiting.Objects, waiting.Bytes, places)
	if waiting.Pending > 0 {
		s += fmt.Sprintf(" (%d still being written to the store)", waiting.Pending)
	}
	if stuck.Objects == 0 {
		return s
	}
	var why []string
	for _, r := range []struct {
		n    int64
		what string
	}{{stuck.Refused, "refused by a friend"}, {stuck.Dropped, "dropped at a full or gone friend"},
		{stuck.Gone, "held by a friend removed or gone past replace_after"},
		{stuck.Immutable, "written with other bytes than a friend acknowledged or holds (EP-11; the changed keys are listed under the bucket)"},
		{stuck.Unmatchable, "cannot be matched to the store: restore the store's xattrs or rewrite the key"},
		{stuck.Unplaced, "with no room for them in the pool"}} {
		if r.n > 0 {
			why = append(why, fmt.Sprintf("%d %s", r.n, r.what))
		}
	}
	return s + fmt.Sprintf("; %d of them have no copy on its way to make up %s (%s)", stuck.Objects, places,
		strings.Join(why, ", "))
}

// offsiteLines are the lines status prints under bucket b.
func offsiteLines(b StatusBucket) []string {
	o, at := b.Offsite, plural(int64(b.Copies), "copy", "copies")
	switch {
	case o.Latest == 0:
		return []string{"nothing written yet"}
	case o.LatestSnapshot == nil:
		if o.Complete {
			return []string{fmt.Sprintf("every write up to %s is offsite at %s", textTime(o.Through), at)}
		}
		short := shortPhrase(o.Waiting, o.Stuck, b.Copies)
		if o.Through == "" {
			return []string{fmt.Sprintf("no write is offsite at %s yet: %s", at, short)}
		}
		// Strictly before the first write that is not: capture times
		// are whole seconds.
		return []string{fmt.Sprintf("every write captured before %s is offsite at %s; later ones are not yet: %s",
			textTime(o.Before), at, short)}
	case o.LatestSnapshot.Offsite:
		return []string{offsitePhrase(o.LatestSnapshot, b.Copies)}
	}
	// The latest snapshot is short of copies, by o.Waiting.
	lines := []string{notYetPhrase(o.LatestSnapshot, b.Copies) + ": " + shortPhrase(o.Waiting, o.Stuck, b.Copies)}
	if s := o.Snapshot; s != nil {
		if s.ID != "" {
			lines = append(lines, fmt.Sprintf("snapshot %s, an earlier one, is offsite at %s", snapshotName(s), at))
		} else {
			lines = append(lines, offsitePhrase(s, b.Copies))
		}
	}
	return lines
}

// waitPoll is how often `status --wait` reads lendisk.db at most; it
// waits at least ten times as long as the last read took, so a large
// bucket does not keep a small machine busy (#20 review).
const waitPoll = 2 * time.Second

// waitInterval is the pause after a read of the watermark that took took.
func waitInterval(took time.Duration) time.Duration { return max(waitPoll, 10*took) }

// waitOffsite is `status --wait bucket` (REPL-5): it polls lendisk.db,
// which the daemon writes as acknowledgements arrive (WAL, so reading
// never blocks it), until the latest snapshot captured before the wait
// began is offsite at the bucket's copies, and prints which. It fails
// with code 3 on timeout (0 waits for ever), with code 4 at once when
// that cannot happen as things stand (objects short of copies with no
// copy on its way: refused, dropped, no room), and with 1 when the
// daemon is not running at the start, since nothing would progress. A
// daemon that stops during the wait (a restart) is noted once and
// waited for, within the timeout.
func (a *app) waitOffsite(ctx context.Context, cfg *config.Loaded, db *state.DB, bucket string, timeout time.Duration,
	quiet bool) error {
	bs, err := db.Buckets(ctx)
	if err != nil {
		return err
	}
	var b *state.Bucket
	for i := range bs {
		if bs[i].Name == bucket {
			b = &bs[i]
		}
	}
	if b == nil {
		return fmt.Errorf("no bucket %q on this node: `lendisk status` lists them", bucket)
	}
	profiles, err := profile.Load(filepath.Join(cfg.ConfigDir(), "profiles"))
	if err != nil {
		return err
	}
	p := profiles.Get(b.Profile)
	start := a.now()
	target, err := db.LatestWrite(ctx, bucket)
	if err != nil {
		return err
	}
	snap, err := db.LatestSnapshotWrite(ctx, bucket, target.Seq)
	if err != nil {
		return err
	}
	if snap.Seq > 0 {
		target = snap
	}
	name := func(o state.Offsite) (*StatusSnapshot, string) {
		if snap.Seq > 0 {
			return snapshotOf(snap, p, o.Watermark), ""
		}
		return nil, textTime(utc(target.CapturedAt))
	}
	first, reported := true, false
	for ; ; first = false {
		began := a.now()
		o, err := db.Offsite(ctx, bucket, target.Seq)
		if err != nil {
			return err
		}
		took := a.now().Sub(began)
		s, at := name(o)
		copies := plural(int64(o.Copies), "copy", "copies")
		if o.Reached() {
			if !quiet {
				switch {
				case target.Seq == 0:
					fmt.Fprintln(a.out, "nothing written yet")
				case s != nil:
					fmt.Fprintln(a.out, offsitePhrase(s, o.Copies))
				default:
					fmt.Fprintf(a.out, "every write before the wait is offsite at %s (the last captured at %s)\n", copies, at)
				}
			}
			return nil
		}
		what := fmt.Sprintf("the writes before the wait (the last captured at %s) are not offsite at %s yet", at, copies)
		if s != nil {
			what = notYetPhrase(s, o.Copies)
		}
		what += ": " + shortPhrase(shortfall(o.Waiting), shortfall(o.Stuck), o.Copies)
		if o.Stuck.Objects > 0 {
			return codeError{fmt.Errorf("%s; waiting cannot change that: `lendisk status` shows the places and alerts", what), 4}
		}
		if !daemonRunning(cfg.DataDir) {
			if first {
				return errors.New("the daemon is not running, so nothing is being replicated: start `lendisk serve`, then wait again")
			}
			if !reported {
				fmt.Fprintln(a.errOut, "lendisk: the daemon stopped; waiting for it to run again")
				reported = true
			}
		}
		if timeout > 0 && a.now().Sub(start) >= timeout {
			return codeError{fmt.Errorf("timed out after %v: %s", timeout, what), 3}
		}
		if err := a.pause(ctx, waitInterval(took)); err != nil {
			return err
		}
	}
}

// pause waits d, or less if ctx ends.
func (a *app) pause(ctx context.Context, d time.Duration) error {
	if a.sleep != nil {
		return a.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// daemonRunning reports whether the daemon answers on the gateway's
// socket, which it serves for as long as it runs.
func daemonRunning(dataDir string) bool {
	c, err := net.DialTimeout("unix", gateway.SocketPath(dataDir), time.Second)
	if err != nil {
		return false
	}
	c.Close()
	return true
}
