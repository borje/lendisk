package cli

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/state"
)

// offsiteNode is a node with a bucket and friends B, C and D written
// straight into its lendisk.db, the way the daemon would have placed and
// acknowledged rows: what `status` reads.
type offsiteNode struct {
	*node
	db  *state.DB
	now time.Time
}

func newOffsiteNode(t *testing.T, bucket, profile string) *offsiteNode {
	t.Helper()
	n := newNode(t)
	n.mustInit(t)
	n.addBucket(t, bucket, profile)
	o := &offsiteNode{node: n, db: openDB(t, n), now: time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)}
	o.db.Clock = func() time.Time { return o.now }
	for _, f := range []string{"B", "C", "D"} {
		if err := o.db.AddFriendship(t.Context(), state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 1 << 30,
			PeerNumber: 1 << 30, CreatedAt: o.now}); err != nil {
			t.Fatal(err)
		}
	}
	return o
}

// put captures a completed put, placed as placement would place it.
func (o *offsiteNode) put(bucket, key, class string, size int64) int64 {
	o.t.Helper()
	ctx := context.Background()
	seqs, err := o.db.BeginCaptures(ctx, []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: class}})
	if err != nil {
		o.t.Fatal(err)
	}
	if _, err := o.db.FinishPut(ctx, seqs[0], size, "h-"+key); err != nil {
		o.t.Fatal(err)
	}
	return seqs[0]
}

// places lists the places row seq is queued at.
func (o *offsiteNode) queuedAt(seq int64) []string {
	o.t.Helper()
	rows, err := o.db.Query(`SELECT place FROM placement WHERE seq = ? AND state = 'queued' ORDER BY place`, seq)
	if err != nil {
		o.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		rows.Scan(&p)
		out = append(out, p)
	}
	return out
}

// ack acknowledges row seq wherever it is queued, except at the places
// in refuse, which refuse it for good (a BadDigest for bytes that match).
func (o *offsiteNode) ack(seq int64, refuse ...string) {
	o.t.Helper()
	ctx := context.Background()
	var sha string
	o.db.QueryRow(`SELECT sha256 FROM queue WHERE seq = ?`, seq).Scan(&sha)
	for _, p := range o.queuedAt(seq) {
		var err error
		if strings.Contains(strings.Join(refuse, " "), p) {
			err = o.db.RefusePlacement(ctx, p, seq, "BadDigest")
		} else {
			err = o.db.AckPlacement(ctx, p, seq, sha, o.now)
		}
		if err != nil {
			o.t.Fatal(err)
		}
	}
}

// answered records that a re-placement pass has answered the pool as
// it is now and placed nothing more, as the daemon's worker would; the
// fixture runs no worker. Until a pass has, an object short of places
// waits rather than being stuck (passDue, #37).
func (o *offsiteNode) answered() {
	o.t.Helper()
	fp, err := o.db.PoolFingerprint(context.Background())
	if err != nil {
		o.t.Fatal(err)
	}
	if _, err := o.db.Exec(`INSERT OR REPLACE INTO replace_pass (id, pool, done_at) VALUES (1, ?, ?)`,
		fp, o.now.Unix()); err != nil {
		o.t.Fatal(err)
	}
}

// daemonUp stands in for a running daemon: something answers on the
// gateway's socket, which is how `status --wait` tells one is running.
func (o *offsiteNode) daemonUp() {
	o.t.Helper()
	sock := gateway.SocketPath(o.data)
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		o.t.Fatal(err)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		o.t.Fatal(err)
	}
	o.t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
}

func bucketOf(t *testing.T, st Status, name string) StatusBucket {
	t.Helper()
	for _, b := range st.Buckets {
		if b.Name == name {
			return b
		}
	}
	t.Fatalf("status has no bucket %s", name)
	return StatusBucket{}
}

const (
	snapA = "snapshots/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	snapB = "snapshots/fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
)

// REPL-11 (#81): the daemon's folder stores the bucket's short puts and
// clears the marks of what changed, and status, reading the stored rows
// with the keys changed since, follows each acknowledgement as before.
func TestTheDaemonFoldsTheOffsiteWatermarksChanges(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	t.Cleanup(func() { cancel(); <-stopped })
	go func() {
		defer close(stopped)
		(&offsiteFolder{DB: o.db, Every: 10 * time.Millisecond, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Run(ctx)
	}()
	folded := func() {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			var marks, bases int
			if err := o.db.QueryRow(`SELECT (SELECT count(*) FROM offsite_dirty),
				(SELECT count(*) FROM offsite_basis WHERE bucket = 'laptop')`).Scan(&marks, &bases); err != nil {
				t.Fatal(err)
			}
			if marks == 0 && bases == 1 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("not folded: %d marks, %d bases", marks, bases)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	d1 := o.put("laptop", "data/1", "data", 1000)
	o.ack(d1)
	folded()
	if b := bucketOf(t, o.status(), "laptop"); !b.Offsite.Complete || b.Offsite.Watermark != d1 {
		t.Fatalf("data/1 acknowledged, folded: offsite %+v", b.Offsite)
	}
	d2 := o.put("laptop", "data/2", "data", 1000)
	if b := bucketOf(t, o.status(), "laptop"); b.Offsite.Complete || b.Offsite.Watermark != d1 || b.Offsite.Waiting.Objects != 1 {
		t.Fatalf("data/2 queued, not folded yet: offsite %+v", b.Offsite)
	}
	folded()
	if b := bucketOf(t, o.status(), "laptop"); b.Offsite.Complete || b.Offsite.Watermark != d1 || b.Offsite.Waiting.Objects != 1 {
		t.Fatalf("data/2 queued, folded: offsite %+v", b.Offsite)
	}
	o.ack(d2)
	if b := bucketOf(t, o.status(), "laptop"); !b.Offsite.Complete || b.Offsite.Watermark != d2 {
		t.Fatalf("data/2 acknowledged: offsite %+v", b.Offsite)
	}
}

// #118: the daemon checks the stored queue sums against the rows at
// start and then every TallyEvery, repairs a bucket's that drifted
// (here by hand) and logs it as a warning naming the bucket: the
// repository size the quota exchange tells friends is right again.
func TestTheDaemonRepairsADriftedQueueTallyAndWarns(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	o.ack(o.put("laptop", "data/1", "data", 1000))
	ctx := t.Context()
	want, err := o.db.RepositorySize(ctx)
	if err != nil || want != 1000 {
		t.Fatalf("repository size %d, want 1000 (%v)", want, err)
	}
	if _, err := o.db.Exec(`UPDATE queue_tally SET live_data_bytes = live_data_bytes + 5000 WHERE bucket = 'laptop'`); err != nil {
		t.Fatal(err)
	}
	var log syncBuffer
	ctx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	t.Cleanup(func() { cancel(); <-stopped })
	go func() {
		defer close(stopped)
		(&offsiteFolder{DB: o.db, Every: 10 * time.Millisecond, TallyEvery: 10 * time.Millisecond,
			Log: slog.New(slog.NewTextHandler(&log, nil))}).Run(ctx)
	}()
	for deadline := time.Now().Add(10 * time.Second); ; {
		got, err := o.db.RepositorySize(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got == want && strings.Contains(log.String(), "level=WARN") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("repository size %d, want %d; log:\n%s", got, want, log.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if l := log.String(); !strings.Contains(l, "stored queue sums differed") || !strings.Contains(l, "bucket=laptop") ||
		strings.Count(l, "level=WARN") != 1 {
		t.Fatalf("the repair's log:\n%s", l)
	}
	// Skewed again: the next check, TallyEvery later, repairs it too.
	if _, err := o.db.Exec(`UPDATE queue_tally SET live_data_bytes = 0 WHERE bucket = 'laptop'`); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); strings.Count(log.String(), "level=WARN") < 2; {
		if time.Now().After(deadline) {
			t.Fatalf("not repaired again; log:\n%s", log.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, err := o.db.RepositorySize(ctx); err != nil || got != want {
		t.Fatalf("repository size %d, want %d (%v)", got, want, err)
	}
}

// REPL-4, REPL-11 (#20): for restic, status names the latest snapshot
// and whether it is offsite at the bucket's copies, by restic's short ID;
// while the next one is on its way it says what it waits for, and names
// the earlier one that is offsite. status --json carries the same under
// buckets[].offsite.
func TestStatusSaysWhichResticSnapshotIsOffsite(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	if _, text, _ := o.run("", "", "status"); !strings.Contains(text, "    nothing written yet\n") {
		t.Fatalf("status of an empty bucket:\n%s", text)
	}
	for _, s := range []int64{o.put("laptop", "config", "metadata", 100), o.put("laptop", "data/1", "data", 1000),
		o.put("laptop", "index/1", "metadata", 100), o.put("laptop", snapA, "snapshot", 100)} {
		o.ack(s)
	}
	b := bucketOf(t, o.status(), "laptop")
	if !b.Offsite.Complete || b.Offsite.Snapshot == nil || b.Offsite.Snapshot.ID != strings.TrimPrefix(snapA, "snapshots/") ||
		!b.Offsite.Snapshot.Offsite || b.Offsite.LatestSnapshot == nil || b.Offsite.LatestSnapshot.Key != snapA ||
		b.Offsite.Watermark != b.Offsite.Latest || b.Offsite.Snapshot.CapturedAt != "2026-11-08T10:00:00Z" {
		t.Fatalf("offsite %+v", b.Offsite)
	}
	if _, text, _ := o.run("", "", "status"); !strings.Contains(text, "    snapshot 01234567 is offsite at 2 copies\n") {
		t.Fatalf("status:\n%s", text)
	}

	o.now = o.now.Add(24 * time.Hour)
	o.put("laptop", "data/2", "data", 5000)
	o.put("laptop", "data/3", "data", 7000)
	o.ack(o.put("laptop", "index/2", "metadata", 100))
	o.put("laptop", snapB, "snapshot", 100)
	b = bucketOf(t, o.status(), "laptop")
	if b.Offsite.Complete || b.Offsite.Snapshot.Key != snapA || b.Offsite.LatestSnapshot.Key != snapB ||
		b.Offsite.LatestSnapshot.Offsite || b.Offsite.Waiting.Objects != 3 || b.Offsite.Waiting.Bytes != 12100 ||
		b.Offsite.Stuck.Objects != 0 {
		t.Fatalf("offsite with the second snapshot on its way: %+v", b.Offsite)
	}
	_, text, _ := o.run("", "", "status")
	for _, want := range []string{
		"    snapshot fedcba98 is not offsite at 2 copies yet: 3 objects, 12100 bytes still to reach 2 places\n",
		"    snapshot 01234567, an earlier one, is offsite at 2 copies\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("status lacks %q:\n%s", want, text)
		}
	}
}

// REPL-11 (#20): Kopia's snapshot keys are index blobs, which name no
// snapshot, so status names the time: every snapshot Kopia finished
// before the second the offsite index was written in is offsite. A
// generic bucket has no snapshot keys: status names the time before
// which every write is offsite. Capture times are whole seconds, so both
// claims are strictly before a second that may also hold a write that
// is not offsite (#20 review).
func TestStatusNamesATimeForKopiaAndGenericBuckets(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "kopia", "kopia")
	o.addBucket(t, "files", "generic")
	idx := "xn0_044bfd62a9837c42e60a1084b0fc2482-sa69963c6d5b144b1146-c1"
	idx2 := "xn0_8c87047a9eb0795baa9b460fb8844608-s6331a632e5515605146-c1"
	for _, s := range []int64{o.put("kopia", "kopia.repository", "metadata", 100), o.put("kopia", "p186cf", "data", 1000),
		o.put("kopia", "qe46e1", "data", 100), o.put("kopia", idx, "snapshot", 100),
		o.put("files", "a.txt", "data", 10)} {
		o.ack(s)
	}
	// In the same second: another session's pack and index, and another
	// generic write, none of them offsite yet.
	o.put("kopia", "p681b4", "data", 1000)
	o.put("kopia", idx2, "snapshot", 100)
	o.put("files", "b.txt", "data", 20)
	st := o.status()
	if k := bucketOf(t, st, "kopia").Offsite; k.Snapshot == nil || k.Snapshot.Key != idx || k.Snapshot.ID != "" ||
		k.LatestSnapshot.Key != idx2 || k.Complete || k.Before != "2026-11-08T10:00:00Z" {
		t.Fatalf("kopia offsite %+v", k)
	}
	if g := bucketOf(t, st, "files").Offsite; g.Snapshot != nil || g.Through != "2026-11-08T10:00:00Z" || g.Complete ||
		g.Waiting.Objects != 1 || g.Before != "2026-11-08T10:00:00Z" {
		t.Fatalf("generic offsite %+v", g)
	}
	_, text, _ := o.run("", "", "status")
	for _, want := range []string{
		"    the snapshot key " + idx2 + " of 2026-11-08 10:00:00 UTC is not offsite at 2 copies yet: 2 objects, 1100 bytes still to reach 2 places\n",
		"    every snapshot finished before 2026-11-08 10:00:00 UTC is offsite at 2 copies (through " + idx + ")\n",
		"    every write captured before 2026-11-08 10:00:00 UTC is offsite at 2 copies; later ones are not yet: 1 objects, 20 bytes still to reach 2 places\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("status lacks %q:\n%s", want, text)
		}
	}
}

// REPL-5 (#20 review): status --wait waits at least ten times as long
// as its last read of the watermark took, and never less than 2 s, so a
// large bucket does not keep a small machine busy.
func TestStatusWaitBacksOffOnASlowRead(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	o.daemonUp()
	s1 := o.put("laptop", snapA, "snapshot", 100)
	now := o.now
	o.node.clock = func() time.Time { now = now.Add(700 * time.Millisecond); return now } // every read is slow
	var pauses []time.Duration
	o.node.sleep = func(_ context.Context, d time.Duration) error {
		pauses = append(pauses, d)
		if len(pauses) == 2 {
			o.ack(s1)
		}
		return nil
	}
	if code, _, errOut := o.run("", "", "status", "--wait", "laptop", "--timeout", "1h"); code != 0 {
		t.Fatalf("status --wait: exit %d %s", code, errOut)
	}
	// The clock moves 0.7 s per reading, and the read itself reads it too
	// (lendisk.db judges which friends are gone by it): at least 0.7 s.
	if len(pauses) != 2 || pauses[0] < 7*time.Second || pauses[0]%(7*time.Second) != 0 {
		t.Fatalf("pauses %v, want ten times a read of n × 0.7 s", pauses)
	}
	o.node.clock = func() time.Time { return now }
	s2 := o.put("laptop", snapB, "snapshot", 100)
	pauses = nil
	o.node.sleep = func(_ context.Context, d time.Duration) error { pauses = append(pauses, d); o.ack(s2); return nil }
	if code, _, errOut := o.run("", "", "status", "--wait", "laptop"); code != 0 || len(pauses) != 1 || pauses[0] != 2*time.Second {
		t.Fatalf("a fast read: exit %d, pauses %v %s", code, pauses, errOut)
	}
}

// REPL-5 (#20): status --wait <bucket> exits 0 once the latest snapshot
// captured before the wait started is offsite at copies, printing which;
// a snapshot captured during the wait is not waited for.
func TestStatusWaitExitsOnceTheSnapshotIsOffsite(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	o.daemonUp()
	d := o.put("laptop", "data/1", "data", 1000)
	s := o.put("laptop", snapA, "snapshot", 100)
	sleeps := 0
	o.node.sleep = func(context.Context, time.Duration) error {
		sleeps++
		switch sleeps {
		case 1: // the daemon sends the data; a new backup starts
			o.ack(d)
			o.put("laptop", "data/2", "data", 1000)
			o.put("laptop", snapB, "snapshot", 100)
		case 2:
			o.ack(s)
		}
		return nil
	}
	code, out, errOut := o.run("", "", "status", "--wait", "laptop")
	if code != 0 || out != "snapshot 01234567 is offsite at 2 copies\n" || sleeps != 2 {
		t.Fatalf("status --wait: exit %d after %d polls\n%s%s", code, sleeps, out, errOut)
	}
	// Already offsite: no wait, even with no daemon.
	o2 := newOffsiteNode(t, "laptop", "restic")
	o2.ack(o2.put("laptop", snapA, "snapshot", 100))
	o2.node.sleep = func(context.Context, time.Duration) error { t.Fatal("waited"); return nil }
	if code, out, errOut := o2.run("", "", "status", "--wait", "laptop"); code != 0 || !strings.Contains(out, "is offsite") {
		t.Fatalf("status --wait when already offsite: exit %d\n%s%s", code, out, errOut)
	}
}

// REPL-5 (#20): on timeout status --wait exits 3 and says what is still
// short; the timeout runs on the injected clock.
func TestStatusWaitTimesOut(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	o.daemonUp()
	o.put("laptop", "data/1", "data", 1000)
	o.put("laptop", snapA, "snapshot", 100)
	now := o.now
	o.node.clock = func() time.Time { return now }
	o.node.sleep = func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }
	code, out, errOut := o.run("", "", "status", "--wait", "laptop", "--timeout", "10m")
	if code != 3 || out != "" || !strings.Contains(errOut, "timed out after 10m0s: snapshot 01234567 is not offsite at 2 copies yet: 2 objects, 1100 bytes still to reach 2 places") {
		t.Fatalf("status --wait --timeout 10m: exit %d\n%s%s", code, out, errOut)
	}
	if got := now.Sub(o.now); got < 10*time.Minute || got > 11*time.Minute {
		t.Fatalf("waited %v of injected time", got)
	}
	if code, _, _ := o.run("", "", "status", "--wait", "laptop", "--timeout", "-1s"); code != 2 {
		t.Fatalf("a negative timeout: exit %d, want 2", code)
	}
	if code, _, _ := o.run("", "", "status", "--wait", "laptop", "--json"); code != 2 {
		t.Fatalf("--wait with --json: exit %d, want 2", code)
	}
	if code, _, errOut := o.run("", "", "status", "--wait", "nosuch"); code != 1 || !strings.Contains(errOut, `no bucket "nosuch"`) {
		t.Fatalf("--wait for an unknown bucket: exit %d %s", code, errOut)
	}
}

// REPL-5, REPL-11 (#20, the #57 review's finding): with one friend
// refusing everything, the snapshot is not claimed offsite. status says
// why: the objects only that friend was to hold have no copy on its way
// to make up copies. status --wait does not wait silently for what
// cannot come: it exits 4 with the same reason.
func TestStatusWaitStopsWhenAFriendRefusesEverything(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	o.daemonUp()
	var seqs []int64
	for _, k := range []string{"data/1", "data/2", "data/3", "data/4", "data/5", "data/6"} {
		seqs = append(seqs, o.put("laptop", k, "data", 1000))
	}
	seqs = append(seqs, o.put("laptop", "index/1", "metadata", 100), o.put("laptop", snapA, "snapshot", 100))
	refused := 0
	for _, s := range seqs[:6] {
		for _, p := range o.queuedAt(s) {
			if p == "D" {
				refused++
			}
		}
	}
	if refused == 0 {
		t.Fatal("no data at D")
	}
	for _, s := range seqs {
		o.ack(s, "D")
	}
	o.answered()
	b := bucketOf(t, o.status(), "laptop")
	if b.Offsite.Snapshot != nil || b.Offsite.LatestSnapshot.Offsite || b.Offsite.Stuck.Objects != int64(refused) ||
		b.Offsite.Stuck.Refused != int64(refused) || b.BelowCopies.Objects != int64(refused) {
		t.Fatalf("with D refusing everything: offsite %+v, below %+v", b.Offsite, b.BelowCopies)
	}
	_, text, _ := o.run("", "", "status")
	stuck := "of them have no copy on its way to make up 2 places (" + strconv.Itoa(refused) + " refused by a friend)"
	if !strings.Contains(text, "    snapshot 01234567 is not offsite at 2 copies yet: "+strconv.Itoa(refused)+" objects, "+
		strconv.Itoa(refused*1000)+" bytes still to reach 2 places; "+strconv.Itoa(refused)+" "+stuck) {
		t.Fatalf("status:\n%s", text)
	}
	o.node.sleep = func(context.Context, time.Duration) error { t.Fatal("waited for what cannot come"); return nil }
	code, out, errOut := o.run("", "", "status", "--wait", "laptop")
	if code != 4 || out != "" || !strings.Contains(errOut, stuck) {
		t.Fatalf("status --wait: exit %d\n%s%s", code, out, errOut)
	}
}

// REPL-5, REPL-11 (#20 second review): a completed upload whose row
// cannot be matched to the object in the store (its xattrs lost) may
// never settle. status says so, with what to do, rather than "still
// being written"; status --wait exits 4.
func TestStatusWaitStopsOnAnUnmatchableUpload(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	o.daemonUp()
	seqs, err := o.db.BeginCaptures(t.Context(), []state.Capture{{Bucket: "laptop", Key: "data/big", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := o.db.MarkUnmatchable(t.Context(), seqs[0], "no upload ID"); err != nil {
		t.Fatal(err)
	}
	o.put("laptop", "index/1", "metadata", 100)
	o.put("laptop", snapA, "snapshot", 100)
	b := bucketOf(t, o.status(), "laptop")
	if b.Offsite.Stuck.Unmatchable != 1 || b.Offsite.Waiting.Pending != 0 {
		t.Fatalf("offsite %+v", b.Offsite)
	}
	why := "1 of them have no copy on its way to make up 2 places (1 cannot be matched to the store: restore the store's xattrs or rewrite the key)"
	_, text, _ := o.run("", "", "status")
	if !strings.Contains(text, why) || strings.Contains(text, "still being written") {
		t.Fatalf("status:\n%s", text)
	}
	o.node.sleep = func(context.Context, time.Duration) error { t.Fatal("waited for what cannot come"); return nil }
	if code, out, errOut := o.run("", "", "status", "--wait", "laptop"); code != 4 || out != "" || !strings.Contains(errOut, why) {
		t.Fatalf("status --wait: exit %d\n%s%s", code, out, errOut)
	}
}

// REPL-5 (#20): status --wait reads lendisk.db; it needs the daemon to
// make progress, so with none answering it says so and exits 1 at once
// rather than wait for nothing.
func TestStatusWaitWithNoDaemon(t *testing.T) {
	parallel(t, 0)
	o := newOffsiteNode(t, "laptop", "restic")
	o.put("laptop", snapA, "snapshot", 100)
	o.node.sleep = func(context.Context, time.Duration) error { t.Fatal("waited"); return nil }
	code, _, errOut := o.run("", "", "status", "--wait", "laptop")
	if code != 1 || !strings.Contains(errOut, "the daemon is not running") || !strings.Contains(errOut, "lendisk serve") {
		t.Fatalf("status --wait with no daemon: exit %d %s", code, errOut)
	}
}
