package gateway

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// #88: a file still being written into the store outside the endpoint
// (a copy in progress, an edit half done) is never hashed and taken as
// a version: its bytes are truncated, and as a write they would
// replicate, for a rewritable key over the friends' good copies.

// settling is a reconciler with settle time settle on clock c.
func (h *harness) settling(settle time.Duration, c *fakeNow) *reconciler {
	h.t.Helper()
	r := h.reconciler(ReconcileSettings{Settle: settle})
	r.now = c.now
	return r
}

// appendTo appends b to key's file in bucket's directory, as a copy in
// progress goes on writing it. Like edit, it moves the modification
// time on as a later write would see it: the kernel stamps a write with
// its coarse clock, which within one tick of an earlier write (or of a
// time edit moved on) is the same or earlier, and a file of the verified
// inode and modification time with another size is rot, not an edit
// (#114).
func (h *harness) appendTo(bucket, key string, b []byte) {
	h.t.Helper()
	p := filepath.Join(h.store, bucket, filepath.FromSlash(key))
	fi, err := os.Stat(p)
	if err != nil {
		h.t.Fatal(err)
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		h.t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		h.t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		h.t.Fatal(err)
	}
	if after, _ := os.Stat(p); after != nil && !after.ModTime().After(fi.ModTime()) {
		if err := os.Chtimes(p, fi.ModTime(), fi.ModTime().Add(time.Millisecond)); err != nil {
			h.t.Fatal(err)
		}
	}
}

// A file dropped into a bucket directory that changed within the settle
// time is left for a later pass; once it has been still for the settle
// time it is hashed whole and enqueued.
func TestAFileStillBeingCopiedIsNotEnqueued(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	clock := &fakeNow{t: time.Now()}
	r := h.settling(time.Hour, clock)
	h.drop("laptop", "data/ab/pack", []byte("the first half of a pack"))
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("a file being copied was enqueued: %v", got)
	}
	h.appendTo("laptop", "data/ab/pack", []byte(", and the second half"))
	clock.add(30 * time.Minute)
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("a file written to half an hour ago was enqueued: %v", got)
	}
	clock.add(time.Hour)
	h.pass(r)
	if got := h.reconciled("laptop"); got["data/ab/pack"] != state.OpPut {
		t.Fatalf("reconciled %v; want the copied file once it is still", got)
	}
	v, err := h.db.OwnVersion(t.Context(), "laptop", "data/ab/pack")
	if err != nil || v.SHA256 != sum([]byte("the first half of a pack, and the second half")) {
		t.Fatalf("version %+v %v; want the whole file", v, err)
	}
}

// An edit in place of an acknowledged rewritable key, half written, is
// not enqueued over the friend's copy: neither the reconciler nor the
// sampler takes it until it has been still for the settle time.
func TestAHalfWrittenEditNeverReplicatesOverGoodCopies(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	good := []byte("restic config, the version friends hold")
	h.fetch = &fakeFetch{held: map[string][]byte{"C config": good}}
	h.serve(false)
	h.putConfig(good)
	before, _ := h.db.OwnVersion(t.Context(), "laptop", "config")
	h.edit("laptop", "config", []byte("new conf"))
	clock := &fakeNow{t: time.Now()}
	r := h.settling(time.Hour, clock)
	h.pass(r)
	sclock := &fakeNow{t: time.Now().Add(-week)} // a week of samples ends now
	s := h.sampler(SampleSettings{Percent: 100}, sclock, nil)
	h.step(s)
	sclock.add(week)
	h.step(s)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("a half-written edit was enqueued: %v", got)
	}
	if rs := h.rots(); len(rs) != 0 {
		t.Fatalf("a half-written edit was taken for rot: %+v", rs)
	}
	if v, _ := h.db.OwnVersion(t.Context(), "laptop", "config"); v.Seq != before.Seq || v.SHA256 != sum(good) {
		t.Fatalf("version %+v; want the friends' version", v)
	}
	h.appendTo("laptop", "config", []byte(", written whole"))
	clock.add(2 * time.Hour)
	h.pass(r)
	v, _ := h.db.OwnVersion(t.Context(), "laptop", "config")
	if got := h.reconciled("laptop"); got["config"] != state.OpPut || v.SHA256 != sum([]byte("new conf, written whole")) {
		t.Fatalf("reconciled %v, version %+v; want the whole edit once still", got, v)
	}
}

// A file whose change time is ahead of the node's clock (a clock set
// back) settles by being seen unchanged for the settle time, and a
// change meanwhile starts the wait again.
func TestAFileChangedAheadOfTheClockSettlesByWatching(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	clock := &fakeNow{t: time.Now().Add(-24 * time.Hour)}
	r := h.settling(10*time.Minute, clock)
	h.drop("laptop", "data/00", []byte("first"))
	h.pass(r)
	clock.add(6 * time.Minute)
	h.appendTo("laptop", "data/00", []byte(" and more"))
	h.pass(r)
	clock.add(6 * time.Minute)
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("a file changed 6 minutes ago by watch was enqueued: %v", got)
	}
	clock.add(5 * time.Minute)
	h.pass(r)
	if got := h.reconciled("laptop"); got["data/00"] != state.OpPut {
		t.Fatalf("reconciled %v; want the file once seen still for the settle time", got)
	}
}

// A file that changes while it is hashed is not enqueued with the bytes
// read; the next pass takes it whole.
func TestAFileChangingWhileHashedIsNotEnqueued(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	h.drop("laptop", "data/00", []byte("the start"))
	grow := true
	r := h.reconciler(ReconcileSettings{Pace: func(_ context.Context, rd io.Reader) io.Reader {
		if grow {
			grow = false
			time.Sleep(10 * time.Millisecond) // past the filesystem's timestamp granularity
			h.appendTo("laptop", "data/00", []byte(" and the end"))
		}
		return rd
	}})
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("a file changing while hashed was enqueued: %v", got)
	}
	time.Sleep(10 * time.Millisecond)
	h.pass(r)
	v, _ := h.db.OwnVersion(t.Context(), "laptop", "data/00")
	if got := h.reconciled("laptop"); got["data/00"] != state.OpPut || v.SHA256 != sum([]byte("the start and the end")) {
		t.Fatalf("reconciled %v, version %+v; want the whole file", got, v)
	}
}

// A bucket with a file left to settle is passed again once it has, not
// only at the next daily pass.
func TestASettlingFileIsTakenWithoutWaitingForTheNextPass(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	h.drop("laptop", "data/00", []byte("dropped"))
	r := h.reconciler(ReconcileSettings{Settle: 300 * time.Millisecond, Every: time.Hour, Poll: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); r.loop(ctx) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(20 * time.Second)
	for h.reconciled("laptop")["data/00"] != state.OpPut {
		if time.Now().After(deadline) {
			t.Fatal("the settled file was not enqueued before the next pass")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
