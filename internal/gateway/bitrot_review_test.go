package gateway

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/state"
)

// Regression tests from the review of #31: rot is told from an edit by
// the file's identity (inode, size, modification time) recorded when
// this node wrote or verified the bytes, never by a clock.

// sampleWeek runs a sampler at 100% over a week of its clock.
func (h *harness) sampleWeek(s SampleSettings) {
	h.t.Helper()
	if s.Percent == 0 {
		s.Percent = 100
	}
	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	sm := h.sampler(s, clock, nil)
	h.step(sm)
	clock.add(week)
	h.step(sm)
}

// An edit in place right after the capture (formerly: in the same
// second as queue.local) is the user's write, not rot: it is kept and
// enqueued.
func TestAnEditRightAfterItsCaptureIsAnEdit(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	good := bytes.Repeat([]byte("good pack "), 100)
	h.fetch = &fakeFetch{held: map[string][]byte{"C data/01": good}}
	h.serve(false)
	if _, err := h.client("laptop").PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("data/01"), Body: bytes.NewReader(good)}); err != nil {
		t.Fatal(err)
	}
	edit := bytes.Repeat([]byte("user edit "), 100)
	h.edit("laptop", "data/01", edit)
	h.ack("C", "data/01")
	h.sampleWeek(SampleSettings{})
	if b, _ := h.stored("data/01"); !bytes.Equal(b, edit) {
		t.Fatalf("the user's edit was replaced: %q", b)
	}
	if rs := h.rots(); len(rs) != 0 {
		t.Fatalf("an edit taken for rot: %+v", rs)
	}
	if got := h.reconciled("laptop"); got["data/01"] != state.OpPut {
		t.Fatalf("reconciled %v; want the edit enqueued", got)
	}
}

// A key marked held from a listing, its bytes never hashed (recovered,
// undeleted, a fetch that failed), is unverified: other bytes there are
// the user's write, never rot, and are not replaced.
func TestBytesNeverVerifiedAreNeverRot(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	good := bytes.Repeat([]byte("good pack "), 100)
	h.fetch = &fakeFetch{held: map[string][]byte{"C data/01": good}}
	h.serve(false)
	h.putAll("laptop", "data/01")
	v, _ := h.db.OwnVersion(t.Context(), "laptop", "data/01")
	// As recovery records it: the version friends hold, not held here.
	if _, err := h.db.Exec(`UPDATE queue SET sha256 = ?, size = ?, local = NULL, stamp = NULL WHERE seq = ?`,
		sum(good), len(good), v.Seq); err != nil {
		t.Fatal(err)
	}
	h.ack("C", "data/01")
	other := []byte("bytes the user put there")
	if err := os.WriteFile(filepath.Join(h.store, "laptop", "data", "01"), other, 0o600); err != nil {
		t.Fatal(err)
	}
	h.pass(h.reconciler(ReconcileSettings{})) // marks it held, unverified
	if v, _ := h.db.OwnVersion(t.Context(), "laptop", "data/01"); !v.Held() || v.Stamp != "" {
		t.Fatalf("after the pass: %+v", v)
	}
	h.sampleWeek(SampleSettings{})
	if b, _ := h.stored("data/01"); !bytes.Equal(b, other) {
		t.Fatalf("bytes never verified were replaced as rot: %q", b)
	}
	if rs := h.rots(); len(rs) != 0 {
		t.Fatalf("rots %+v", rs)
	}
	if v, _ := h.db.OwnVersion(t.Context(), "laptop", "data/01"); v.SHA256 != sum(other) {
		t.Fatalf("version %+v; want the bytes there enqueued as a write", v)
	}
}

// A file whose times are ahead of the node's clock (fake-hwclock, a
// network filesystem's server clock) is hashed once, not at every pass:
// the identity recorded is the file's own, whatever the clock says.
func TestAFileAheadOfTheClockIsHashedOnce(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.db.Clock = func() time.Time { return time.Now().Add(-time.Hour) }
	h.serve(false)
	h.putAll("laptop", "data/00")
	p := filepath.Join(h.store, "laptop", "data", "00")
	future := time.Now().Add(24 * time.Hour)
	if err := os.Chtimes(p, future, future); err != nil {
		t.Fatal(err)
	}
	var hashed atomic.Int32
	r := h.reconciler(ReconcileSettings{Pace: func(_ context.Context, rd io.Reader) io.Reader { hashed.Add(1); return rd }})
	for range 3 {
		h.pass(r)
	}
	if n := hashed.Load(); n != 1 {
		t.Fatalf("hashed %d times in 3 passes; want once", n)
	}
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("reconciled %v", got)
	}
}

// Rot under a metadata-only change (a chmod, as a tagging or retention
// call would change the file's attributes) is still rot: the bytes are
// not the user's, and for a rewritable key they would otherwise be
// replicated over the friends' good copy.
func TestRotUnderAMetadataChangeIsStillRot(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	good := []byte("restic config, rewritable: version 2")
	h.fetch = &fakeFetch{held: map[string][]byte{"C config": good}}
	h.serve(false)
	if _, err := h.client("laptop").PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("config"), Body: bytes.NewReader(good)}); err != nil {
		t.Fatal(err)
	}
	h.ack("C", "config")
	rows, _ := h.queueRows()
	h.rot("laptop", "config")
	if err := os.Chmod(filepath.Join(h.store, "laptop", "config"), 0o640); err != nil {
		t.Fatal(err)
	}
	h.sampleWeek(SampleSettings{})
	if b, _ := h.stored("config"); !bytes.Equal(b, good) {
		t.Fatalf("store holds %q; want the friend's verified copy", b)
	}
	if rs := h.rots(); len(rs) != 1 || rs[0].RepairedAt.IsZero() {
		t.Fatalf("rots %+v", rs)
	}
	if n, _ := h.queueRows(); n != rows {
		t.Fatalf("queue rows %d; want %d: rot was enqueued as the user's write", n, rows)
	}
}

// The sample holds no key lock while it reads a mismatched object again:
// a write of the key meanwhile goes through, and the sampler, finding
// the row changed under the lock, takes nothing for rot.
func TestTheSampleHoldsNoLockWhileItHashes(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.fetch = &fakeFetch{held: map[string][]byte{}}
	h.serve(false)
	h.putAll("laptop", "data/00")
	h.ack("C", "data/00")
	h.rot("laptop", "data/00")
	reading, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	pace := func(_ context.Context, rd io.Reader) io.Reader {
		if reads.Add(1) == 2 { // the second read of the mismatch
			close(reading)
			<-release
		}
		return rd
	}
	done := make(chan struct{})
	go func() { defer close(done); h.sampleWeek(SampleSettings{Pace: pace}) }()
	within(t, "the sampler's second read", reading)
	if _, err := h.client("laptop").PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("data/00"), Body: bytes.NewReader([]byte("written meanwhile"))}); err != nil {
		t.Fatal(err)
	}
	close(release)
	within(t, "the sampler's step", done)
	if rs := h.rots(); len(rs) != 0 {
		t.Fatalf("rots %+v", rs)
	}
	if b, _ := h.stored("data/00"); !bytes.Equal(b, []byte("written meanwhile")) {
		t.Fatalf("store holds %q", b)
	}
}
