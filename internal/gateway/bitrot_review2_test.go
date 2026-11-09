package gateway

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Regression tests from the second review of #31.

// putConfig stores restic's config (a rewritable key) with body and
// records that C acknowledged it.
func (h *harness) putConfig(body []byte) {
	h.t.Helper()
	if _, err := h.client("laptop").PutObject(h.t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("config"), Body: bytes.NewReader(body)}); err != nil {
		h.t.Fatal(err)
	}
	h.ack("C", "config")
}

// A file whose inode and modification time are the ones verified but
// whose size is not lost bytes without a write (a tail fsck cut off): no
// tool writes a file without moving its modification time. That is rot,
// repaired from a friend, not the user's write, which for a rewritable
// key would replicate over the friends' good copy.
func TestATruncatedFileIsRotNotAnEdit(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	good := []byte("restic config, rewritable: a longer body that loses its tail")
	h.fetch = &fakeFetch{held: map[string][]byte{"C config": good}}
	h.serve(false)
	h.putConfig(good)
	rows, _ := h.queueRows()
	p := filepath.Join(h.store, "laptop", "config")
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(p, 10); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatal(err)
	}
	h.pass(h.reconciler(ReconcileSettings{}))
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("the reconciler took the truncation for an edit: %v", got)
	}
	h.sampleWeek(SampleSettings{})
	if b, _ := h.stored("config"); !bytes.Equal(b, good) {
		t.Fatalf("store holds %q; want the friend's copy", b)
	}
	if rs := h.rots(); len(rs) != 1 || rs[0].RepairedAt.IsZero() {
		t.Fatalf("rots %+v", rs)
	}
	if n, _ := h.queueRows(); n != rows {
		t.Fatalf("queue rows %d; want %d: the truncation was enqueued", n, rows)
	}
}

// An unverified file of a rewritable key (a damaged store copied back
// onto a recovered node) whose bytes differ from the version a friend
// holds is not taken for the user's write: EP-11 would not hold it back,
// and it would replicate over the friend's copy. Its version comes back
// from the friend, recorded and alerted as rot of an unverified file.
func TestAnUnverifiedRewritableKeyIsRepairedNotCaptured(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	good := []byte("restic config, rewritable, version 1")
	h.fetch = &fakeFetch{held: map[string][]byte{"C config": good}}
	h.serve(false)
	h.putConfig(good)
	if _, err := h.db.Exec(`UPDATE queue SET stamp = NULL WHERE key = 'config'`); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.store, "laptop", "config"), []byte("damaged copy of the config"), 0o600); err != nil {
		t.Fatal(err)
	}
	rows, _ := h.queueRows()
	h.sampleWeek(SampleSettings{})
	if b, _ := h.stored("config"); !bytes.Equal(b, good) {
		t.Fatalf("store holds %q; want the friend's copy", b)
	}
	rs := h.rots()
	if len(rs) != 1 || rs[0].RepairedAt.IsZero() || !strings.Contains(rs[0].Detail, "unverified") {
		t.Fatalf("rots %+v", rs)
	}
	if n, _ := h.queueRows(); n != rows {
		t.Fatalf("queue rows %d; want %d: the damaged copy was enqueued", n, rows)
	}
}

// A write that lands between the sampler's second read and its taking
// the key's lock decides: the sampler, finding the row and the file
// changed, takes nothing for rot and enqueues nothing of what it read.
func TestAWriteBeforeTheSamplersLockWins(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.fetch = &fakeFetch{held: map[string][]byte{}}
	h.serve(false)
	h.putAll("laptop", "data/00")
	h.ack("C", "data/00")
	h.edit("laptop", "data/00", []byte("EDITED of data/00"))
	written := []byte("written before the lock")
	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	s := h.sampler(SampleSettings{Percent: 100}, clock, nil)
	s.beforeLock = func() {
		if _, err := h.client("laptop").PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
			Key: aws.String("data/00"), Body: bytes.NewReader(written)}); err != nil {
			t.Error(err)
		}
	}
	h.step(s)
	clock.add(week)
	h.step(s)
	if rs := h.rots(); len(rs) != 0 {
		t.Fatalf("rots %+v", rs)
	}
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("enqueued what it read: %v", got)
	}
	if v, _ := h.db.OwnVersion(t.Context(), "laptop", "data/00"); v.SHA256 != sum(written) {
		t.Fatalf("version %+v; want the write", v)
	}
}

// The same for the reconciler's re-hash of an edited file.
func TestAWriteBeforeTheReconcilersLockWins(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	h.putAll("laptop", "data/00")
	h.edit("laptop", "data/00", []byte("EDITED of data/00"))
	written := []byte("written before the lock")
	r := h.reconciler(ReconcileSettings{})
	r.beforeLock = func() {
		if _, err := h.client("laptop").PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
			Key: aws.String("data/00"), Body: bytes.NewReader(written)}); err != nil {
			t.Error(err)
		}
	}
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("enqueued what it read: %v", got)
	}
	if v, _ := h.db.OwnVersion(t.Context(), "laptop", "data/00"); v.SHA256 != sum(written) {
		t.Fatalf("version %+v; want the write", v)
	}
}
