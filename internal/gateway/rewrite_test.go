package gateway

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/state"
)

// rewriteMeta is the metadata a borrower sends with a rewrite of a key
// whose stored version hashes to replaces (LEND-4, #74).
func rewriteMeta(body []byte, replaces string) map[string]string {
	m := hashMeta(body)
	m[MetaReplaces] = replaces
	return m
}

// LEND-4, RET-1 (v0.11.3, #74): a rewrite the borrower marks with the
// hash of the version it replaces is stored, and the replaced version is
// moved into a tomb, as a delete would move it, with the retention
// window: its bytes and inode kept, and both versions counted against
// quota until the old one is purged, after the window and not before.
// The mark is not stored with the object.
func TestMarkedRewriteKeepsTheReplacedVersionAsATomb(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	ctx := context.Background()
	v1, v2, v3 := []byte("maintenance schedule one"), []byte("schedule two"), []byte("schedule three, longer")
	if err := l.put("kopia.maintenance", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(filepath.Join(l.dir, "kopia.maintenance"))
	usage := l.usage()
	if err := l.put("kopia.maintenance", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatalf("marked rewrite: %v", err)
	}
	if got, ok := l.stored("kopia.maintenance"); !ok || !bytes.Equal(got, v2) {
		t.Fatalf("the key holds %q", got)
	}
	ts := l.tombs("kopia.maintenance")
	if len(ts) != 1 {
		t.Fatalf("tombstones %+v", ts)
	}
	tomb := ts[0]
	if tomb.SHA256 != sum(v1) || tomb.Size != int64(len(v1)) || tomb.Pending || !tomb.Rewrite ||
		!tomb.DeletedAt.Equal(c.Now()) || !tomb.PurgeAfter.Equal(c.Now().Add(30*day)) {
		t.Fatalf("tombstone %+v", tomb)
	}
	if after, err := os.Stat(filepath.Join(l.dir, filepath.FromSlash(tomb.Tomb))); err != nil || !os.SameFile(before, after) {
		t.Fatalf("the tomb is not the replaced object renamed: %v", err)
	}
	l.wantUsage(usage + ov + int64(len(v2)))
	head, err := l.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: l.bucket, Key: aws.String("kopia.maintenance")})
	if err != nil || head.Metadata["lendisk-sha256"] != sum(v2) || head.Metadata[MetaReplaces] != "" {
		t.Fatalf("stored metadata %v (%v)", head.Metadata, err)
	}

	// A second rewrite makes a second tomb; the retry of it, after a lost
	// answer, is the same bytes and changes nothing.
	c.advance(time.Hour)
	if err := l.put("kopia.maintenance", v3, rewriteMeta(v3, sum(v2))); err != nil {
		t.Fatal(err)
	}
	if err := l.put("kopia.maintenance", v3, rewriteMeta(v3, sum(v2))); err != nil {
		t.Fatalf("retried rewrite: %v", err)
	}
	if ts := l.tombs("kopia.maintenance"); len(ts) != 2 || ts[1].SHA256 != sum(v2) {
		t.Fatalf("tombstones %+v", ts)
	}
	full := usage + 2*ov + int64(len(v2)+len(v3))
	l.wantUsage(full)

	// The window of each is its own: v1's tomb goes first.
	c.advance(30*day - time.Hour - time.Second)
	l.purge()
	if len(l.tombs("kopia.maintenance")) != 2 {
		t.Fatal("purged early")
	}
	c.advance(time.Second)
	l.purge()
	if ts := l.tombs("kopia.maintenance"); len(ts) != 1 || ts[0].SHA256 != sum(v2) {
		t.Fatalf("after v1's window: %+v", ts)
	}
	if _, err := l.tombBytes(tomb); !os.IsNotExist(err) {
		t.Fatalf("v1's tomb after its window: %v", err)
	}
	l.wantUsage(full - ov - int64(len(v1)))
	c.advance(time.Hour)
	l.purge()
	if ts := l.tombs("kopia.maintenance"); len(ts) != 0 {
		t.Fatalf("after v2's window: %+v", ts)
	}
	l.wantUsage(usage - ov - int64(len(v1)) + ov + int64(len(v3)))
	if got, _ := l.stored("kopia.maintenance"); !bytes.Equal(got, v3) {
		t.Fatalf("the key holds %q", got)
	}
}

// LEND-4 (#74): only a rewrite marked with the hash the key holds is
// stored. An unmarked write of other bytes, a stale mark (an older
// version's hash), a mark on a key that holds nothing but a deleted
// object's tomb, a body that only claims its hash, and a rewrite over
// quota (TestRewriteOverQuotaRefused) are refused as before, and nothing
// moves. A marked write of an absent key with no tombs is an ordinary
// new object.
func TestUnmarkedOrStaleRewriteRefused(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	v1, v2, v3 := []byte("config one"), []byte("config two"), []byte("config three")
	if err := l.put("config", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("config", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	unchanged := func(when string) {
		t.Helper()
		if got, _ := l.stored("config"); !bytes.Equal(got, v2) {
			t.Fatalf("after %s the key holds %q", when, got)
		}
		if ts := l.tombs("config"); len(ts) != 1 || ts[0].SHA256 != sum(v1) {
			t.Fatalf("after %s the tombstones are %+v", when, ts)
		}
		l.wantUsage(usage)
	}
	for name, try := range map[string]func() error{
		"unmarked":           func() error { return l.put("config", v3, hashMeta(v3)) },
		"stale mark":         func() error { return l.put("config", v3, rewriteMeta(v3, sum(v1))) },
		"mark of the bytes":  func() error { return l.put("config", v3, rewriteMeta(v3, sum(v3))) },
		"multipart, marked":  func() error { _, err := l.create("config", rewriteMeta(v3, sum(v2))); return err },
		"multipart unmarked": func() error { return l.mpu("config", sum(v3), v3) },
	} {
		if err := try(); code(err) != "PreconditionFailed" || status(err) != 412 {
			t.Errorf("%s: %v, want 412", name, err)
		}
		unchanged(name)
	}
	if err := l.put("config", v3, rewriteMeta(v2, sum(v2))); code(err) != "BadDigest" {
		t.Errorf("body claiming another hash: %v", err)
	}
	unchanged("a body claiming another hash")
	if err := l.put("config", v3, map[string]string{"lendisk-sha256": sum(v3), MetaReplaces: "not-a-hash"}); code(err) != "InvalidRequest" {
		t.Errorf("malformed mark: %v", err)
	}
	unchanged("a malformed mark")

	// A deleted key: a rewrite never replaces a tomb; same bytes revive.
	if err := l.del("config"); err != nil {
		t.Fatal(err)
	}
	if err := l.put("config", v3, rewriteMeta(v3, sum(v2))); code(err) != "PreconditionFailed" {
		t.Fatalf("rewrite of a deleted key: %v", err)
	}
	if ts := l.tombs("config"); len(ts) != 2 {
		t.Fatalf("tombstones %+v", ts)
	}
	// An absent key that never had a tomb takes a marked write as new.
	if err := l.put("fresh", v3, rewriteMeta(v3, sum(v1))); err != nil {
		t.Fatalf("marked write of a fresh key: %v", err)
	}
	if got, _ := l.stored("fresh"); !bytes.Equal(got, v3) || len(l.tombs("fresh")) != 0 {
		t.Fatalf("fresh: %q %+v", got, l.tombs("fresh"))
	}
}

// RET-1 crash safety for a rewrite (#74): the tombstone row is written,
// pending, before the replaced version moves; the new version is
// published after the move; the row is kept once it is. If the new
// version is not published (the store fails, or the daemon dies first),
// the replaced version is moved back, at once or by startup's settling,
// so the key is never left without a version; once the new one is
// published, the old tomb is kept.
func TestRewriteCrashOrderAndUndo(t *testing.T) {
	var atMove []state.Tombstone
	var liveAtMove []byte
	fail := errors.New("the store failed")
	var fault error
	var l *lent
	l = lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(ld *Lending) {
			ld.beforeTombMove = func() {
				atMove, _ = h.db.PendingTombstones(context.Background())
				liveAtMove, _ = l.stored("kopia.repository")
			}
			ld.afterRewriteMove = func() error {
				if _, ok := l.stored("kopia.repository"); ok {
					t.Error("the replaced version is still at its key after the move")
				}
				return fault
			}
		}
	})
	v1, v2 := []byte("format one"), []byte("format two")
	if err := l.put("kopia.repository", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()

	// The store fails after the move: the old version is moved back.
	fault = fail
	if err := l.put("kopia.repository", v2, rewriteMeta(v2, sum(v1))); err == nil {
		t.Fatal("a failed publish answered success")
	}
	if len(atMove) != 1 || !atMove[0].Pending || !atMove[0].Rewrite || !bytes.Equal(liveAtMove, v1) {
		t.Fatalf("before the move: rows %+v, live %q", atMove, liveAtMove)
	}
	if got, _ := l.stored("kopia.repository"); !bytes.Equal(got, v1) || len(l.tombs("kopia.repository")) != 0 {
		t.Fatalf("after a failed rewrite: %q %+v", got, l.tombs("kopia.repository"))
	}
	l.wantUsage(usage)

	// It succeeds: the row is kept.
	fault = nil
	if err := l.put("kopia.repository", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatal(err)
	}
	ts := l.tombs("kopia.repository")
	if len(ts) != 1 || ts[0].Pending {
		t.Fatalf("after the rewrite: %+v", ts)
	}

	// A crash after the move and before the publish: the row is pending,
	// the tomb holds v2 and the key nothing. Startup moves v2 back.
	ctx := context.Background()
	v3 := []byte("format three")
	if err := os.Rename(filepath.Join(l.dir, "kopia.repository"), filepath.Join(l.dir, ".lendisk", "tomb", "kopia.repository", "99")); err != nil {
		t.Fatal(err)
	}
	if _, err := l.h.db.AddTombstone(ctx, state.Tombstone{Peer: l.peer(), Sealed: *l.bucket, Key: "kopia.repository",
		Tomb: ".lendisk/tomb/kopia.repository/99", SHA256: sum(v2), Size: int64(len(v2)), Rewrite: true,
		DeletedAt: time.Now(), PurgeAfter: time.Now().Add(30 * day)}); err != nil {
		t.Fatal(err)
	}
	// And one after the publish, before the row was kept: kept.
	if err := l.put("cfg", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("cfg", v3, rewriteMeta(v3, sum(v1))); err != nil {
		t.Fatal(err)
	}
	cfg := l.tombs("cfg")[0]
	if err := l.h.db.PendTombstone(ctx, cfg.ID); err != nil {
		t.Fatal(err)
	}
	if err := l.lending().settleTombs(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := l.stored("kopia.repository"); !bytes.Equal(got, v2) {
		t.Fatalf("a rewrite cut short before its publish left the key holding %q", got)
	}
	if ts := l.tombs("kopia.repository"); len(ts) != 1 || ts[0].SHA256 != sum(v1) {
		t.Fatalf("after settling: %+v", ts)
	}
	if got, _ := l.stored("cfg"); !bytes.Equal(got, v3) {
		t.Fatalf("cfg holds %q", got)
	}
	if ts := l.tombs("cfg"); len(ts) != 1 || ts[0].Pending {
		t.Fatalf("cfg's tomb after settling: %+v", ts)
	}
	if got, _ := l.tombBytes(cfg); !bytes.Equal(got, v1) {
		t.Fatal("cfg's tomb lost")
	}
}

// LEND-4, RET-1 (#74): a rewrite racing a delete, and racing rewrites
// of one key, lose no version: each version ever acknowledged is live
// or in a tomb afterwards, and the key holds at most one.
func TestRewriteRacingDeleteLosesNothing(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	for round := 0; round < 20; round++ {
		key := "k" + string(rune('a'+round))
		v1, v2, v3 := []byte(key+" one"), []byte(key+" two"), []byte(key+" three")
		if err := l.put(key, v1, hashMeta(v1)); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		errs := make([]error, 3)
		wg.Add(3)
		go func() { defer wg.Done(); errs[0] = l.put(key, v2, rewriteMeta(v2, sum(v1))) }()
		go func() { defer wg.Done(); errs[1] = l.put(key, v3, rewriteMeta(v3, sum(v1))) }()
		go func() { defer wg.Done(); errs[2] = l.del(key) }()
		wg.Wait()
		held := map[string]bool{}
		if got, ok := l.stored(key); ok {
			held[string(got)] = true
		}
		for _, ts := range l.tombs(key) {
			b, err := l.tombBytes(ts)
			if err != nil || ts.Pending {
				t.Fatalf("%s: tomb %+v: %v", key, ts, err)
			}
			held[string(b)] = true
		}
		want := [][]byte{v1}
		if errs[0] == nil {
			want = append(want, v2)
		}
		if errs[1] == nil {
			want = append(want, v3)
		}
		for _, v := range want {
			if !held[string(v)] {
				t.Fatalf("%s: %q lost (answers %v); held %v", key, v, errs, held)
			}
		}
		if errs[0] == nil && errs[1] == nil {
			t.Fatalf("%s: both rewrites of v1 succeeded", key)
		}
	}
}

// LEND-2 (#74): a rewrite is admitted against quota like a new object,
// since both versions count until the old one is purged; refused, the
// old version stays live.
func TestRewriteOverQuotaRefused(t *testing.T) {
	v1, v2 := []byte("config one"), []byte("config two")
	q := lendTo(t, 3*ov+int64(len(v1)), nil)
	if err := q.put("config", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := q.put("config", v2, rewriteMeta(v2, sum(v1))); code(err) != "QuotaExceeded" {
		t.Fatalf("rewrite over quota: %v", err)
	}
	if got, _ := q.stored("config"); !bytes.Equal(got, v1) || len(q.tombs("config")) != 0 {
		t.Fatalf("after a rewrite over quota the key holds %q, tombs %+v", got, q.tombs("config"))
	}
}

// NET-7 list, ID-5 (#74): a live key the borrower rewrote is listed with
// the hashes of the versions its rewrites replaced, newest first, while
// their tombs are kept, so recovery can tell the newest version where
// friends disagree. A deleted object's tomb is no replaced version.
func TestListCarriesReplacedVersions(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	v1, v2, v3 := []byte("one"), []byte("two"), []byte("three")
	if err := l.put("kopia.maintenance", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Second)
	if err := l.put("kopia.maintenance", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Second)
	if err := l.put("kopia.maintenance", v3, rewriteMeta(v3, sum(v2))); err != nil {
		t.Fatal(err)
	}
	if err := l.put("data/1", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/1"); err != nil {
		t.Fatal(err)
	}
	if err := l.put("p0", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	be, err := OpenBackend(l.h.store)
	if err != nil {
		t.Fatal(err)
	}
	idx := &LentIndex{Store: be, DB: l.h.db}
	objs, next, err := idx.List(context.Background(), l.id.NodeID(), *l.bucket, "", MaxListPage)
	if err != nil || next != "" {
		t.Fatalf("list: %v %q", err, next)
	}
	got := map[string]LentObject{}
	for _, o := range objs {
		got[o.Key] = o
	}
	if r := got["kopia.maintenance"].Replaced; len(r) != 2 || r[0] != sum(v2) || r[1] != sum(v1) {
		t.Fatalf("kopia.maintenance listed %+v", got["kopia.maintenance"])
	}
	if len(got["data/1"].Replaced) != 0 || got["data/1"].TombstonedAt.IsZero() || len(got["p0"].Replaced) != 0 {
		t.Fatalf("listed %+v", got)
	}
}
