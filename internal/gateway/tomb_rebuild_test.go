package gateway

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/state"
)

// forgetTombstones deletes every tombstone row, as a lender whose
// lendisk.db was rebuilt without them has none.
func (l *lent) forgetTombstones() {
	l.h.t.Helper()
	if _, err := l.h.db.ExecContext(context.Background(), `DELETE FROM tombstone`); err != nil {
		l.h.t.Fatal(err)
	}
}

// loseDatabase stops the gateway and replaces lendisk.db with a new,
// empty one: the store is all that is left.
func (h *harness) loseDatabase() {
	h.t.Helper()
	h.stop()
	dir := filepath.Dir(h.store)
	if err := h.db.Close(); err != nil {
		h.t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "lendisk.db*"))
	if len(files) == 0 {
		h.t.Fatal("no lendisk.db to lose")
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			h.t.Fatal(err)
		}
	}
	db, err := state.Open(context.Background(), dir, quiet)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { db.Close() })
	h.db = db
}

// sameTomb compares a rebuilt row with the one the delete wrote.
func sameTomb(got, want state.Tombstone) bool {
	return got.Key == want.Key && got.Tomb == want.Tomb && got.SHA256 == want.SHA256 && got.ETag == want.ETag &&
		got.Size == want.Size && got.DeletedAt.Equal(want.DeletedAt) && got.PurgeAfter.Equal(want.PurgeAfter) &&
		!got.Pending && !got.Rewrite
}

// RET-1, RET-2 (#75): a lender whose tombstone rows are lost rebuilds
// them at startup from the tombs in each lent bucket: the key and delete
// time from the tomb's path, the hash and ETag from its xattrs, the
// purge time the delete time plus the friendship's retention. The tomb
// is then protected as before: listed tombstoned, other bytes refused,
// purged only at the end of its window.
func TestLostTombstoneRowsRebuiltAtStartup(t *testing.T) {
	l, c := lendWithClock(t, 10*day, 0)
	a, b := []byte("a pack pruned first"), []byte("a pack pruned later")
	if err := l.put("data/aa/pack", a, hashMeta(a)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("data/bb/pack", b, hashMeta(b)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/aa/pack"); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * day)
	if err := l.del("data/bb/pack"); err != nil {
		t.Fatal(err)
	}
	wantA, wantB := l.tombs("data/aa/pack")[0], l.tombs("data/bb/pack")[0]

	l.h.stop()
	l.forgetTombstones()
	l.h.serve(false)

	if ts := l.tombs("data/aa/pack"); len(ts) != 1 || !sameTomb(ts[0], wantA) {
		t.Fatalf("data/aa/pack rebuilt as %+v, want %+v", ts, wantA)
	}
	if ts := l.tombs("data/bb/pack"); len(ts) != 1 || !sameTomb(ts[0], wantB) {
		t.Fatalf("data/bb/pack rebuilt as %+v, want %+v", ts, wantB)
	}
	// A second start rebuilds nothing more.
	l.h.stop()
	l.h.serve(false)
	if ts := l.tombs("data/aa/pack"); len(ts) != 1 {
		t.Fatalf("rows after a second start: %+v", ts)
	}
	// Protected as before: other bytes are refused, the window holds.
	other := []byte("other bytes")
	if err := l.put("data/aa/pack", other, hashMeta(other)); code(err) != "PreconditionFailed" {
		t.Fatalf("put of other bytes over a rebuilt tomb: %v", err)
	}
	c.advance(8*day - time.Second)
	l.purge()
	if got, err := l.tombBytes(wantA); err != nil || !bytes.Equal(got, a) {
		t.Fatalf("purged before its window: %v", err)
	}
	c.advance(time.Second)
	l.purge()
	if _, err := l.tombBytes(wantA); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("tomb after its window: %v", err)
	}
	if got, err := l.tombBytes(wantB); err != nil || !bytes.Equal(got, b) {
		t.Fatalf("the later tomb purged with the first: %v", err)
	}
}

// RET-1 (#75): a tomb whose name gives no delete time (not a number, or
// a time after now) is kept a whole window from the rebuild, the most
// it can have had left.
func TestRebuiltTombWithoutADeleteTimeKeepsAWholeWindow(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	for _, key := range []string{"data/x", "data/y"} {
		body := []byte("bytes of " + key)
		if err := l.put(key, body, hashMeta(body)); err != nil {
			t.Fatal(err)
		}
		if err := l.del(key); err != nil {
			t.Fatal(err)
		}
	}
	x, y := l.tombs("data/x")[0], l.tombs("data/y")[0]
	l.h.stop()
	l.forgetTombstones()
	rename := func(ts state.Tombstone, name string) string {
		rel := filepath.ToSlash(filepath.Join(filepath.Dir(filepath.FromSlash(ts.Tomb)), name))
		if err := os.Rename(filepath.Join(l.dir, filepath.FromSlash(ts.Tomb)), filepath.Join(l.dir, filepath.FromSlash(rel))); err != nil {
			t.Fatal(err)
		}
		return rel
	}
	xTomb := rename(x, "copied-by-hand")
	yTomb := rename(y, strconv.FormatInt(c.Now().Add(365*day).UnixNano(), 10)) // after now
	c.advance(5 * day)
	l.h.serve(false)
	for key, tomb := range map[string]string{"data/x": xTomb, "data/y": yTomb} {
		ts := l.tombs(key)
		if len(ts) != 1 || ts[0].Tomb != tomb || !ts[0].DeletedAt.Equal(c.Now()) || !ts[0].PurgeAfter.Equal(c.Now().Add(30*day)) {
			t.Fatalf("%s rebuilt as %+v, want deleted now (%v), purged a window later", key, ts, c.Now())
		}
	}
}

// RET-1, PEER-8 (#75): after lendisk.db is lost, the friend befriended
// again and its replicator creating its bucket again, the lender
// rebuilds that bucket's tombstone rows: the key is listed tombstoned,
// counted, refuses other bytes, and comes back with its own.
func TestTombstonesRebuiltWhenTheBorrowerCreatesItsBucketAgain(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	ctx := context.Background()
	body := []byte("a snapshot the borrower deleted")
	if err := l.put("snapshots/s1", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("snapshots/s1"); err != nil {
		t.Fatal(err)
	}
	want := l.tombs("snapshots/s1")[0]

	l.h.loseDatabase()
	l.h.befriend(l.id)
	l.h.serve(false)
	l.c = l.h.pairClient(l.id)
	// The bucket is still in the store: the borrower's replicator takes
	// BucketAlreadyOwnedByYou as made (replica.Client.ensureBucket).
	if _, err := l.c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: l.bucket}); err != nil && code(err) != "BucketAlreadyOwnedByYou" {
		t.Fatal(err)
	}
	ts := l.tombs("snapshots/s1")
	if len(ts) != 1 || !sameTomb(ts[0], want) {
		t.Fatalf("rebuilt %+v, want %+v", ts, want)
	}
	be, err := OpenBackend(l.h.store)
	if err != nil {
		t.Fatal(err)
	}
	idx := &LentIndex{Store: be, DB: l.h.db}
	page, _, err := idx.List(ctx, l.id.NodeID(), *l.bucket, "", 10)
	if err != nil || len(page) != 1 || page[0].Key != "snapshots/s1" || !page[0].TombstonedAt.Equal(want.DeletedAt) {
		t.Fatalf("listed %+v %v", page, err)
	}
	if err := l.h.db.SetLentUsage(ctx, l.peer(), 0, c.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := l.lending().reconcile(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(2*ov + int64(len(body)))
	other := []byte("other bytes")
	if err := l.put("snapshots/s1", other, hashMeta(other)); code(err) != "PreconditionFailed" {
		t.Fatalf("put of other bytes: %v", err)
	}
	if err := l.put("snapshots/s1", body, hashMeta(body)); err != nil {
		t.Fatalf("re-put of its own bytes: %v", err)
	}
	if got, ok := l.stored("snapshots/s1"); !ok || !bytes.Equal(got, body) {
		t.Fatal("not revived")
	}
}

// RET-1 (#75): removing a bucket's last tomb removes its emptied
// .lendisk directory too, so an emptied bucket can be deleted.
func TestLastTombGoneLeavesNoHousekeepingDir(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	body := []byte("the only object")
	if err := l.put("data/only", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/only"); err != nil {
		t.Fatal(err)
	}
	c.advance(31 * day)
	l.purge()
	if _, err := os.Lstat(filepath.Join(l.dir, HousekeepingDir)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf(".lendisk after the last purge: %v", err)
	}
	if err := l.lending().Next.DeleteBucket(context.Background(), l.store); err != nil {
		t.Fatalf("deleting the emptied bucket: %v", err)
	}
}
