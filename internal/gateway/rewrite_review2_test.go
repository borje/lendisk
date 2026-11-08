package gateway

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/state"
)

// LEND-4, RET-1 (#74, second review): a rewritable key the owner deleted
// and then wrote again with other bytes (a Kopia repository made anew)
// is sent as a rewrite marked with the version the friend last
// acknowledged, which is the deleted one, now in its tomb. The lender
// stores it and keeps the tomb, so nothing is lost within the window. A
// mark naming an older tomb than the newest is refused: the deleted
// version is the one the borrower must name.
func TestRewriteOfADeletedKeyKeepsItsTomb(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	v1, v2, v3 := []byte("repository one"), []byte("repository two"), []byte("repository three")
	if err := l.put("kopia.repository", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("kopia.repository", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatal(err)
	}
	if err := l.del("kopia.repository"); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	if err := l.put("kopia.repository", v3, rewriteMeta(v3, sum(v1))); !isCode(err, "PreconditionFailed") && code(err) != "PreconditionFailed" {
		t.Fatalf("a mark of an older tomb: %v", err)
	}
	if err := l.put("kopia.repository", v3, hashMeta(v3)); code(err) != "PreconditionFailed" {
		t.Fatalf("unmarked other bytes onto a deleted key: %v", err)
	}
	if err := l.put("kopia.repository", v3, rewriteMeta(v3, sum(v2))); err != nil {
		t.Fatalf("a rewrite of the deleted version: %v", err)
	}
	if got, _ := l.stored("kopia.repository"); !bytes.Equal(got, v3) {
		t.Fatalf("the key holds %q", got)
	}
	ts := l.tombs("kopia.repository")
	if len(ts) != 2 {
		t.Fatalf("tombstones %+v", ts)
	}
	for i, want := range [][]byte{v1, v2} {
		if got, err := l.tombBytes(ts[i]); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("tomb %d holds %q (%v)", i, got, err)
		}
	}
	l.wantUsage(usage + ov + int64(len(v3)))
}

// stopsAtLength is a store that reads exactly a put's declared length
// and never asks for the end of the body, and records whether the key's
// lock was held when it published.
type stopsAtLength struct {
	backend.Backend
	l      *Lending
	locked chan bool
}

func (s *stopsAtLength) PutObject(ctx context.Context, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	if in.ContentLength == nil || *in.ContentLength == 0 {
		return s.Backend.PutObject(ctx, in)
	}
	b := make([]byte, *in.ContentLength)
	if _, err := io.ReadFull(in.Body, b); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	unlock, free := s.l.keys.tryLock(*in.Bucket, *in.Key)
	if free {
		unlock()
	}
	s.locked <- !free
	in.Body = bytes.NewReader(b)
	return s.Backend.PutObject(ctx, in)
}

// #74 (second review): a plain put takes the key's lock once it has read
// its declared length, even from a store that stops reading there and
// never reads the end of the body, as a rewrite does.
func TestPutTakesTheKeyLockWhenTheStoreStopsAtTheLength(t *testing.T) {
	locked := make(chan bool, 4)
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(ld *Lending) { ld.Next = &stopsAtLength{Backend: ld.Next, l: ld, locked: locked} }
	})
	v := []byte("a pack")
	if err := l.putDirect("data/k", bytes.NewReader(v), int64(len(v)), hashMeta(v)); err != nil {
		t.Fatal(err)
	}
	if held := <-locked; !held {
		t.Fatal("the put published without the key's lock")
	}
}

// NET-7 list (#74, second review): a rewrite that publishes and keeps its
// row between the list's read of the store and its read of the
// tombstones is listed live, with the new version and the replaced one
// in Replaced, not as tombstoned with the old hash. A key listed from a
// pending rewrite row carries the versions earlier rewrites replaced.
func TestListSeesARewriteFinishedBetweenItsReads(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	ctx := context.Background()
	v0, v1, v2 := []byte("schedule zero"), []byte("schedule one"), []byte("schedule two")
	if err := l.put("kopia.maintenance", v0, hashMeta(v0)); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Second)
	if err := l.put("kopia.maintenance", v1, rewriteMeta(v1, sum(v0))); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Second)
	// A rewrite of v1 in its gap: v1 moved, the row pending.
	tomb := ".lendisk/tomb/kopia.maintenance/9"
	if err := os.Rename(filepath.Join(l.dir, "kopia.maintenance"), filepath.Join(l.dir, filepath.FromSlash(tomb))); err != nil {
		t.Fatal(err)
	}
	id, err := l.h.db.AddTombstone(ctx, state.Tombstone{Peer: l.peer(), Sealed: *l.bucket, Key: "kopia.maintenance",
		Tomb: tomb, SHA256: sum(v1), Size: int64(len(v1)), Rewrite: true, DeletedAt: c.Now(), PurgeAfter: c.Now().Add(30 * day)})
	if err != nil {
		t.Fatal(err)
	}
	be, err := OpenBackend(l.h.store)
	if err != nil {
		t.Fatal(err)
	}
	list := func(x *LentIndex) LentObject {
		t.Helper()
		objs, _, err := x.List(ctx, l.id.NodeID(), *l.bucket, "", MaxListPage)
		if err != nil || len(objs) != 1 {
			t.Fatalf("listed %+v %v", objs, err)
		}
		return objs[0]
	}
	if o := list(&LentIndex{Store: be, DB: l.h.db}); o.SHA256 != sum(v1) || !o.TombstonedAt.IsZero() ||
		len(o.Replaced) != 1 || o.Replaced[0] != sum(v0) {
		t.Fatalf("mid-rewrite: %+v", o)
	}
	// The rewrite publishes v2 and keeps its row between the two reads.
	x := &LentIndex{Store: be, DB: l.h.db, afterStoreList: func() {
		n := int64(len(v2))
		if _, err := be.PutObject(ctx, s3response.PutObjectInput{Bucket: aws.String(l.store), Key: aws.String("kopia.maintenance"),
			ContentLength: &n, Body: bytes.NewReader(v2), Metadata: hashMeta(v2)}); err != nil {
			t.Error(err)
		}
		if err := l.h.db.KeepTombstone(ctx, id); err != nil {
			t.Error(err)
		}
	}}
	if o := list(x); o.SHA256 != sum(v2) || !o.TombstonedAt.IsZero() || len(o.Replaced) != 2 || o.Replaced[0] != sum(v1) {
		t.Fatalf("after a rewrite between the reads: %+v", o)
	}
}

// LEND-4 (#74, second review): a borrower's put of a key whose multipart
// upload is being completed waits for the completion (which holds the
// key's lock) and is then refused, or, carrying the same bytes, answered
// with the stored object's ETag: never published over it.
func TestPutRacingACompletionWaitsForIt(t *testing.T) {
	for _, same := range []bool{false, true} {
		answered := make(chan error, 1)
		var during error
		var l *lent
		mp, other := []byte("from the upload"), []byte("from a put")
		l = lendTo(t, 1<<30, func(h *harness) {
			h.lending = func(ld *Lending) {
				ld.beforeComplete = func() {
					body := other
					if same {
						body = mp
					}
					go func() { answered <- l.putDirect("data/k", bytes.NewReader(body), int64(len(body)), hashMeta(body)) }()
					select {
					case err := <-answered:
						during = err
						answered <- err
					case <-time.After(500 * time.Millisecond):
					}
				}
			}
		})
		if err := l.mpu("data/k", sum(mp), mp); err != nil {
			t.Fatalf("same %v: the completion: %v", same, err)
		}
		if during != nil {
			t.Fatalf("same %v: the put answered while the completion held the key: %v", same, during)
		}
		err := <-answered
		switch {
		case same && err != nil:
			t.Fatalf("a put of the same bytes after the completion: %v", err)
		case !same && !isCode(err, "PreconditionFailed"):
			t.Fatalf("a put of other bytes after the completion: %v", err)
		}
		if got, _ := l.stored("data/k"); !bytes.Equal(got, mp) {
			t.Fatalf("same %v: stored %q", same, got)
		}
		l.h.stop()
	}
}
