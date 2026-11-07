package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/borje/lendisk/internal/state"
)

// day is a retention unit.
const day = 24 * time.Hour

// clock is a test's injected clock, in whole seconds.
type clock struct{ now atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.now.Store(time.Unix(1_900_000_000, 0).Unix())
	return c
}
func (c *clock) Now() time.Time          { return time.Unix(c.now.Load(), 0) }
func (c *clock) advance(d time.Duration) { c.now.Add(int64(d / time.Second)) }

// lendWithClock is lendTo with Lending's clock injected and the
// friendship's two retention offers set (zero: none recorded).
func lendWithClock(t *testing.T, own, peer time.Duration) (*lent, *clock) {
	t.Helper()
	c := newClock()
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(l *Lending) { l.Now = c.Now }
	})
	if own != 0 || peer != 0 {
		setRetention(t, l, own, peer)
	}
	return l, c
}

func setRetention(t *testing.T, l *lent, own, peer time.Duration) {
	t.Helper()
	if _, err := l.h.db.ExecContext(context.Background(), `UPDATE friendship SET own_retention = ?, peer_retention = ? WHERE peer_key = ?`,
		int64(own/time.Second), int64(peer/time.Second), l.peer()); err != nil {
		t.Fatal(err)
	}
}

func (l *lent) del(key string) error {
	_, err := l.c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: l.bucket, Key: aws.String(key)})
	return err
}

func (l *lent) tombs(key string) []state.Tombstone {
	l.h.t.Helper()
	ts, err := l.h.db.TombstonesOf(context.Background(), l.peer(), *l.bucket, key)
	if err != nil {
		l.h.t.Fatal(err)
	}
	return ts
}

// tombBytes reads the bytes a tombstone keeps, straight from the store.
func (l *lent) tombBytes(ts state.Tombstone) ([]byte, error) {
	return os.ReadFile(filepath.Join(l.dir, filepath.FromSlash(ts.Tomb)))
}

func (l *lent) listKeys() []string {
	l.h.t.Helper()
	out, err := l.c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: l.bucket})
	if err != nil {
		l.h.t.Fatal(err)
	}
	var keys []string
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	return keys
}

func (l *lent) purge() {
	l.h.t.Helper()
	if err := l.lending().purge(context.Background()); err != nil {
		l.h.t.Fatal(err)
	}
}

// RET-1: a borrower's delete moves the object into the bucket's tomb
// directory by a rename on the same filesystem (the same inode), records
// its hash and purge time, and hides it from the borrower's GET, HEAD
// and listings, while its bytes and overhead still count against quota.
func TestDeleteMovesTheObjectIntoTheTomb(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	ctx := context.Background()
	body := []byte("a pack restic will prune")
	keep := []byte("a pack it keeps")
	if err := l.put("data/ab/pack", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("data/ab/other", keep, hashMeta(keep)); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(l.dir, "data", "ab", "pack"))
	if err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	if err := l.del("data/ab/pack"); err != nil {
		t.Fatal(err)
	}
	if _, ok := l.stored("data/ab/pack"); ok {
		t.Fatal("the object is still at its key")
	}
	ts := l.tombs("data/ab/pack")
	if len(ts) != 1 {
		t.Fatalf("tombstones %+v", ts)
	}
	tomb := ts[0]
	if tomb.SHA256 != sum(body) || tomb.Size != int64(len(body)) || tomb.Pending || !tomb.DeletedAt.Equal(c.Now()) ||
		!tomb.PurgeAfter.Equal(c.Now().Add(30*day)) || !strings.HasPrefix(tomb.Tomb, ".lendisk/tomb/data/ab/pack/") {
		t.Fatalf("tombstone %+v", tomb)
	}
	after, err := os.Stat(filepath.Join(l.dir, filepath.FromSlash(tomb.Tomb)))
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("the tomb is not the object renamed: %v", err)
	}
	if got, _ := l.tombBytes(tomb); !bytes.Equal(got, body) {
		t.Fatalf("tomb holds %q", got)
	}
	if fi, err := os.Stat(filepath.Join(l.dir, ".lendisk", "tomb")); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("tomb directory: %v %v", fi, err)
	}
	l.wantUsage(usage)

	if _, err := l.c.GetObject(ctx, &s3.GetObjectInput{Bucket: l.bucket, Key: aws.String("data/ab/pack")}); code(err) != "NoSuchKey" {
		t.Fatalf("GET of a tombstoned key: %v", err)
	}
	if _, err := l.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: l.bucket, Key: aws.String("data/ab/pack")}); status(err) != 404 {
		t.Fatalf("HEAD of a tombstoned key: %v", err)
	}
	if keys := l.listKeys(); fmt.Sprint(keys) != "[data/ab/other]" {
		t.Fatalf("listed %v", keys)
	}
	v1, err := l.c.ListObjects(ctx, &s3.ListObjectsInput{Bucket: l.bucket})
	if err != nil || len(v1.Contents) != 1 {
		t.Fatalf("list v1 %+v %v", v1, err)
	}
	// Deleting it again, or a key never stored, changes nothing.
	if err := l.del("data/ab/pack"); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/never"); err != nil {
		t.Fatal(err)
	}
	if ts := l.tombs("data/ab/pack"); len(ts) != 1 {
		t.Fatalf("a second delete made %+v", ts)
	}
	l.wantUsage(usage)
}

// RET-1: the window is 30 days by default; the lender's own offer is its
// minimum, and the borrower's offer, exchanged at redeem, can only make
// it longer. The purge time is fixed when the object is deleted.
func TestRetentionIsTheLongerOfTheTwoOffers(t *testing.T) {
	for _, c := range []struct {
		own, peer, want time.Duration
	}{
		{0, 0, 30 * day},
		{10 * day, 0, 10 * day},
		{10 * day, 40 * day, 40 * day},
		{40 * day, 10 * day, 40 * day},
		{7 * day, 7 * day, 7 * day},
	} {
		l, clk := lendWithClock(t, c.own, c.peer)
		body := []byte("x")
		if err := l.put("k", body, hashMeta(body)); err != nil {
			t.Fatal(err)
		}
		if err := l.del("k"); err != nil {
			t.Fatal(err)
		}
		ts := l.tombs("k")
		if len(ts) != 1 || !ts[0].PurgeAfter.Equal(clk.Now().Add(c.want)) {
			t.Errorf("own %v, peer %v: purge at %v, want %v later", c.own, c.peer, ts, c.want)
		}
		// Changing the offers later leaves the recorded time.
		setRetention(t, l, 1*day, 1*day)
		if ts := l.tombs("k"); !ts[0].PurgeAfter.Equal(clk.Now().Add(c.want)) {
			t.Errorf("purge time moved: %+v", ts)
		}
		l.h.stop()
	}
}

// RET-1, LEND-1: the purger removes a tomb once its window has passed,
// and not a second earlier, and frees its bytes and overhead; the
// tomb's emptied directories go with it.
func TestPurgeAfterTheWindowFreesQuota(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	body := bytes.Repeat([]byte("p"), 500)
	if err := l.put("data/cd/pack", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	if err := l.del("data/cd/pack"); err != nil {
		t.Fatal(err)
	}
	tomb := l.tombs("data/cd/pack")[0]
	c.advance(30*day - time.Second)
	l.purge()
	if got, err := l.tombBytes(tomb); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("purged early: %v", err)
	}
	l.wantUsage(usage)
	c.advance(time.Second)
	l.purge()
	if _, err := l.tombBytes(tomb); !os.IsNotExist(err) {
		t.Fatalf("tomb after its window: %v", err)
	}
	if ts := l.tombs("data/cd/pack"); len(ts) != 0 {
		t.Fatalf("row after purge: %+v", ts)
	}
	l.wantUsage(usage - ov - int64(len(body)))
	if ents, _ := os.ReadDir(filepath.Join(l.dir, ".lendisk", "tomb")); len(ents) != 0 {
		t.Fatalf("emptied tomb directories left: %v", ents)
	}
	// The key is free again: other bytes may be stored there.
	other := []byte("new")
	if err := l.put("data/cd/pack", other, hashMeta(other)); err != nil {
		t.Fatalf("put after the purge: %v", err)
	}
}

// RET-1, LEND-4: a put of the same bytes to a tombstoned key revives the
// object (the tomb renamed back, the tombstone dropped, the count
// unchanged); other bytes, by put or multipart, are refused with 412 and
// leave the tomb alone; a body that only claims the hash is refused.
func TestRePutRevivesSameHashAndRefusesAnother(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	body := []byte("snapshot one")
	other := []byte("snapshot two")
	if err := l.put("snapshots/a", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(filepath.Join(l.dir, "snapshots", "a"))
	usage := l.usage()
	if err := l.del("snapshots/a"); err != nil {
		t.Fatal(err)
	}
	tomb := l.tombs("snapshots/a")[0]
	if err := l.put("snapshots/a", other, hashMeta(other)); code(err) != "PreconditionFailed" || status(err) != 412 {
		t.Fatalf("other bytes onto a tombstoned key: %v", err)
	}
	if _, err := l.create("snapshots/a", hashMeta(other)); code(err) != "PreconditionFailed" {
		t.Fatalf("multipart of other bytes onto a tombstoned key: %v", err)
	}
	if err := l.put("snapshots/a", other, hashMeta(body)); code(err) != "BadDigest" {
		t.Fatalf("other bytes claiming the tomb's hash: %v", err)
	}
	if got, _ := l.tombBytes(tomb); !bytes.Equal(got, body) {
		t.Fatal("the tomb changed")
	}
	if _, ok := l.stored("snapshots/a"); ok {
		t.Fatal("a refused put is stored")
	}
	l.wantUsage(usage)

	if err := l.put("snapshots/a", body, hashMeta(body)); err != nil {
		t.Fatalf("same bytes again: %v", err)
	}
	after, err := os.Stat(filepath.Join(l.dir, "snapshots", "a"))
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("not revived from the tomb: %v", err)
	}
	if ts := l.tombs("snapshots/a"); len(ts) != 0 {
		t.Fatalf("tombstone kept: %+v", ts)
	}
	if _, err := l.tombBytes(tomb); !os.IsNotExist(err) {
		t.Fatal("tomb kept")
	}
	if fmt.Sprint(l.listKeys()) != "[snapshots/a]" {
		t.Fatalf("listed %v", l.listKeys())
	}
	l.wantUsage(usage)

	// A multipart upload of the same bytes revives it too.
	if err := l.del("snapshots/a"); err != nil {
		t.Fatal(err)
	}
	if err := l.mpu("snapshots/a", sum(body), body); err != nil {
		t.Fatalf("multipart of the same bytes: %v", err)
	}
	if got, ok := l.stored("snapshots/a"); !ok || !bytes.Equal(got, body) || len(l.tombs("snapshots/a")) != 0 {
		t.Fatalf("multipart revival: %q %+v", got, l.tombs("snapshots/a"))
	}
	l.wantUsage(usage)
}

// RET-2 AC: a borrower holding every credential its own node holds (its
// identity, so the pair's derived S3 secret) cannot cause the loss of
// a tombstoned object within the window: every operation it can send
// through the S3 surface leaves the tomb's bytes as they were, and the
// purge before the window's end keeps them. #15's hostile-peer harness
// runs this same list over the tunnel from a hostile B.
func TestBorrowerCannotPurgeEarly(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	ctx := context.Background()
	body := []byte("the only offsite copy")
	if err := l.put("data/ef/pack", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	// An upload begun while the object was live, to complete after.
	upID, err := l.create("data/ef/pack", hashMeta(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/ef/pack"); err != nil {
		t.Fatal(err)
	}
	tomb := l.tombs("data/ef/pack")[0]
	other := []byte("ransom note")
	k := func(s string) *string { return aws.String(s) }
	attacks := map[string]func() error{
		"delete again": func() error { return l.del("data/ef/pack") },
		"delete version null": func() error {
			_, err := l.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: l.bucket, Key: k("data/ef/pack"), VersionId: k("null")})
			return err
		},
		"delete a version": func() error {
			_, err := l.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: l.bucket, Key: k("data/ef/pack"), VersionId: k("01HZZZZZZZZZZZZZZZZZZZZZZZ")})
			return err
		},
		"multi-delete": func() error {
			_, err := l.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: l.bucket, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: k("data/ef/pack")}, {Key: k(tomb.Tomb)}}}})
			return err
		},
		"overwrite":               func() error { return l.put("data/ef/pack", other, hashMeta(other)) },
		"overwrite claiming hash": func() error { return l.put("data/ef/pack", other, hashMeta(body)) },
		"multipart other bytes":   func() error { return l.mpu("data/ef/pack", sum(other), other) },
		"complete an old upload": func() error {
			p, err := l.part("data/ef/pack", upID, 1, other)
			if err != nil {
				return err
			}
			return l.complete("data/ef/pack", upID, p)
		},
		"abort an old upload": func() error {
			_, err := l.c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: l.bucket, Key: k("data/ef/pack"), UploadId: upID})
			return err
		},
		"delete the tomb":         func() error { return l.del(tomb.Tomb) },
		"delete the tomb dir":     func() error { return l.del(".lendisk/tomb/data/ef/pack") },
		"delete under .LENDISK":   func() error { return l.del(strings.Replace(tomb.Tomb, ".lendisk", ".LENDISK", 1)) },
		"delete through ..":       func() error { return l.del("data/../" + tomb.Tomb) },
		"delete nested .lendisk":  func() error { return l.del("data/.lendisk/tomb/x") },
		"put onto the tomb":       func() error { return l.put(tomb.Tomb, other, hashMeta(other)) },
		"multipart onto the tomb": func() error { return l.mpu(tomb.Tomb, sum(other), other) },
		"read the tomb": func() error {
			_, err := l.c.GetObject(ctx, &s3.GetObjectInput{Bucket: l.bucket, Key: k(tomb.Tomb)})
			return err
		},
		"copy from the tomb": func() error {
			_, err := l.c.CopyObject(ctx, &s3.CopyObjectInput{Bucket: l.bucket, Key: k("data/ef/pack"), CopySource: k(*l.bucket + "/" + tomb.Tomb)})
			return err
		},
		"tag the tomb": func() error {
			_, err := l.c.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{Bucket: l.bucket, Key: k(tomb.Tomb), Tagging: &types.Tagging{TagSet: []types.Tag{{Key: k("a"), Value: k("b")}}}})
			return err
		},
		"versioning then delete": func() error {
			l.c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: l.bucket, VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}})
			return l.del("data/ef/pack")
		},
		"lifecycle expiry": func() error {
			_, err := l.c.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{Bucket: l.bucket, LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{Status: types.ExpirationStatusEnabled, Filter: &types.LifecycleRuleFilter{Prefix: k("")}, Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)}}}}})
			return err
		},
		"delete the bucket": func() error { _, err := l.c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: l.bucket}); return err },
		"delete by the store name": func() error {
			_, err := l.c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: k(l.store), Key: k(tomb.Tomb)})
			return err
		},
	}
	names := make([]string, 0, len(attacks))
	for n := range attacks {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		attacks[n]()
		if got, err := l.tombBytes(tomb); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("after %q the tomb holds %q (%v)", n, got, err)
		}
		if ts := l.tombs("data/ef/pack"); len(ts) != 1 || ts[0].ID != tomb.ID {
			t.Fatalf("after %q the tombstones are %+v", n, ts)
		}
	}
	// The tomb directory is unreachable by name through S3 at all.
	for _, key := range []string{tomb.Tomb, ".lendisk/tomb", "x/.lendisk/y"} {
		if _, err := l.c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: l.bucket, Key: k(key)}); err == nil {
			t.Errorf("HEAD %s answered", key)
		}
	}
	if keys := l.listKeys(); len(keys) != 0 {
		t.Fatalf("listing shows %v", keys)
	}
	if out, err := l.c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: l.bucket, Prefix: k(".lendisk/")}); err == nil && len(out.Contents) != 0 {
		t.Fatalf("listing the tomb prefix shows %v", out.Contents)
	}
	c.advance(30*day - time.Second)
	l.purge()
	if got, err := l.tombBytes(tomb); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("within the window the tomb holds %q (%v)", got, err)
	}
}

// RET-1, crash safety: the row is written (pending) before the rename
// and kept after it; a revival marks it pending before the link back.
// Whichever step a crash interrupts, startup settles the row from what
// the store holds, and no bytes are lost: a row whose tomb was never
// made is dropped (the object is live), one whose tomb and live object
// hold the same bytes was revived (the tomb goes), and one whose tomb
// alone holds them is kept.
func TestPendingTombstonesSettledAtStartup(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	ctx := context.Background()
	put := func(key string) []byte {
		b := []byte("bytes of " + key)
		if err := l.put(key, b, hashMeta(b)); err != nil {
			t.Fatal(err)
		}
		return b
	}
	notMoved, moved, revived := put("data/a"), put("data/b"), put("data/c")
	if err := l.del("data/b"); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/c"); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	// data/a: the row written, the rename not made.
	if _, err := l.h.db.AddTombstone(ctx, state.Tombstone{Peer: l.peer(), Sealed: *l.bucket, Key: "data/a",
		Tomb: ".lendisk/tomb/data/a/1", SHA256: sum(notMoved), Size: int64(len(notMoved)), DeletedAt: c.Now(),
		PurgeAfter: c.Now().Add(30 * day)}); err != nil {
		t.Fatal(err)
	}
	// data/b: renamed, the row not yet kept.
	b := l.tombs("data/b")[0]
	if err := l.h.db.PendTombstone(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	// data/c: a revival linked back, the tomb not yet removed.
	cc := l.tombs("data/c")[0]
	if err := l.h.db.PendTombstone(ctx, cc.ID); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(l.dir, filepath.FromSlash(cc.Tomb)), filepath.Join(l.dir, "data", "c")); err != nil {
		t.Fatal(err)
	}
	l.h.gw.lending.settleTombs(ctx)

	if got, ok := l.stored("data/a"); !ok || !bytes.Equal(got, notMoved) || len(l.tombs("data/a")) != 0 {
		t.Fatalf("data/a: %q %+v", got, l.tombs("data/a"))
	}
	if ts := l.tombs("data/b"); len(ts) != 1 || ts[0].Pending {
		t.Fatalf("data/b: %+v", ts)
	}
	if got, _ := l.tombBytes(b); !bytes.Equal(got, moved) {
		t.Fatal("data/b's tomb lost")
	}
	if got, ok := l.stored("data/c"); !ok || !bytes.Equal(got, revived) || len(l.tombs("data/c")) != 0 {
		t.Fatalf("data/c: %q %+v", got, l.tombs("data/c"))
	}
	if _, err := l.tombBytes(cc); !os.IsNotExist(err) {
		t.Fatal("data/c's tomb kept beside the live object")
	}
	l.wantUsage(usage)
	// The gateway does this before serving.
	if _, err := l.h.db.AddTombstone(ctx, state.Tombstone{Peer: l.peer(), Sealed: *l.bucket, Key: "data/a",
		Tomb: ".lendisk/tomb/data/a/2", SHA256: sum(notMoved), Size: int64(len(notMoved)), DeletedAt: c.Now(),
		PurgeAfter: c.Now().Add(30 * day)}); err != nil {
		t.Fatal(err)
	}
	l.h.stop()
	l.h.serve(false)
	if p, _ := l.h.db.PendingTombstones(ctx); len(p) != 0 {
		t.Fatalf("pending after a start: %+v", p)
	}
}

// NET-7 list: the lender's index carries a tombstoned key with its size,
// hash and when it was tombstoned, in key order among the live keys,
// until it is purged; a key both live and tombstoned is listed live.
func TestLentIndexListsTombstonedKeys(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	ctx := context.Background()
	bodies := map[string][]byte{}
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		bodies[k] = []byte("body " + k)
		if err := l.put(k, bodies[k], hashMeta(bodies[k])); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"b", "d", "e"} {
		if err := l.del(k); err != nil {
			t.Fatal(err)
		}
	}
	be, err := OpenBackend(l.h.store)
	if err != nil {
		t.Fatal(err)
	}
	idx := &LentIndex{Store: be, DB: l.h.db}
	var got []LentObject
	after := ""
	for pages := 0; ; pages++ {
		page, next, err := idx.List(ctx, l.id.NodeID(), *l.bucket, after, 2)
		if err != nil || len(page) > 2 || pages > 5 {
			t.Fatalf("page %+v %v", page, err)
		}
		got = append(got, page...)
		if next == "" {
			break
		}
		after = next
	}
	var desc []string
	for _, o := range got {
		if o.Size != int64(len(bodies[o.Key])) || o.SHA256 != sum(bodies[o.Key]) {
			t.Errorf("%s: %+v", o.Key, o)
		}
		state := "live"
		if !o.TombstonedAt.IsZero() {
			state = "tomb"
			if !o.TombstonedAt.Equal(c.Now()) {
				t.Errorf("%s tombstoned at %v", o.Key, o.TombstonedAt)
			}
		}
		desc = append(desc, o.Key+"="+state)
	}
	if fmt.Sprint(desc) != "[a=live b=tomb c=live d=tomb e=tomb]" {
		t.Fatalf("listed %v", desc)
	}
	c.advance(31 * day)
	l.purge()
	page, next, err := idx.List(ctx, l.id.NodeID(), *l.bucket, "", 10)
	if err != nil || len(page) != 2 || next != "" {
		t.Fatalf("after the purge %+v %q %v", page, next, err)
	}
}

// LEND-1: the reconcile counts tombstones like live objects, so the
// count it sets is what a delete left.
func TestReconcileCountsTombstones(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	ctx := context.Background()
	body := bytes.Repeat([]byte("r"), 300)
	if err := l.put("data/a", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/a"); err != nil {
		t.Fatal(err)
	}
	if err := l.h.db.SetLentUsage(ctx, l.peer(), 1, time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := l.lending().reconcile(ctx, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	l.wantUsage(2*ov + 300)
}

// RET-1, COMPAT-4, P-5: with lendisk stopped, a direct reader of a lent
// bucket sees the tool's layout as the borrower left it after its last
// delete, plus housekeeping directories it ignores: restic reads it as
// a repository, and a plain-file walk outside them finds the live keys.
func TestDirectReaderSeesTheToolsLayout(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	restic, rerr := exec.LookPath("restic")
	repo := t.TempDir()
	var snaps []string
	if rerr == nil {
		src := t.TempDir()
		os.WriteFile(filepath.Join(src, "f"), bytes.Repeat([]byte("restic data "), 2000), 0o600)
		env := append(os.Environ(), "RESTIC_PASSWORD=pw", "RESTIC_CACHE_DIR="+t.TempDir(), "RESTIC_REPOSITORY="+repo)
		run := func(args ...string) string {
			t.Helper()
			cmd := exec.Command(restic, args...)
			cmd.Env = env
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("restic %v: %v\n%s", args, err, out)
			}
			return string(out)
		}
		run("init")
		run("backup", src)
		os.WriteFile(filepath.Join(src, "g"), []byte("second"), 0o600)
		run("backup", src)
		ents, _ := os.ReadDir(filepath.Join(repo, "snapshots"))
		for _, e := range ents {
			snaps = append(snaps, e.Name())
		}
		if len(snaps) != 2 {
			t.Fatalf("snapshots %v", snaps)
		}
	} else {
		// No restic: a stand-in layout for the plain-file reader.
		for _, k := range []string{"config", "keys/k1", "snapshots/s1", "snapshots/s2", "data/ab/p1"} {
			os.MkdirAll(filepath.Join(repo, filepath.Dir(k)), 0o700)
			os.WriteFile(filepath.Join(repo, k), []byte("stand-in "+k), 0o600)
		}
		snaps = []string{"s1", "s2"}
	}
	// The borrower stores the repository with the lender, then deletes
	// its newer snapshot, as `restic forget` does.
	want := map[string][]byte{}
	filepath.WalkDir(repo, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(repo, p)
		b, _ := os.ReadFile(p)
		want[filepath.ToSlash(rel)] = b
		return nil
	})
	for k, b := range want {
		if err := l.put(k, b, hashMeta(b)); err != nil {
			t.Fatalf("%s: %v", k, err)
		}
	}
	sort.Strings(snaps)
	gone := "snapshots/" + snaps[1]
	if err := l.del(gone); err != nil {
		t.Fatal(err)
	}
	delete(want, gone)
	l.h.stop() // lendisk is not running

	got := map[string][]byte{}
	var housekeeping []string
	filepath.WalkDir(l.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(l.dir, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() && (rel == ".lendisk" || rel == ".sgwtmp") {
			housekeeping = append(housekeeping, rel)
			return fs.SkipDir
		}
		if !d.IsDir() {
			b, _ := os.ReadFile(p)
			got[rel] = b
		}
		return nil
	})
	if len(got) != len(want) {
		t.Fatalf("plain reader sees %d files, want %d", len(got), len(want))
	}
	for k, b := range want {
		if !bytes.Equal(got[k], b) {
			t.Fatalf("%s differs", k)
		}
	}
	if !strings.Contains(fmt.Sprint(housekeeping), ".lendisk") {
		t.Fatalf("housekeeping %v", housekeeping)
	}
	if rerr != nil {
		t.Log("restic not installed; checked the plain-file reader only")
		return
	}
	cmd := exec.Command(restic, "-r", l.dir, "--no-lock", "snapshots", "--json")
	cmd.Env = append(os.Environ(), "RESTIC_PASSWORD=pw", "RESTIC_CACHE_DIR="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil || strings.Count(string(out), `"id":"`) != 1 || !strings.Contains(string(out), snaps[0]) {
		t.Fatalf("restic snapshots on the lent bucket: %v\n%s", err, out)
	}
	cmd = exec.Command(restic, "-r", l.dir, "--no-lock", "check", "--read-data")
	cmd.Env = append(os.Environ(), "RESTIC_PASSWORD=pw", "RESTIC_CACHE_DIR="+t.TempDir())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restic check on the lent bucket: %v\n%s", err, out)
	}
}

// RET-1, crash safety: the row is written, pending, before the object is
// renamed, so a crash between them leaves a row startup can settle,
// never a tomb nobody records.
func TestTombstoneRecordedBeforeTheMove(t *testing.T) {
	var seen []state.Tombstone
	var live bool
	var l *lent
	l = lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(ld *Lending) {
			ld.beforeTombMove = func() {
				seen, _ = h.db.PendingTombstones(context.Background())
				_, live = l.stored("data/x")
			}
		}
	})
	body := []byte("x")
	if err := l.put("data/x", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/x"); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0].Key != "data/x" || !seen[0].Pending || !live {
		t.Fatalf("before the move: rows %+v, object live %v", seen, live)
	}
}

// A key whose tomb path would run through another tomb (ENOTDIR) fails
// its delete at once and keeps the object, instead of the delete
// spinning with the key's lock held.
func TestDeleteFailsWhenNoTombCanBeNamed(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	body := []byte("first")
	if err := l.put("a", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("a"); err != nil {
		t.Fatal(err)
	}
	ns := filepath.Base(l.tombs("a")[0].Tomb)
	other := []byte("second")
	if err := l.put("a/"+ns, other, hashMeta(other)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- l.del("a/" + ns) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("delete succeeded without a tomb")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delete hangs choosing a tomb")
	}
	if got, err := os.ReadFile(filepath.Join(l.dir, "a", ns)); err != nil || !bytes.Equal(got, other) {
		t.Fatalf("object after the failed delete: %q, %v", got, err)
	}
}

// A purge working from a due list read before a revive leaves the
// revived row alone: it neither removes its tomb nor frees its bytes.
func TestPurgeSkipsARowRevivedSinceTheDueList(t *testing.T) {
	l, c := lendWithClock(t, day, day)
	body := []byte("kept")
	if err := l.put("k", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("k"); err != nil {
		t.Fatal(err)
	}
	ts := l.tombs("k")
	c.advance(2 * day)
	if err := l.h.db.PendTombstone(context.Background(), ts[0].ID); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	if err := l.lending().purgeOne(context.Background(), ts[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := l.tombBytes(ts[0]); err != nil {
		t.Fatalf("tomb of a pending row removed: %v", err)
	}
	if got := l.usage(); got != usage {
		t.Fatalf("usage %d after skipping, want %d", got, usage)
	}
}
