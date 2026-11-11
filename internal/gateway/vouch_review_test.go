package gateway

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/state"
)

// These tests cover the follow-ups to the review of #113 (LEND-1,
// LEND-4, #115).

// corruptUploadID overwrites the upload-ID attribute of key's object
// with bytes posix cannot read, as a damaged disk might.
func (l *lent) corruptUploadID(key string) {
	l.h.t.Helper()
	if err := syscall.Setxattr(filepath.Join(l.dir, filepath.FromSlash(key)), "user."+mpMetaAttr, []byte("not upload metadata"), 0); err != nil {
		l.h.t.Fatal(err)
	}
	if _, err := l.lending().Uploads.UploadID(l.store, key); err == nil {
		l.h.t.Fatal("the corrupt upload ID still reads")
	}
}

// LEND-4 (#115): a verified multipart object whose upload-ID attribute
// went corrupt does not hold up a live upload of its key: HEAD checks
// the object against its own declared hash and answers it, and a
// rewrite in parts of the key completes.
func TestACorruptUploadIDOnAVerifiedObjectDoesNotBlockALiveUploadOfTheKey(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	v1, v2 := []byte("config one"), []byte("config two")
	if err := l.mpu("config", sum(v1), v1); err != nil {
		t.Fatal(err)
	}
	l.corruptUploadID("config")
	id, err := l.create("config", rewriteMeta(v2, sum(v1)))
	if err != nil {
		t.Fatal(err)
	}
	p, err := l.part("config", id, 1, v2)
	if err != nil {
		t.Fatal(err)
	}
	if h, ok, err := l.head("config"); err != nil || !ok || h != sum(v1) {
		t.Fatalf("HEAD beside a live upload: %q %v %v; want the verified object's hash", h, ok, err)
	}
	if err := l.complete("config", id, p); err != nil {
		t.Fatalf("the rewrite's completion: %v", err)
	}
	if got, _ := l.stored("config"); !bytes.Equal(got, v2) {
		t.Fatalf("the key holds %q", got)
	}
	l.wantNoRow(*id)
	want := ov + (ov + int64(len(v2))) + (ov + int64(len(v1))) // the bucket, v2, v1 in its tomb
	l.wantUsage(want)
	l.reconcileNow()
	l.wantUsage(want)
}

// setDeclared overwrites the hash key's object declares in its
// metadata (in the xattr holding x-amz-meta-lendisk-sha256) with v.
func (l *lent) setDeclared(key string, v []byte) {
	l.h.t.Helper()
	path := filepath.Join(l.dir, filepath.FromSlash(key))
	head, _, err := l.headDirect(key)
	if err != nil {
		l.h.t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, err := syscall.Listxattr(path, buf)
	if err != nil {
		l.h.t.Fatal(err)
	}
	for _, name := range strings.Split(string(buf[:n]), "\x00") {
		val := make([]byte, 4096)
		m, err := syscall.Getxattr(path, name, val)
		// posix keeps the user metadata as one JSON map.
		if err == nil && strings.Contains(string(val[:m]), head) {
			if err := syscall.Setxattr(path, name, []byte(strings.ReplaceAll(string(val[:m]), head, string(v))), 0); err != nil {
				l.h.t.Fatal(err)
			}
			return
		}
	}
	l.h.t.Fatalf("no xattr of %s holds its declared hash %s", key, head)
}

// headDirect is the hash key's object declares, read from the store.
func (l *lent) headDirect(key string) (string, bool, error) {
	o, has, err := l.lending().existing(context.Background(), l.store, key)
	return o.sha256, has, err
}

// wantRot checks the lender recorded key's object as rotten, found
// hashing to found.
func (l *lent) wantRot(key, found string) {
	l.h.t.Helper()
	rs, err := l.h.db.Rots(context.Background())
	if err != nil {
		l.h.t.Fatal(err)
	}
	for _, r := range rs {
		if r.Bucket == l.store && r.Key == key && r.Borrower == l.peer() && r.Found == found {
			return
		}
	}
	l.h.t.Fatalf("no rot recorded for %s: %+v", key, rs)
}

// LEND-4, VER-5 (#115): a verified object whose attributes are damaged
// (its upload ID unreadable) and whose bytes do not match the hash it
// declares, which nothing in lendisk.db vouches for, is never removed:
// beside a live upload of its key HEAD answers 500, the object stays
// as it is, and it is recorded and alerted as rot, as the bit-rot
// sampler records a friend's rotten object. It is hashed once. The
// damaged part may be the declared hash (flipped, or junk) or the
// bytes.
func TestARottedObjectWithACorruptUploadIDIsRecordedAndKept(t *testing.T) {
	body := []byte("the object")
	for _, tc := range []struct {
		name   string
		damage func(l *lent)
	}{
		{"bytes rotted", func(l *lent) {
			if err := os.WriteFile(filepath.Join(l.dir, "data", "mp"), []byte("rot object"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"declared hash flipped", func(l *lent) { l.setDeclared("data/mp", []byte(sum([]byte("flipped")))) }},
		{"declared hash junk", func(l *lent) { l.setDeclared("data/mp", []byte("junk")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads, alerts atomic.Int32
			l := lendTo(t, 1<<30, func(h *harness) {
				h.lending = func(ln *Lending) {
					ln.afterVouchRead = func() { reads.Add(1) }
					ln.alerted = func() { alerts.Add(1) }
				}
			})
			if err := l.mpu("data/mp", sum(body), body); err != nil {
				t.Fatal(err)
			}
			l.corruptUploadID("data/mp")
			id, _ := l.uploadParts("data/mp", body, body) // before the damage, which a create would refuse
			tc.damage(l)
			before, _ := l.stored("data/mp")
			for range 2 {
				if _, _, err := l.head("data/mp"); status(err) != 500 {
					t.Fatalf("HEAD of an object nothing vouches for: %v; want 500", err)
				}
			}
			if got, ok := l.stored("data/mp"); !ok || !bytes.Equal(got, before) {
				t.Fatalf("the object was changed or removed: %q %v", got, ok)
			}
			l.wantRot("data/mp", sum(before))
			if n := reads.Load(); n != 1 {
				t.Fatalf("the object was hashed for %d requests, want once", n)
			}
			if alerts.Load() != 1 {
				t.Fatalf("%d alerts, want 1", alerts.Load())
			}
			l.wantRow(*id)
			l.reconcileNow()
			if got, ok := l.stored("data/mp"); !ok || !bytes.Equal(got, before) {
				t.Fatalf("the reconcile changed or removed the object: %q %v", got, ok)
			}
			l.wantRow(*id)
		})
	}
}

// staleRow records an upload of key declaring the hash of declared that
// the store does not have: one a crash left recorded (gone).
func (l *lent) staleRow(key string, declared []byte, replaces string) string {
	l.h.t.Helper()
	id := "stale-" + sum([]byte(key))[:16]
	if err := l.h.db.AddLentUpload(context.Background(), state.LentUpload{UploadID: id, Peer: l.peer(), Sealed: *l.bucket,
		Key: key, SHA256: sum(declared), CreatedAt: time.Now().Add(-time.Hour), Replaces: replaces}); err != nil {
		l.h.t.Fatal(err)
	}
	return id
}

// LEND-1, LEND-4 (#115): a row whose upload is gone, at a key whose
// object's upload ID cannot be read, is settled only by its own hash,
// which lendisk.db holds; the object's declared hash alone settles
// nothing. HEAD and the reconcile each remove an object that declares
// the row's hash and fails it, and drop the row; the reconcile drops
// the row of an object that matches it (a hidden leftover beside it
// falls to the abandon rule), and that of a sound object of another
// hash, which the row's upload did not link. A row whose object
// declares junk and matches nothing is kept, and the object too,
// recorded as rot.
func TestAnUnreadableUploadIDIsSettledOnlyByATrustedHash(t *testing.T) {
	l, crash := crashingLender(t, nil)
	headID := l.crashedCompletion(crash, "data/head", []byte("declared"), []byte("sent instead"))
	badID := l.crashedCompletion(crash, "data/bad", []byte("declared"), []byte("sent instead"))
	good := []byte("the declared bytes")
	goodID := l.crashedCompletion(crash, "data/good", good, good)
	hiddenID, parts := l.uploadParts("data/hidden", good, good)
	hidden := l.linkedThenCrashed("data/hidden", hiddenID, parts)
	junkID := l.crashedCompletion(crash, "data/junk", []byte("declared"), []byte("sent instead"))
	if err := l.mpu("data/other", sum(good), good); err != nil {
		t.Fatal(err)
	}
	otherID := l.staleRow("data/other", []byte("another object"), "")
	for _, key := range []string{"data/head", "data/bad", "data/good", "data/hidden", "data/junk", "data/other"} {
		l.corruptUploadID(key)
	}
	l.setDeclared("data/junk", []byte("junk"))

	if _, ok, err := l.head("data/head"); err != nil || ok {
		t.Fatalf("HEAD of an object that fails its upload's hash: ok %v, %v; want 404", ok, err)
	}
	l.wantNoRow(headID)
	l.reconcileNow()
	for _, id := range []string{badID, goodID, *hiddenID, otherID} {
		l.wantNoRow(id)
	}
	l.wantRow(junkID)
	for key, want := range map[string]bool{"data/head": false, "data/bad": false, "data/good": true, "data/hidden": true,
		"data/junk": true, "data/other": true} {
		if _, ok := l.stored(key); ok != want {
			t.Fatalf("%s kept: %v, want %v", key, ok, want)
		}
	}
	l.wantThere(hidden, "a leftover beside an object verified by its hash alone")
	l.wantRot("data/junk", sum([]byte("sent instead")))
}

// LEND-1, LEND-4 (#115): a row matched to an object whose upload ID
// cannot be read by its hash alone is not proven the upload that
// linked it. A sound object answers and the row goes (counted as the
// object, an overcount the reconcile corrects); one that fails is
// removed, and a rewrite's replaced version is not moved back from its
// tomb on that evidence.
func TestARowMatchedOnlyByHashIsSettledWithoutUndoingARewrite(t *testing.T) {
	l, crash := crashingLender(t, nil)
	body := []byte("the object")
	if err := l.mpu("data/same", sum(body), body); err != nil {
		t.Fatal(err)
	}
	sameID := l.staleRow("data/same", body, "")
	l.corruptUploadID("data/same")
	if h, ok, err := l.head("data/same"); err != nil || !ok || h != sum(body) {
		t.Fatalf("HEAD of a sound object: %q %v %v", h, ok, err)
	}
	l.wantNoRow(sameID)

	v1, v2 := []byte("config one"), []byte("config two")
	if err := l.put("config", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	id, err := l.create("config", rewriteMeta(v2, sum(v1)))
	if err != nil {
		t.Fatal(err)
	}
	p, err := l.part("config", id, 1, []byte("not config two"))
	if err != nil {
		t.Fatal(err)
	}
	crash.Store(true)
	if err := l.complete("config", id, p); status(err) != 500 {
		t.Fatalf("the completion the lender crashed in: %v", err)
	}
	l.corruptUploadID("config")
	if _, ok, err := l.head("config"); err != nil || ok {
		t.Fatalf("HEAD of a failed rewrite: ok %v, %v; want 404", ok, err)
	}
	l.wantNoRow(*id)
	if ts := l.tombs("config"); len(ts) != 1 || ts[0].SHA256 != sum(v1) {
		t.Fatalf("the replaced version's tomb: %+v", ts)
	}
	l.reconcileNow()
	l.wantUsage(ov + (ov + int64(len(body))) + (ov + int64(len(v1))))
}

// LEND-4 (#115): a retried completion whose own object, linked before a
// crash, fails its hash answers NoSuchUpload, not BadDigest: the fault
// is the lender's, and the borrower asks for the key next (#87), finds
// it gone and sends the object again rather than refusing the place.
func TestARetriedCompletionWhoseObjectFailsItsHashAnswersNoSuchUpload(t *testing.T) {
	l, crash := crashingLender(t, nil)
	sent := []byte("sent instead")
	id, parts := l.uploadParts("data/mp", []byte("declared"), sent)
	crash.Store(true)
	if err := l.complete("data/mp", id, parts...); status(err) != 500 {
		t.Fatalf("the completion the lender crashed in: %v", err)
	}
	if err := l.complete("data/mp", id, parts...); code(err) != "NoSuchUpload" {
		t.Fatalf("the retried completion: %v; want NoSuchUpload", err)
	}
	if _, ok, err := l.head("data/mp"); err != nil || ok {
		t.Fatalf("HEAD after: ok %v, %v; want 404", ok, err)
	}
	l.wantNoRow(*id)
	l.wantUsage(ov)
}

// listsGone is a store whose upload listing also shows one upload the
// store no longer has, as posix's does when it hides an upload to
// complete it between the listing and a ListParts.
type listsGone struct {
	backend.Backend
	gone atomic.Pointer[s3response.Upload]
}

func (s *listsGone) ListMultipartUploads(ctx context.Context, in *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	res, err := s.Backend.ListMultipartUploads(ctx, in)
	if up := s.gone.Load(); err == nil && up != nil && in.Bucket != nil && *in.Bucket != "" {
		res.Uploads = append(res.Uploads, *up)
	}
	return res, err
}

// LEND-1, LEND-4 (#115): an upload the reconcile lists but whose parts
// the store answers NoSuchUpload for is not taken as live: its row goes
// to the check of rows whose upload is gone, which verifies the object
// it linked before a crash (here a bad one, removed) and drops the row.
func TestAnUploadGoneBeforeItsPartsAreListedIsCheckedAsGone(t *testing.T) {
	var store *listsGone
	l, crash := crashingLender(t, func(ln *Lending) {
		store = &listsGone{Backend: ln.Next}
		ln.Next = store
	})
	id := l.crashedCompletion(crash, "data/mp", []byte("declared"), []byte("sent instead"))
	store.gone.Store(&s3response.Upload{Key: "data/mp", UploadID: id, Initiated: l.lending().now()})
	l.reconcileNow()
	l.wantNoRow(id)
	if _, ok := l.stored("data/mp"); ok {
		t.Fatal("the reconcile kept an unverified object whose upload it took as live")
	}
	l.wantUsage(ov)
}

// LEND-1 (#115): a verified object whose row cannot be dropped counts
// nothing, so the check the kept row brings next counts it once. The
// database refuses to drop the row twice, then lets it go. Part 2 is
// counted but left out of the object, so a second count shows.
func TestAVerificationWhoseRowCannotBeDroppedCountsTheObjectOnce(t *testing.T) {
	l, crash := crashingLender(t, nil)
	p1, p2 := []byte("the object"), []byte("a part sent, not completed")
	id, err := l.create("data/mp", hashMeta(p1))
	if err != nil {
		t.Fatal(err)
	}
	c1, err := l.part("data/mp", id, 1, p1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.part("data/mp", id, 2, p2); err != nil {
		t.Fatal(err)
	}
	crash.Store(true)
	if err := l.complete("data/mp", id, c1); status(err) != 500 {
		t.Fatalf("the completion the lender crashed in: %v", err)
	}
	if _, err := l.h.db.Exec(`CREATE TRIGGER keep_upload BEFORE DELETE ON lent_upload BEGIN SELECT RAISE(ABORT, 'disk full'); END`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if h, ok, err := l.head("data/mp"); err != nil || !ok || h != sum(p1) {
			t.Fatalf("HEAD of the verified object: %q %v %v", h, ok, err)
		}
		l.wantRow(*id)
	}
	if _, err := l.h.db.Exec(`DROP TRIGGER keep_upload`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.head("data/mp"); err != nil {
		t.Fatal(err)
	}
	l.wantNoRow(*id)
	l.wantUsage(ov + ov + int64(len(p1)))
}
