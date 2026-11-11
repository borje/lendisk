package gateway

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3response"
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

// LEND-4 (#115): an object whose upload-ID attribute cannot be read and
// which fails its own declared hash, beside a live upload of its key,
// is removed on that full read, so HEAD answers 404 and the borrower
// sends it again; the live upload is untouched.
func TestACorruptUploadIDOnARottedObjectRemovesItSoTheBorrowerSendsItAgain(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	body := []byte("the object")
	if err := l.mpu("data/mp", sum(body), body); err != nil {
		t.Fatal(err)
	}
	l.corruptUploadID("data/mp")
	if err := os.WriteFile(filepath.Join(l.dir, "data", "mp"), []byte("rot object"), 0o600); err != nil {
		t.Fatal(err)
	}
	id, parts := l.uploadParts("data/mp", body, body)
	if _, ok, err := l.head("data/mp"); err != nil || ok {
		t.Fatalf("HEAD of a rotted object: ok %v, %v; want 404", ok, err)
	}
	if _, ok := l.stored("data/mp"); ok {
		t.Fatal("the rotted object is kept")
	}
	l.wantRow(*id)
	if err := l.complete("data/mp", id, parts...); err != nil {
		t.Fatalf("the live upload's completion: %v", err)
	}
	if got, _ := l.stored("data/mp"); !bytes.Equal(got, body) {
		t.Fatalf("the key holds %q", got)
	}
	l.wantUsage(ov + ov + int64(len(body)))
}

// LEND-1, LEND-4 (#115): a row whose linked object's upload ID cannot be
// read is not kept for ever: the object is checked against the hash it
// declares, which is the upload's. HEAD and the reconcile each remove
// one that fails and drop the row; the reconcile keeps one that matches
// and drops the row, and a hidden leftover beside it falls to the
// abandon rule.
func TestAnUnreadableUploadIDIsSettledByTheDeclaredHash(t *testing.T) {
	l, crash := crashingLender(t, nil)
	headID := l.crashedCompletion(crash, "data/head", []byte("declared"), []byte("sent instead"))
	badID := l.crashedCompletion(crash, "data/bad", []byte("declared"), []byte("sent instead"))
	good := []byte("the declared bytes")
	goodID := l.crashedCompletion(crash, "data/good", good, good)
	hiddenID, parts := l.uploadParts("data/hidden", good, good)
	hidden := l.linkedThenCrashed("data/hidden", hiddenID, parts)
	for _, key := range []string{"data/head", "data/bad", "data/good", "data/hidden"} {
		l.corruptUploadID(key)
	}

	if _, ok, err := l.head("data/head"); err != nil || ok {
		t.Fatalf("HEAD of an object that fails its declared hash: ok %v, %v; want 404", ok, err)
	}
	l.wantNoRow(headID)
	l.reconcileNow()
	for _, id := range []string{badID, goodID, *hiddenID} {
		l.wantNoRow(id)
	}
	for key, want := range map[string]bool{"data/head": false, "data/bad": false, "data/good": true, "data/hidden": true} {
		if _, ok := l.stored(key); ok != want {
			t.Fatalf("%s kept: %v, want %v", key, ok, want)
		}
	}
	l.wantThere(hidden, "a leftover beside an object verified by its declared hash alone")
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
