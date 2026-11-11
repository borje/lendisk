package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"

	"github.com/borje/lendisk/internal/state"
)

// These tests cover what a lender crash during a multipart completion
// leaves under a lent bucket's .sgwtmp (LEND-1, LEND-4, #112).

// keyDir is posix's directory of key's uploads in the lent bucket.
func (l *lent) keyDir(key string) string {
	return filepath.Join(l.dir, ".sgwtmp", "multipart", fmt.Sprintf("%x", sha256.Sum256([]byte(key))))
}

// uploadParts starts an upload of key declaring the hash of declared and
// sends parts.
func (l *lent) uploadParts(key string, declared []byte, parts ...[]byte) (*string, []types.CompletedPart) {
	l.h.t.Helper()
	id, err := l.create(key, hashMeta(declared))
	if err != nil {
		l.h.t.Fatal(err)
	}
	var done []types.CompletedPart
	for i, p := range parts {
		cp, err := l.part(key, id, int32(i+1), p)
		if err != nil {
			l.h.t.Fatal(err)
		}
		done = append(done, cp)
	}
	return id, done
}

// linkedThenCrashed completes upload id in the store behind the
// lender's back, leaving behind a copy of its directory under the name
// posix hides it by: the lender crashed after the object was linked
// and before the hidden directory was removed. It returns that
// directory.
func (l *lent) linkedThenCrashed(key string, id *string, parts []types.CompletedPart) string {
	t := l.h.t
	t.Helper()
	dir := filepath.Join(l.keyDir(key), *id)
	hidden := dir + ".0123456789abcdef0123456789abcdef-" + fmt.Sprint(len(parts)) + inProgressSuffix
	if err := os.Mkdir(hidden, 0o700); err != nil {
		t.Fatal(err)
	}
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range es {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(hidden, e.Name()), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	be, err := OpenBackend(l.h.store)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := be.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: &l.store,
		Key: aws.String(key), UploadId: id, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	return hidden
}

// reconcileNow reconciles the borrower from a counter set wrong.
func (l *lent) reconcileNow() {
	t := l.h.t
	t.Helper()
	ctx := context.Background()
	if err := l.h.db.SetLentUsage(ctx, l.peer(), 99_999, time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := l.lending().reconcile(ctx, 0); err != nil {
		t.Fatal(err)
	}
}

func (l *lent) wantGone(path, what string) {
	l.h.t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		l.h.t.Fatalf("%s: %v", what, err)
	}
}

func (l *lent) wantThere(path, what string) {
	l.h.t.Helper()
	if _, err := os.Lstat(path); err != nil {
		l.h.t.Fatalf("%s: %v", what, err)
	}
}

// LEND-1, LEND-4 (#112): a crash after the object was linked and before
// posix removed the hidden upload directory leaves the parts twice on
// disk. The first reconcile hashes the object, finds it is the one the
// upload declared, and removes the leftover at once, so the borrower is
// charged for the object only; a sibling upload of the same key stays.
func TestLinkedLeftoverHashCheckedAndRemovedAtOnce(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	p1, p2 := bytes.Repeat([]byte("1"), 5<<20), []byte("tail")
	whole := append(append([]byte{}, p1...), p2...)
	sibling, _ := l.uploadParts("data/mp", whole, []byte("sibling part"))
	id, parts := l.uploadParts("data/mp", whole, p1, p2)
	hidden := l.linkedThenCrashed("data/mp", id, parts)

	l.reconcileNow()
	l.wantUsage(ov + (ov + int64(len(whole))) + (ov + int64(len("sibling part"))))
	l.wantGone(hidden, "the leftover of the linked upload")
	if _, err := l.h.db.GetLentUpload(ctx, *id); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("the linked upload's row: %v", err)
	}
	if data, _ := l.stored("data/mp"); !bytes.Equal(data, whole) {
		t.Fatalf("the object: %d bytes", len(data))
	}
	l.wantThere(filepath.Join(l.keyDir("data/mp"), *sibling), "the sibling upload")
	if u, err := l.h.db.GetLentUpload(ctx, *sibling); err != nil || u.Counted != int64(len("sibling part")) {
		t.Fatalf("the sibling's row: %+v %v", u, err)
	}
}

// LEND-4 (#112): when the object a crash left linked does not hash to
// what its upload declared, the reconcile drops the object, so the
// borrower, which never had the completion acknowledged, sends it
// again; the leftover is not deleted on that evidence and stays counted.
func TestLinkedLeftoverFailingItsHashKeptAndObjectDropped(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	sent := []byte("sent instead of the declared bytes")
	id, parts := l.uploadParts("data/mp", []byte("declared"), sent)
	hidden := l.linkedThenCrashed("data/mp", id, parts)

	l.reconcileNow()
	if _, ok := l.stored("data/mp"); ok {
		t.Fatal("the object that failed its hash is kept")
	}
	l.wantThere(hidden, "the leftover of an upload whose object failed its hash")
	l.wantUsage(ov + ov + int64(len(sent)))
	if _, err := l.h.db.GetLentUpload(ctx, *id); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("the failed upload's row: %v", err)
	}
}

// LEND-4 (#112): an object a crash left linked and unverified, with no
// leftover directory, is hashed by the reconcile too, not only when the
// borrower retries its completion.
func TestObjectLinkedBeforeACrashHashCheckedByTheReconcile(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	id, parts := l.uploadParts("data/mp", []byte("declared"), []byte("sent instead"))
	be, err := OpenBackend(l.h.store)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := be.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: &l.store,
		Key: aws.String("data/mp"), UploadId: id, MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}); err != nil {
		t.Fatal(err)
	}
	l.reconcileNow()
	if _, ok := l.stored("data/mp"); ok {
		t.Fatal("the unverified object is kept")
	}
	l.wantUsage(ov)
}

// hideAsCompleting renames upload id's directory as posix does when it
// begins completing it with parts, and leaves it so: the crash.
func (l *lent) hideAsCompleting(key string, id *string, parts []types.CompletedPart) string {
	t := l.h.t
	t.Helper()
	token, err := backend.ComputeMultipartETagFromPartETags(*id, parts)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(l.keyDir(key), *id)
	hidden := dir + "." + strings.Trim(token, `"`) + inProgressSuffix
	if err := os.Rename(dir, hidden); err != nil {
		t.Fatal(err)
	}
	return hidden
}

// LEND-1 (#112): a completion retried after a crash hid its upload is
// answered by posix from the hidden directory's name alone, with no
// object, and fails; the parts are still on disk, so they stay counted,
// and the reconcile keeps counting them though the upload has no row.
func TestFailedCompletionLeftoverStaysCounted(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	ctx := context.Background()
	p1, p2 := bytes.Repeat([]byte("1"), 5<<20), []byte("tail")
	whole := append(append([]byte{}, p1...), p2...)
	id, parts := l.uploadParts("data/mp", whole, p1, p2)
	hidden := l.hideAsCompleting("data/mp", id, parts)
	want := ov + ov + int64(len(whole))
	l.wantUsage(want)

	in := &s3.CompleteMultipartUploadInput{Bucket: &l.store, Key: aws.String("data/mp"), UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts}}
	if _, _, err := l.lending().completeLent(ctx, l.peer(), *l.bucket, in); !errors.Is(err, errInternal) {
		t.Fatalf("the retried completion: %v", err)
	}
	if err := l.complete("data/mp", id, parts...); code(err) != "NoSuchUpload" {
		t.Fatalf("the completion retried again: %v", err)
	}
	l.wantUsage(want)
	if _, err := l.h.db.GetLentUpload(ctx, *id); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("the failed upload's row: %v", err)
	}
	l.reconcileNow()
	l.wantUsage(want)
	l.wantThere(hidden, "the leftover")
}

// LEND-1 (#112): a fresh leftover whose upload has no row (the lender
// forgot it, or never wrote it) is counted all the same.
func TestFreshLeftoverWithoutARowCounted(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	part := []byte("a part")
	id, parts := l.uploadParts("data/mp", part, part)
	hidden := l.hideAsCompleting("data/mp", id, parts)
	if err := l.h.db.DropLentUpload(context.Background(), *id); err != nil {
		t.Fatal(err)
	}
	l.reconcileNow()
	l.wantUsage(ov + ov + int64(len(part)))
	l.wantThere(hidden, "the leftover")
}

// LEND-1 (#112): a key directory the lender cannot read is skipped; the
// rest of the borrower's reconcile still runs and sets its counter.
func TestUnreadableKeyDirectorySkipped(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	part := []byte("a part")
	l.uploadParts("data/locked", part, part)
	id, parts := l.uploadParts("data/mp", part, part)
	l.hideAsCompleting("data/mp", id, parts)
	locked := l.keyDir("data/locked")
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(locked, 0o700) })
	if _, err := os.ReadDir(locked); err == nil {
		t.Skip("this user reads a directory of mode 0 (root?)")
	}
	l.reconcileNow()
	l.wantUsage(ov + ov + int64(len(part)))
}

// tempName is a name posix's named temp file fallback (os.CreateTemp
// with the pattern "<sha256 hex of the object>.") gives.
func tempName(obj string, n uint32) string {
	return fmt.Sprintf("%x.%d", sha256.Sum256([]byte(obj)), n)
}

// LEND-1 (#112): named temp files a crash left under .sgwtmp (posix's
// fallback when O_TMPFILE is unavailable), for a put and for a part,
// count against the borrower and are removed once still for the
// abandon period. A temp file this process holds open is a write
// streaming now: neither counted nor removed. Nothing else is touched:
// files of other names, and symlinks, which are not followed.
func TestCrashLeftTempFilesCountedThenRemoved(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().Unix())
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(l *Lending) { l.Now = func() time.Time { return time.Unix(now.Load(), 0) } }
	})
	tmp := filepath.Join(l.dir, ".sgwtmp")
	keyDir := l.keyDir("data/x")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(path string, n int) string {
		t.Helper()
		if err := os.WriteFile(path, bytes.Repeat([]byte("t"), n), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	put := write(filepath.Join(tmp, tempName("data/x", 1234567890)), 1000)
	part := write(filepath.Join(keyDir, tempName(".sgwtmp/multipart/x/1", 42)), 300)
	streaming := write(filepath.Join(tmp, tempName("data/y", 7)), 5000)
	f, err := os.Open(streaming)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	other := write(filepath.Join(tmp, "not-posix-s"), 70)
	outside := write(filepath.Join(t.TempDir(), "outside"), 90)
	link := filepath.Join(tmp, tempName("data/z", 9))
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	l.reconcileNow()
	l.wantUsage(ov + 1000 + 300)
	now.Add(int64(8 * 24 * time.Hour / time.Second))
	l.reconcileNow()
	l.wantUsage(ov)
	l.wantGone(put, "a put's temp file")
	l.wantGone(part, "a part's temp file")
	l.wantThere(streaming, "a temp file held open")
	l.wantThere(other, "a file posix did not name")
	l.wantThere(link, "a symlink")
	l.wantThere(outside, "the symlink's target")
}

// LEND-1, LEND-4 (#112): a completion the borrower retries after such a
// crash verifies the object and removes the leftover then and there;
// one whose object fails its hash keeps the leftover counted.
func TestRetriedCompletionSettlesTheLeftover(t *testing.T) {
	l := lendTo(t, 1<<30, nil)
	good := []byte("the declared bytes")
	id, parts := l.uploadParts("data/good", good, good)
	hidden := l.linkedThenCrashed("data/good", id, parts)
	if err := l.complete("data/good", id, parts...); err != nil {
		t.Fatalf("the retried completion: %v", err)
	}
	l.wantGone(hidden, "the leftover of a verified object")
	l.wantUsage(ov + ov + int64(len(good)))

	sent := []byte("sent instead")
	id, parts = l.uploadParts("data/bad", []byte("declared"), sent)
	hidden = l.linkedThenCrashed("data/bad", id, parts)
	if err := l.complete("data/bad", id, parts...); code(err) != "NoSuchUpload" { // #115
		t.Fatalf("the retried completion of a bad object: %v", err)
	}
	l.wantThere(hidden, "the leftover of an object that failed its hash")
	l.wantUsage(ov + ov + int64(len(good)) + ov + int64(len(sent)))
}
