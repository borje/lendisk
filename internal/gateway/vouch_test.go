package gateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// These tests cover what keeps a borrower from taking as stored an
// object a lender crash left linked and unverified (LEND-4, REPL-3,
// LEND-1, #113).

var errCrashed = errors.New("the lender crashed")

// crashingLender lends to one borrower; while crash is set, the next
// completion stops as a lender crash between the store linking its
// object and the hash check would: the object at its key, unverified,
// and its row kept.
func crashingLender(t *testing.T, setup func(*Lending)) (*lent, *atomic.Bool) {
	t.Helper()
	crash := &atomic.Bool{}
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(ln *Lending) {
			ln.afterCompleteLink = func() error {
				if crash.Swap(false) {
					return errCrashed
				}
				return nil
			}
			if setup != nil {
				setup(ln)
			}
		}
	})
	return l, crash
}

// crashedCompletion uploads sent under key declaring the hash of
// declared, and completes it into a crash; it returns the upload ID.
func (l *lent) crashedCompletion(crash *atomic.Bool, key string, declared, sent []byte) string {
	l.h.t.Helper()
	id, parts := l.uploadParts(key, declared, sent)
	crash.Store(true)
	if err := l.complete(key, id, parts...); status(err) != 500 {
		l.h.t.Fatalf("the completion the lender crashed in: %v", err)
	}
	if _, ok := l.stored(key); !ok {
		l.h.t.Fatal("the crash left no object linked")
	}
	return *id
}

// head asks for key as the borrower does after NoSuchUpload (#87): its
// declared hash, or ok false for 404.
func (l *lent) head(key string) (string, bool, error) {
	out, err := l.c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: l.bucket, Key: aws.String(key)})
	if status(err) == 404 {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return out.Metadata[metaSHA256], true, nil
}

func (l *lent) wantNoRow(id string) {
	l.h.t.Helper()
	if _, err := l.h.db.GetLentUpload(context.Background(), id); !errors.Is(err, state.ErrNotFound) {
		l.h.t.Fatalf("the upload's row: %v", err)
	}
}

func (l *lent) wantRow(id string) {
	l.h.t.Helper()
	if _, err := l.h.db.GetLentUpload(context.Background(), id); err != nil {
		l.h.t.Fatalf("the upload's row is gone: %v", err)
	}
}

// LEND-4, REPL-3 (#113): end to end, a borrower's replication worker
// sends an object in parts to a real lender; the parts go bad on the
// lender's disk and the lender crashes after the store linked the
// object, before it hashed it. The worker's retry meets NoSuchUpload and
// asks for the key (#87): the lender verifies the object first, removes
// it, and answers 404, so the worker sends the object again instead of
// acknowledging the bad copy from its declared hash.
func TestABorrowerRetryingAfterALenderCrashSendsABadObjectAgain(t *testing.T) {
	var crashArmed atomic.Bool
	var l *lent
	var crashes atomic.Int32
	l, _ = crashingLender(t, func(ln *Lending) {
		ln.afterCompleteLink = func() error {
			if !crashArmed.Swap(false) {
				return nil
			}
			crashes.Add(1)
			// The parts went bad on the lender's disk.
			f, err := os.OpenFile(filepath.Join(l.dir, "data", "big"), os.O_WRONLY, 0)
			if err != nil {
				t.Error(err)
				return errCrashed
			}
			defer f.Close()
			if _, err := f.WriteAt([]byte("rot"), 1000); err != nil {
				t.Error(err)
			}
			return errCrashed
		}
	})
	ctx := t.Context()
	db, err := state.Open(ctx, t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	lender := l.h.self.NodeID()
	if err := db.AddFriendship(ctx, state.Friendship{Peer: lender.String(), Address: "lender", OwnNumber: 1 << 40,
		PeerNumber: 1 << 40, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	store := t.TempDir()
	body := bytes.Repeat([]byte("0123456789abcdef"), (5<<20)/16+1) // two parts
	if err := os.MkdirAll(filepath.Join(store, "laptop", "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store, "laptop", "data", "big"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "data/big", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], int64(len(body)), sum(body)); err != nil {
		t.Fatal(err)
	}
	p := pace.New(time.Now)
	p.Set(pace.Settings{PartSize: 5 << 20})
	w := &replica.Worker{DB: db, StoreDir: store, Log: quiet, Pace: p,
		Connect: func(f state.Friendship) (replica.Replica, error) {
			return replica.NewFriend(l.id, lender, f.Address, func(ctx context.Context, _ identity.NodeID, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", l.h.socket)
			}, replica.Options{})
		},
		Poll: 10 * time.Millisecond, MinBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
		PauseRetry: 50 * time.Millisecond}
	crashArmed.Store(true)
	wctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Run(wctx) }()
	t.Cleanup(func() { cancel(); <-done })

	for deadline := time.Now().Add(time.Minute); ; time.Sleep(20 * time.Millisecond) {
		var s string
		db.QueryRow(`SELECT state FROM placement WHERE place = ? AND seq = ?`, lender.String(), seqs[0]).Scan(&s)
		if s == state.PlacementAcked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the object was never acknowledged (row %q)", s)
		}
	}
	if crashes.Load() != 1 {
		t.Fatalf("the lender crashed %d times, want once", crashes.Load())
	}
	if got, ok := l.stored("data/big"); !ok || !bytes.Equal(got, body) {
		t.Fatalf("the borrower acknowledged a copy the lender holds bad (%d bytes)", len(got))
	}
	if us, err := l.h.db.LentUploads(ctx, l.peer()); err != nil || len(us) != 0 {
		t.Fatalf("rows left: %+v %v", us, err)
	}
	want := ov + ov + int64(len(body))
	l.wantUsage(want)
	l.reconcileNow()
	l.wantUsage(want)
}

// LEND-4 (#113): HEAD of a key whose object an upload linked before a
// crash answers only once the lender has verified it: a good object
// answers its hash and is counted as an object, a bad one is removed
// and answers 404, and either's row goes with the check.
func TestHeadVerifiesAnObjectLinkedBeforeACrash(t *testing.T) {
	l, crash := crashingLender(t, nil)
	good, sent := []byte("the declared bytes"), []byte("sent instead")
	goodID := l.crashedCompletion(crash, "data/good", good, good)
	badID := l.crashedCompletion(crash, "data/bad", []byte("declared"), sent)

	if _, ok, err := l.head("data/bad"); err != nil || ok {
		t.Fatalf("HEAD of an object that fails its hash: ok %v, %v; want 404", ok, err)
	}
	if _, ok := l.stored("data/bad"); ok {
		t.Fatal("the object that failed its hash is kept")
	}
	if h, ok, err := l.head("data/good"); err != nil || !ok || h != sum(good) {
		t.Fatalf("HEAD of a good object: %q %v %v", h, ok, err)
	}
	l.wantNoRow(goodID)
	l.wantNoRow(badID)
	want := ov + ov + int64(len(good))
	l.wantUsage(want)
	l.reconcileNow()
	l.wantUsage(want)
}

// LEND-4 (#113): ListParts answering NoSuchUpload for an upload whose
// object was linked before a crash verifies the object before it
// answers, so the HEAD the borrower sends next finds it gone.
func TestListPartsNoSuchUploadVerifiesTheLinkedObject(t *testing.T) {
	l, crash := crashingLender(t, nil)
	id := l.crashedCompletion(crash, "data/mp", []byte("declared"), []byte("sent instead"))
	_, err := l.c.ListParts(context.Background(), &s3.ListPartsInput{Bucket: l.bucket, Key: aws.String("data/mp"), UploadId: &id})
	if code(err) != "NoSuchUpload" {
		t.Fatalf("ListParts of the completed upload: %v", err)
	}
	if _, ok := l.stored("data/mp"); ok {
		t.Fatal("ListParts answered NoSuchUpload with the unverified object still at its key")
	}
	l.wantNoRow(id)
	l.wantUsage(ov)
}

// LEND-4, RET-1 (#113): no write is answered from an object an upload
// linked before a crash: a completion of another upload of the same
// hash, a put of the same hash, and a delete whose tomb a put of that
// hash would revive each find the bad object removed first, and store
// the borrower's bytes.
func TestNoWriteIsAnsweredFromAnObjectLinkedBeforeACrash(t *testing.T) {
	good := []byte("the declared bytes")
	for _, tc := range []struct {
		name  string
		write func(l *lent) error
	}{
		{"completion", func(l *lent) error {
			id, parts := l.uploadParts("data/mp", good, good)
			return l.complete("data/mp", id, parts...)
		}},
		{"put", func(l *lent) error { return l.put("data/mp", good, hashMeta(good)) }},
		{"delete then put", func(l *lent) error {
			if _, err := l.c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: l.bucket, Key: aws.String("data/mp")}); err != nil {
				return err
			}
			if ts := l.tombs("data/mp"); len(ts) != 0 {
				l.h.t.Fatalf("an unverified object was tombed: %+v", ts)
			}
			return l.put("data/mp", good, hashMeta(good))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, crash := crashingLender(t, nil)
			id := l.crashedCompletion(crash, "data/mp", good, []byte("sent instead"))
			if err := tc.write(l); err != nil {
				t.Fatal(err)
			}
			if got, _ := l.stored("data/mp"); !bytes.Equal(got, good) {
				t.Fatalf("the key holds %q", got)
			}
			l.wantNoRow(id)
			l.wantUsage(ov + ov + int64(len(good)))
		})
	}
}

// LEND-4 (#113): the check costs nothing to a key with no unverified
// upload: HEAD of a put's object and of a verified multipart object
// hashes nothing, and an object linked before a crash is hashed once.
func TestOnlyAnUnverifiedObjectIsHashedOnHeadAndOnlyOnce(t *testing.T) {
	var read atomic.Int32
	l, crash := crashingLender(t, func(ln *Lending) { ln.afterVouchRead = func() { read.Add(1) } })
	body := []byte("an object")
	if err := l.put("data/put", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.mpu("data/mp", sum(body), body); err != nil {
		t.Fatal(err)
	}
	l.crashedCompletion(crash, "data/crashed", body, body)
	for _, key := range []string{"data/put", "data/mp", "data/crashed", "data/crashed", "data/put"} {
		if h, ok, err := l.head(key); err != nil || !ok || h != sum(body) {
			t.Fatalf("HEAD %s: %q %v %v", key, h, ok, err)
		}
	}
	if n := read.Load(); n != 1 {
		t.Fatalf("%d objects read for HEAD, want the unverified one once", n)
	}
}

// LEND-4, #74 (#113): an object a rewrite in parts published before a
// crash, which fails its hash, is removed and the version it replaced
// moved back from its tomb, as a completion that finds the mismatch
// does: by the reconcile, and by HEAD, which then answers the replaced
// version's hash.
func TestAFailedRewriteFoundAfterACrashMovesTheReplacedVersionBack(t *testing.T) {
	l, crash := crashingLender(t, nil)
	v1, v2 := []byte("config one"), []byte("config two")
	for _, key := range []string{"config", "other"} {
		if err := l.put(key, v1, hashMeta(v1)); err != nil {
			t.Fatal(err)
		}
		id, err := l.create(key, rewriteMeta(v2, sum(v1)))
		if err != nil {
			t.Fatal(err)
		}
		p, err := l.part(key, id, 1, []byte("not config two"))
		if err != nil {
			t.Fatal(err)
		}
		crash.Store(true)
		if err := l.complete(key, id, p); status(err) != 500 {
			t.Fatalf("the completion the lender crashed in: %v", err)
		}
		if ts := l.tombs(key); len(ts) != 1 || ts[0].SHA256 != sum(v1) {
			t.Fatalf("the replaced version's tomb: %+v", ts)
		}
	}

	if h, ok, err := l.head("other"); err != nil || !ok || h != sum(v1) {
		t.Fatalf("HEAD after the failed rewrite: %q %v %v; want the replaced version", h, ok, err)
	}
	l.reconcileNow()
	for _, key := range []string{"config", "other"} {
		if got, _ := l.stored(key); !bytes.Equal(got, v1) {
			t.Fatalf("%s holds %q, want the replaced version back", key, got)
		}
		if ts := l.tombs(key); len(ts) != 0 {
			t.Fatalf("%s's tombs: %+v", key, ts)
		}
	}
	l.wantUsage(ov + 2*(ov+int64(len(v1))))
}

// LEND-1, LEND-4 (#113): a leftover whose linked object cannot be read
// keeps its row and its hidden directory, counted, past the abandon
// period: it is never removed unverified. Once the object reads, the
// reconcile verifies it and removes the leftover.
func TestAnUnreadableLinkedObjectKeepsItsRowAndLeftover(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().Unix())
	l := lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(l *Lending) { l.Now = func() time.Time { return time.Unix(now.Load(), 0) } }
	})
	body := []byte("the declared bytes")
	id, parts := l.uploadParts("data/mp", body, body)
	hidden := l.linkedThenCrashed("data/mp", id, parts)
	obj := filepath.Join(l.dir, "data", "mp")
	if err := os.Chmod(obj, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(obj, 0o600) })
	if _, err := os.ReadFile(obj); err == nil {
		t.Skip("this user reads a file of mode 0 (root?)")
	}
	now.Add(int64(8 * 24 * time.Hour / time.Second))
	l.reconcileNow()
	l.wantThere(hidden, "the leftover of an object that cannot be read")
	l.wantRow(*id)

	if err := os.Chmod(obj, 0o600); err != nil {
		t.Fatal(err)
	}
	l.reconcileNow()
	l.wantGone(hidden, "the leftover of a verified object")
	l.wantNoRow(*id)
	l.wantUsage(ov + ov + int64(len(body)))
}

// LEND-4 (#113): an object that fails its hash and cannot be removed
// keeps its row, so the next reconcile checks it again; meanwhile HEAD
// does not answer for it.
func TestAFailedRemovalKeepsTheRowForTheNextReconcile(t *testing.T) {
	l, crash := crashingLender(t, nil)
	id := l.crashedCompletion(crash, "data/mp", []byte("declared"), []byte("sent instead"))
	dir := filepath.Join(l.dir, "data")
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	if f, err := os.Create(filepath.Join(dir, "probe")); err == nil {
		f.Close()
		t.Skip("this user writes a directory of mode 0500 (root?)")
	}
	l.reconcileNow()
	l.wantRow(id)
	if _, _, err := l.head("data/mp"); status(err) != 500 {
		t.Fatalf("HEAD of a bad object that could not be removed: %v", err)
	}

	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	l.reconcileNow()
	l.wantNoRow(id)
	if _, ok := l.stored("data/mp"); ok {
		t.Fatal("the object that failed its hash is kept")
	}
	l.wantUsage(ov)
}

// LEND-1 (#113): a retried completion reads its upload's row under the
// borrower's pub lock, so a reconcile that settles the upload in the
// meantime is not counted twice. The hook starts a reconcile once the
// completion has read the row and the object: it must wait for the
// completion. Part 2 is counted but left out of the object, so a count
// from a stale row shows.
func TestARetriedCompletionRacingAReconcileCountsTheObjectOnce(t *testing.T) {
	var l *lent
	var raced atomic.Bool
	reconciled := make(chan error, 1)
	l, crash := crashingLender(t, func(ln *Lending) {
		ln.afterVouchRead = func() {
			if raced.Swap(true) {
				return
			}
			go func() { reconciled <- l.lending().reconcileOne(context.Background(), l.peer()) }()
			select {
			case err := <-reconciled:
				reconciled <- err // ran inside the completion's reads
			case <-time.After(500 * time.Millisecond): // waits for the completion
			}
		}
	})
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
	if err := l.complete("data/mp", id, c1); err != nil {
		t.Fatalf("the retried completion: %v", err)
	}
	select {
	case err := <-reconciled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the reconcile never ran")
	}
	l.wantNoRow(*id)
	l.wantUsage(ov + ov + int64(len(p1)))
}
