package gateway

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

// reviveDuring returns a hook that has l's lender undelete the one key
// after after, once, to its end: an undelete inside another request's
// gap.
func reviveDuring(t *testing.T, l *lent, after string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			u, err := l.h.gw.Undelete(context.Background(), l.id.NodeID(), *l.bucket, time.Time{}, false, after, 1)
			if err != nil || len(u.Restored) != 1 {
				t.Errorf("the undelete in the gap: %+v, %v", u, err)
			}
		})
	}
}

// RET-1, RET-3, EP-11 (#32): a put of the tomb's bytes finds the key
// absent; an undelete then revives it and drops the row before the put
// reads the tombs, so the put takes the publish path and the store
// refuses it (If-None-Match) before reading a byte. That refusal is the
// same bytes stored, not another hash: the put answers the stored ETag,
// so the borrower raises no key_changed, and the count is the object's
// once.
func TestSameBytesPutAfterARevivalFoundTheKeyAbsentSucceeds(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	body := []byte("a pack put again as an undelete brings it back")
	if err := l.put("data/1", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/1"); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	l.lending().afterPutLooked = reviveDuring(t, l, "data/")
	if err := l.put("data/1", body, hashMeta(body)); err != nil {
		t.Fatalf("a put of the same bytes racing a revival: %v", err)
	}
	if b, ok := l.stored("data/1"); !ok || !bytes.Equal(b, body) || len(l.tombs("data/1")) != 0 {
		t.Fatalf("data/1 holds %q, tombs %+v", b, l.tombs("data/1"))
	}
	l.wantUsage(usage)
}

// RET-1, LEND-4 (#32): the same gap with other bytes is still refused,
// and the revived version stays.
func TestOtherBytesPutAfterARevivalFoundTheKeyAbsentRefused(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	body, other := []byte("the version undeleted"), []byte("other bytes")
	if err := l.put("data/1", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/1"); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	l.lending().afterPutLooked = reviveDuring(t, l, "data/")
	if err := l.put("data/1", other, hashMeta(other)); code(err) != "PreconditionFailed" {
		t.Fatalf("a put of other bytes racing a revival answered %v, want 412", err)
	}
	if b, ok := l.stored("data/1"); !ok || !bytes.Equal(b, body) || len(l.tombs("data/1")) != 0 {
		t.Fatalf("data/1 holds %q, tombs %+v", b, l.tombs("data/1"))
	}
	l.wantUsage(usage)
}

// RET-1 (#32): an undelete revives a key while a rewrite of it anew
// (other bytes, marked with the tomb's hash) streams. The rewrite chose
// to publish beside the tomb, which is gone when it takes the key's
// lock, so it is refused and the revived version stays: no version is
// lost.
func TestUndeleteRacingARewriteAnewKeepsTheVersion(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	v1, v2 := []byte("version one"), []byte("version two, written anew")
	if err := l.put("k", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("k"); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	chosen, release := make(chan struct{}), make(chan struct{})
	l.lending().afterUndeleteChoose = func() { close(chosen); <-release }
	undone := make(chan Undeleted, 1)
	go func() {
		u, err := l.h.gw.Undelete(context.Background(), l.id.NodeID(), *l.bucket, time.Time{}, false, "", 1000)
		if err != nil {
			t.Error(err)
		}
		undone <- u
	}()
	<-chosen // the undelete holds k's lock
	pr, pw := io.Pipe()
	answer := make(chan error, 1)
	go func() { answer <- l.putDirect("k", pr, int64(len(v2)), rewriteMeta(v2, sum(v1))) }()
	// The rewrite has passed its checks once it reads; at its last byte
	// it waits for k's lock.
	if _, err := pw.Write(v2); err != nil {
		t.Fatal(err)
	}
	close(release)
	if u := <-undone; len(u.Restored) != 1 {
		t.Fatalf("the undelete: %+v", u)
	}
	pw.Close()
	if err := <-answer; !isCode(err, "PreconditionFailed") {
		t.Fatalf("the rewrite anew answered %v, want 412", err)
	}
	if b, ok := l.stored("k"); !ok || !bytes.Equal(b, v1) || len(l.tombs("k")) != 0 {
		t.Fatalf("k holds %q, tombs %+v", b, l.tombs("k"))
	}
	l.wantUsage(usage)
}
