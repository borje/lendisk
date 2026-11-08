package gateway

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

// RET-2, LEND-4 (#74 review, B1): a plain put of an absent key that began
// streaming before another put published the key, and whose body ends
// while a rewrite of that key has moved the replaced version into its
// tomb and not yet published, must not publish into that gap: the
// rewrite would then publish over it with no tomb, and an acknowledged
// version would be lost within the window. The plain put waits for the
// key's lock once its body is verified, so it publishes after the
// rewrite and is refused (412); every acknowledged version is live or in
// a tomb.
func TestPlainPutCannotPublishInARewritesGap(t *testing.T) {
	v1, v2, v3 := []byte("version one"), []byte("version two"), []byte("plain put three")
	pr, pw := io.Pipe()
	answer := make(chan error, 1)
	var l *lent
	l = lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(ld *Lending) {
			ld.afterRewriteMove = func() error {
				// B's body ends now, in the rewrite's gap.
				go func() { pw.Write(v3[1:]); pw.Close() }()
				select {
				case err := <-answer:
					answer <- err
				case <-time.After(2 * time.Second):
					// B waits for the key's lock: the fixed behaviour.
				}
				return nil
			}
		}
	})
	go func() { answer <- l.putDirect("k", pr, int64(len(v3)), hashMeta(v3)) }()
	// B has passed its checks, with the key absent, once it reads.
	if _, err := pw.Write(v3[:1]); err != nil {
		t.Fatal(err)
	}
	if err := l.put("k", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("k", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	var errB error
	select {
	case errB = <-answer:
	case <-time.After(10 * time.Second):
		t.Fatal("the plain put never answered")
	}
	held := map[string]bool{}
	if got, ok := l.stored("k"); ok {
		held[string(got)] = true
	}
	for _, ts := range l.tombs("k") {
		if b, err := l.tombBytes(ts); err == nil {
			held[string(b)] = true
		}
	}
	if !held[string(v1)] || !held[string(v2)] {
		t.Fatalf("the rewrite's versions: held %v", held)
	}
	if errB == nil && !held[string(v3)] {
		t.Fatalf("the plain put was acknowledged and its bytes are gone: held %v", held)
	}
	if !isCode(errB, "PreconditionFailed") {
		t.Fatalf("the plain put answered %v, want 412", errB)
	}
	if got, _ := l.stored("k"); !bytes.Equal(got, v2) {
		t.Fatalf("the key holds %q", got)
	}
}

// LEND-4, RET-2 (#74; found by TestRewriteRacingDeleteLosesNothing on a
// loaded runner): a marked put of an absent key decides on arrival that
// it writes the key anew over its newest tomb, but publishes only once
// its body has ended. If, meanwhile, another rewrite published the key
// and a delete tombed that version, the mark names a tomb that is no
// longer the newest: the put is stale and refused (412), as it would be
// had it arrived then, and the key stays deleted.
func TestStaleRewriteOfADeletedKeyRefused(t *testing.T) {
	v1, v2, v3 := []byte("version one"), []byte("version two"), []byte("stale three")
	pr, pw := io.Pipe()
	answer := make(chan error, 1)
	var l *lent
	var once sync.Once
	l = lendTo(t, 1<<30, func(h *harness) {
		h.lending = func(ld *Lending) {
			ld.afterRewriteMove = func() error {
				once.Do(func() {
					// v1 is in its tomb and v2 not yet published: B finds
					// the key absent and v1 its newest tomb.
					go func() { answer <- l.putDirect("k", pr, int64(len(v3)), rewriteMeta(v3, sum(v1))) }()
					// B has passed its checks once it reads.
					if _, err := pw.Write(v3[:1]); err != nil {
						t.Error(err)
					}
				})
				return nil
			}
		}
	})
	if err := l.put("k", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("k", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := l.del("k"); err != nil {
		t.Fatal(err)
	}
	if _, err := pw.Write(v3[1:]); err != nil {
		t.Fatal(err)
	}
	pw.Close()
	var errB error
	select {
	case errB = <-answer:
	case <-time.After(10 * time.Second):
		t.Fatal("the stale rewrite never answered")
	}
	if !isCode(errB, "PreconditionFailed") {
		t.Fatalf("the stale rewrite answered %v, want 412", errB)
	}
	if got, ok := l.stored("k"); ok {
		t.Fatalf("the key holds %q, want it deleted", got)
	}
	held := map[string]bool{}
	for _, ts := range l.tombs("k") {
		if b, err := l.tombBytes(ts); err == nil {
			held[string(b)] = true
		}
	}
	if !held[string(v1)] || !held[string(v2)] {
		t.Fatalf("tombs hold %v, want both versions", held)
	}
}
