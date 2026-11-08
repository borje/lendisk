package gateway

import (
	"bytes"
	"io"
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
