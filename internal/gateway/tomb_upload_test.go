package gateway

import (
	"bytes"
	"testing"
)

// RET-1, RET-2, found by #15's harness running the attack list in
// another order: completing an upload begun before the delete, which
// declared the tombstoned object's hash, revives the tombstoned object
// whatever bytes the parts hold, and stores none of them. Nothing is
// lost: the original bytes are live again, and a later delete starts a
// new window.
func TestCompletingAnOldUploadRevivesTheObject(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	body := []byte("the only offsite copy")
	if err := l.put("data/ef/pack", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	upID, err := l.create("data/ef/pack", hashMeta(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/ef/pack"); err != nil {
		t.Fatal(err)
	}
	p, err := l.part("data/ef/pack", upID, 1, []byte("ransom note"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.complete("data/ef/pack", upID, p); err != nil {
		t.Fatalf("completing the old upload: %v", err)
	}
	if got, ok := l.stored("data/ef/pack"); !ok || !bytes.Equal(got, body) {
		t.Fatalf("the key holds %q (%v); want the revived original", got, ok)
	}
	if ts := l.tombs("data/ef/pack"); len(ts) != 0 {
		t.Fatalf("tombstones left: %+v", ts)
	}
}
