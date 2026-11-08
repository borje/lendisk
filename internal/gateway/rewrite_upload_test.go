package gateway

import (
	"bytes"
	"testing"
)

// mpuMarked uploads body in one part under key, created with meta, and
// returns the completion's error.
func (l *lent) mpuMarked(key string, body []byte, meta map[string]string) error {
	id, err := l.create(key, meta)
	if err != nil {
		return err
	}
	p, err := l.part(key, id, 1, body)
	if err != nil {
		return err
	}
	return l.complete(key, id, p)
}

// LEND-4, RET-1, #74 (#23): a borrower's multipart upload marked at its
// create with the hash of the version the key holds is a rewrite, as a
// marked PUT is: on completion the replaced version becomes a tomb and
// the new one is stored, both counted until the purge. An unmarked or
// stale-marked create of other bytes is refused before any part.
func TestMarkedMultipartRewriteKeepsTheReplacedVersionAsATomb(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	v1, v2 := []byte("kopia repository one"), []byte("kopia repository two, larger")
	if err := l.put("kopia.repository", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	if _, err := l.create("kopia.repository", hashMeta(v2)); status(err) != 412 {
		t.Fatalf("an unmarked create of other bytes: %v", err)
	}
	if _, err := l.create("kopia.repository", rewriteMeta(v2, sum(v2))); status(err) != 412 {
		t.Fatalf("a create marked with a hash the key does not hold: %v", err)
	}
	if err := l.mpuMarked("kopia.repository", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatalf("marked multipart rewrite: %v", err)
	}
	if got, ok := l.stored("kopia.repository"); !ok || !bytes.Equal(got, v2) {
		t.Fatalf("the key holds %q", got)
	}
	ts := l.tombs("kopia.repository")
	if len(ts) != 1 || ts[0].SHA256 != sum(v1) || !ts[0].Rewrite || ts[0].Pending {
		t.Fatalf("tombstones %+v", ts)
	}
	l.wantUsage(usage + ov + int64(len(v2)))
}

// LEND-4, #74 (#23): a marked upload whose key no longer holds the
// marked version at completion (another rewrite won meanwhile) is
// refused and the key keeps the winner; one whose parts do not hash to
// the declared hash is refused and the replaced version stays the key's.
func TestMarkedMultipartRewriteRefusedWhenStaleOrWrong(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	v1, v2, v3 := []byte("config one"), []byte("config two"), []byte("config three")
	if err := l.put("config", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	id, err := l.create("config", rewriteMeta(v2, sum(v1)))
	if err != nil {
		t.Fatal(err)
	}
	p, err := l.part("config", id, 1, v2)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.put("config", v3, rewriteMeta(v3, sum(v1))); err != nil {
		t.Fatal(err)
	}
	if err := l.complete("config", id, p); status(err) != 412 {
		t.Fatalf("a stale marked completion: %v", err)
	}
	if got, _ := l.stored("config"); !bytes.Equal(got, v3) {
		t.Fatalf("the key holds %q", got)
	}
	usage := l.usage()
	tombs := len(l.tombs("config"))
	if err := l.mpuMarked("config", []byte("not v2"), rewriteMeta(v2, sum(v3))); status(err) != 400 {
		t.Fatalf("a marked completion of other bytes: %v", err)
	}
	if got, _ := l.stored("config"); !bytes.Equal(got, v3) {
		t.Fatalf("after a refused rewrite the key holds %q", got)
	}
	if n := len(l.tombs("config")); n != tombs {
		t.Fatalf("tombs %d, want %d", n, tombs)
	}
	l.wantUsage(usage)
}

// LEND-4, #74 (#23): a deleted key written anew in parts, marked with
// the version its newest tomb holds, is stored beside the tomb.
func TestMarkedMultipartOfADeletedKeyKeepsItsTomb(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	v1, v2 := []byte("pack one"), []byte("pack two")
	if err := l.put("data/pack", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/pack"); err != nil {
		t.Fatal(err)
	}
	if err := l.mpu("data/pack", sum(v2), v2); status(err) != 412 {
		t.Fatalf("unmarked other bytes over a tomb: %v", err)
	}
	if err := l.mpuMarked("data/pack", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatalf("marked: %v", err)
	}
	if got, _ := l.stored("data/pack"); !bytes.Equal(got, v2) {
		t.Fatalf("the key holds %q", got)
	}
	if ts := l.tombs("data/pack"); len(ts) != 1 || ts[0].SHA256 != sum(v1) {
		t.Fatalf("tombs %+v", ts)
	}
}
