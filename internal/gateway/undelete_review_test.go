package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// RET-1, RET-3 (#32 review): a key that cannot be revived does not stop
// the page. Here a/b is tombstoned and a was written since, so a/b's
// directory cannot be made: a/b is reported failed with the reason, and
// the keys before and after it are restored and reported, so the
// borrower records every key the lender revived.
func TestUndeleteReportsAKeyItCannotReviveAndGoesOn(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	for _, k := range []string{"0x", "a/b", "c"} {
		if err := l.put(k, []byte(k), hashMeta([]byte(k))); err != nil {
			t.Fatal(err)
		}
		if err := l.del(k); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.put("a", []byte("a file where a directory was"), hashMeta([]byte("a file where a directory was"))); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	u := l.undelete(time.Time{}, false, "", 1000)
	if got := restoredKeys(u); len(got) != 2 || got[0] != "0x" || got[1] != "c" {
		t.Fatalf("restored %v, skipped %+v", got, u.Skipped)
	}
	if len(u.Skipped) != 1 || u.Skipped[0].Key != "a/b" || u.Skipped[0].Reason != SkipFailed || u.Skipped[0].Detail == "" {
		t.Fatalf("skipped %+v", u.Skipped)
	}
	if ts := l.tombs("a/b"); len(ts) != 1 || ts[0].Pending {
		t.Fatalf("a/b's tombstone %+v", ts)
	}
	l.wantUsage(usage)
	// Asked again, the same: nothing lost, nothing stuck behind it.
	if u := l.undelete(time.Time{}, false, "", 1000); len(u.Restored) != 0 || len(u.Skipped) != 1 {
		t.Fatalf("again: %+v", u)
	}
}

// RET-3 (#32 review): a request cancelled mid-page (the borrower's call
// timing out, its daemon stopping) ends the page after the key in hand,
// whose revival runs to its end, and reports what it restored, with Next
// at the last key handled; a key not reached keeps its tomb.
func TestUndeleteCancelledMidPageReportsWhatItDid(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	for _, k := range []string{"data/1", "data/2", "data/3"} {
		if err := l.put(k, []byte(k), hashMeta([]byte(k))); err != nil {
			t.Fatal(err)
		}
		if err := l.del(k); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.lending().afterUndeleteKey = func(string) { cancel() }
	u, err := l.h.gw.Undelete(ctx, l.id.NodeID(), *l.bucket, time.Time{}, false, "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if got := restoredKeys(u); len(got) != 1 || got[0] != "data/1" || u.Next != "data/1" {
		t.Fatalf("restored %v, next %q", got, u.Next)
	}
	if b, ok := l.stored("data/1"); !ok || string(b) != "data/1" || len(l.tombs("data/1")) != 0 {
		t.Fatal("data/1 is not live")
	}
	if len(l.tombs("data/2")) != 1 {
		t.Fatal("a key the page did not reach lost its tomb")
	}
	l.lending().afterUndeleteKey = nil
	if got := restoredKeys(l.undelete(time.Time{}, false, u.Next, 1000)); len(got) != 2 {
		t.Fatalf("the next page restored %v", got)
	}
}

// RET-3 (#32 review): with rewrites, a tomb deleted before since is not
// a version since then: the key comes back as the oldest tomb since,
// and a key whose only tomb is older stays.
func TestUndeleteWithRewritesKeepsToSince(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	v1, v2 := []byte("version one"), []byte("version two")
	if err := l.put("config", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("config", v2, rewriteMeta(v2, sum(v1))); err != nil { // v1's rewrite tomb, before since
		t.Fatal(err)
	}
	if err := l.put("old", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("old"); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Hour)
	since := c.Now()
	if err := l.del("config"); err != nil { // v2's delete tomb, since
		t.Fatal(err)
	}
	u := l.undelete(since, true, "", 1000)
	if got := restoredKeys(u); len(got) != 1 || got[0] != "config" || u.Restored[0].SHA256 != sum(v2) {
		t.Fatalf("restored %+v", u)
	}
	if b, _ := l.stored("config"); !bytes.Equal(b, v2) {
		t.Fatalf("config holds %q", b)
	}
	if _, ok := l.stored("old"); ok {
		t.Fatal("a tomb from before since was revived")
	}
}

// RET-1, RET-3 (#32 review): an undelete racing a put of the same key
// holds the key's lock and the borrower's publication lock: whichever
// goes first, the key ends live with the tomb's bytes, the tomb row is
// gone, and the count is the object's once.
func TestUndeleteRacingAPutOfTheKey(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	usage := l.usage()
	for i := range 20 {
		key := fmt.Sprintf("data/%02d", i)
		body := []byte("pack " + key)
		if err := l.put(key, body, hashMeta(body)); err != nil {
			t.Fatal(err)
		}
		if err := l.del(key); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var perr, uerr error
		wg.Go(func() { perr = l.put(key, body, hashMeta(body)) })
		wg.Go(func() {
			_, uerr = l.h.gw.Undelete(context.Background(), l.id.NodeID(), *l.bucket, time.Time{}, false, key[:len(key)-1], 1000)
		})
		wg.Wait()
		if perr != nil || uerr != nil {
			t.Fatalf("%s: put %v, undelete %v", key, perr, uerr)
		}
		if b, ok := l.stored(key); !ok || !bytes.Equal(b, body) || len(l.tombs(key)) != 0 {
			t.Fatalf("%s holds %q, tombs %+v", key, b, l.tombs(key))
		}
		usage += int64(len(body)) + ov
		l.wantUsage(usage)
	}
}

// RET-1, RET-3 (#32 review): a crash in the middle of an undelete's
// revival, after the tomb is linked back and before it is unlinked,
// leaves the row pending; settleTombs at the next start finishes it.
func TestAnUndeleteCrashIsSettledAtStart(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	body := []byte("a pack")
	if err := l.put("data/1", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/1"); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	crash := errors.New("the process died")
	l.lending().afterReviveLink = func() error { return crash }
	u := l.undelete(time.Time{}, false, "", 1000)
	if len(u.Restored) != 0 || len(u.Skipped) != 1 || u.Skipped[0].Reason != SkipFailed {
		t.Fatalf("a crashed revival: %+v", u)
	}
	if ts := l.tombs("data/1"); len(ts) != 1 || !ts[0].Pending {
		t.Fatalf("the row after the crash: %+v", ts)
	}
	l.lending().afterReviveLink = nil
	if err := l.lending().settleTombs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b, ok := l.stored("data/1"); !ok || !bytes.Equal(b, body) || len(l.tombs("data/1")) != 0 {
		t.Fatalf("after settling: %q, tombs %+v", b, l.tombs("data/1"))
	}
	l.wantUsage(usage)
}

// RET-1, RET-3 (#32 review): an undelete holds the key's lock from its
// choice of tomb to the end of the revival, so a purge of that tomb due
// meanwhile waits for it and then finds nothing to purge: the object is
// live, not lost between the choice and the revival.
func TestUndeleteHoldsTheKeyAgainstThePurge(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	body := []byte("a pack due for purging")
	if err := l.put("data/1", body, hashMeta(body)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("data/1"); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	c.advance(31 * day)
	chosen, release := make(chan struct{}), make(chan struct{})
	l.lending().afterUndeleteChoose = func() { close(chosen); <-release }
	done := make(chan Undeleted, 1)
	go func() {
		u, _ := l.h.gw.Undelete(context.Background(), l.id.NodeID(), *l.bucket, time.Time{}, false, "", 1000)
		done <- u
	}()
	<-chosen
	purged := make(chan error, 1)
	go func() { purged <- l.lending().purge(context.Background()) }()
	// The purge cannot finish while the undelete holds the key; give one
	// that does not hold it the chance to. The outcome does not depend on
	// this wait.
	select {
	case err := <-purged:
		purged <- err
	case <-time.After(time.Second):
	}
	close(release)
	if u := <-done; len(u.Restored) != 1 {
		t.Fatalf("undelete %+v", u)
	}
	if err := <-purged; err != nil {
		t.Fatal(err)
	}
	if b, ok := l.stored("data/1"); !ok || !bytes.Equal(b, body) || len(l.tombs("data/1")) != 0 {
		t.Fatalf("after the race: %q, tombs %+v", b, l.tombs("data/1"))
	}
	l.wantUsage(usage)
}
