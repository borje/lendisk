package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// undelete asks l's lender to undelete l's bucket since since.
func (l *lent) undelete(since time.Time, rewrites bool, after string, limit int) Undeleted {
	l.h.t.Helper()
	u, err := l.h.gw.Undelete(context.Background(), l.id.NodeID(), *l.bucket, since, rewrites, after, limit)
	if err != nil {
		l.h.t.Fatal(err)
	}
	return u
}

func restoredKeys(u Undeleted) []string {
	var out []string
	for _, o := range u.Restored {
		out = append(out, o.Key)
	}
	return out
}

// RET-3 (#32): undelete moves each key's tomb deleted since the time
// given back to its key and drops the row; a tomb deleted before stays.
// The count does not move: a tomb counts until purged, so reviving it
// moves it from tomb to live unchanged (LEND-1).
func TestUndeleteRestoresTheTombsDeletedSince(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	old, a, b := []byte("an old pack"), []byte("pack a"), []byte("pack b, pruned")
	for k, v := range map[string][]byte{"data/00/old": old, "data/aa/a": a, "data/bb/b": b} {
		if err := l.put(k, v, hashMeta(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.del("data/00/old"); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * day)
	attack := c.Now()
	for _, k := range []string{"data/aa/a", "data/bb/b"} {
		if err := l.del(k); err != nil {
			t.Fatal(err)
		}
	}
	usage := l.usage()
	u := l.undelete(attack, false, "", 1000)
	if got := restoredKeys(u); len(got) != 2 || got[0] != "data/aa/a" || got[1] != "data/bb/b" || u.Next != "" || len(u.Skipped) != 0 {
		t.Fatalf("restored %v, skipped %+v, next %q", got, u.Skipped, u.Next)
	}
	if o := u.Restored[1]; o.SHA256 != sum(b) || o.Size != int64(len(b)) || !o.TombstonedAt.Equal(attack) {
		t.Fatalf("restored %+v", o)
	}
	for k, v := range map[string][]byte{"data/aa/a": a, "data/bb/b": b} {
		if got, ok := l.stored(k); !ok || !bytes.Equal(got, v) {
			t.Fatalf("%s holds %q", k, got)
		}
		if ts := l.tombs(k); len(ts) != 0 {
			t.Fatalf("%s keeps tombstones %+v", k, ts)
		}
	}
	if _, ok := l.stored("data/00/old"); ok || len(l.tombs("data/00/old")) != 1 {
		t.Fatal("a tomb deleted before since was revived")
	}
	l.wantUsage(usage)
	// Again: nothing left since then.
	if u := l.undelete(attack, false, "", 1000); len(u.Restored) != 0 || len(u.Skipped) != 0 {
		t.Fatalf("a second undelete: %+v", u)
	}
	// Without a since, every tomb in the window.
	if got := restoredKeys(l.undelete(time.Time{}, false, "", 1000)); len(got) != 1 || got[0] != "data/00/old" {
		t.Fatalf("restored %v", got)
	}
	l.wantUsage(usage)
	if keys := l.listKeys(); len(keys) != 3 {
		t.Fatalf("the borrower lists %v", keys)
	}
}

// RET-1 (#32): a key live again with other bytes is not overwritten:
// undelete skips it and reports both versions, and the tomb stays for
// its window, so no version is lost.
func TestUndeleteSkipsAKeyLiveWithOtherBytes(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	v1, v2 := []byte("config one"), []byte("config two")
	if err := l.put("config", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.del("config"); err != nil {
		t.Fatal(err)
	}
	// A rewrite marked with the newest tomb's hash is stored beside it.
	if err := l.put("config", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	u := l.undelete(time.Time{}, false, "", 1000)
	if len(u.Restored) != 0 || len(u.Skipped) != 1 {
		t.Fatalf("undelete %+v", u)
	}
	if s := u.Skipped[0]; s.Key != "config" || s.SHA256 != sum(v1) || s.Live != sum(v2) || s.Reason != SkipConflict {
		t.Fatalf("skipped %+v", s)
	}
	if got, _ := l.stored("config"); !bytes.Equal(got, v2) {
		t.Fatalf("the key holds %q", got)
	}
	if ts := l.tombs("config"); len(ts) != 1 {
		t.Fatalf("tombstones %+v", ts)
	}
	l.wantUsage(usage)
}

// RET-1, RET-3 (#32): of a key's delete tombs since the time given, the
// newest comes back: the version the key held when it was last deleted,
// as S3's removing a delete marker makes the latest version before it
// current. Rewrite tombs (#74) are not revived unless asked; asked, the
// key comes back as it was at since: its oldest tomb since then.
func TestUndeleteChoosesTheVersion(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	v1, v2 := []byte("kopia.repository, good"), []byte("kopia.repository, the attacker's")
	if err := l.put("kopia.repository", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Hour)
	before := c.Now()
	c.advance(time.Hour)
	// The attacker rewrites the key, then deletes it.
	if err := l.put("kopia.repository", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Minute)
	if err := l.del("kopia.repository"); err != nil {
		t.Fatal(err)
	}
	// A key rewritten and live: no delete tomb, so nothing by default.
	if err := l.put("kopia.maintenance", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	if err := l.put("kopia.maintenance", v2, rewriteMeta(v2, sum(v1))); err != nil {
		t.Fatal(err)
	}
	usage := l.usage()
	u := l.undelete(before, false, "", 1000)
	if got := restoredKeys(u); len(got) != 1 || u.Restored[0].SHA256 != sum(v2) || len(u.Skipped) != 0 {
		t.Fatalf("by default: %+v", u)
	}
	if got, _ := l.stored("kopia.repository"); !bytes.Equal(got, v2) {
		t.Fatalf("the key holds %q", got)
	}
	if ts := l.tombs("kopia.repository"); len(ts) != 1 || !ts[0].Rewrite {
		t.Fatalf("the rewrite's tomb: %+v", ts)
	}
	l.wantUsage(usage)

	// Asked for rewrites: the version at since. A key live with other
	// bytes is still not overwritten: delete it first.
	u = l.undelete(before, true, "", 1000)
	if len(u.Restored) != 0 || len(u.Skipped) != 2 || u.Skipped[0].Reason != SkipConflict || u.Skipped[1].Reason != SkipConflict {
		t.Fatalf("with rewrites, both live: %+v", u)
	}
	if err := l.del("kopia.repository"); err != nil {
		t.Fatal(err)
	}
	u = l.undelete(before, true, "", 1000)
	if len(u.Restored) != 1 || u.Restored[0].Key != "kopia.repository" || u.Restored[0].SHA256 != sum(v1) {
		t.Fatalf("with rewrites: %+v", u)
	}
	if got, _ := l.stored("kopia.repository"); !bytes.Equal(got, v1) {
		t.Fatalf("the key holds %q", got)
	}
	if ts := l.tombs("kopia.repository"); len(ts) != 1 || ts[0].SHA256 != sum(v2) || ts[0].Rewrite {
		t.Fatalf("the attacker's version is kept as a tomb: %+v", ts)
	}
	l.wantUsage(usage)
}

// RET-3, PEER-8 (#32): undelete pages through the caller's own bucket
// only: another borrower's sealed name, or a bucket never made, holds
// nothing of the caller's.
func TestUndeletePagesTheCallersOwnBucket(t *testing.T) {
	l, _ := lendWithClock(t, 0, 0)
	for _, k := range []string{"data/aa/1", "data/bb/2", "data/cc/3"} {
		if err := l.put(k, []byte(k), hashMeta([]byte(k))); err != nil {
			t.Fatal(err)
		}
		if err := l.del(k); err != nil {
			t.Fatal(err)
		}
	}
	u := l.undelete(time.Time{}, false, "", 2)
	if got := restoredKeys(u); len(got) != 2 || u.Next != "data/bb/2" {
		t.Fatalf("first page %v next %q", got, u.Next)
	}
	u = l.undelete(time.Time{}, false, u.Next, 2)
	if got := restoredKeys(u); len(got) != 1 || got[0] != "data/cc/3" || u.Next != "" {
		t.Fatalf("second page %v next %q", got, u.Next)
	}
	other := newIdentity(t)
	ctx := context.Background()
	if _, err := l.h.gw.Undelete(ctx, other.NodeID(), *l.bucket, time.Time{}, false, "", 10); !errors.Is(err, ErrNoLentBucket) {
		t.Fatalf("another borrower: %v", err)
	}
	if _, err := l.h.gw.Undelete(ctx, l.id.NodeID(), l.id.SealedName("never"), time.Time{}, false, "", 10); !errors.Is(err, ErrNoLentBucket) {
		t.Fatalf("a bucket never made: %v", err)
	}
}
