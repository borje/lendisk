package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/state"
)

func sum(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// recovering is a node mid-recovery with friends places, a bucket of
// profile and a list to read of it at each.
func recovering(t *testing.T, profile string, places ...string) (*state.DB, *Lister) {
	t.Helper()
	ctx := t.Context()
	db := openDB(t)
	now := time.Unix(1_800_000_000, 0)
	for _, f := range places {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 1 << 30, PeerNumber: 1 << 30,
			CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: profile, Copies: 2, AccessKey: "A", SecretKey: "S",
		CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddRelists(ctx, places, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	return db, &Lister{DB: db, Self: newID(t), Now: func() time.Time { return now }, Log: quiet}
}

// served answers each place's list from a fixed page set.
func served(byPlace map[string][]control.ListObject) func(context.Context, state.Friendship, string, string) (control.ListPage, error) {
	return func(_ context.Context, f state.Friendship, _, after string) (control.ListPage, error) {
		var out []control.ListObject
		for _, o := range byPlace[f.Peer] {
			if o.Key > after {
				out = append(out, o)
			}
		}
		return control.ListPage{Objects: out}, nil
	}
}

func heldKeys(t *testing.T, db *state.DB, place string) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT key, sha256 FROM placement WHERE place = ? AND state = 'acked'`, place)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, h string
		rows.Scan(&k, &h)
		out[k] = h
	}
	return out
}

// #29 review (user decision 2026-11-07): restic names its packs,
// indexes and snapshots by their SHA-256, so a friend listing another
// hash for one is rejected by the name, however it sorts: a hostile
// friend listed first makes no honest copy not count.
func TestHostileFriendsPacksRejectedByName(t *testing.T) {
	db, l := recovering(t, "restic", "A0", "B")
	pack, idx := sum("pack"), sum("index")
	good := []control.ListObject{{Key: "data/" + pack[:2] + "/" + pack, Size: 9, SHA256: pack},
		{Key: "index/" + idx, Size: 4, SHA256: idx}}
	bad := []control.ListObject{{Key: good[0].Key, Size: 9, SHA256: sum("forged")},
		{Key: good[1].Key, Size: 4, SHA256: sum("forged index")}}
	l.Page = served(map[string][]control.ListObject{"A0": bad, "B": good})
	rs, err := l.Round(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if r.Place == "A0" && (r.Err != nil || r.Rejected != 2 || r.Listed != 0) || r.Place == "B" && (r.Err != nil || r.Listed != 2) {
			t.Fatalf("result %+v", r)
		}
	}
	if got := heldKeys(t, db, "A0"); len(got) != 0 {
		t.Fatalf("the forging friend counts as holding %v", got)
	}
	if got := heldKeys(t, db, "B"); got[good[0].Key] != pack || got[good[1].Key] != idx {
		t.Fatalf("B holds %v", got)
	}
	if cs, _ := db.Conflicts(t.Context()); len(cs) != 0 {
		t.Fatalf("conflicts %+v", cs)
	}
}

// #29 review: a hostile friend's list cannot inject keys a borrower may
// not name, impossible sizes or malformed hashes (each entry is
// rejected), and cannot keep the recovery listing forever: past the cap
// its list is abandoned with what it recorded.
func TestHostileListerCannotInjectOrStall(t *testing.T) {
	db, l := recovering(t, "generic", "B")
	ok := sum("ok")
	l.Page = served(map[string][]control.ListObject{"B": {
		{Key: "../escape", Size: 1, SHA256: ok},
		{Key: ".lendisk/tomb/x", Size: 1, SHA256: ok},
		{Key: "fine", Size: 5, SHA256: ok},
		{Key: "huge", Size: gateway.MaxObjectSize + 1, SHA256: ok},
		{Key: "minus", Size: -1, SHA256: ok},
		{Key: "nohash", Size: 1, SHA256: "zz"},
		{Key: "upper", Size: 1, SHA256: strings.ToUpper(ok)},
	}})
	rs, err := l.Round(t.Context())
	if err != nil || len(rs) != 1 || rs[0].Err != nil || rs[0].Listed != 1 || rs[0].Rejected != 6 {
		t.Fatalf("round %+v %v", rs, err)
	}
	if got := heldKeys(t, db, "B"); len(got) != 1 || got["fine"] != ok {
		t.Fatalf("held %v", got)
	}

	// An endless list, one key a page, each past the last.
	db, l = recovering(t, "generic", "B")
	l.MaxKeys = 50
	pages := 0
	l.Page = func(_ context.Context, _ state.Friendship, _, after string) (control.ListPage, error) {
		pages++
		k := fmt.Sprintf("k%08d", pages)
		return control.ListPage{Objects: []control.ListObject{{Key: k, Size: 1, SHA256: sum(k)}}, Next: k}, nil
	}
	rs, err = l.Round(t.Context())
	if err != nil || len(rs) != 1 || rs[0].Err == nil || !strings.Contains(rs[0].Err.Error(), "more than 50") {
		t.Fatalf("endless list: %+v %v", rs, err)
	}
	if pages > 60 {
		t.Fatalf("read %d pages", pages)
	}
	if n, _ := l.Pending(t.Context()); n != 0 {
		t.Fatal("the endless list is still to read")
	}
	if got := heldKeys(t, db, "B"); len(got) != 0 {
		t.Fatalf("the abandoned list still counts: %d keys", len(got))
	}
}

// #29 review: only the lender's "no such bucket" means it holds nothing
// of it; any other refusal, a 404 from a node without the call
// included, is a failure tried again after the backoff.
func TestOnlyNoSuchBucketMeansHoldsNothing(t *testing.T) {
	db, l := recovering(t, "restic", "B", "C")
	l.Page = func(_ context.Context, f state.Friendship, _, _ string) (control.ListPage, error) {
		if f.Peer == "B" {
			return control.ListPage{}, &control.CallError{Status: http.StatusNotFound, Message: "no bucket", Code: control.CodeNoSuchBucket}
		}
		return control.ListPage{}, &control.CallError{Status: http.StatusNotFound, Message: "no such control API call on this node"}
	}
	rs, err := l.Round(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if r.Place == "B" && r.Err != nil || r.Place == "C" && r.Err == nil {
			t.Fatalf("result %+v", r)
		}
	}
	if rels, _ := db.Relists(t.Context()); len(rels) != 1 || rels[0].Place != "C" || rels[0].Failures != 1 {
		t.Fatalf("still to read %+v", rels)
	}
}

// ID-5: a place that failed is asked again after MinBackoff, doubling
// per failure in a row to at most MaxBackoff.
func TestListerBackoffDoubles(t *testing.T) {
	l := &Lister{MinBackoff: time.Minute, MaxBackoff: 10 * time.Minute}
	t0 := time.Unix(1_800_000_000, 0)
	for _, c := range []struct {
		failures int64
		after    time.Duration
		due      bool
	}{
		{0, 0, true},
		{1, 59 * time.Second, false}, {1, time.Minute, true},
		{2, 119 * time.Second, false}, {2, 2 * time.Minute, true},
		{3, 4 * time.Minute, true}, {3, 3 * time.Minute, false},
		{9, 9 * time.Minute, false}, {9, 10 * time.Minute, true},
	} {
		r := state.Relist{Failures: c.failures, TriedAt: t0}
		if got := l.due(r, t0.Add(c.after)); got != c.due {
			t.Errorf("%d failures, %v later: due %v, want %v", c.failures, c.after, got, c.due)
		}
	}
}
