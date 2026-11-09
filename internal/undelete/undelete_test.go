package undelete

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

func hash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// friend is a fake friend: its list of the bucket and what its undelete
// restores, page by page, or the error it answers.
type friend struct {
	list     []control.ListObject
	restores []control.ListObject
	skipped  []control.UndeleteSkip
	err      error
	busy     int // 503s before it answers
	asked    []control.UndeleteRequest
}

// RET-3 (#32): the daemon previews an undelete from every friend's list
// (each tombstoned key deleted since then, once however many friends
// hold it, with the snapshots named), and once confirmed asks every
// friend to restore, waiting out a friend's pace, records what each
// restored in the index, reports the keys a friend left as they were,
// and rejects what cannot be the borrower's object. A friend that
// fails, or holds nothing of the bucket, does not stop the others.
func TestTheRunnerPreviewsThenRestores(t *testing.T) {
	ctx := context.Background()
	db, err := state.Open(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Unix(1_800_000_000, 0)
	db.Clock = func() time.Time { return now }
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "AK", SecretKey: "s", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, name := range []string{"B", "C", "D", "E"} {
		p, _ := identity.NewPhrase()
		id, _ := identity.FromPhrase(p)
		ids[name] = id.NodeID().String()
		if err := db.AddFriendship(ctx, state.Friendship{Peer: ids[name], Address: "tc" + name, OwnNumber: 1 << 40,
			PeerNumber: 1 << 40, CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	p, _ := identity.NewPhrase()
	self, _ := identity.FromPhrase(p)
	since := now.Add(-time.Hour)
	before, after := now.Add(-2*time.Hour), now.Add(-time.Minute)
	pack, snap, old := "data/"+hash("pack")[:2]+"/"+hash("pack"), "snapshots/"+hash("snap"), "data/"+hash("old")[:2]+"/"+hash("old")
	entry := func(key, body string, at *time.Time) control.ListObject {
		return control.ListObject{Key: key, Size: int64(len(body)), SHA256: hash(body), TombstonedAt: at}
	}
	friends := map[string]*friend{
		ids["B"]: {
			list:     []control.ListObject{entry(pack, "pack", &after), entry(snap, "snap", &after), entry(old, "old", &before)},
			restores: []control.ListObject{entry(pack, "pack", &after), entry(snap, "snap", &after), entry("data/xx/forged", "other", &after)},
			skipped:  []control.UndeleteSkip{{Key: "config", SHA256: hash("c1"), Live: hash("c2"), Reason: "conflict"}},
			busy:     1,
		},
		ids["C"]: {list: []control.ListObject{entry(pack, "pack", &after)}, restores: []control.ListObject{entry(pack, "pack", &after)}},
		ids["D"]: {err: &control.CallError{Status: 404, Code: control.CodeNoSuchBucket}},
		ids["E"]: {err: errors.New("no route to the friend")},
	}
	var waited []time.Duration
	r := &Runner{DB: db, Self: self, Now: func() time.Time { return now },
		Wait: func(_ context.Context, d time.Duration) error { waited = append(waited, d); return nil },
		List: func(_ context.Context, f state.Friendship, sealed, after string) (control.ListPage, error) {
			fr := friends[f.Peer]
			if sealed != self.SealedName("laptop") || after != "" {
				t.Errorf("listed %q after %q", sealed, after)
			}
			return control.ListPage{Objects: fr.list}, fr.err
		},
		Call: func(_ context.Context, f state.Friendship, req control.UndeleteRequest) (control.UndeleteResponse, error) {
			fr := friends[f.Peer]
			fr.asked = append(fr.asked, req)
			if fr.busy > 0 {
				fr.busy--
				return control.UndeleteResponse{}, &control.CallError{Status: http.StatusServiceUnavailable, RetryAfter: 3 * time.Second}
			}
			return control.UndeleteResponse{Restored: fr.restores, Skipped: fr.skipped}, fr.err
		},
	}
	id, err := db.RequestUndelete(ctx, "laptop", since, false, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Once(ctx); err != nil {
		t.Fatal(err)
	}
	q, err := db.GetUndelete(ctx, id)
	if err != nil || q.Stage != state.UndeletePreviewed {
		t.Fatalf("after a pass: %+v %v", q, err)
	}
	var pv Preview
	if err := json.Unmarshal([]byte(q.Preview), &pv); err != nil {
		t.Fatal(err)
	}
	if pv.Keys != 2 || pv.Bytes != int64(len("pack")+len("snap")) || pv.Snapshots != 1 || !slices.Equal(pv.Named, []string{snap}) ||
		pv.Since != since.Unix() || len(pv.Friends) != 4 {
		t.Fatalf("preview %+v", pv)
	}
	byPeer := map[string]FriendPreview{}
	for _, f := range pv.Friends {
		byPeer[f.Peer] = f
	}
	if byPeer[ids["B"]].Keys != 2 || byPeer[ids["C"]].Keys != 1 || byPeer[ids["D"]].Keys != 0 || byPeer[ids["D"]].Error != "" ||
		!strings.Contains(byPeer[ids["E"]].Error, "no route") {
		t.Fatalf("per friend %+v", byPeer)
	}
	// Nothing is restored before the user confirms.
	if len(friends[ids["B"]].asked) != 0 {
		t.Fatal("asked a friend to restore before the user confirmed")
	}
	if err := db.ConfirmUndelete(ctx, id, now); err != nil {
		t.Fatal(err)
	}
	if err := r.Once(ctx); err != nil {
		t.Fatal(err)
	}
	q, _ = db.GetUndelete(ctx, id)
	var res Result
	if err := json.Unmarshal([]byte(q.Result), &res); q.Stage != state.UndeleteDone || err != nil {
		t.Fatalf("after the undelete: %+v %v", q, err)
	}
	got := map[string]FriendResult{}
	for _, f := range res.Friends {
		got[f.Peer] = f
	}
	b, c := got[ids["B"]], got[ids["C"]]
	// Friends are asked in node ID order: whichever restores the pack
	// first records it, the other acknowledges it.
	if b.Restored != 2 || b.Recorded+b.Acked != 2 || b.Rejected != 1 || b.ConflictCount != 1 || b.Conflicts[0].Key != "config" || b.Error != "" {
		t.Fatalf("B %+v", b)
	}
	if c.Restored != 1 || c.Acked+c.Recorded != 1 || b.Recorded+c.Recorded != 2 {
		t.Fatalf("C %+v", c)
	}
	if got[ids["D"]].Error != "" || got[ids["D"]].Restored != 0 || !strings.Contains(got[ids["E"]].Error, "no route") {
		t.Fatalf("D %+v, E %+v", got[ids["D"]], got[ids["E"]])
	}
	if a := friends[ids["B"]].asked; len(a) != 2 || a[1].Since != since.Unix() || a[1].Bucket != self.SealedName("laptop") ||
		!slices.Equal(waited, []time.Duration{3 * time.Second}) {
		t.Fatalf("asked B %+v, waited %v", a, waited)
	}
	src, ok, err := db.Source(ctx, "laptop", pack)
	if err != nil || !ok || src.SHA256 != hash("pack") || len(src.Places) != 2 {
		t.Fatalf("the pack's source %+v %v %v", src, ok, err)
	}
	if _, ok, _ := db.Source(ctx, "laptop", "data/xx/forged"); ok {
		t.Fatal("a rejected entry was recorded")
	}
	// A request the user declined is dropped, and never run.
	id2, _ := db.RequestUndelete(ctx, "laptop", time.Time{}, false, now)
	if err := db.CancelUndelete(ctx, id2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetUndelete(ctx, id2); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("a declined request: %v", err)
	}
}
