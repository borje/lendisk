package undelete

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// RET-3 (#32 review), on a recovered node with no history of the bucket:
//
//   - a friend answering a key the confirmed preview did not list (one
//     it made up) gets it recorded nowhere and reported unknown;
//   - a failure to record a page is retried before the next page is
//     asked for, so what the friend revived is not lost to the index;
//   - a key the friend failed to revive is reported failed;
//   - a key the friend revived whose answer never arrived (a call that
//     timed out after the lender's work) shows live in its list after
//     the run, and is adopted as restored there.
func TestTheRunnerTrustsOnlyThePreviewAndHealsLostAnswers(t *testing.T) {
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
	p, _ := identity.NewPhrase()
	bID, _ := identity.FromPhrase(p)
	b := bID.NodeID().String()
	if err := db.AddFriendship(ctx, state.Friendship{Peer: b, Address: "tcB", OwnNumber: 1 << 40, PeerNumber: 1 << 40, CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	p, _ = identity.NewPhrase()
	self, _ := identity.FromPhrase(p)
	at := now.Add(-time.Minute)
	key := func(name string) string { return "data/" + hash(name)[:2] + "/" + hash(name) }
	entry := func(name string, tomb bool) control.ListObject {
		o := control.ListObject{Key: key(name), Size: int64(len(name)), SHA256: hash(name)}
		if tomb {
			o.TombstonedAt = &at
		}
		return o
	}
	undone := false
	r := &Runner{DB: db, Self: self, Now: func() time.Time { return now },
		Wait: func(context.Context, time.Duration) error { return nil },
		List: func(_ context.Context, _ state.Friendship, _, _ string) (control.ListPage, error) {
			// Before the undelete both are tombstoned; after it the lost
			// one is live too.
			return control.ListPage{Objects: []control.ListObject{entry("lost", !undone), entry("pack", !undone)}}, nil
		},
		Call: func(context.Context, state.Friendship, control.UndeleteRequest) (control.UndeleteResponse, error) {
			undone = true
			return control.UndeleteResponse{
				Restored: []control.ListObject{entry("pack", true), entry("made-up", true)},
				Skipped:  []control.UndeleteSkip{{Key: key("stuck"), Reason: "failed", Detail: "no room for its directory"}},
			}, nil
		},
	}
	fails := 2
	r.Record = func(ctx context.Context, place, bucket string, request int64, objs []state.Restored) (state.RestoreResult, error) {
		if fails > 0 {
			fails--
			return state.RestoreResult{}, errors.New("database is locked")
		}
		return db.RecordRestored(ctx, place, bucket, request, objs)
	}
	id, err := db.RequestUndelete(ctx, "laptop", time.Time{}, false, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.ConfirmUndelete(ctx, id, now); err != nil {
		t.Fatal(err)
	}
	if err := r.Once(ctx); err != nil {
		t.Fatal(err)
	}
	q, _ := db.GetUndelete(ctx, id)
	var res Result
	if err := json.Unmarshal([]byte(q.Result), &res); err != nil || len(res.Friends) != 1 {
		t.Fatalf("result %q: %v", q.Result, err)
	}
	fr := res.Friends[0]
	if fr.Error != "" || fr.Recorded != 1 || fr.Unknown != 1 || fr.Failed != 1 || fr.Adopted != 1 || fr.FailedKeys[0].Key != key("stuck") {
		t.Fatalf("B %+v", fr)
	}
	for name, want := range map[string]bool{"pack": true, "lost": true, "made-up": false} {
		if _, ok, _ := db.Source(ctx, "laptop", key(name)); ok != want {
			t.Errorf("%s in the index: %v", name, ok)
		}
	}
}
