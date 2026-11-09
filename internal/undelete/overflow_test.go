package undelete

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// fakeTarget is an overflow target: live objects by key, and for a
// versioned one the keys deleted with the version before each marker.
type fakeTarget struct {
	live     map[string]string // key -> body
	bodies   map[string]string // every key's body
	deleted  []replica.Deleted
	restored []string
	// restoring, if set, runs at each Restore, before it copies.
	restoring func(key string)
}

func (f *fakeTarget) ListDeleted(ctx context.Context, bucket string, since time.Time) ([]replica.Deleted, error) {
	var out []replica.Deleted
	for _, d := range f.deleted {
		if _, live := f.live[d.Key]; !live && !d.DeletedAt.Before(since) {
			out = append(out, d)
		}
	}
	return out, nil
}

func (f *fakeTarget) Restore(ctx context.Context, bucket string, d replica.Deleted) error {
	if f.restoring != nil {
		f.restoring(d.Key)
	}
	f.restored = append(f.restored, d.Key)
	f.live[d.Key] = f.bodies[d.Key]
	return nil
}

func (f *fakeTarget) Stat(ctx context.Context, bucket, key string) (replica.ObjectStat, bool, error) {
	b, ok := f.live[key]
	return replica.ObjectStat{Size: int64(len(b)), SHA256: hash(b)}, ok, nil
}

func (f *fakeTarget) ListLive(ctx context.Context, bucket string) ([]string, error) {
	var out []string
	for k := range f.live {
		out = append(out, k)
	}
	return out, nil
}

// targetNode is a node with bucket laptop and overflow target g, with
// versioning as given, holding the copies of keys (no friends: the
// target takes every data copy), each deleted since.
func targetNode(t *testing.T, versioning string, keys ...string) (*state.DB, *Runner, *fakeTarget, *time.Time) {
	t.Helper()
	ctx := context.Background()
	db, err := state.Open(ctx, t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	now := time.Unix(1_800_000_000, 0)
	db.Clock = func() time.Time { return now }
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "AK", SecretKey: "s", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	o := state.Overflow{Name: "g", Endpoint: "http://g", Bucket: "offsite", MaxBytes: 1 << 40, EnvPrefix: "X_",
		Retention: 24 * time.Hour, Versioning: versioning, AddedAt: now}
	if err := db.AddOverflow(ctx, o); err != nil {
		t.Fatal(err)
	}
	ft := &fakeTarget{live: map[string]string{}, bodies: map[string]string{}}
	for _, k := range keys {
		body := bodyOf(k)
		ft.bodies[k] = body
		seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: k, Op: state.OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FinishPut(ctx, seqs[0], int64(len(body)), hash(body)); err != nil {
			t.Fatal(err)
		}
		if err := db.AckPlacement(ctx, o.Place(), seqs[0], hash(body), now); err != nil {
			t.Fatal(err)
		}
		ft.live[k] = body
	}
	now = now.Add(time.Minute)
	for _, k := range keys {
		seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: k, Op: state.OpDelete, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.FinishDelete(ctx, seqs...); err != nil {
			t.Fatal(err)
		}
		held, err := db.HoldOverflowDelete(ctx, o.Place(), seqs[0], now)
		if err != nil {
			t.Fatal(err)
		}
		if !held {
			delete(ft.live, k)
			ft.deleted = append(ft.deleted, replica.Deleted{Key: k, DeletedAt: now, VersionID: "v1",
				Size: int64(len(bodyOf(k))), SHA256: hash(bodyOf(k))})
		}
		if err := db.AckPlacement(ctx, o.Place(), seqs[0], "", now); err != nil {
			t.Fatal(err)
		}
	}
	p, _ := identity.NewPhrase()
	self, _ := identity.FromPhrase(p)
	r := &Runner{DB: db, Self: self, Now: func() time.Time { return now },
		Overflow: func(state.Overflow) (Target, error) { return ft, nil }}
	return db, r, ft, &now
}

func runUndelete(t *testing.T, db *state.DB, r *Runner, since time.Time) (Preview, Result) {
	t.Helper()
	ctx := context.Background()
	id, err := db.RequestUndelete(ctx, "laptop", since, false, r.now())
	if err != nil {
		t.Fatal(err)
	}
	q, _ := db.GetUndelete(ctx, id)
	p, err := r.Preview(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PreviewUndelete(ctx, id, "{}", p.keys, r.now()); err != nil {
		t.Fatal(err)
	}
	if err := db.ConfirmUndelete(ctx, id, r.now()); err != nil {
		t.Fatal(err)
	}
	q, _ = db.GetUndelete(ctx, id)
	res, err := r.Undelete(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	return p, res
}

// packKey is the restic key of a pack holding body.
func packKey(body string) string { h := hash(body); return "data/" + h[:2] + "/" + h }

var packA, packB = packKey("pack a"), packKey("pack b")

func bodyOf(key string) string {
	for _, b := range []string{"pack a", "pack b"} {
		if packKey(b) == key {
			return b
		}
	}
	return key
}

// RET-3, S3-4 (#39): at a versioned target, undelete previews the keys
// whose delete marker is newer than since and copies the version before
// each onto its key; the index gains each back, acknowledged at the
// target.
func TestUndeleteAtAVersionedTargetCopiesTheVersionBack(t *testing.T) {
	db, r, ft, _ := targetNode(t, state.VersioningEnabled, packA, packB)
	ctx := context.Background()
	p, res := runUndelete(t, db, r, time.Time{})
	if p.Keys != 2 || len(p.Friends) != 1 || p.Friends[0].Peer != "overflow:g" || p.Friends[0].Keys != 2 {
		t.Fatalf("preview %+v", p)
	}
	if len(ft.restored) != 2 {
		t.Fatalf("restored at the target: %v", ft.restored)
	}
	if fr := res.Friends[0]; fr.Restored != 2 || fr.Recorded != 2 || fr.Error != "" {
		t.Fatalf("result %+v", fr)
	}
	for _, k := range []string{packA, packB} {
		if live, err := db.KeyLive(ctx, "laptop", k); err != nil || !live {
			t.Fatalf("%s not live in the index: %v", k, err)
		}
	}
	// Run again: nothing more comes back.
	_, res = runUndelete(t, db, r, time.Time{})
	if fr := res.Friends[0]; fr.Restored != 0 || len(ft.restored) != 2 {
		t.Fatalf("second run %+v, restored %v", fr, ft.restored)
	}
}

// RET-3: a copy a run made whose record was lost is adopted after the
// next run (the target holds it live, the index deleted).
func TestALostRecordAtAVersionedTargetIsAdopted(t *testing.T) {
	db, r, ft, _ := targetNode(t, state.VersioningEnabled, packA)
	ft.live[packA] = bodyOf(packA) // copied back, never recorded
	_, res := runUndelete(t, db, r, time.Time{})
	if fr := res.Friends[0]; fr.Adopted != 1 {
		t.Fatalf("result %+v", fr)
	}
	if live, _ := db.KeyLive(context.Background(), "laptop", packA); !live {
		t.Fatal("not adopted")
	}
}

// RET-3, S3-4 (#39): at a target without versioning, undelete cancels
// the held deletes made since then inside the window: the object, still
// at the target, is live again in the index, and the delete is never
// sent. Deletes before since stay held.
func TestUndeleteAtAnUnversionedTargetCancelsTheHeldDeletes(t *testing.T) {
	db, r, ft, now := targetNode(t, state.VersioningOff, packA)
	ctx := context.Background()
	*now = now.Add(time.Hour)
	p, res := runUndelete(t, db, r, now.Add(-30*time.Minute))
	if p.Keys != 0 {
		t.Fatalf("preview of deletes since after them: %+v", p)
	}
	if fr := res.Friends[0]; fr.Restored != 0 {
		t.Fatalf("restored %+v", fr)
	}
	if holds, _ := db.OverflowTombHolds(ctx); holds["g"].Tombs != 1 {
		t.Fatalf("a delete before since is no longer held: %+v", holds)
	}
	if _, ok := ft.live[packA]; !ok {
		t.Fatal("the target lost the object")
	}
	p, res = runUndelete(t, db, r, time.Time{})
	if p.Keys != 1 || p.Friends[0].Keys != 1 {
		t.Fatalf("preview %+v", p)
	}
	if fr := res.Friends[0]; fr.Restored != 1 || fr.Recorded != 1 || fr.Error != "" {
		t.Fatalf("result %+v", fr)
	}
	if live, _ := db.KeyLive(ctx, "laptop", packA); !live {
		t.Fatal("not live in the index")
	}
	holds, err := db.OverflowTombHolds(ctx)
	if err != nil || holds["g"].Tombs != 0 {
		t.Fatalf("the held delete was not cancelled: %+v %v", holds, err)
	}
	*now = now.Add(48 * time.Hour)
	if due, _ := db.DueOverflowTombs(ctx, "overflow:g", *now, 10); len(due) != 0 {
		t.Fatalf("a cancelled delete is due: %+v", due)
	}
}

// RET-3, REPL-13 (#39): a copy re-placement drained from a versioned
// target (its delete marker is the drain's, the index holds the key
// live) is not brought back there.
func TestUndeleteLeavesACopyDrainedFromAVersionedTarget(t *testing.T) {
	db, r, ft, now := targetNode(t, state.VersioningEnabled, packA)
	ctx := context.Background()
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: packB, Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], int64(len(bodyOf(packB))), hash(bodyOf(packB))); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE placement SET state = 'drained' WHERE seq = ? AND place = 'overflow:g'`, seqs[0]); err != nil {
		t.Fatal(err)
	}
	ft.bodies[packB] = bodyOf(packB)
	ft.deleted = append(ft.deleted, replica.Deleted{Key: packB, DeletedAt: *now, VersionID: "v1",
		Size: int64(len(bodyOf(packB))), SHA256: hash(bodyOf(packB))})
	p, res := runUndelete(t, db, r, time.Time{})
	if p.Keys != 1 || len(ft.restored) != 1 || ft.restored[0] != packA || res.Friends[0].Restored != 1 {
		t.Fatalf("preview %+v, restored %v, result %+v", p, ft.restored, res.Friends[0])
	}
}

// #39 review: a key deleted, written again (acknowledged at the target),
// and deleted again, before the worker dropped the first, superseded
// held delete: undelete judges the key by its newest held delete, so it
// brings back the version the target holds, and neither delete is ever
// sent.
func TestUndeleteJudgesAKeyByItsNewestHeldDelete(t *testing.T) {
	db, r, ft, now := targetNode(t, state.VersioningOff, "config")
	ctx := context.Background()
	place := "overflow:g"
	v2 := "version two"
	*now = now.Add(time.Minute)
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "config", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], int64(len(v2)), hash(v2)); err != nil {
		t.Fatal(err)
	}
	if err := db.AckPlacement(ctx, place, seqs[0], hash(v2), *now); err != nil {
		t.Fatal(err)
	}
	ft.live["config"] = v2
	ft.bodies["config"] = v2
	*now = now.Add(time.Minute)
	seqs, err = db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "config", Op: state.OpDelete, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	if held, err := db.HoldOverflowDelete(ctx, place, seqs[0], *now); err != nil || !held {
		t.Fatal(held, err)
	}
	if err := db.AckPlacement(ctx, place, seqs[0], "", *now); err != nil {
		t.Fatal(err)
	}
	_, res := runUndelete(t, db, r, time.Time{})
	if fr := res.Friends[0]; fr.Restored != 1 || fr.Failed != 0 {
		t.Fatalf("v2 not restored: %+v", fr)
	}
	if live, _ := db.KeyLive(ctx, "laptop", "config"); !live {
		t.Fatal("not live in the index")
	}
	*now = now.Add(48 * time.Hour)
	if due, _ := db.DueOverflowTombs(ctx, place, *now, 10); len(due) != 0 {
		t.Fatalf("a held delete of the restored key is due: %+v", due)
	}
}

// #39 review: at a versioned target, a key whose put is queued or in
// flight there by the time of the copy is not copied over it: reported
// kept, the target's newer version stands.
func TestUndeleteAtAVersionedTargetKeepsAPutInFlight(t *testing.T) {
	db, r, ft, _ := targetNode(t, state.VersioningEnabled, packA, packB)
	ctx := context.Background()
	// While packA is copied back, packB is written again here.
	ft.restoring = func(key string) {
		if key != packA {
			return
		}
		seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: packB, Op: state.OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FinishPut(ctx, seqs[0], 3, hash("new")); err != nil {
			t.Fatal(err)
		}
	}
	_, res := runUndelete(t, db, r, time.Time{})
	if fr := res.Friends[0]; len(ft.restored) != 1 || fr.KeptCount != 1 || fr.Kept[0].Key != packB || fr.Kept[0].Ours != hash("new") || fr.Restored != 1 {
		t.Fatalf("restored %v, result %+v", ft.restored, fr)
	}
}

// #39 review: a key with a drained copy's held delete, already due, and
// a later delete's, inside the window: undelete claims both, brings the
// key back, and neither is ever sent.
func TestUndeleteClaimsADrainedCopysHeldDeleteToo(t *testing.T) {
	db, r, ft, now := targetNode(t, state.VersioningOff)
	ctx := context.Background()
	place := "overflow:g"
	body := bodyOf(packA)
	ft.live[packA], ft.bodies[packA] = body, body
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: packA, Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], int64(len(body)), hash(body)); err != nil {
		t.Fatal(err)
	}
	put := seqs[0]
	if err := db.AckPlacement(ctx, place, put, hash(body), *now); err != nil {
		t.Fatal(err)
	}
	// Re-placement drains it: held, and due a day later.
	if _, err := db.ExecContext(ctx, `UPDATE placement SET state = 'dropping' WHERE seq = ? AND place = ?`, put, place); err != nil {
		t.Fatal(err)
	}
	if held, err := db.HoldOverflowDelete(ctx, place, put, *now); err != nil || !held {
		t.Fatal(held, err)
	}
	if err := db.AckPlacement(ctx, place, put, "", *now); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(23 * time.Hour)
	seqs, err = db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: packA, Op: state.OpDelete, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state, size)
		SELECT ?1, ?2, 'laptop', ?3, 'delete', 'queued', 0
		WHERE NOT EXISTS (SELECT 1 FROM placement WHERE seq = ?1 AND place = ?2)`, seqs[0], place, packA); err != nil {
		t.Fatal(err)
	}
	if held, err := db.HoldOverflowDelete(ctx, place, seqs[0], *now); err != nil || !held {
		t.Fatal(held, err)
	}
	if err := db.AckPlacement(ctx, place, seqs[0], "", *now); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Hour) // the drain's held delete is due
	_, res := runUndelete(t, db, r, time.Time{})
	if fr := res.Friends[0]; fr.Restored != 1 || fr.Failed != 0 {
		t.Fatalf("result %+v", fr)
	}
	if holds, _ := db.OverflowTombHolds(ctx); holds["g"].Tombs != 0 {
		t.Fatalf("held deletes left: %+v", holds)
	}
	if due, _ := db.DueOverflowTombs(ctx, place, now.Add(48*time.Hour), 10); len(due) != 0 {
		t.Fatalf("due: %+v", due)
	}
}
