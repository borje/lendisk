package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ID-5, EP-7 (#74): friends that disagree on a rewritable key, one
// holding a newer version that replaced the other's there (its lender
// keeps the replaced version as a tombstone and lists its hash), are no
// conflict: the version no place replaced counts, and the read-through
// serves it. The user may still pick another. A key that is not
// rewritable keeps the rule (a tie is a conflict); so does a rewritable
// key whose places name no newer version, or name each other's.
func TestRewritableKeyTieBrokenByTheReplacedVersions(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	list(t, db, "B", "laptop",
		Listed{Key: "config", Size: 5, SHA256: "v2", Class: "metadata", Rewritable: true, Replaced: []string{"v1", "v0"}},
		Listed{Key: "keys/1", Size: 5, SHA256: "k2", Class: "metadata"},
		Listed{Key: "lone", Size: 5, SHA256: "a", Class: "metadata", Rewritable: true},
		Listed{Key: "cross", Size: 5, SHA256: "x1", Class: "metadata", Rewritable: true, Replaced: []string{"x2"}},
	)
	list(t, db, "C", "laptop",
		Listed{Key: "config", Size: 5, SHA256: "v1", Class: "metadata", Rewritable: true, Replaced: []string{"v0"}},
		Listed{Key: "keys/1", Size: 5, SHA256: "k1", Class: "metadata", Replaced: []string{"k2"}},
		Listed{Key: "lone", Size: 5, SHA256: "b", Class: "metadata", Rewritable: true},
		Listed{Key: "cross", Size: 5, SHA256: "x2", Class: "metadata", Rewritable: true, Replaced: []string{"x1"}},
	)
	k := verdict(t, db, "laptop", "config")
	if k.Verdict != VerdictNewest || k.SHA256 != "v2" {
		t.Fatalf("config: %+v", k)
	}
	if _, in, _ := db.KeyConflict(ctx, "laptop", "config"); in {
		t.Fatal("config is refused as a conflict")
	}
	if a := ackedAt(t, db, "B", "laptop"); a["config"] != "v2" {
		t.Fatalf("B acked %v", a)
	}
	if a := ackedAt(t, db, "C", "laptop"); a["config"] != "" {
		t.Fatalf("C, holding the replaced version, acked %v", a)
	}
	for _, key := range []string{"keys/1", "lone", "cross"} {
		if k := verdict(t, db, "laptop", key); k.Verdict != VerdictConflict {
			t.Errorf("%s: %+v, want a conflict", key, k)
		}
	}
	if es, _ := db.QueueEntries(ctx, "laptop"); len(es) != 1 || es[0].Key != "config" || !es[0].Rewritable {
		t.Fatalf("queue rows %+v", es)
	}
	// The user may still pick the other version; it then counts.
	if err := db.ResolveKey(ctx, "laptop", "config", "C"); err != nil {
		t.Fatalf("resolving a key decided by the newest version: %v", err)
	}
	if k := verdict(t, db, "laptop", "config"); k.SHA256 != "v1" {
		t.Fatalf("after resolve: %+v", k)
	}
	if err := db.ResolveKey(ctx, "laptop", "missing", "C"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resolving an unknown key: %v", err)
	}
}

// EP-7 (#74): a capture of a key its bucket's profile names rewritable
// says so on its queue row, and the worker reads it at the FIFO head,
// so it sends a changed version as a rewrite; other keys do not.
func TestRewritableCaptureReachesTheFIFOHead(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	friend(t, db, "B")
	seqs, err := db.BeginCaptures(ctx, []Capture{
		{Bucket: "vault", Key: "kopia.maintenance", Op: OpPut, Class: "metadata", Rewritable: true},
		{Bucket: "vault", Key: "n0123", Op: OpPut, Class: "snapshot"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range seqs {
		if _, err := db.FinishPut(ctx, s, 1, "h"); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []bool{true, false} {
		p, ok, err := db.NextPlacement(ctx, "B")
		if err != nil || !ok || p.Seq != seqs[i] || p.Rewritable != want {
			t.Fatalf("head %d: %+v %v %v, want rewritable %v", i, p, ok, err, want)
		}
		if err := db.AckPlacement(ctx, "B", p.Seq, p.SHA256, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}
