package state

import (
	"context"
	"slices"
	"testing"
	"time"
)

func sourcePlaces(s Source) []string {
	var out []string
	for _, p := range s.Places {
		out = append(out, p.Place)
	}
	return out
}

// EP-10, ID-6 (#30): a recovered key's source is the version that
// counts and the places acknowledged holding that hash, most recently
// reachable first; a place listing another version is no source; a
// conflict, a deleted key and an unknown key have none.
func TestSourceIsTheWinningHashAtItsPlaces(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C", "D")
	list(t, db, "B", "laptop", Listed{Key: "data/1", Size: 10, SHA256: "h1", Class: "data"},
		Listed{Key: "config", Size: 3, SHA256: "c1", Class: "metadata"})
	list(t, db, "C", "laptop", Listed{Key: "data/1", Size: 10, SHA256: "h1", Class: "data"},
		Listed{Key: "config", Size: 3, SHA256: "c2", Class: "metadata"})
	list(t, db, "D", "laptop", Listed{Key: "data/1", Size: 10, SHA256: "bad", Class: "data"},
		Listed{Key: "config", Size: 3, SHA256: "c3", Class: "metadata"})
	now := time.Unix(1_800_000_000, 0)
	if err := db.SetPlaceCondition(ctx, "B", ConditionOK, "", now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(ctx, "C", ConditionOK, "", now); err != nil {
		t.Fatal(err)
	}

	s, ok, err := db.Source(ctx, "laptop", "data/1")
	if err != nil || !ok {
		t.Fatalf("source of data/1: %+v %v %v", s, ok, err)
	}
	if s.SHA256 != "h1" || s.Size != 10 || !slices.Equal(sourcePlaces(s), []string{"C", "B"}) {
		t.Fatalf("source of data/1: %+v", s)
	}
	if !s.Places[0].LastOK.Equal(now) {
		t.Fatalf("C's last reachable time %v, want %v", s.Places[0].LastOK, now)
	}
	// Three places, three versions: a conflict has no source.
	if _, ok, err := db.Source(ctx, "laptop", "config"); err != nil || ok {
		t.Fatalf("a conflict has a source: %v %v", ok, err)
	}
	if _, ok, err := db.Source(ctx, "laptop", "data/none"); err != nil || ok {
		t.Fatalf("an unknown key has a source: %v %v", ok, err)
	}
	// A key deleted on this node since recovery has none.
	seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/1", Op: OpDelete, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := db.Source(ctx, "laptop", "data/1"); err != nil || ok {
		t.Fatalf("a deleted key has a source: %v %v", ok, err)
	}
}

// ID-6 (#30): the bucket's live keys, which a recovered node lists
// before it holds them: the version that counts of each key, and each
// key in conflict (so a read of it says why), in key order, by prefix,
// after a key; a deleted key is not live.
func TestLiveKeysListRecoveredKeys(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	list(t, db, "B", "laptop", Listed{Key: "data/aa/1", Size: 10, SHA256: "h1", Class: "data"},
		Listed{Key: "data/bb/2", Size: 20, SHA256: "h2", Class: "data"},
		Listed{Key: "keys/k", Size: 4, SHA256: "k1", Class: "metadata"},
		Listed{Key: "snapshots/s", Size: 5, SHA256: "s1", Class: "snapshot"})
	list(t, db, "C", "laptop", Listed{Key: "keys/k", Size: 6, SHA256: "k2", Class: "metadata"},
		Listed{Key: "snapshots/s", Size: 5, SHA256: "s1", Class: "snapshot"})
	seqs, err := db.BeginCaptures(ctx, []Capture{{Bucket: "laptop", Key: "data/bb/2", Op: OpDelete, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishDelete(ctx, seqs...); err != nil {
		t.Fatal(err)
	}
	keys := func(prefix, after string, limit int) []string {
		t.Helper()
		ks, err := db.LiveKeys(ctx, "laptop", prefix, after, limit)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, k := range ks {
			out = append(out, k.Key)
		}
		return out
	}
	if got := keys("", "", 10); !slices.Equal(got, []string{"data/aa/1", "keys/k", "snapshots/s"}) {
		t.Fatalf("live keys %v", got)
	}
	if got := keys("data/", "", 10); !slices.Equal(got, []string{"data/aa/1"}) {
		t.Fatalf("under data/ %v", got)
	}
	if got := keys("", "data/aa/1", 1); !slices.Equal(got, []string{"keys/k"}) {
		t.Fatalf("after data/aa/1, one: %v", got)
	}
	ks, err := db.LiveKeys(ctx, "laptop", "keys/", "", 10)
	if err != nil || len(ks) != 1 || ks[0].Size != 6 {
		t.Fatalf("a conflict is listed with its largest size: %+v %v", ks, err)
	}
}
