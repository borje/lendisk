package gateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/borje/lendisk/internal/state"
)

// NET-7 list, ID-5 (#74 review, M3): between a rewrite's move of the
// replaced version into its tomb and its publication of the new one, the
// key is listed as the version it still holds, live, not as deleted: a
// recovery list read in that gap must not record the key as gone.
func TestKeyInARewritesGapListsAsItsOldVersion(t *testing.T) {
	l, c := lendWithClock(t, 0, 0)
	ctx := context.Background()
	v1 := []byte("format one")
	if err := l.put("kopia.repository", v1, hashMeta(v1)); err != nil {
		t.Fatal(err)
	}
	tomb := ".lendisk/tomb/kopia.repository/1"
	os.MkdirAll(filepath.Join(l.dir, ".lendisk", "tomb", "kopia.repository"), 0o700)
	if err := os.Rename(filepath.Join(l.dir, "kopia.repository"), filepath.Join(l.dir, filepath.FromSlash(tomb))); err != nil {
		t.Fatal(err)
	}
	if _, err := l.h.db.AddTombstone(ctx, state.Tombstone{Peer: l.peer(), Sealed: *l.bucket, Key: "kopia.repository",
		Tomb: tomb, SHA256: sum(v1), Size: int64(len(v1)), Rewrite: true, DeletedAt: c.Now(),
		PurgeAfter: c.Now().Add(30 * day)}); err != nil {
		t.Fatal(err)
	}
	be, err := OpenBackend(l.h.store)
	if err != nil {
		t.Fatal(err)
	}
	objs, _, err := (&LentIndex{Store: be, DB: l.h.db}).List(ctx, l.id.NodeID(), *l.bucket, "", MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 || objs[0].Key != "kopia.repository" || !objs[0].TombstonedAt.IsZero() ||
		objs[0].SHA256 != sum(v1) || objs[0].Size != int64(len(v1)) {
		t.Fatalf("listed %+v", objs)
	}
}
