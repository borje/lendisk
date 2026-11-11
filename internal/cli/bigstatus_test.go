package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// REPL-11 (#81): `status` on a bucket of a million objects reads the
// offsite watermark in well under a second, once the daemon has folded
// the bucket's offsite changes; it logs what each of status's other
// reads of the database takes, and the whole command. Seeding a million
// rows takes a while and some 300 MB of disk, so the test runs only
// with LENDISK_BIG_STATUS set to the number of objects:
//
//	LENDISK_BIG_STATUS=1000000 go test -run TestStatusOfAMillionObjectBucket -v ./internal/cli
func TestStatusOfAMillionObjectBucket(t *testing.T) {
	objects, _ := strconv.Atoi(os.Getenv("LENDISK_BIG_STATUS"))
	if objects <= 0 {
		t.Skip("LENDISK_BIG_STATUS is not set")
	}
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	if code, _, e := n.run("", "", "bucket", "add", "laptop", "--profile", "restic", "--copies", "2"); code != 0 {
		t.Fatal(e)
	}
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := state.Open(ctx, n.data, log)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, f := range []string{"B", "C", "D"} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + f,
			OwnNumber: 1 << 50, PeerNumber: 1 << 50, CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	// Every object at two of the three friends, a tenth of the data
	// deleted since and the deletes acknowledged everywhere, as after a
	// long run of backups and prunes.
	seeded := time.Now()
	for _, q := range []string{
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?2)
		INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
			SELECT 'laptop', 'data/' || i, 'put', 'data', 'done', 1000, 'h' || i, ?1 FROM n`,
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1200)
		INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
			SELECT 'laptop', 'index/' || i, 'put', 'metadata', 'done', 100, 'm' || i, ?1 FROM n`,
		`INSERT INTO queue (bucket, key, op, class, state, size, sha256, created_at)
			VALUES ('laptop', 'snapshots/1', 'put', 'snapshot', 'done', 100, 's1', ?1)`,
		`INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
			SELECT q.seq, f.peer_key, q.bucket, q.key, 'put', 'acked', q.sha256, ?1, 100
			FROM queue q, friendship f WHERE q.class != 'data' OR f.peer_key != (CASE q.seq % 3 WHEN 0 THEN 'B' WHEN 1 THEN 'C' ELSE 'D' END)`,
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?2 / 10)
		INSERT INTO queue (bucket, key, op, class, state, created_at)
			SELECT 'laptop', 'data/' || i, 'delete', 'data', 'done', ?1 FROM n`,
		`INSERT INTO placement (seq, place, bucket, key, op, state, acked_at, size)
			SELECT q.seq, f.peer_key, q.bucket, q.key, 'delete', 'acked', ?1, 0
			FROM queue q, friendship f WHERE q.op = 'delete'`,
	} {
		if _, err := db.ExecContext(ctx, q, now.Unix(), objects); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("seeded %d objects in %v", objects, time.Since(seeded))
	folded := time.Now()
	if err := db.FoldOffsite(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("the daemon's first fold took %v", time.Since(folded))
	// Each of status's reads of the database, timed in one snapshot as
	// status takes them: the offsite one is #81's, the others are what
	// is left of status's cost on a large bucket.
	db.ReplaceAfter = 7 * 24 * time.Hour
	view, done, err := db.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var offsite time.Duration
	for _, r := range []struct {
		name string
		read func() error
	}{
		{"offsite", func() error {
			b, err := view.GetBucket(ctx, "laptop")
			if err != nil {
				return err
			}
			profiles, err := profile.Load("")
			if err != nil {
				return err
			}
			off, err := offsiteStatus(ctx, view, b, profiles.Get(b.Profile))
			if err == nil && !off.Complete {
				err = fmt.Errorf("not complete: %+v", off)
			}
			return err
		}},
		{"queue", func() error { _, err := view.QueueStats(ctx); return err }},
		{"below copies", func() error { _, err := view.BelowCopies(ctx); return err }},
		{"replacing", func() error { _, err := view.Replacing(ctx); return err }},
		{"replicas", func() error { _, err := replicaStatus(ctx, view); return err }},
		{"places", func() error { _, err := places(ctx, view, time.Now()); return err }},
	} {
		start := time.Now()
		if err := r.read(); err != nil {
			t.Fatalf("%s: %v", r.name, err)
		}
		took := time.Since(start)
		if r.name == "offsite" {
			offsite = took
		}
		t.Logf("%s: %v", r.name, took)
	}
	done()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	took := time.Duration(1 << 62)
	var st Status
	for range 2 {
		start := time.Now()
		st = n.status()
		took = min(took, time.Since(start))
	}
	t.Logf("status over %d objects took %v", objects, took)
	if len(st.Buckets) != 1 || !st.Buckets[0].Offsite.Complete {
		t.Fatalf("status: %+v", st.Buckets)
	}
	if offsite > 100*time.Millisecond {
		t.Fatalf("status's offsite read took %v, want well under a second", offsite)
	}
}
