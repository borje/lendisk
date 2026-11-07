package state

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #29 re-review: a friend added after recovery has no list of the
// bucket, so its silence is no evidence for the lone-metadata rule:
// only places whose list was read to its end count.
func TestFriendAddedLaterIsNotListed(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B")
	if _, err := db.RecordListed(ctx, "B", "laptop", []Listed{{Key: "index/9", Size: 1, SHA256: "h9", Class: "metadata"}},
		1, "index/9", false); err != nil {
		t.Fatal(err)
	}
	friend(t, db, "Z") // `peer add` while the daemon is still listing
	if _, err := db.RecordListed(ctx, "B", "laptop", nil, 0, "", true); err != nil {
		t.Fatal(err)
	}
	settle(t, db)
	if v := verdict(t, db, "laptop", "index/9"); v.Verdict != VerdictSingle {
		t.Fatalf("with a friend added since: %+v", v)
	}
	if rs := mustRelists(t, db); len(rs) != 0 {
		t.Fatalf("a finished list is still to read: %+v", rs)
	}
}

// #29 re-review: settling a large bucket is many short write
// transactions, not one: other writers (the gateway's captures) wait at
// most a batch, never the whole bucket, and never get SQLITE_BUSY;
// abandoning a place settles that place's keys only.
func TestSettlingLargeBucketKeepsWritersMoving(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	const keys = settleKeys
	ctx := context.Background()
	db := recoveringWith(t, "B", "X")
	page := make([]Listed, 0, 1000)
	for i := 0; i < keys; i++ {
		page = append(page, Listed{Key: fmt.Sprintf("data/%07d", i), Size: 1, SHA256: "h", Class: "data"})
		if len(page) == cap(page) {
			if _, err := db.RecordListed(ctx, "X", "laptop", page, len(page), page[len(page)-1].Key, false); err != nil {
				t.Fatal(err)
			}
			page = page[:0]
		}
	}

	// A writer beside the settling, as the gateway's captures are.
	var worst atomic.Int64
	var writeErr error
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			start := time.Now()
			if err := db.SetNode(ctx, "probe", fmt.Sprint(i)); err != nil {
				writeErr = err
				return
			}
			if d := int64(time.Since(start)); d > worst.Load() {
				worst.Store(d)
			}
		}
	}()
	start := time.Now()
	if err := db.AbandonRelist(ctx, "X", "laptop"); err != nil {
		t.Fatal(err)
	}
	if err := db.SettleFinished(ctx); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	close(stop)
	wg.Wait()
	if writeErr != nil {
		t.Fatalf("a write beside the settling failed: %v", writeErr)
	}
	// A write waits one batch at most (about 0.15 s; settleWait allows
	// several), never the whole bucket (tens of seconds).
	if w := time.Duration(worst.Load()); w > settleWait {
		t.Fatalf("a write waited %v while settling took %v", w, took)
	}
	t.Logf("abandoning %d keys took %v; the longest write beside it waited %v", keys, took, time.Duration(worst.Load()))
	if ks, _ := db.RecoveredKeys(ctx, "laptop"); len(ks) != 0 {
		t.Fatalf("%d keys left", len(ks))
	}
	if es, _ := db.QueueEntries(ctx, "laptop"); len(es) != 0 {
		t.Fatalf("%d queue rows left", len(es))
	}
}
