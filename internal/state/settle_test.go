package state

import (
	"context"
	"fmt"
	"strings"
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
	// A write waits one batch at most (SettleBudget, 0.25 s, or 2,000
	// keys; settleWait allows several), never the whole bucket (tens of
	// seconds).
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

// #29: a batch ends after SettleBudget, so a slow machine settles fewer
// keys per write transaction; past its budget at once, a batch still
// decides one key, and settling finishes.
func TestSettlingPastItsBudgetStillFinishes(t *testing.T) {
	budget, pauseFor := SettleBudget, SettlePause
	SettleBudget, SettlePause = 0, 0
	t.Cleanup(func() { SettleBudget, SettlePause = budget, pauseFor })
	ctx := context.Background()
	db := recoveringWith(t, "B", "X")
	var only []Listed
	for i := 0; i < 5; i++ {
		only = append(only, Listed{Key: fmt.Sprintf("data/%d", i), Size: 1, SHA256: "h", Class: "data"})
	}
	if _, err := db.RecordListed(ctx, "X", "laptop", only, len(only), only[len(only)-1].Key, false); err != nil {
		t.Fatal(err)
	}
	lone := []Listed{{Key: "index/1", Size: 1, SHA256: "h1", Class: "metadata"},
		{Key: "index/2", Size: 1, SHA256: "h2", Class: "metadata"}}
	if _, err := db.RecordListed(ctx, "B", "laptop", lone, len(lone), "", true); err != nil {
		t.Fatal(err)
	}
	if err := db.AbandonRelist(ctx, "X", "laptop"); err != nil {
		t.Fatal(err)
	}
	if err := db.SettleFinished(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := db.Unsettled(ctx); err != nil || n != 0 {
		t.Fatalf("%d lists unsettled: %v", n, err)
	}
	if ks, _ := db.RecoveredKeys(ctx, "laptop"); len(ks) != 2 {
		t.Fatalf("abandoned keys left, or lone keys lost: %+v", ks)
	}
	for _, k := range []string{"index/1", "index/2"} {
		if v := verdict(t, db, "laptop", k); v.Verdict != VerdictSingle {
			t.Fatalf("%s: %+v", k, v)
		}
	}
}

// #72: a list whose settling fails is recorded and skipped: the lists
// after it still settle, and it is tried again each round until it
// settles.
func TestAListThatFailsToSettleBlocksNoOther(t *testing.T) {
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	if err := db.AddRelists(ctx, []string{"B", "C"}, []string{"work"}); err != nil {
		t.Fatal(err)
	}
	for b, key := range map[string]string{"laptop": "index/bad", "work": "index/9"} {
		if _, err := db.RecordListed(ctx, "B", b, []Listed{{Key: key, Size: 1, SHA256: "h", Class: "metadata"}}, 1, "", true); err != nil {
			t.Fatal(err)
		}
		if _, err := db.RecordListed(ctx, "C", b, nil, 0, "", true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`CREATE TRIGGER fail BEFORE UPDATE ON recovered_key WHEN NEW.key = 'index/bad'
		BEGIN SELECT RAISE(FAIL, 'boom'); END`); err != nil {
		t.Fatal(err)
	}
	for round := int64(1); round <= 2; round++ {
		err := db.SettleFinished(ctx)
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("round %d: the failure is not reported: %v", round, err)
		}
		// The laptop lists, B's the first of all, fail; the work lists
		// after them settle.
		if v := verdict(t, db, "work", "index/9"); v.Verdict != VerdictConflict {
			t.Fatalf("round %d: a later list did not settle: %+v", round, v)
		}
		fs, err := db.SettleFailed(ctx)
		if err != nil || len(fs) != 2 {
			t.Fatalf("round %d: failed %+v %v", round, fs, err)
		}
		for _, f := range fs {
			if f.Bucket != "laptop" || !strings.Contains(f.SettleError, "boom") || f.SettleFailures != round {
				t.Fatalf("round %d: failed %+v", round, fs)
			}
		}
		if n, _ := db.Unsettled(ctx); n != 2 {
			t.Fatalf("round %d: %d unsettled", round, n)
		}
	}
	if _, err := db.Exec(`DROP TRIGGER fail`); err != nil {
		t.Fatal(err)
	}
	settle(t, db)
	if v := verdict(t, db, "laptop", "index/bad"); v.Verdict != VerdictConflict {
		t.Fatalf("retried list: %+v", v)
	}
	if fs, err := db.SettleFailed(ctx); err != nil || len(fs) != 0 {
		t.Fatalf("failures left %+v %v", fs, err)
	}
}

// #72: a finished list's lone-metadata settling records its progress
// with each batch, so rounds whose budget ends after one batch still
// finish it, each resuming where the last stopped.
func TestLoneMetadataSettlingResumesAfterItsBudget(t *testing.T) {
	batch, pauseFor := SettleBatch, SettlePause
	// Every round's budget ends in the pause after its first batch.
	SettleBatch, SettlePause = 2, time.Hour
	t.Cleanup(func() { SettleBatch, SettlePause = batch, pauseFor })
	ctx := context.Background()
	db := recoveringWith(t, "B", "C")
	var lone []Listed
	for i := 0; i < 10; i++ {
		lone = append(lone, Listed{Key: fmt.Sprintf("index/%02d", i), Size: 1, SHA256: "h", Class: "metadata"})
	}
	if _, err := db.RecordListed(ctx, "B", "laptop", lone, len(lone), "", true); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecordListed(ctx, "C", "laptop", nil, 0, "", true); err != nil {
		t.Fatal(err)
	}
	conflicts := func() int {
		cs, err := db.Conflicts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(cs)
	}
	rounds := 0
	for ; rounds < 15; rounds++ {
		if n, _ := db.Unsettled(ctx); n == 0 {
			break
		}
		round, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		err := db.SettleFinished(round)
		cancel()
		if rounds == 0 && (err == nil || conflicts() != 2) {
			t.Fatalf("the first round decided %d keys: %v", conflicts(), err)
		}
	}
	// Two lists ended in the bucket, five batches each, and a round
	// that marks the last settled.
	if n, _ := db.Unsettled(ctx); n != 0 || rounds > 11 {
		t.Fatalf("%d lists unsettled after %d rounds", n, rounds)
	}
	if c := conflicts(); c != len(lone) {
		t.Fatalf("%d of %d lone keys decided", c, len(lone))
	}
}
