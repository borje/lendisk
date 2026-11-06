package state

import (
	"context"
	"testing"
	"time"
)

// PEER-8, SEC-16: the lender's mapping table holds each borrower's sealed
// names and nothing else; one borrower's rows are invisible to another.
func TestLentBucketsArePerBorrower(t *testing.T) {
	ctx := context.Background()
	db := openPeers(t)
	now := time.Unix(1_800_000_000, 0)
	for _, r := range []struct{ peer, sealed string }{{"bob", "sbbb"}, {"bob", "saaa"}, {"carol", "sccc"}} {
		if err := db.AddLentBucket(ctx, r.peer, r.sealed, now); err != nil {
			t.Fatal(err)
		}
	}
	// Adding twice keeps the first row (a retried CreateBucket).
	if err := db.AddLentBucket(ctx, "bob", "saaa", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	bs, err := db.LentBuckets(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 2 || bs[0].Sealed != "saaa" || bs[1].Sealed != "sbbb" || !bs[0].CreatedAt.Equal(now) || bs[0].Peer != "bob" {
		t.Fatalf("bob's buckets %+v", bs)
	}
	if ok, err := db.HasLentBucket(ctx, "bob", "sbbb"); err != nil || !ok {
		t.Fatalf("bob's sbbb: %v %v", ok, err)
	}
	if ok, err := db.HasLentBucket(ctx, "carol", "sbbb"); err != nil || ok {
		t.Fatalf("carol sees bob's sbbb: %v %v", ok, err)
	}
	if bs, _ := db.LentBuckets(ctx, "dave"); len(bs) != 0 {
		t.Fatalf("dave has %+v", bs)
	}
}
