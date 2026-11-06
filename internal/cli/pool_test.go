package cli

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// EP-6 (#19): `bucket add --copies N` sets the bucket's copies, and
// without it the bucket gets replication.copies. With copies = 1, from
// the flag or from the setting, bucket add warns, always. When the pool
// has fewer places than copies, bucket add says so (OPS-2).
func TestBucketAddCopiesAndTheCopiesOneWarning(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	code, out, errOut := n.run("", "", "bucket", "add", "three", "--profile", "restic", "--copies", "3")
	if code != 0 {
		t.Fatalf("bucket add: %s", errOut)
	}
	if !strings.Contains(out, "copies 3") || !strings.Contains(out, "has 0 places, fewer than copies 3") {
		t.Fatalf("bucket add --copies 3 said:\n%s", out)
	}
	if strings.Contains(out, "Warning: copies") {
		t.Fatalf("warning at copies 3:\n%s", out)
	}
	for _, args := range [][]string{{"flag", "--copies", "1"}, {"setting", "--replication-copies", "1"}} {
		code, out, errOut := n.run("", "", append([]string{"bucket", "add", args[0], "--profile", "restic"}, args[1:]...)...)
		if code != 0 || !strings.Contains(out, "copies 1") || !strings.Contains(out, singleCopyWarning) {
			t.Fatalf("bucket add with %v (%d %s):\n%s", args[1:], code, errOut, out)
		}
	}
	if code, _, _ := n.run("", "", "bucket", "add", "none", "--profile", "restic", "--copies", "0"); code != 2 {
		t.Fatalf("--copies 0 exit %d, want 2", code)
	}
	st := n.status()
	got := map[string]StatusBucket{}
	for _, b := range st.Buckets {
		got[b.Name] = b
	}
	if got["three"].Copies != 3 || got["flag"].Copies != 1 || got["setting"].Copies != 1 {
		t.Fatalf("buckets %+v", st.Buckets)
	}
	if !got["flag"].SingleCopy || !got["setting"].SingleCopy || got["three"].SingleCopy {
		t.Fatalf("single_copy %+v", st.Buckets)
	}
	_, out, _ = n.run("", "", "status", "--json")
	if !strings.Contains(out, `"single_copy": false`) || !strings.Contains(out, `"single_copy": true`) {
		t.Fatalf("status --json:\n%s", out)
	}
	_, text, _ := n.run("", "", "status")
	lines := strings.Split(text, "\n")
	var under []string
	for i, l := range lines {
		// EP-6: the line recommends 2.
		if strings.TrimSpace(l) == "! one copy: losing that friend's disk loses the offsite copy; 2 is recommended" {
			under = append(under, strings.Fields(lines[i-1])[0])
		}
	}
	if strings.Join(under, " ") != "flag setting" {
		t.Fatalf("the one-copy line is under %v:\n%s", under, text)
	}
}

// EP-6 (#19): with exactly one friend in the pool, whatever the
// buckets' copies, status heads the friends with a warning, and
// status --json says so under pool; not with two, and a friend gone
// past replace_after is not counted. It is not an alert.
func TestStatusWarnsOfAOneFriendPool(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	ctx := context.Background()
	friend := func(peer string) {
		t.Helper()
		db, err := state.Open(ctx, n.data, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := db.AddFriendship(ctx, state.Friendship{Peer: peer, Address: "tc" + peer, OwnNumber: 1 << 30,
			PeerNumber: 1 << 30, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(friends int, single bool) {
		t.Helper()
		st := n.status()
		if st.Pool.Friends != friends || st.Pool.SingleFriend != single {
			t.Fatalf("pool %+v, want %d friends", st.Pool, friends)
		}
		if len(st.Alerts) != 0 {
			t.Fatalf("alerts %+v", st.Alerts)
		}
		_, text, _ := n.run("", "", "status")
		if strings.Contains(text, singleFriendLine) != single {
			t.Fatalf("one-friend line %v with %d friends:\n%s", !single, friends, text)
		}
	}
	check(0, false)
	friend("B")
	check(1, true)
	friend("C")
	check(2, false)
	db, err := state.Open(ctx, n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := db.SetPlaceCondition(ctx, "C", state.ConditionOK, "", now.Add(-9*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPlaceCondition(ctx, "C", state.ConditionRetrying, "dial: no route", now.Add(-8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	db.Close()
	check(1, true) // C is gone past replace_after
	// With reroute_after 10 days C is not even unreachable yet: status
	// counts the pool as the daemon would.
	_, out, errOut := n.run("", "", "status", "--json", "--replication-reroute-after", "240h")
	if !strings.Contains(out, `"friends": 2`) {
		t.Fatalf("status with reroute_after 240h (%s):\n%s", errOut, out)
	}
}

// REPL-4, OPS-1 (#19): status shows the bytes held at fewer than copies
// places: here the one friend is the only place for a copies-2 bucket.
func TestStatusShowsBytesBelowCopies(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	if code, _, errOut := n.run("", "", "bucket", "add", "laptop", "--profile", "restic"); code != 0 {
		t.Fatal(errOut)
	}
	ctx := context.Background()
	db, err := state.Open(ctx, n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddFriendship(ctx, state.Friendship{Peer: "B", Address: "tcB", OwnNumber: 1 << 30, PeerNumber: 1 << 30,
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		key, class string
		size       int64
	}{{"data/1", "data", 1000}, {"data/2", "data", 500}} {
		seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: c.key, Op: state.OpPut, Class: c.class}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FinishPut(ctx, seqs[0], c.size, "h"+c.key); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	st := n.status()
	if len(st.Buckets) != 1 || st.Buckets[0].BelowCopies.Objects != 2 || st.Buckets[0].BelowCopies.Bytes != 1500 {
		t.Fatalf("buckets %+v", st.Buckets)
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "1500 bytes at fewer than 2 places") {
		t.Fatalf("status:\n%s", text)
	}
}
