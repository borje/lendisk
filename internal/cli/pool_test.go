package cli

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// nudge is what bucket add and status say of a bucket at copies = 1
// that the user did not ask for (EP-6).
const nudge = "one lost friend loses data"

// EP-6 (#19): `bucket add --copies N` sets the bucket's copies; with
// copies = 1 from the default (replication.copies) bucket add and status
// nudge towards 2, and say nothing when the user gave --copies 1. When
// the pool has fewer places than copies, bucket add says so (OPS-2).
func TestBucketAddCopiesAndTheCopiesOneNudge(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	code, out, errOut := n.run("", "", "bucket", "add", "three", "--profile", "restic", "--copies", "3")
	if code != 0 {
		t.Fatalf("bucket add: %s", errOut)
	}
	if !strings.Contains(out, "copies 3") || !strings.Contains(out, "has 0 places, fewer than copies 3") {
		t.Fatalf("bucket add --copies 3 said:\n%s", out)
	}
	if strings.Contains(out, nudge) {
		t.Fatalf("nudge at copies 3:\n%s", out)
	}
	code, out, errOut = n.run("", "", "bucket", "add", "explicit", "--profile", "restic", "--copies", "1")
	if code != 0 || strings.Contains(out, nudge) {
		t.Fatalf("bucket add --copies 1 (%d %s):\n%s", code, errOut, out)
	}
	code, out, errOut = n.run("", "", "bucket", "add", "implicit", "--profile", "restic", "--replication-copies", "1")
	if code != 0 || !strings.Contains(out, "copies 1") || !strings.Contains(out, nudge) {
		t.Fatalf("bucket add with replication.copies = 1 (%d %s):\n%s", code, errOut, out)
	}
	if code, _, _ := n.run("", "", "bucket", "add", "none", "--profile", "restic", "--copies", "0"); code != 2 {
		t.Fatalf("--copies 0 exit %d, want 2", code)
	}
	st := n.status()
	got := map[string]StatusBucket{}
	for _, b := range st.Buckets {
		got[b.Name] = b
	}
	if got["three"].Copies != 3 || got["implicit"].Copies != 1 || got["explicit"].Copies != 1 {
		t.Fatalf("buckets %+v", st.Buckets)
	}
	if !got["implicit"].CopiesNudge || got["explicit"].CopiesNudge || got["three"].CopiesNudge {
		t.Fatalf("nudges %+v", st.Buckets)
	}
	_, text, _ := n.run("", "", "status")
	var lines []string
	for _, l := range strings.Split(text, "\n") {
		if strings.Contains(l, nudge) {
			lines = append(lines, l)
		}
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "implicit") {
		t.Fatalf("status nudges %q:\n%s", lines, text)
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
