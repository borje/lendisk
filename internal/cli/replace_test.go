package cli

import (
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// EP-6, REPL-13 (#26): `bucket set <name> --copies N` changes an existing
// bucket's copies, with no daemon needed; the daemon then adds or
// removes copies. Copies 1 warns as `bucket add` does, and a pool
// smaller than copies is said. A missing or bad --copies is a usage
// error, an unknown bucket an error.
func TestBucketSetChangesCopies(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	if code, _, errOut := n.run("", "", "bucket", "add", "laptop", "--profile", "restic"); code != 0 {
		t.Fatal(errOut)
	}
	code, out, errOut := n.run("", "", "bucket", "set", "laptop", "--copies", "3")
	if code != 0 || !strings.Contains(out, "laptop now keeps 3 copies") || !strings.Contains(out, "fewer than copies 3") {
		t.Fatalf("bucket set --copies 3: %d %s\n%s", code, errOut, out)
	}
	if b := bucketOf(t, n.status(), "laptop"); b.Copies != 3 {
		t.Fatalf("copies %d", b.Copies)
	}
	code, out, _ = n.run("", "", "bucket", "set", "laptop", "--copies", "1")
	if code != 0 || !strings.Contains(out, singleCopyWarning) {
		t.Fatalf("bucket set --copies 1: %d\n%s", code, out)
	}
	for _, args := range [][]string{{"laptop"}, {"laptop", "--copies", "0"}} {
		if code, _, _ := n.run("", "", append([]string{"bucket", "set"}, args...)...); code != 2 {
			t.Fatalf("bucket set %v: exit %d, want 2", args, code)
		}
	}
	if code, _, errOut := n.run("", "", "bucket", "set", "nosuch", "--copies", "2"); code != 1 || !strings.Contains(errOut, "no bucket") {
		t.Fatalf("bucket set of no bucket: %d %s", code, errOut)
	}
}

// OPS-1, REPL-13 (#26): status shows re-placement in progress: the
// copies placed after a friend was removed that wait to be sent, then
// the moved copy that waits to be deleted at the place it left, and
// since when objects have been below copies with nowhere to go.
func TestStatusShowsReplacementInProgress(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	n.mustInit(t)
	if code, _, errOut := n.run("", "", "bucket", "add", "laptop", "--profile", "restic"); code != 0 {
		t.Fatal(errOut)
	}
	ctx := t.Context()
	db := openDB(t, n)
	for _, f := range []string{"B", "C", "D"} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 1 << 30, PeerNumber: 1 << 30,
			CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	seqs, err := db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: "data/1", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinishPut(ctx, seqs[0], 1000, "h1"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"B", "C"} {
		if err := db.AckPlacement(ctx, p, seqs[0], "h1", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.RemoveFriendship(ctx, "C", false, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Replace(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	r := bucketOf(t, n.status(), "laptop").Replacing
	if r.Queued != 1 || r.QueuedBytes != 1000 || r.Dropping != 0 {
		t.Fatalf("replacing %+v, want one copy queued", r)
	}
	_, text, _ := n.run("", "", "status")
	if !strings.Contains(text, "re-placement: 1 copies (1000 bytes) placed after the pool changed wait to be sent") {
		t.Fatalf("status:\n%s", text)
	}
	if err := db.AckPlacement(ctx, "D", seqs[0], "h1", time.Now()); err != nil {
		t.Fatal(err)
	}
	st := n.status()
	if r := bucketOf(t, st, "laptop").Replacing; r.Queued != 0 || r.Dropping != 1 {
		t.Fatalf("replacing %+v, want the moved copy dropping", r)
	}
	for _, p := range st.Places {
		if p.Place == "C" && p.Dropping != 1 {
			t.Fatalf("place %+v", p)
		}
	}
	// copies 4 with three places left: below copies with nowhere to go.
	if err := db.SetCopies(ctx, "laptop", 4); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Replace(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	if r := bucketOf(t, n.status(), "laptop").Replacing; r.BelowSince == "" {
		t.Fatalf("replacing %+v, want below_since", r)
	}
}
