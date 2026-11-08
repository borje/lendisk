package cli

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/state"
)

// Scenario 13, a friend leaves (REPL-13, PEER-5, LEND-8; #26), scaled
// down: scenario 1's circle at 24 MB per friendship, and a restic
// backup of about 24 MB at two copies, which B, C and D hold between
// them. A ends its friendship with D: B and C at 24 MB cannot hold two
// copies of everything, so A re-places what fits and the rest stays
// below copies. A and B set 32 MB, as do A and C, with no invite: A
// re-places the copies D held at B and C, and the snapshot is offsite
// at two copies again, every data object at both of them. (What status
// says the numbers should be is LEND-7's protectable size, #37.)
func TestScenarioFriendLeaves(t *testing.T) {
	parallel(t, 4)
	f := newFourFriendsAt(t, "24M")
	id := f.backup("one", 24)
	f.waitOffsite(id)
	f.drain()

	var dID string
	var d *procNode
	for pid, pn := range f.friends {
		if d == nil || pid > dID {
			dID, d = pid, pn
		}
	}
	if code, out, e := f.an.run("", "", "peer", "remove", dID); code != 0 {
		t.Fatalf("peer remove D: %s%s", out, e)
	}
	delete(f.friends, dID)
	// A re-places what fits, and no more: B and C are full.
	within(t, 90*time.Second, "re-placement within the room left", func() bool {
		b := bucketOf(t, f.an.status(), "laptop")
		return b.Replacing.Queued == 0 && b.BelowCopies.Objects > 0
	})
	if b := bucketOf(t, f.an.status(), "laptop"); b.Offsite.Complete {
		t.Fatalf("offsite with D's copies gone and no room for them: %+v", b)
	}

	// A and B, and A and C, agree 32 MB.
	for pid, pn := range f.friends {
		if code, out, e := f.an.run("", "", "peer", "quota", pid, "32M"); code != 0 {
			t.Fatalf("A's number: %s%s", out, e)
		}
		if code, out, e := pn.node.run("", "", "peer", "quota", f.aID.NodeID().String(), "32M"); code != 0 {
			t.Fatalf("the friend's number: %s%s", out, e)
		}
	}
	// The daemons see a `peer quota` from another process on their 10 s
	// poll; the friends learn A's number by the quota call.
	within(t, 90*time.Second, "32 MB on both friendships, and the copies placed", func() bool {
		for pid := range f.friends {
			if f.an.peerOf(pid).Quota != 32<<20 {
				return false
			}
		}
		return bucketOf(t, f.an.status(), "laptop").BelowCopies.Objects == 0
	})
	f.waitOffsite(id)
	f.drain()
	if b := bucketOf(t, f.an.status(), "laptop"); !b.Offsite.Complete || b.BelowCopies.Objects != 0 {
		t.Fatalf("after the raise: %+v", b)
	}
	// Every data object at both B and C, the metadata too.
	want := replicable(t, f.an, "laptop")
	for pid, pn := range f.friends {
		got := lentObjects(t, pn.node, f.aID, "laptop")
		for k, h := range want {
			if got[k] != h {
				t.Errorf("%s's copy of %s is %q, want %q", pid[:8], k, got[k], h)
			}
		}
	}
}

// REPL-13, RET-1 (#26): a friend unreachable past replace_after has its
// copies placed elsewhere, from this node's store, on A's injected
// clock; once the new copies are acknowledged, the old ones are queued
// as deletes at the friend. When it returns, it receives them and keeps
// each as a tombstone for the window, and holds the moved objects no
// more; the bucket is offsite throughout at the friend that took them.
func TestAFriendBackAfterReplaceAfterTombstonesItsMovedCopies(t *testing.T) {
	parallel(t, 3)
	clock := filepath.Join(t.TempDir(), "clock")
	an, bn, cn := friendNode(t), friendNode(t), friendNode(t)
	bn.regions, cn.regions = an.regions, an.regions
	env := []string{envReplFast + "=1", envRerouteEvery + "=200ms"}
	a := newProcNode(t, an, append(env, envClock+"="+clock)...)
	b, c := newProcNode(t, bn, env...), newProcNode(t, cn, env...)
	b.start()
	c.start()
	a.start()
	cr := an.addBucket(t, "laptop", "restic")
	if code, _, e := an.run("", "", "bucket", "set", "laptop", "--copies", "1"); code != 0 {
		t.Fatal(e)
	}
	for _, n := range []*node{bn, cn} {
		if code, _, e := an.run("\n", "", "peer", "add", n.invite("--quota", "80M")); code != 0 {
			t.Fatalf("peer add: %s", e)
		}
	}
	aID := an.identity()
	ids := map[*node]string{bn: bn.identity().NodeID().String(), cn: cn.identity().NodeID().String()}
	cl := s3client(a.addr, cr)
	put := func(key string) {
		t.Helper()
		data := make([]byte, 20<<10)
		rand.Read(data)
		if _, err := cl.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String(key),
			Body: bytes.NewReader(data)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 4 {
		put(fmt.Sprintf("data/%d", i))
	}
	an.drained(ids[bn], 60*time.Second)
	an.drained(ids[cn], 60*time.Second)
	// X is whichever friend holds data/0; Y the other.
	x, y := bn, cn
	if _, ok := lentObjects(t, bn, aID, "laptop")["data/0"]; !ok {
		x, y = cn, bn
	}
	moved := lentObjects(t, x, aID, "laptop")
	pX := map[*node]*procNode{bn: b, cn: c}[x]
	pX.p.kill()
	put("index/late") // metadata, placed at X too: A's sender to X fails on it
	within(t, 60*time.Second, "A sees X fail", func() bool { return an.place(ids[x]).State == "retrying" })
	if err := os.WriteFile(clock, []byte((8 * 24 * time.Hour).String()), 0o600); err != nil {
		t.Fatal(err)
	}
	within(t, 90*time.Second, "X's copies at Y and queued as deletes at X", func() bool {
		got := lentObjects(t, y, aID, "laptop")
		for k := range moved {
			if _, ok := got[k]; !ok {
				return false
			}
		}
		return an.place(ids[x]).Dropping == int64(len(moved))
	})
	pX.start()
	within(t, 90*time.Second, "X's moved copies deleted there", func() bool { return an.place(ids[x]).Dropping == 0 })
	held := lentObjects(t, x, aID, "laptop")
	db := openDB(t, x)
	sealed := aID.SealedName("laptop")
	for k, h := range moved {
		if _, ok := held[k]; ok {
			t.Errorf("X still holds %s live", k)
		}
		ts, err := db.TombstonesOf(t.Context(), aID.NodeID().String(), sealed, k)
		if err != nil {
			t.Fatal(err)
		}
		if len(ts) != 1 || ts[0].SHA256 != h {
			t.Errorf("X's tombstones of %s: %+v, want one of %s", k, ts, h)
		}
	}
	var drained int
	openDB(t, an).QueryRow(`SELECT count(*) FROM placement WHERE place = ? AND state = ?`, ids[x], state.PlacementDrained).Scan(&drained)
	if drained != len(moved) {
		t.Errorf("%d rows drained at X, want %d", drained, len(moved))
	}
	if bk := bucketOf(t, an.status(), "laptop"); !bk.Offsite.Complete {
		t.Errorf("not offsite once X is back: %+v", bk.Offsite)
	}
}
