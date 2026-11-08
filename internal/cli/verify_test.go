package cli

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/gateway"
)

// replicaOf is bucket's replica at peer in n's status.
func replicaOf(t *testing.T, n *node, bucket, peer string) StatusReplica {
	t.Helper()
	for _, r := range bucketOf(t, n.status(), bucket).Replicas {
		if r.Peer == peer {
			return r
		}
	}
	return StatusReplica{}
}

// VER-1, VER-3, VER-4 (#27): `lendisk verify <bucket> --peer <id>`
// runs the challenge on demand through the daemon, and `--full` checks
// every object B holds by hash. `status` shows, per replica, when it was
// last verified, in text and JSON, and `peer list` shows it per friend.
// A friend that silently lost an object fails: exit 4, the replica is
// unverified and alerted. Usage and setup errors are told apart.
func TestVerifyOnDemandAndStatus(t *testing.T) {
	parallel(t, 2)
	an, bn := friendNode(t), friendNode(t)
	bn.regions = an.regions
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	cr := an.addBucket(t, "laptop", "restic")
	if code, _, e := an.run("", "", "bucket", "set", "laptop", "--copies", "1"); code != 0 {
		t.Fatal(e)
	}
	if code, _, e := an.run("", "", "verify", "laptop", "--peer", bn.identity().NodeID().String()); code != 1 ||
		!strings.Contains(e, "not a friend") {
		t.Fatalf("verify at a stranger: %d %s", code, e)
	}
	b.start()
	a.start()
	befriend(t, bn, an)
	bKey := bn.identity().NodeID().String()
	cl := s3client(a.addr, cr)
	for i := range 5 {
		body := make([]byte, 3000+i)
		rand.Read(body)
		if _, err := cl.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
			Key: aws.String(fmt.Sprintf("data/%02d", i)), Body: bytes.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	an.drained(bKey, 60*time.Second)
	// The daemon's daily challenge runs once B holds something.
	within(t, 60*time.Second, "B's replica verified by the daily challenge", func() bool {
		return replicaOf(t, an, "laptop", bKey).LastVerified != ""
	})

	for _, c := range []struct {
		args []string
		code int
		want string
	}{
		{[]string{"verify", "laptop"}, 2, "--peer"},
		{[]string{"verify"}, 2, ""},
		{[]string{"verify", "photos", "--peer", bKey}, 1, "no bucket"},
		{[]string{"verify", "laptop", "--peer", "not-an-id"}, 2, "node ID"},
	} {
		if code, _, e := an.run("", "", c.args...); code != c.code || !strings.Contains(e, c.want) {
			t.Errorf("%v: %d %s", c.args, code, e)
		}
	}
	code, out, e := an.run("", "", "verify", "laptop", "--peer", bKey)
	if code != 0 || !strings.Contains(out, "verified") {
		t.Fatalf("verify: %d %s%s", code, out, e)
	}
	code, out, e = an.run("", "", "verify", "laptop", "--peer", bKey, "--full")
	if code != 0 || !strings.Contains(out, "5 objects") {
		t.Fatalf("verify --full: %d %s%s", code, out, e)
	}
	r := replicaOf(t, an, "laptop", bKey)
	if r.LastVerified == "" || r.LastFull == "" || r.Unverified {
		t.Fatalf("replica %+v", r)
	}
	_, text, _ := an.run("", "", "status")
	if !strings.Contains(text, "verified at "+bKey[:8]) {
		t.Fatalf("status:\n%s", text)
	}
	if p := an.peerOf(bKey); p.LastVerified == nil {
		t.Fatalf("peer list: %+v", p)
	}
	_, list, _ := an.run("", "", "peer", "list")
	if strings.Contains(list, " never ") {
		t.Fatalf("peer list:\n%s", list)
	}

	// B silently loses an object.
	dir := filepath.Join(bn.data, "store", gateway.LentName(an.identity().NodeID(), an.identity().SealedName("laptop")))
	if err := os.Remove(filepath.Join(dir, "data", "03")); err != nil {
		t.Fatal(err)
	}
	code, out, e = an.run("", "", "verify", "laptop", "--peer", bKey, "--full")
	if code != 4 || !strings.Contains(e, "data/03") {
		t.Fatalf("verify --full after the loss: %d %s%s", code, out, e)
	}
	within(t, 30*time.Second, "the alert", func() bool { return an.hasAlert(alert.KindReplicaUnverified, bKey+"/laptop") })
	if r := replicaOf(t, an, "laptop", bKey); !r.Unverified || !strings.Contains(r.Detail, "data/03") {
		t.Fatalf("replica %+v", r)
	}
	if p := an.peerOf(bKey); !p.Unverified {
		t.Fatalf("peer list: %+v", p)
	}
	_, text, _ = an.run("", "", "status")
	if !strings.Contains(text, "UNVERIFIED at "+bKey[:8]) {
		t.Fatalf("status:\n%s", text)
	}

	// With the daemon stopped, verify says so.
	a.p.kill()
	if code, _, e := an.run("", "", "verify", "laptop", "--peer", bKey); code != 1 || !strings.Contains(e, "daemon is not running") {
		t.Fatalf("verify with no daemon: %d %s", code, e)
	}
}

// Scenario 5, a bad lender (VER-1, REPL-13, OPS-2; #27), on A's
// injected clock: scenario 1's circle, a restic backup offsite at two
// copies, every replica verified. B silently deletes half of A's packs
// in its store. Within a day, A's daily challenge of B fails: A is
// alerted that B's replica is unverified, and re-places B's copies at C
// and D, so every data object is at both of them and the snapshot is
// offsite at two copies without B.
func TestScenarioBadLender(t *testing.T) {
	parallel(t, 4)
	clock := filepath.Join(t.TempDir(), "clock")
	f := newFourFriends(t, envClock+"="+clock)
	id := f.backup("one", 8)
	f.waitOffsite(id)
	f.drain()
	var bID string
	for pid := range f.friends {
		if bID == "" || pid < bID {
			bID = pid
		}
	}
	b := f.friends[bID]
	within(t, 90*time.Second, "every replica verified", func() bool {
		for pid := range f.friends {
			if replicaOf(t, f.an, "laptop", pid).LastVerified == "" {
				return false
			}
		}
		return true
	})
	// B deletes half of the packs it holds of A's, directly in its store.
	dir := filepath.Join(b.node.data, "store", gateway.LentName(f.aID.NodeID(), f.aID.SealedName("laptop")))
	var packs []string
	for k := range lentObjects(t, b.node, f.aID, "laptop") {
		if strings.HasPrefix(k, "data/") {
			packs = append(packs, k)
		}
	}
	slices.Sort(packs)
	if len(packs) == 0 {
		t.Fatal("B holds no pack")
	}
	for _, k := range packs[:(len(packs)+1)/2] {
		if err := os.Remove(filepath.Join(dir, filepath.FromSlash(k))); err != nil {
			t.Fatal(err)
		}
	}
	// A day later on A's clock, the daily challenge is due again.
	if err := os.WriteFile(clock, []byte((24*time.Hour + time.Minute).String()), 0o600); err != nil {
		t.Fatal(err)
	}
	within(t, 90*time.Second, "A alerted that B's replica is unverified", func() bool {
		return f.an.hasAlert(alert.KindReplicaUnverified, bID+"/laptop")
	})
	want := replicable(t, f.an, "laptop")
	within(t, 180*time.Second, "every data object at both C and D", func() bool {
		for pid, pn := range f.friends {
			if pid == bID {
				continue
			}
			got := lentObjects(t, pn.node, f.aID, "laptop")
			for k, h := range want {
				if strings.HasPrefix(k, "data/") && got[k] != h {
					return false
				}
			}
		}
		return true
	})
	bk := bucketOf(t, f.an.status(), "laptop")
	if !bk.Offsite.Complete || bk.BelowCopies.Objects != 0 {
		t.Fatalf("after re-placement: %+v", bk)
	}
	if r := replicaOf(t, f.an, "laptop", bID); !r.Unverified {
		t.Fatalf("B's replica %+v", r)
	}
}
