package cli

import (
	"bytes"
	"crypto/rand"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
)

// covers is status's number per friendship (LEND-8), by node ID.
func covers(c StatusCapacity) map[string]int64 {
	out := map[string]int64{}
	for _, f := range c.Friends {
		if f.Cover > 0 {
			out[f.Peer] = f.Cover
		}
	}
	return out
}

// alertMessage is A's pending alert of kind, or "".
func alertMessage(n *node, kind string) string {
	for _, a := range n.status().Alerts {
		if a.Kind == kind {
			return a.Message
		}
	}
	return ""
}

// LEND-8 AC (#37), scaled down from GB to MB: A, B and C, each pair
// friends at 50, here 20 MB. A's repository grows past what B and C
// protect at two copies; `status` on A names B and C and a number for
// each, and so does the protectable_size alert (OPS-2). After A and B
// both set 80, here 32 MB, for their friendship, its quota is that on
// both sides and A's copies resume at B with no further action: every
// data object is at B. C, still at 20 MB, is the one friendship named.
func TestQuotaACNamesTheNumbersAndCopiesResumeAtTheRaisedFriend(t *testing.T) {
	parallel(t, 3)
	f := newCircleAt(t, 3, "20M", envAlertInterval+"=300ms")
	aKey := f.aID.NodeID().String()
	cl := s3client(f.a.addr, f.creds)
	put := func(n int) {
		t.Helper()
		for range n {
			data := make([]byte, 1<<20)
			rand.Read(data)
			h := sha256Hex(data)
			if _, err := cl.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
				Key: aws.String("data/" + h[:2] + "/" + h), Body: bytes.NewReader(data)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	put(12)
	f.drain()
	if c := capacityOf(t, f.an.status()); c.Alerting || c.Protectable < 19<<20 {
		t.Fatalf("12 MiB at two 20 MB friends: %+v", c)
	}

	put(12) // 24 MiB: past what B and C protect at two copies
	within(t, 60*time.Second, "objects below copies", func() bool {
		return bucketOf(t, f.an.status(), "laptop").BelowCopies.Objects > 0
	})
	f.drain()
	c := capacityOf(t, f.an.status())
	cv := covers(c)
	if !c.Alerting || c.Deficit <= 0 || len(cv) != 2 {
		t.Fatalf("past protection: %+v", c)
	}
	_, text, _ := f.an.run("", "", "status")
	for id := range f.friends {
		if n := cv[id]; n <= 20<<20 || n > 32<<20 {
			t.Errorf("%s: number %d, want above 20 MB and at most the AC's 32", id[:8], n)
		}
		if !strings.Contains(text, id+": "+formatSize(cv[id])+" (now 20M)") {
			t.Errorf("status does not name %s and %s:\n%s", id, formatSize(cv[id]), text)
		}
	}
	within(t, 30*time.Second, "the protectable_size alert", func() bool { return alertMessage(f.an, alert.KindProtectable) != "" })
	msg := alertMessage(f.an, alert.KindProtectable)
	for id := range f.friends {
		if !strings.Contains(msg, "friend "+id+": "+formatSize(cv[id])) {
			t.Errorf("the alert does not name %s: %s", id[:8], msg)
		}
	}

	ids := slices.Sorted(maps.Keys(f.friends))
	bID, cID := ids[0], ids[1]
	b := f.friends[bID]
	if code, out, e := f.an.run("", "", "peer", "quota", bID, "32M"); code != 0 {
		t.Fatalf("A's number: %s%s", out, e)
	}
	if code, out, e := b.run("", "", "peer", "quota", aKey, "32M"); code != 0 {
		t.Fatalf("B's number: %s%s", out, e)
	}
	// The daemons see a `peer quota` from another process on their 10 s
	// poll, and the friend learns the number by the quota call.
	within(t, 90*time.Second, "32 MB on both sides", func() bool {
		return f.an.peerOf(bID).Quota == 32<<20 && b.peerOf(aKey).Quota == 32<<20
	})
	want := replicable(t, f.an, "laptop")
	within(t, 120*time.Second, "every data object at B", func() bool {
		got := lentObjects(t, b.node, f.aID, "laptop")
		for k, h := range want {
			if strings.HasPrefix(k, "data/") && got[k] != h {
				return false
			}
		}
		return true
	})
	if cv := covers(capacityOf(t, f.an.status())); cv[bID] != 0 || cv[cID] == 0 {
		t.Errorf("with B at 32 MB, the numbers named: %v, want C alone", cv)
	}
}

// Scenario 6, quota (LEND-7, LEND-8, OPS-2; #37), scaled down: A, B and
// C, each pair friends at 20 MB, A's restic bucket at two copies. A's
// repository grows past what its pool holds: the local backup still
// succeeds; status shows the protectable size below the repository
// size, copies paused for quota, and per friendship the number that
// would cover it. A and B set that number for their friendship, as do A
// and C: both quotas grow in both directions, and replication resumes
// until the snapshot is offsite at two copies.
func TestScenarioQuota(t *testing.T) {
	parallel(t, 3)
	f := newCircleAt(t, 3, "20M", envAlertInterval+"=300ms")
	aKey := f.aID.NodeID().String()
	f.waitOffsite(f.backup("one", 8))
	f.drain()

	id := f.backup("two", 16) // restic fails the test if this does not succeed
	within(t, 60*time.Second, "objects below copies", func() bool {
		return bucketOf(t, f.an.status(), "laptop").BelowCopies.Objects > 0
	})
	f.drain()
	c := capacityOf(t, f.an.status())
	cv := covers(c)
	if c.Protectable >= c.RepositorySize || len(cv) != 2 {
		t.Fatalf("past the pool: %+v", c)
	}
	_, text, _ := f.an.run("", "", "status")
	if !strings.Contains(text, "copies beyond it are paused for quota") {
		t.Errorf("status:\n%s", text)
	}
	for pid := range f.friends {
		if !strings.Contains(text, pid+": "+formatSize(cv[pid])) {
			t.Errorf("status does not name %s:\n%s", pid[:8], text)
		}
	}
	within(t, 30*time.Second, "the protectable_size alert", func() bool { return alertMessage(f.an, alert.KindProtectable) != "" })

	for pid, pn := range f.friends {
		n := formatSize(cv[pid])
		if code, out, e := f.an.run("", "", "peer", "quota", pid, n); code != 0 {
			t.Fatalf("A's number: %s%s", out, e)
		}
		if code, out, e := pn.run("", "", "peer", "quota", aKey, n); code != 0 {
			t.Fatalf("the friend's number: %s%s", out, e)
		}
	}
	within(t, 90*time.Second, "both quotas grown in both directions", func() bool {
		for pid, pn := range f.friends {
			if f.an.peerOf(pid).Quota != cv[pid] || pn.peerOf(aKey).Quota != cv[pid] {
				return false
			}
		}
		return true
	})
	f.waitOffsite(id)
	f.drain()
	if b := bucketOf(t, f.an.status(), "laptop"); !b.Offsite.Complete || b.BelowCopies.Objects != 0 {
		t.Fatalf("after the raise: %+v", b)
	}
	if c := capacityOf(t, f.an.status()); c.Alerting || len(covers(c)) != 0 {
		t.Fatalf("after the raise: %+v", c)
	}
}
