package cli

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
)

// LEND-8 AC mechanics, PEER-4, OPS-2 (#24), with two daemons. A and B
// are friends at 80M; while B is off, A captures four 60 KiB objects,
// placed at B. Both lower their numbers to 200K, so when B is back it
// takes three and answers the fourth 507: A's copies to B pause for
// quota and A raises quota_exhausted. A sets 80M: the quota stays 200K,
// since B's number does not allow more, and B learns A's number by the
// quota call. B sets 80M: within seconds the quota is 80M on both sides
// and A's paused copy goes to B with no further action, although A's
// sender would otherwise wait an hour (PauseRetry) before trying B
// again. `peer list` on each side shows everything PEER-4 lists.
func TestBothNumbersRaisedResumeCopiesWithNoFurtherAction(t *testing.T) {
	parallel(t, 2)
	an, bn := friendNode(t), newNode(t)
	bn.regions = an.regions // one relay for both
	phrase, err := identity.NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	if code, _, e := bn.run(phrase+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	code, out, e := an.run("", "", "bucket", "add", "laptop", "--profile", "restic", "--copies", "1")
	if code != 0 {
		t.Fatal(e)
	}
	ak, sk := reAccess.FindStringSubmatch(out), reSecret.FindStringSubmatch(out)
	if ak == nil || sk == nil {
		t.Fatalf("no credential in:\n%s", out)
	}
	env := []string{envReplFast + "=1", envPauseRetry + "=1h", envAlertInterval + "=300ms", envQuotaHold + "=1s"}
	a, b := newProcNode(t, an, env...), newProcNode(t, bn, env...)
	b.start()
	a.start()
	if code, _, e := an.run("\n", "", "peer", "add", bn.invite("--quota", "80M")); code != 0 {
		t.Fatalf("peer add: %s", e)
	}
	aID := an.identity()
	aKey, bKey := aID.NodeID().String(), bn.identity().NodeID().String()

	b.p.kill()
	cl := s3client(a.addr, creds{ak[1], sk[1]})
	ctx := t.Context()
	for i := range 4 {
		data := make([]byte, 60<<10)
		rand.Read(data)
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String(fmt.Sprintf("data/0%d", i)),
			Body: bytes.NewReader(data)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []*node{an, bn} {
		other := map[*node]string{an: bKey, bn: aKey}[n]
		if code, out, e := n.run("", "", "peer", "quota", other, "200K"); code != 0 {
			t.Fatalf("peer quota 200K: %s%s", out, e)
		}
	}
	b.start()
	within(t, 30*time.Second, "A paused for quota at B", func() bool { return an.place(bKey).State == "quota" })
	within(t, 10*time.Second, "quota_exhausted on A", func() bool { return an.hasAlert(alert.KindQuotaExhausted, bKey) })
	if got := len(lentObjects(t, bn, aID, "laptop")); got != 3 {
		t.Fatalf("B holds %d of A's objects at 200K, want 3", got)
	}

	if code, out, e := an.run("", "", "peer", "quota", bKey, "80M"); code != 0 || !strings.Contains(out, "stays 200K each way until they raise") {
		t.Fatalf("A raises alone: %s%s", out, e)
	}
	// The daemons see a `peer quota` from another process on their 10 s
	// poll, so 10 s was the poll itself; LEND-8 asks for no further
	// action, not a time.
	within(t, 60*time.Second, "B learns A's number", func() bool { return bn.peerOf(aKey).PeerNumber == 80<<20 })
	if q := bn.peerOf(aKey).Quota; q != 200<<10 {
		t.Fatalf("B's quota after A alone raised: %d", q)
	}
	time.Sleep(time.Second)
	if p := an.place(bKey); p.State != "quota" || p.Queue.Objects != 1 {
		t.Fatalf("A's copies resumed before B allowed it: %+v", p)
	}

	if code, out, e := bn.run("", "", "peer", "quota", aKey, "80M"); code != 0 {
		t.Fatalf("B raises: %s%s", out, e)
	}
	raised := time.Now()
	within(t, 60*time.Second, "80M on both sides", func() bool {
		return an.peerOf(bKey).Quota == 80<<20 && bn.peerOf(aKey).Quota == 80<<20
	})
	an.drained(bKey, 20*time.Second)
	t.Logf("A's copies resumed at B %v after B's raise", time.Since(raised).Round(100*time.Millisecond))
	if got := len(lentObjects(t, bn, aID, "laptop")); got != 4 {
		t.Fatalf("B holds %d of A's objects after both raised, want 4", got)
	}
	within(t, 10*time.Second, "quota_exhausted cleared on A", func() bool { return !an.hasAlert(alert.KindQuotaExhausted, bKey) })

	// PEER-4 on both sides.
	within(t, 10*time.Second, "A's use at B reconciled", func() bool { return an.peerOf(bKey).UseThere > 4*(60<<10) })
	pa, pb := an.peerOf(bKey), bn.peerOf(aKey)
	for side, p := range map[string]PeerJSON{"A's view of B": pa, "B's view of A": pb} {
		if (p.Reachability != "direct" && p.Reachability != "relayed") || p.LastSeen == nil || p.Protocol != control.Protocol ||
			p.RepositorySize == nil || !p.NumberTold || p.OwnNumber != 80<<20 || p.PeerNumber != 80<<20 || p.Unverified {
			t.Errorf("%s: %+v", side, p)
		}
	}
	// VER-4 (#27): B holds A's objects, which A's daily challenge
	// verifies; A holds nothing of B's, which has no bucket.
	within(t, 30*time.Second, "B's replica verified", func() bool { return an.peerOf(bKey).LastVerified != nil })
	if pb.LastVerified != nil {
		t.Errorf("B's view of A, which holds nothing of B's: %+v", pb)
	}
	if pa.Remaining != pa.Quota-pa.UseThere || *pa.RepositorySize != 0 {
		t.Errorf("A's view of B: %+v", pa)
	}
	if pb.UseHere < 4*(60<<10) || *pb.RepositorySize != 1<<30 {
		t.Errorf("B's view of A: %+v (A's repository, 240 KiB, rounds up to 1 GiB)", pb)
	}
	_, text, _ := an.run("", "", "peer", "list")
	if !strings.Contains(text, bKey) || strings.Contains(text, "UNVERIFIED") || !strings.Contains(text, control.Protocol) {
		t.Errorf("peer list:\n%s", text)
	}
}
