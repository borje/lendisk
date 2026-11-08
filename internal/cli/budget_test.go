package cli

import (
	"bytes"
	"crypto/rand"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/identity"
)

// #29 re-review, end to end: recover's listing has a time budget and
// friends can be skipped. B answers its lists slowly, past recover's
// budget; C is skipped; a --skip naming no friend is warned of. recover
// still saves the identity and config and finishes, leaving both lists
// pending, and `lendisk serve` lists both to the end.
func TestRecoverWithinBudgetAndSkip(t *testing.T) {
	parallel(t, 4)
	if testing.Short() {
		t.Skip("long")
	}
	phraseA, _ := identity.NewPhrase()
	an, bn, cn := newNode(t), newNode(t), newNode(t)
	bn.regions, cn.regions = an.regions, an.regions
	for n, p := range map[*node]string{an: phraseA, bn: "", cn: ""} {
		if p == "" {
			p, _ = identity.NewPhrase()
		}
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	own := an.addBucket(t, "laptop", "generic")
	fast := envReplFast + "=1"
	a, b, c := newProcNode(t, an, fast), newProcNode(t, bn, fast, envListDelay+"=4s"), newProcNode(t, cn, fast)
	b.start()
	c.start()
	a.start()
	befriend(t, bn, an)
	befriend(t, cn, an)
	aID, bID, cID := an.identity(), bn.identity(), cn.identity()
	bKey, cKey := bID.NodeID().String(), cID.NodeID().String()
	bAddr := bn.status().Node.Address
	body := make([]byte, 512)
	for _, k := range []string{"one", "two"} {
		rand.Read(body)
		if _, err := s3client(a.addr, own).PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
			Key: aws.String(k), Body: bytes.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	an.drained(bKey, 60*time.Second)
	an.drained(cKey, 60*time.Second)
	want := replicable(t, an, "laptop")
	deadline := time.Now().Add(60 * time.Second)
	for {
		mb, errB := heldManifest(t, bn, aID)
		if errB == nil && len(mb.Friends) == 2 && len(mb.Buckets) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("B's copy of A's manifest: %+v %v", mb, errB)
		}
		time.Sleep(200 * time.Millisecond)
	}
	a.p.kill()

	fresh := newNode(t)
	fresh.regions = an.regions
	fresh.dialTimeout = 20 * time.Second
	fresh.listBudget = time.Second
	stranger := newID(t).NodeID().String()
	start := time.Now()
	code, out, errOut := fresh.run("y\n"+phraseA+"\n", "", "recover", "--skip", cKey, "--skip", stranger, bAddr)
	took := time.Since(start)
	if code != 0 {
		t.Fatalf("recover: %d\n%s\n%s", code, out, errOut)
	}
	t.Logf("recover took %v and printed:\n%s", took, out)
	for _, s := range []string{"at " + bKey + ": not finished in time", "at " + cKey + ": skipped",
		stranger + ": not a friend", "lendisk serve"} {
		if !strings.Contains(out, s) {
			t.Errorf("recover's output lacks %q", s)
		}
	}
	for _, p := range []string{filepath.Join(filepath.Dir(fresh.config), "identity.key"), fresh.config} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("recover did not write %s: %v", p, err)
		}
	}
	if p := fresh.status().Recovery.Pending; len(p) != 2 {
		t.Fatalf("pending after recover %+v", p)
	}

	fa := newProcNode(t, fresh, fast)
	fa.start()
	deadline = time.Now().Add(90 * time.Second)
	for len(fresh.status().Recovery.Pending) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("serve did not list the pending friends: %+v", fresh.status().Recovery.Pending)
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, place := range []string{bKey, cKey} {
		if got := ackedAt(t, fresh, place, "laptop"); !maps.Equal(got, want) {
			t.Fatalf("held at %s: %v, want %v", place, got, want)
		}
	}
}

func newID(t *testing.T) *identity.Identity {
	t.Helper()
	p, _ := identity.NewPhrase()
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
