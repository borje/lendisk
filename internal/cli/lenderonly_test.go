package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/identity"
)

// initLender runs `init --lender-only` for n, from a new phrase.
func initLender(t *testing.T, n *node) {
	t.Helper()
	p, err := identity.NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	if code, _, e := n.run(p+"\n", "", "init", "--restore", "--lender-only"); code != 0 {
		t.Fatal(e)
	}
}

// startLending starts a lender-only daemon, which has no loopback
// endpoint to wait for, and waits until it lends.
func (pn *procNode) startLending() {
	pn.t.Helper()
	pn.p = pn.startProc(pn.t, pn.addr, pn.logw, 0, pn.env...)
	deadline := time.Now().Add(60 * time.Second)
	for {
		b, _ := os.ReadFile(pn.logf)
		if bytes.Contains(b, []byte(lendingOnlyLog)) {
			return
		}
		if pn.p.exited() || time.Now().After(deadline) {
			pn.t.Fatalf("lender-only daemon not serving:\n%s", tail(b))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// PEER-9 (#41): `init --lender-only` writes role = "lender" and asks
// nothing about a repository it does not have; the node has no S3
// endpoint and refuses a bucket. A config that says otherwise, or
// capacity flags, are usage errors that write nothing.
func TestInitLenderOnlyWritesTheRole(t *testing.T) {
	parallel(t, 0)
	n := newNode(t)
	code, out, e := n.run("abandon\nabandon\nart\n", testPhrase, "init", "--lender-only")
	if code != 0 {
		t.Fatal(e)
	}
	if strings.Contains(out, "repository size") {
		t.Errorf("a lender-only init asked for a repository size:\n%s", out)
	}
	b, err := os.ReadFile(n.config)
	if err != nil || !strings.Contains(string(b), "role = \"lender\"") {
		t.Fatalf("config: %v\n%s", err, b)
	}
	st := n.status()
	if st.Node.Role != "lender" || st.Endpoint.S3 != "" {
		t.Errorf("status: role %q, S3 %q", st.Node.Role, st.Endpoint.S3)
	}
	if code, _, e := n.run("", "", "bucket", "add", "laptop", "--profile", "restic"); code == 0 || !strings.Contains(e, "role = \"full\"") {
		t.Errorf("bucket add on a lender-only node: %d %s", code, e)
	}

	for _, c := range []struct {
		config string
		args   []string
	}{
		{"role = \"full\"\n", nil},
		{"", []string{"--repository-size", "100G"}},
		{"", []string{"--copies", "2"}},
	} {
		m := newNode(t)
		if c.config != "" {
			os.MkdirAll(filepath.Dir(m.config), 0o700)
			if err := os.WriteFile(m.config, []byte(c.config), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		code, _, e := m.run("abandon\nabandon\nart\n", testPhrase, append([]string{"init", "--lender-only"}, c.args...)...)
		if code != 2 || !strings.Contains(e, "lender") {
			t.Errorf("init --lender-only %v with %q: %d %s", c.args, c.config, code, e)
		}
		if _, err := identity.Load(filepath.Dir(m.config)); err == nil {
			t.Errorf("init --lender-only %v with %q wrote an identity", c.args, c.config)
		}
	}
}

// PEER-9 AC (#41): in the lender-only profile nothing listens on
// loopback, nor anywhere else: the only new socket is tailcat's UDP one,
// and S3 for friends is inside its tunnel.
func TestLenderOnlyNothingListensOnLoopback(t *testing.T) {
	// Not parallel: it reads this process's sockets, which a parallel
	// test would add to (see parallel_test.go).
	n := newNode(t)
	if code, _, e := n.run("abandon\nabandon\nart\n", testPhrase, "init", "--lender-only"); code != 0 {
		t.Fatal(e)
	}
	before, err := ownSockets()
	if err != nil {
		t.Skipf("cannot inspect sockets: %v", err)
	}
	if d := n.serve(t); d.addr != "" {
		t.Errorf("a lender-only daemon serves S3 at %s", d.addr)
	}
	after, _ := ownSockets()
	old := map[sock]bool{}
	for _, s := range before {
		old[s] = true
	}
	udp := 0
	for _, s := range after {
		if old[s] {
			continue
		}
		host, _, _ := net.SplitHostPort(s.local)
		ip := net.ParseIP(host)
		switch {
		case s.proto == "tcp" && s.listen:
			t.Errorf("TCP listener %s", s.local)
		case s.proto == "tcp":
			// a connection out, to the test's relay on loopback
		case ip != nil && ip.IsLoopback():
			t.Errorf("UDP socket on loopback: %s", s.local)
		case s.proto == "udp":
			if ip == nil || !ip.IsUnspecified() {
				t.Errorf("unexpected UDP socket %s", s.local)
			}
			udp++
		}
	}
	if udp > 2 { // tailcat binds one socket per address family
		t.Errorf("%d new UDP sockets, want tailcat's (at most one per family)", udp)
	}
}

// PEER-9 (#41): a lender-only B befriends A as any node does (B
// invites), holds A's backup and pushes its own manifest; its status
// and peer list say it lends only. Then B switches to role = "full" and
// restarts: it adds a bucket that replicates to A, and neither side's
// friendship changed.
func TestLenderOnlyNodeLendsThenAddsABucket(t *testing.T) {
	parallel(t, 2)
	if testing.Short() {
		t.Skip("long")
	}
	an := friendNode(t)
	bn := newNode(t)
	bn.regions = an.regions // one relay for both
	initLender(t, bn)
	ca := an.addBucket(t, "laptop", "generic")
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	b.startLending()
	a.start()
	befriend(t, bn, an) // the lender-only node invites
	aID, bID := an.identity(), bn.identity()
	aKey, bKey := aID.NodeID().String(), bID.NodeID().String()

	put := func(addr string, c creds, bucket, prefix string) {
		t.Helper()
		cl := s3client(addr, c)
		buf := make([]byte, 64<<10)
		for i := range 4 {
			rand.Read(buf)
			if _, err := cl.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(bucket),
				Key: aws.String(fmt.Sprintf("%s-%02d", prefix, i)), Body: bytes.NewReader(buf)}); err != nil {
				t.Fatalf("put to %s: %v", bucket, err)
			}
		}
	}
	put(a.addr, ca, "laptop", "one")
	an.drained(bKey, 90*time.Second)
	sameObjects(t, replicable(t, an, "laptop"), lentObjects(t, bn, aID, "laptop"))
	if len(lentObjects(t, bn, aID, "laptop")) != 4 {
		t.Fatal("B holds fewer than the 4 objects written")
	}
	// Its identity is recoverable from A like any node's (ID-4).
	bn.delivered(0)
	if _, err := heldManifest(t, an, bID); err != nil {
		t.Errorf("A holds no manifest of B: %v", err)
	}

	st := bn.status()
	if st.Node.Role != "lender" || st.Endpoint.S3 != "" || len(st.Buckets) != 0 {
		t.Errorf("B's status: role %q, S3 %q, %d buckets", st.Node.Role, st.Endpoint.S3, len(st.Buckets))
	}
	if c := capacityOf(t, st); c.LentUsed == 0 || c.Received == 0 || c.ReceivedUnused != c.Received {
		t.Errorf("B's capacity: %+v", c)
	}
	_, out, _ := bn.run("", "", "status")
	if !strings.Contains(out, "Lending only:") || strings.Contains(out, "Places:") || strings.Contains(out, singleFriendLine) || !strings.Contains(out, "role = \"full\"") {
		t.Errorf("B's status text:\n%s", out)
	}
	_, out, _ = bn.run("", "", "peer", "list")
	if !strings.Contains(out, "lends only") || !strings.Contains(out, aKey) {
		t.Errorf("B's peer list:\n%s", out)
	}
	bPeers, aPeers := bn.peers(), an.peers()

	// Switch the role and restart.
	b.p.kill()
	cfg, err := os.ReadFile(bn.config)
	if err != nil {
		t.Fatal(err)
	}
	full := strings.Replace(string(cfg), "role = \"lender\"", "role = \"full\"", 1)
	if full == string(cfg) {
		t.Fatalf("no role line in B's config:\n%s", cfg)
	}
	if err := os.WriteFile(bn.config, []byte(full), 0o600); err != nil {
		t.Fatal(err)
	}
	b.start() // now on loopback too
	cb := bn.addBucket(t, "desktop", "generic")
	put(b.addr, cb, "desktop", "two")
	bn.drained(aKey, 90*time.Second)
	sameObjects(t, replicable(t, bn, "desktop"), lentObjects(t, an, bID, "desktop"))
	if len(lentObjects(t, an, bID, "desktop")) != 4 {
		t.Fatal("A holds fewer than the 4 objects B wrote")
	}
	same := func(who string, before, after []PeerJSON) {
		t.Helper()
		if len(before) != 1 || len(after) != 1 {
			t.Fatalf("%s's friends: %d then %d", who, len(before), len(after))
		}
		x, y := before[0], after[0]
		if x.ID != y.ID || x.Address != y.Address || x.Quota != y.Quota || x.OwnNumber != y.OwnNumber ||
			x.PeerNumber != y.PeerNumber || !x.Since.Equal(y.Since) || x.OwnRetentionSeconds != y.OwnRetentionSeconds ||
			x.PeerRetentionSeconds != y.PeerRetentionSeconds {
			t.Errorf("%s's friendship changed:\n%+v\n%+v", who, x, y)
		}
	}
	same("B", bPeers, bn.peers())
	same("A", aPeers, an.peers())
	if st := bn.status(); st.Node.Role != "full" || st.Endpoint.S3 == "" {
		t.Errorf("B after the switch: role %q, S3 %q", st.Node.Role, st.Endpoint.S3)
	}
}

// PEER-9 (#41): switching back to the lender role while the node has a
// bucket would silently stop its replication, so serve refuses to start.
func TestLenderRoleRefusedWithBuckets(t *testing.T) {
	serving(t, 0) // serve runs in this process, though it stops before the gateway
	n := newNode(t)
	n.mustInit(t)
	n.addBucket(t, "laptop", "restic")
	code, _, e := n.run("", "", "serve", "--role", "lender")
	if code != 1 || !strings.Contains(e, "laptop") || !strings.Contains(e, "role = \"full\"") {
		t.Errorf("serve as a lender with a bucket: %d %s", code, e)
	}
}
