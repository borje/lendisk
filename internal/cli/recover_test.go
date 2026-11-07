package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/transport"
)

// ackedAt is what n's placement records place holding of bucket: key →
// hash.
func ackedAt(t *testing.T, n *node, place, bucket string) map[string]string {
	t.Helper()
	rows, err := openDB(t, n).Query(`SELECT key, sha256 FROM placement WHERE place = ? AND bucket = ? AND state = 'acked'`,
		place, bucket)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, h string
		if err := rows.Scan(&k, &h); err != nil {
			t.Fatal(err)
		}
		out[k] = h
	}
	return out
}

// heldManifest opens what lender keeps of borrower's manifest.
func heldManifest(t *testing.T, lender *node, borrower *identity.Identity) (*manifest.Manifest, error) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(manifest.Dir(filepath.Join(lender.data, "store")), borrower.NodeID().String()))
	if err != nil {
		return nil, err
	}
	return manifest.Open(borrower, b)
}

// The recovery demo (ID-5, NET-7, NET-7a; #29 and the #28 review): "my
// NAS died and I got my data back from my friend".
//
//   - A backs up a restic bucket replicated to B and C, then its machine
//     and every file of it are gone.
//   - B, the friend whose address is on A's paper sheet, serves an
//     older manifest than C holds: recover asks every friend and keeps
//     the newest, overflow credential and all, which it shows once and
//     writes nowhere.
//   - With nothing but the phrase and B's address, a fresh machine gets
//     A's identity, address, friends (with the keys they dial A with),
//     buckets and the version to continue from; placement is rebuilt
//     from B's list. C went offline before it was listed: its objects
//     count as held at B, and it is listed when it returns.
//   - Nothing is downloaded, no per-pair credential is in any file on
//     any side, and the phrase is in none.
//   - `lendisk serve` starts, B and C admit A (each holds A's next
//     manifest version, above every copy they held), C is listed, and B
//     dialling A with its dial key is admitted as A's friend.
func TestRecoverFromPhraseAndOneLendersAddress(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	phraseA, _ := identity.NewPhrase()
	an, bn, cn := newNode(t), newNode(t), newNode(t)
	bn.regions, cn.regions = an.regions, an.regions // one relay for all
	for n, p := range map[*node]string{an: phraseA, bn: "", cn: ""} {
		if p == "" {
			p, _ = identity.NewPhrase()
		}
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	own := an.addBucket(t, "laptop", "restic")
	fast := envReplFast + "=1"
	a, b, c := newProcNode(t, an, fast), newProcNode(t, bn, fast), newProcNode(t, cn, fast)
	b.start()
	c.start()
	a.start()
	befriend(t, bn, an)
	befriend(t, cn, an)
	aID, bID, cID := an.identity(), bn.identity(), cn.identity()
	aKey, bKey, cKey := aID.NodeID().String(), bID.NodeID().String(), cID.NodeID().String()
	aAddr, bAddr, cAddr := an.status().Node.Address, bn.status().Node.Address, cn.status().Node.Address

	s3c := s3client(a.addr, own)
	body := make([]byte, 2048)
	// restic's layout: packs, indexes and snapshots named by their hash.
	for _, k := range []string{"config", "keys/k1", "index/", "data/", "data/", "snapshots/", "locks/l1"} {
		rand.Read(body)
		if h := sha256Hex(body); k == "data/" {
			k += h[:2] + "/" + h
		} else if strings.HasSuffix(k, "/") {
			k += h
		}
		if _, err := s3c.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String(k),
			Body: bytes.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	an.drained(bKey, 60*time.Second)
	an.drained(cKey, 60*time.Second)
	want := replicable(t, an, "laptop")
	if len(want) != 6 {
		t.Fatalf("replicable %v", want)
	}
	sameObjects(t, want, lentObjects(t, bn, aID, "laptop"))
	sameObjects(t, want, lentObjects(t, cn, aID, "laptop"))

	// B and C hold the manifest naming both and the bucket.
	var old *manifest.Manifest
	deadline := time.Now().Add(60 * time.Second)
	for {
		mb, errB := heldManifest(t, bn, aID)
		mc, errC := heldManifest(t, cn, aID)
		if errB == nil && errC == nil && mb.Seq == mc.Seq && len(mb.Friends) == 2 && len(mb.Buckets) == 1 &&
			mb.Settings.Address == aAddr {
			old = mb
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the manifest at B and C: %+v %v / %+v %v", mb, errB, mc, errC)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// A's machine is gone with every file on it.
	a.p.kill()
	os.RemoveAll(filepath.Dir(filepath.Dir(an.config)))

	// C holds a newer version than B: B, the address on the sheet, is the
	// stale (or hostile) one. It names an overflow target with its
	// credential, which recover shows and must not write.
	newer := *old
	newer.Seq = old.Seq + 1
	const overflowSecret = "OVERFLOWSECRETdoNotWrite0123456789"
	newer.Overflow = []manifest.Overflow{{Name: "spill", Endpoint: "https://s3.example.net", Bucket: "spill",
		AccessKey: "OVERFLOWACCESS", SecretKey: overflowSecret}}
	sealed, err := manifest.Seal(aID, &newer, nil)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		dl, err := transport.NewFriendDialer(aID, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		defer dl.Close() // before recover dials with the same key
		hc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{DisableKeepAlives: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dl.Dial(ctx, cID.NodeID(), cAddr) }}}
		if _, err := (&control.Client{HTTP: hc}).PutManifest(t.Context(), sealed); err != nil {
			t.Fatal(err)
		}
	}()

	// A fresh machine: the phrase and B's address, nothing else. C goes
	// offline once the manifests are fetched, before it is listed.
	fresh := newNode(t)
	fresh.regions = an.regions
	fresh.dialTimeout = 15 * time.Second
	fresh.afterFetch = func() { c.p.kill() }
	if code, _, e := fresh.run("n\n"+phraseA+"\n", "", "recover", bAddr); code == 0 || !strings.Contains(e, "not confirmed") {
		t.Fatalf("recover without confirming the old node is stopped: %d %s", code, e)
	}
	code, out, errOut := fresh.run("y\n"+phraseA+"\n", "", "recover", bAddr)
	if code != 0 {
		t.Fatalf("recover: %d\n%s\n%s", code, out, errOut)
	}
	t.Logf("recover printed:\n%s", out)
	for _, s := range []string{
		bKey + ": version " + strconv.FormatInt(old.Seq, 10) + ", older",
		cKey + ": version " + strconv.FormatInt(newer.Seq, 10) + " (newest)",
		"Friends: 2", "laptop (restic, copies 2)",
		"at " + bKey + ": 6 objects", "at " + cKey + ": not listed",
		overflowSecret, "nothing was downloaded", "lendisk serve", "lendisk bucket show",
	} {
		if !strings.Contains(out, s) {
			t.Errorf("recover's output lacks %q", s)
		}
	}
	st := fresh.status()
	if st.Node.ID != aKey || st.Node.Address != aAddr {
		t.Fatalf("recovered node %+v, want %s at %s", st.Node, aKey, aAddr)
	}
	if len(st.Buckets) != 1 || st.Buckets[0].Name != "laptop" || st.Buckets[0].Copies != 2 || st.Buckets[0].Profile != "restic" {
		t.Fatalf("buckets %+v", st.Buckets)
	}
	if st.Manifest.Version != newer.Seq {
		t.Fatalf("manifest version %d, want to continue from %d", st.Manifest.Version, newer.Seq)
	}
	if p := st.Recovery.Pending; len(p) != 1 || p[0].Place != cKey || p[0].Bucket != "laptop" || p[0].Error == "" {
		t.Fatalf("lists pending %+v", p)
	}
	peers := map[string]PeerJSON{}
	for _, p := range fresh.peers() {
		peers[p.ID] = p
	}
	if peers[bKey].Address != bAddr || peers[cKey].Address != cAddr || len(peers) != 2 {
		t.Fatalf("peers %+v", peers)
	}
	for f, id := range map[string]*identity.Identity{bKey: bID, cKey: cID} {
		got, err := openDB(t, fresh).GetFriendship(t.Context(), f)
		if err != nil || got.DialKey != id.DialKeyFor(aID.NodeID()).NodeID().String() {
			t.Fatalf("%s's dial key: %+v %v", f, got, err)
		}
	}
	// Placement and the hash index: everything at B, nothing yet at C.
	if got := ackedAt(t, fresh, bKey, "laptop"); !maps.Equal(got, want) {
		t.Fatalf("held at B %v, want %v", got, want)
	}
	if got := ackedAt(t, fresh, cKey, "laptop"); len(got) != 0 {
		t.Fatalf("held at offline C %v", got)
	}
	if got := storeObjects(t, filepath.Join(fresh.data, "store", "laptop")); len(got) != 0 {
		t.Fatalf("recover downloaded %v", got)
	}
	// NET-7a AC: no per-pair credential in any file, on any side; the
	// overflow credential and the phrase in none either.
	for _, pair := range [][2]*identity.Identity{{aID, bID}, {aID, cID}, {bID, aID}, {cID, aID}} {
		secret, err := pair[0].S3Secret(pair[1].NodeID())
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []*node{fresh, bn, cn} {
			assertNotIn(t, n, secret)
		}
	}
	assertNotIn(t, fresh, overflowSecret, "OVERFLOWACCESS")
	assertNoPhrase(t, filepath.Dir(filepath.Dir(fresh.config)), phraseA)
	for p, m := range map[string]os.FileMode{
		filepath.Dir(fresh.config):                                0o700,
		filepath.Join(filepath.Dir(fresh.config), "identity.key"): 0o600,
		filepath.Join(fresh.data, "lendisk.db"):                   0o600,
	} {
		if fi, err := os.Stat(p); err != nil || fi.Mode().Perm() != m {
			t.Errorf("%s: %v %v, want %04o", p, fi, err, m)
		}
	}

	// The daemon: C returns and is listed; B and C admit A and take its
	// next manifest version, above the one they held.
	c.start()
	fa := newProcNode(t, fresh, fast)
	fa.start()
	deadline = time.Now().Add(90 * time.Second)
	for len(fresh.status().Recovery.Pending) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("C was not listed when it returned: %+v", fresh.status().Recovery)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := ackedAt(t, fresh, cKey, "laptop"); !maps.Equal(got, want) {
		t.Fatalf("held at C %v, want %v", got, want)
	}
	if st := fresh.status(); st.Buckets[0].BelowCopies.Objects != 0 {
		t.Fatalf("below copies after both lists: %+v", st.Buckets[0])
	}
	v := fresh.delivered(newer.Seq)
	for _, n := range []*node{bn, cn} {
		if m, err := heldManifest(t, n, aID); err != nil || m.Seq != v || len(m.Friends) != 2 {
			t.Fatalf("manifest at a friend after recovery: %+v %v, want version %d", m, err, v)
		}
	}
	// B dials A with the key it always used for A, as its daemon would:
	// A admits it as B. B's daemon is stopped first, since two clients of
	// one key at one relay stall each other.
	b.p.kill()
	cl := tunnelClient(t, bID.DialKeyFor(aID.NodeID()), aAddr)
	var ce *control.CallError
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if _, err := cl.GetManifest(ctx); err != nil && !(errors.As(err, &ce) && ce.Status == http.StatusNotFound) {
		t.Fatalf("B's dial key at the recovered A: %v", err)
	}
}

// ID-5 (#29): recover is for a fresh machine, and only once the old node
// is confirmed stopped; a damaged address is a usage error.
func TestRecoverRefusals(t *testing.T) {
	n := newNode(t)
	other := newNode(t)
	other.mustInit(t)
	addr := other.status().Node.Address
	if code, _, e := n.run("", "", "recover", "tcnot-an-address"); code != 2 {
		t.Fatalf("damaged address: exit %d %s", code, e)
	}
	if code, _, e := n.run("\n", "", "recover", addr); code != 1 || !strings.Contains(e, "not confirmed") {
		t.Fatalf("unconfirmed: exit %d %s", code, e)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(n.config), "identity.key")); !os.IsNotExist(err) {
		t.Fatal("an unconfirmed recover wrote the key")
	}
	if code, _, e := n.run("y\n"+testPhrase+"\n", "", "recover", addr); code != 2 || !strings.Contains(e, "own") {
		t.Fatalf("own address: exit %d %s", code, e)
	}
	if code, _, e := other.run("y\n"+testPhrase+"\n", "", "recover", addr); code != 1 || !strings.Contains(e, "fresh machine") {
		t.Fatalf("a machine with a node: exit %d %s", code, e)
	}
}

// S3-1, SEC-3 (#29): overflow targets in the manifest are shown with
// their credential, for the user to put in the environment; with none,
// nothing is said.
func TestOverflowCredentialsShown(t *testing.T) {
	var w bytes.Buffer
	printOverflow(&w, nil)
	if w.Len() != 0 {
		t.Fatalf("no targets printed %q", w.String())
	}
	printOverflow(&w, []manifest.Overflow{{Name: "spill", Endpoint: "https://s3.example.net", Bucket: "b", Prefix: "p/",
		AccessKey: "AK", SecretKey: "SK"}})
	for _, s := range []string{"spill", "https://s3.example.net", "bucket b", "prefix p/", "access key: AK", "secret key: SK",
		"not written"} {
		if !strings.Contains(w.String(), s) {
			t.Errorf("lacks %q:\n%s", s, w.String())
		}
	}
}
