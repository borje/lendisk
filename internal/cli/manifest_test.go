package cli

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// waitManifest polls n's status until ok holds for its manifest section.
func (n *node) waitManifest(what string, within time.Duration, ok func(StatusManifest) bool) StatusManifest {
	n.t.Helper()
	deadline := time.Now().Add(within)
	for {
		m := n.status().Manifest
		if ok(m) {
			return m
		}
		if time.Now().After(deadline) {
			n.t.Fatalf("manifest %s: not after %v: %+v", what, within, m)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// delivered waits until every friend of n holds a manifest version after
// than, and returns that version.
func (n *node) delivered(after int64) int64 {
	n.t.Helper()
	return n.waitManifest("delivered", 60*time.Second, func(m StatusManifest) bool {
		return m.Version > after && len(m.Pending) == 0 && m.Friends > 0
	}).Version
}

// ID-4 (#28 review): a friend whose last push failed, the daily one
// included, is not shown as holding the manifest: it may have lost it.
func TestStatusCountsAFailedRepushAsNotHeld(t *testing.T) {
	n := newNode(t)
	n.mustInit(t)
	db := openDB(t, n)
	ctx := t.Context()
	now := time.Now()
	if err := db.AddFriendship(ctx, state.Friendship{Peer: "bob", Address: "tcbob", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	v, err := db.NoteManifest(ctx, "h", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ManifestDelivered(ctx, "bob", v.Seq, now); err != nil {
		t.Fatal(err)
	}
	if m := n.status().Manifest; len(m.Pending) != 0 {
		t.Fatalf("delivered, yet pending: %+v", m)
	}
	if err := db.ManifestPushFailed(ctx, "bob", "offline", now.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	m := n.status().Manifest
	if len(m.Pending) != 1 || m.Pending[0].Peer != "bob" || m.Pending[0].Held != v.Seq || m.Pending[0].Error != "offline" {
		t.Fatalf("a failed re-push shows as held: %+v", m)
	}
	if _, out, _ := n.run("", "", "status"); !strings.Contains(out, "not yet at 1 of 1 friends") {
		t.Fatalf("status text:\n%s", out)
	}
}

// ID-4 (#28 review): a redeem makes the inviting node's daemon push its
// manifest at once (the control API's Changed kicks the pusher), not at
// its next poll: with an hour between rounds, it tries the new friend
// within seconds.
func TestRedeemPushesManifestAtOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	an, bn := newNode(t), newNode(t)
	bn.regions = an.regions
	for _, n := range []*node{an, bn} {
		p, _ := identity.NewPhrase()
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	slow := envManifestPoll + "=1h"
	a, b := newProcNode(t, an, slow), newProcNode(t, bn, slow)
	b.start()
	a.start()
	bn.waitManifest("built", 30*time.Second, func(m StatusManifest) bool { return m.Version > 0 })
	befriend(t, bn, an) // A redeems B's invite at B's daemon
	aKey := an.identity().NodeID().String()
	db := openDB(t, bn)
	deadline := time.Now().Add(30 * time.Second)
	for {
		ps, err := db.ManifestPushes(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := ps[aKey]; ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("B did not push to its new friend after the redeem")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ID-4 end to end (#28), A borrowing from B through real daemons:
//
//   - A's manifest reaches B at the first round after the friendship,
//     again on every change, and on B's return after B missed one;
//     B's own manifest reaches A though B has no bucket.
//   - B keeps it in its store, 0600, sealed: no bucket name shows.
//   - No S3 account reaches it: not A as B's borrower, not A's own
//     bucket key at A's endpoint.
//   - B loses lendisk.db and restarts: the manifest file is unchanged.
//     B knows A again only once they are friends again (SEC-10), here
//     through a new invite that A answers with the stored terms (#51).
//   - B lost its store too: being befriended again makes the running A
//     push the same version again.
//   - With A's machine gone, a fresh machine with nothing but A's phrase
//     fetches it from B and opens it: every bucket with its sealed name
//     and copies, B with its address, A's own address.
func TestManifestOnLenderSurvivesItsDatabaseLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("long")
	}
	phraseA, _ := identity.NewPhrase()
	an := newNode(t)
	bn := newNode(t)
	bn.regions = an.regions // one relay for both
	for n, p := range map[*node]string{an: phraseA, bn: ""} {
		if p == "" {
			p, _ = identity.NewPhrase()
		}
		if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
			t.Fatal(e)
		}
	}
	own := an.addBucket(t, "laptop", "restic")
	fast := envReplFast + "=1"
	a, b := newProcNode(t, an, fast), newProcNode(t, bn, fast)
	b.start()
	a.start()
	befriend(t, bn, an) // B lends to A, and A to B
	aID, bID := an.identity(), bn.identity()
	aKey, bKey := aID.NodeID().String(), bID.NodeID().String()
	bAddr := bn.status().Node.Address

	v := an.delivered(0)
	file := filepath.Join(manifest.Dir(filepath.Join(bn.data, "store")), aKey)
	held, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("B does not hold A's manifest: %v", err)
	}
	if fi, _ := os.Stat(file); fi.Mode().Perm() != 0o600 {
		t.Fatalf("manifest file mode %04o", fi.Mode().Perm())
	}
	for _, s := range []string{"laptop", aID.SealedName("laptop")} {
		if bytes.Contains(held, []byte(s)) {
			t.Fatalf("the manifest at B shows %q", s)
		}
	}
	// B, with no bucket at all, has its manifest at A too.
	bn.delivered(0)
	bm, err := os.ReadFile(filepath.Join(manifest.Dir(filepath.Join(an.data, "store")), bKey))
	if err != nil {
		t.Fatal(err)
	}
	if m, err := manifest.Open(bID, bm); err != nil || len(m.Buckets) != 0 || len(m.Friends) != 1 || m.Friends[0].ID != aKey {
		t.Fatalf("B's manifest at A: %+v %v", m, err)
	}

	// A's own bucket key cannot name or list the manifests (PEER-8).
	ownS3 := s3client(a.addr, own)
	if out, err := ownS3.ListBuckets(t.Context(), &s3.ListBucketsInput{}); err != nil {
		t.Fatal(err)
	} else {
		for _, bk := range out.Buckets {
			if aws.ToString(bk.Name) != "laptop" {
				t.Fatalf("A's own key lists %q", aws.ToString(bk.Name))
			}
		}
	}
	for _, bucket := range []string{".lendisk", "lendisk-manifests"} {
		if _, err := ownS3.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket),
			Key: aws.String("manifests/" + bKey)}); err == nil {
			t.Fatalf("A's own key read %s", bucket)
		}
	}

	// Every change is pushed.
	an.addBucket(t, "desktop", "restic")
	v = an.delivered(v)

	// A push B missed is made when B returns.
	b.p.kill()
	an.addBucket(t, "phone", "restic")
	missed := an.waitManifest("missed by B", 60*time.Second, func(m StatusManifest) bool {
		return m.Version > v && len(m.Pending) == 1 && m.Pending[0].Peer == bKey && m.Pending[0].Error != ""
	})
	b.start()
	v = an.delivered(v)
	if v != missed.Version {
		t.Fatalf("delivered version %d, want %d", v, missed.Version)
	}
	held, _ = os.ReadFile(file)

	// B loses lendisk.db.
	b.p.kill()
	for _, f := range []string{"lendisk.db", "lendisk.db-wal", "lendisk.db-shm"} {
		os.Remove(filepath.Join(bn.data, f))
	}
	// The saved region went with it. The daemon would pick the nearest
	// public relay again; this test's relay is local, so B picks it here,
	// as init did, and keeps its address.
	if _, _, err := transport.EnsureRegion(t.Context(), openDB(t, bn), "", bn.regions); err != nil {
		t.Fatal(err)
	}
	b.start()
	if got := bn.status().Node.Address; got != bAddr {
		t.Fatalf("B's address changed with its database: %s", got)
	}
	if got, err := os.ReadFile(file); err != nil || !bytes.Equal(got, held) {
		t.Fatalf("B's restart lost or changed A's manifest: %v", err)
	}
	if len(bn.peers()) != 0 {
		t.Fatal("B still has friends after losing its database")
	}
	// B lost its store too: A must push again, though its version did
	// not change, once they are friends again (#28 review).
	if err := os.RemoveAll(manifest.Dir(filepath.Join(bn.data, "store"))); err != nil {
		t.Fatal(err)
	}
	befriend(t, an, bn) // A answers with the friendship it kept (#51)
	if ps := bn.peers(); len(ps) != 1 || ps[0].ID != aKey {
		t.Fatalf("B's friends after the new invite: %+v", ps)
	}
	// The running A pushes again: its version did not change and B
	// acknowledged it moments ago, but the new redeem dropped that.
	deadline := time.Now().Add(60 * time.Second)
	for {
		if held, err = os.ReadFile(file); err == nil {
			break
		}
		if time.Now().After(deadline) {
			ps, _ := openDB(t, an).ManifestPushes(t.Context())
			t.Fatalf("A did not push its manifest again to B, which lost it: %+v", ps[bKey])
		}
		time.Sleep(200 * time.Millisecond)
	}
	if m, err := manifest.Open(aID, held); err != nil || m.Seq != v {
		t.Fatalf("pushed again: %+v %v, want version %d", m, err, v)
	}

	// A's machine is gone; a fresh one has nothing but the phrase.
	a.p.kill()
	fresh := newNode(t)
	fresh.regions = an.regions
	if code, _, e := fresh.run(phraseA+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	if len(fresh.status().Buckets) != 0 {
		t.Fatal("the fresh machine has buckets")
	}
	id := fresh.identity()
	// Recovery dials each lender with this node's dial key for it, as the
	// daemon will (ID-5): B, which lost its database, learnt it again at
	// the new redeem.
	dl, err := transport.NewFriendDialer(id, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer dl.Close()

	// As B's borrower, A's key cannot name or list the manifests either.
	creds, _ := pairCreds(t, id, bID.NodeID())
	them, _ := transport.KeyOf(bAddr)
	hc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dl.Dial(ctx, them, bAddr) },
	}}
	var m *manifest.Manifest
	deadline = time.Now().Add(90 * time.Second) // B's admission may still remember refusing A (DeniedFor)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		m, err = manifest.Fetch(ctx, id, bAddr, dl.Dial)
		cancel()
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		t.Fatalf("fetch with the phrase alone: %v", err)
	}
	borrower := s3Over(hc, creds)
	if out, err := borrower.ListBuckets(t.Context(), &s3.ListBucketsInput{}); err != nil || len(out.Buckets) != 0 {
		t.Fatalf("A lists at B: %v %v", out, err)
	}
	for _, bucket := range []string{".lendisk", "lendisk-manifests", "lent-" + aID.NodeID().Short()} {
		if _, err := borrower.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket),
			Key: aws.String("manifests/" + aKey)}); err == nil {
			t.Fatalf("A as borrower read %s", bucket)
		}
		if _, err := borrower.ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)}); err == nil {
			t.Fatalf("A as borrower listed %s", bucket)
		}
	}

	if m.Seq != v || m.Node != aKey || m.Settings.Address != an.status().Node.Address {
		t.Fatalf("fetched manifest %+v, want version %d", m, v)
	}
	var names []string
	for _, bk := range m.Buckets {
		names = append(names, bk.Name)
		if bk.Sealed != id.SealedName(bk.Name) || bk.Copies < 1 || bk.Profile != "restic" {
			t.Errorf("bucket %+v", bk)
		}
	}
	if !slices.Equal(names, []string{"desktop", "laptop", "phone"}) {
		t.Fatalf("buckets %v", names)
	}
	if len(m.Friends) != 1 || m.Friends[0].ID != bKey || !strings.HasPrefix(m.Friends[0].Address, "tc") {
		t.Fatalf("friends %+v", m.Friends)
	}
	if got, _ := os.ReadFile(file); !bytes.Equal(got, held) {
		t.Fatal("the manifest fetched is not the one B holds")
	}
}
