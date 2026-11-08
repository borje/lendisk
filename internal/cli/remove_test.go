package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// within polls ok every 100 ms until it holds or d has passed.
func within(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, d)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// reachable reports whether a request over hc to the tunnel is answered
// at all within 5 seconds.
func reachable(t *testing.T, hc *http.Client) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://lender:9000"+control.VersionPath, nil)
	resp, err := hc.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

// SEC-15 AC (#25, from #14): during a running transfer from B on its
// dial key, A removes B (`peer remove`, from another process than the
// daemon). B's next request on its open node-ID connection is refused at
// once, the transfer gets the answer PEER-5 prescribes for a removed
// borrower (the tunnel's 403 "no relationship") within 60 seconds, as
// the daemon's next pass cuts its connections, nothing of it is stored,
// and neither of B's keys connects again.
func TestRemovingAFriendEndsItsRunningTransfer(t *testing.T) {
	a, b := friendNode(t), friendNode(t)
	stop := a.serveLogged()
	defer stop()
	befriend(t, a, b)
	ctx := t.Context()
	aAddr := a.status().Node.Address
	aKey, err := transport.KeyOf(aAddr)
	if err != nil {
		t.Fatal(err)
	}
	bID := b.identity()
	bCreds, bSecret := pairCreds(t, bID, aKey)
	sealed := bID.SealedName("laptop")
	onNode, onDial := tunnelHTTP(t, bID, aAddr), tunnelHTTP(t, bID.DialKeyFor(aKey), aAddr)
	bS3 := s3Over(onNode, bCreds)
	if _, err := bS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(sealed)}); err != nil {
		t.Fatal(err)
	}

	// The running transfer: 2 MiB, 32 KiB at a time, over half a minute.
	body := bytes.Repeat([]byte("pack"), 1<<19)
	pr, pw := io.Pipe()
	var sent sync.WaitGroup
	sent.Add(1)
	go func() {
		defer sent.Done()
		for off := 0; off < len(body); off += 32 << 10 {
			if _, err := pw.Write(body[off : off+32<<10]); err != nil {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		pw.Close()
	}()
	defer func() { pw.CloseWithError(io.ErrClosedPipe); sent.Wait() }()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://lender:9000/"+sealed+"/data/ab/pack", pr)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(body))
	req.Header.Set("X-Amz-Content-Sha256", sha256Hex(body))
	req.Header.Set("X-Amz-Meta-Lendisk-Sha256", sha256Hex(body))
	if err := v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: bID.NodeID().String(), SecretAccessKey: bSecret},
		req, sha256Hex(body), "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		status int
		body   string
		err    error
	}
	put := make(chan answer, 1)
	go func() {
		resp, err := onDial.Do(req)
		if err != nil {
			put <- answer{err: err}
			return
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		put <- answer{status: resp.StatusCode, body: string(got)}
	}()
	time.Sleep(2 * time.Second) // well into the transfer

	code, out, errOut := a.run("", "", "peer", "remove", bID.NodeID().String())
	removed := time.Now()
	if code != 0 || !strings.Contains(out, "Ended the friendship with "+bID.NodeID().String()) {
		t.Fatalf("peer remove: %d\n%s%s", code, out, errOut)
	}
	// None is served under the old relationship: refused, or its
	// connection already cut.
	if _, err := bS3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(sealed)}); err == nil {
		t.Fatal("B's request on its open connection was served after its removal")
	}
	var got answer
	select {
	case got = <-put:
	case <-time.After(60 * time.Second):
		t.Fatal("B's transfer was not ended within 60 seconds of its removal")
	}
	t.Logf("B's transfer ended %v after `peer remove`: %d %v", time.Since(removed).Round(time.Millisecond), got.status, got.err)
	if got.err != nil || got.status != http.StatusForbidden || !strings.Contains(got.body, state.NoRelationship) {
		t.Fatalf("B's transfer got %d %q %v, want the removed answer", got.status, got.body, got.err)
	}
	if objs := lentObjects(t, a, bID, "laptop"); len(objs) != 0 {
		t.Fatalf("A stored B's transfer: %v", objs)
	}
	// Disconnect follows the cut: from then on neither key connects.
	for name, hc := range map[string]*http.Client{"node ID": onNode, "dial key": onDial} {
		within(t, 20*time.Second, "B's "+name+" disconnected", func() bool { return !reachable(t, hc) })
		if reachable(t, hc) {
			t.Errorf("B's %s connects again after its removal", name)
		}
	}
	if ps := a.peers(); len(ps) != 0 {
		t.Fatalf("A's friends after removing B: %+v", ps)
	}
	if fs := a.status().FormerFriends; len(fs) != 1 || fs[0].Peer != bID.NodeID().String() || fs[0].ByPeer {
		t.Fatalf("A's former friends: %+v", fs)
	}
	// Removing B again, or a stranger, says it is not a friend.
	if code, _, e := a.run("", "", "peer", "remove", bID.NodeID().String()); code != 1 || !strings.Contains(e, "not a friend") {
		t.Fatalf("removing B again: %d %s", code, e)
	}
	if code, _, e := a.run("", "", "peer", "remove", "not-a-node-id"); code != 2 {
		t.Fatalf("removing a malformed ID: %d %s", code, e)
	}
}

// PEER-5, PEER-12 and scenario 8 (#25): A is friends with B and C and
// removes B. B's daemon is told (the remove call), ends the friendship
// on its side too, recorded as ended by A, and is alerted; A records B
// as told. A–C is untouched: both still list each other, and C's open
// connection at A is still served.
func TestEndingAFriendshipTellsTheFriendAndLeavesTheOthers(t *testing.T) {
	a, b, c := friendNode(t), friendNode(t), friendNode(t)
	a.tuneEnding = func(f *farewell) { f.Interval = 200 * time.Millisecond }
	stop := a.serveLogged()
	defer stop()
	pb := newProcNode(t, b, envAlertInterval+"=200ms")
	pb.start()
	befriend(t, a, b)
	befriend(t, a, c)
	ctx := t.Context()
	aAddr := a.status().Node.Address
	aKey, err := transport.KeyOf(aAddr)
	if err != nil {
		t.Fatal(err)
	}
	aID, bID, cID := a.identity().NodeID().String(), b.identity().NodeID().String(), c.identity()
	cSealed := cID.SealedName("laptop")
	cS3 := borrowOver(t, cID.DialKeyFor(aKey), cID, aKey, aAddr)
	if _, err := cS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cSealed)}); err != nil {
		t.Fatal(err)
	}

	if ps := b.peers(); len(ps) != 1 || ps[0].ID != aID {
		t.Fatalf("B's friends before: %+v", ps)
	}
	if code, out, e := a.run("", "", "peer", "remove", bID); code != 0 {
		t.Fatalf("peer remove: %d %s%s", code, out, e)
	}
	removed := time.Now()
	within(t, 60*time.Second, "B ending the friendship on its side", func() bool {
		fs := b.status().FormerFriends
		return len(b.peers()) == 0 && len(fs) == 1 && fs[0].Peer == aID && fs[0].ByPeer && fs[0].NotifiedAt != ""
	})
	t.Logf("B ended it on its side %v after A's `peer remove`", time.Since(removed).Round(time.Millisecond))
	within(t, 30*time.Second, "B alerted", func() bool {
		return slices.ContainsFunc(b.status().Alerts, func(al StatusAlert) bool {
			return al.Kind == alert.KindFriendRemoved && al.Subject == aID && strings.Contains(al.Message, aID+" ended the friendship")
		})
	})
	within(t, 30*time.Second, "A recording B as told", func() bool {
		fs := a.status().FormerFriends
		return len(fs) == 1 && fs[0].Peer == bID && !fs[0].ByPeer && fs[0].NotifiedAt != ""
	})
	if ps := a.peers(); len(ps) != 1 || ps[0].ID != cID.NodeID().String() {
		t.Fatalf("A's friends: %+v", ps)
	}
	if ps := c.peers(); len(ps) != 1 || ps[0].ID != aID {
		t.Fatalf("C's friends: %+v", ps)
	}
	if _, err := cS3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(cSealed)}); err != nil {
		t.Fatalf("C at A after B's removal: %v", err)
	}
}

// PEER-5 (#25, #28): each side holds the other's replica for the
// retention window and then purges it. A removes B, which stored
// objects, a tomb and its manifest at A, and which A had placed an
// object at. All is kept, and refused to B, until the window ends;
// seven days before, replica_purge_soon is raised; at the end, B's lent
// bucket, its manifest file and every row of B are gone.
func TestAFormerFriendsReplicaIsKeptForTheWindowThenPurged(t *testing.T) {
	a, b := friendNode(t), friendNode(t)
	var mu sync.Mutex
	var ahead time.Duration
	a.clock = func() time.Time { mu.Lock(); defer mu.Unlock(); return time.Now().Add(ahead) }
	advance := func(d time.Duration) { mu.Lock(); ahead += d; mu.Unlock() }
	a.tuneEnding = func(f *farewell) { f.Interval = 100 * time.Millisecond }
	a.tuneAlerts = func(s *alert.Service) { s.Interval = 100 * time.Millisecond }
	if code, _, e := a.run("", "", "bucket", "add", "photos", "--profile", "restic"); code != 0 {
		t.Fatal(e)
	}
	stop := a.serveLogged()
	defer stop()
	befriend(t, a, b)
	ctx := t.Context()
	aAddr := a.status().Node.Address
	aKey, err := transport.KeyOf(aAddr)
	if err != nil {
		t.Fatal(err)
	}
	bID := b.identity()
	sealed := bID.SealedName("laptop")
	bS3 := borrowOver(t, bID, bID, aKey, aAddr)
	if _, err := bS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(sealed)}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"data/ab/one", "data/cd/two"} {
		obj := []byte("B's " + k)
		if _, err := bS3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(sealed), Key: aws.String(k),
			Body: bytes.NewReader(obj), Metadata: hashMeta(obj)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := bS3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(sealed), Key: aws.String("data/cd/two")}); err != nil {
		t.Fatal(err)
	}
	blob, err := manifest.Seal(bID, &manifest.Manifest{Seq: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctl := tunnelClient(t, bID.DialKeyFor(aKey), aAddr)
	if _, err := ctl.PutManifest(ctx, blob); err != nil {
		t.Fatal(err)
	}
	// A's own object, placed at B, its only friend.
	aDB := openDB(t, a)
	seqs, err := aDB.BeginCaptures(ctx, []state.Capture{{Bucket: "photos", Key: "data/ef/mine", Op: state.OpPut, Class: "data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aDB.FinishPut(ctx, seqs[0], 10, strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	countAt := func(table, col string) int {
		var n int
		if err := aDB.QueryRow(`SELECT count(*) FROM `+table+` WHERE `+col+` = ?`, bID.NodeID().String()).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if countAt("placement", "place") == 0 {
		t.Fatal("A's object was not placed at B")
	}
	dir := lentDir(a, bID, "laptop")
	mfile := filepath.Join(manifest.Dir(filepath.Join(a.data, "store")), bID.NodeID().String())
	held := func(when string) {
		t.Helper()
		if objs := lentObjects(t, a, bID, "laptop"); len(objs) != 1 {
			t.Fatalf("%s: B's replica at A holds %v", when, objs)
		}
		if _, err := os.Stat(mfile); err != nil {
			t.Fatalf("%s: B's manifest at A: %v", when, err)
		}
		if n, _, _ := aDB.TombstoneUsage(ctx, bID.NodeID().String()); n != 1 {
			t.Fatalf("%s: B's tombs at A: %d", when, n)
		}
	}
	held("before removal")

	code, out, e := a.run("", "", "peer", "remove", bID.NodeID().String())
	if code != 0 || !strings.Contains(out, "kept") {
		t.Fatalf("peer remove: %d %s%s", code, out, e)
	}
	// Refused to B meanwhile: its manifest too (ID-4: served only to a
	// key still admitted).
	within(t, 30*time.Second, "B's keys cut", func() bool { return !reachable(t, ctl.HTTP) })
	advance(23 * 24 * time.Hour)
	within(t, 30*time.Second, "replica_purge_soon raised 7 days before the purge", func() bool {
		return slices.ContainsFunc(a.status().Alerts, func(al StatusAlert) bool {
			return al.Kind == alert.KindPurgeSoon && al.Subject == bID.NodeID().String()
		})
	})
	held("seven days before the purge")
	advance(7*24*time.Hour + time.Minute)
	within(t, 30*time.Second, "B's replica purged", func() bool { return len(a.status().FormerFriends) == 0 })
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("B's lent bucket after the purge: %v", err)
	}
	if _, err := os.Stat(mfile); !os.IsNotExist(err) {
		t.Fatalf("B's manifest after the purge: %v", err)
	}
	for _, tc := range [][2]string{{"placement", "place"}, {"lent_bucket", "peer_key"}, {"tombstone", "peer_key"},
		{"lent_usage", "peer_key"}, {"manifest_push", "peer_key"}} {
		if n := countAt(tc[0], tc[1]); n != 0 {
			t.Errorf("%s rows of B after the purge: %d", tc[0], n)
		}
	}
	within(t, 30*time.Second, "the purge alert cleared", func() bool {
		return !slices.ContainsFunc(a.status().Alerts, func(al StatusAlert) bool { return al.Kind == alert.KindPurgeSoon })
	})
}
