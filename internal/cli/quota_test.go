package cli

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// peerOf is n's friend id in `peer list --json`.
func (n *node) peerOf(id string) PeerJSON {
	n.t.Helper()
	for _, p := range n.peers() {
		if p.ID == id {
			return p
		}
	}
	n.t.Fatalf("peer list has no %s", id)
	return PeerJSON{}
}

// hasAlert reports whether n's status shows an alert of kind about
// subject.
func (n *node) hasAlert(kind, subject string) bool {
	n.t.Helper()
	for _, a := range n.status().Alerts {
		if a.Kind == kind && a.Subject == subject {
			return true
		}
	}
	return false
}

// LEND-1, LEND-8, PEER-6 (#24): `peer quota` sets this node's number for
// a friendship, refusing one that takes the sum of its numbers (and its
// open invites) past lending.max_total, as `invite` does. The quota is
// the smaller number at once; `peer list` shows both numbers where they
// differ, and that this node's is not told yet while no daemon runs.
func TestPeerQuotaSetsYourNumberWithinTheMaxTotal(t *testing.T) {
	parallel(t, 0)
	n := friendNode(t)
	b, c := newID(t).NodeID().String(), newID(t).NodeID().String()
	db, err := state.Open(t.Context(), n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{b, c} {
		if err := db.AddFriendship(t.Context(), state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 50 << 30,
			PeerNumber: 50 << 30, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	cap := []string{"--lending-max-total", "100G"}

	code, out, e := n.run("", "", append([]string{"peer", "quota", b, "60G"}, cap...)...)
	if code != 1 || !strings.Contains(e, "lending.max_total is 100G") || !strings.Contains(e, "at most 50G") {
		t.Fatalf("a number past the max total: %d %s%s", code, out, e)
	}
	if p := n.peerOf(b); p.OwnNumber != 50<<30 {
		t.Fatalf("a refused number was stored: %+v", p)
	}
	code, out, e = n.run("", "", append([]string{"peer", "quota", b, "40G"}, cap...)...)
	if code != 0 || !strings.Contains(out, "falls from 50G to 40G") {
		t.Fatalf("peer quota 40G: %d %s%s", code, out, e)
	}
	p := n.peerOf(b)
	if p.Quota != 40<<30 || p.OwnNumber != 40<<30 || p.PeerNumber != 50<<30 || p.NumberTold || p.Reachability != "unknown" ||
		p.LastSeen != nil || p.LastVerified != nil || p.RepositorySize != nil || p.Remaining != 40<<30 {
		t.Fatalf("peer list after peer quota: %+v", p)
	}
	_, text, _ := n.run("", "", "peer", "list")
	if !strings.Contains(text, "you 40G, they 50G (yours not told yet)") || !strings.Contains(text, "THEIR REPO") {
		t.Fatalf("peer list:\n%s", text)
	}
	// Raising alone changes nothing until the friend's number allows.
	if code, out, _ := n.run("", "", "peer", "quota", b, "70G"); code != 0 || !strings.Contains(out, "rises from 40G to 50G") {
		t.Fatalf("raising past theirs: %s", out)
	}
	if code, out, _ := n.run("", "", "peer", "quota", b, "70G"); code != 0 || !strings.Contains(out, "stays 50G each way until they raise") {
		t.Fatalf("raising past theirs again: %s", out)
	}
	// An invite's number counts toward the cap too: 70 + 50 + 10 is past
	// 125G, within 130G.
	if code, _, e := n.run("", "", "invite", "--quota", "10G", "--lending-max-total", "125G"); code != 1 || !strings.Contains(e, "lending.max_total") {
		t.Fatalf("an invite past the max total: %d %s", code, e)
	}
	if code, _, e := n.run("", "", "invite", "--quota", "10G", "--lending-max-total", "130G"); code != 0 {
		t.Fatalf("an invite within the max total: %s", e)
	}
	for _, args := range [][]string{{"not-a-node-id", "1G"}, {b, "0"}, {b, "lots"}, {b}} {
		if code, _, _ := n.run("", "", append([]string{"peer", "quota"}, args...)...); code != 2 {
			t.Errorf("peer quota %v: exit %d, want 2", args, code)
		}
	}
	if code, _, e := n.run("", "", "peer", "quota", newID(t).NodeID().String(), "1G"); code != 1 || !strings.Contains(e, "not a friend") {
		t.Fatalf("a stranger: %d %s", code, e)
	}
}

// PEER-6, SEC-15 AC for a lowering (#24): during a running transfer from
// B, A lowers its number below what B stores there (`peer quota`, from
// another process than the daemon). B's next request on its open
// connection is refused with 507 at once; the transfer is answered 507
// within 60 seconds, as the daemon's next pass applies the lowering to
// it; nothing of it is stored, nothing B stored is deleted, and A's user
// is told, by `peer quota` and by the lent_over_quota alert.
func TestLoweringAQuotaEndsARunningTransfer(t *testing.T) {
	serving(t, 0)
	a, b := friendNode(t), friendNode(t)
	a.tuneAlerts = func(s *alert.Service) { s.Interval = 500 * time.Millisecond }
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
	stored := bytes.Repeat([]byte("kept"), 1<<10)
	if _, err := bS3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(sealed), Key: aws.String("data/aa/kept"),
		Body: bytes.NewReader(stored), Metadata: map[string]string{"lendisk-sha256": sha256Hex(stored)}}); err != nil {
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

	code, out, errOut := a.run("", "", "peer", "quota", bID.NodeID().String(), "4K")
	lowered := time.Now()
	if code != 0 || !strings.Contains(out, "They store") || !strings.Contains(out, "nothing is deleted") {
		t.Fatalf("peer quota: %d\n%s%s", code, out, errOut)
	}
	small := []byte("x")
	_, err = bS3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(sealed), Key: aws.String("data/ac/next"),
		Body: bytes.NewReader(small), Metadata: map[string]string{"lendisk-sha256": sha256Hex(small)}})
	if s3Code(err) != "QuotaExceeded" {
		t.Fatalf("B's next request on its open connection after the lowering: %v", err)
	}
	var got answer
	select {
	case got = <-put:
	case <-time.After(60 * time.Second):
		t.Fatal("the running transfer was not answered within 60 seconds of the lowering")
	}
	took := time.Since(lowered)
	if got.err != nil || got.status != 507 || !strings.Contains(got.body, "QuotaExceeded") {
		t.Fatalf("the running transfer got %d %q %v", got.status, got.body, got.err)
	}
	t.Logf("the running transfer was refused %v after the lowering", took.Round(100*time.Millisecond))
	if took > 30*time.Second {
		t.Fatalf("refused only when its body ended (%v), not by the lowering", took)
	}
	if _, err := bS3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(sealed), Key: aws.String("data/ab/pack")}); err == nil {
		t.Fatal("the refused transfer was stored")
	}
	if o, err := bS3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(sealed), Key: aws.String("data/aa/kept")}); err != nil {
		t.Fatalf("what B stored before is gone: %v", err)
	} else {
		o.Body.Close()
	}
	within(t, 10*time.Second, "lent_over_quota on A", func() bool { return a.hasAlert(alert.KindLentOverQuota, bID.NodeID().String()) })
}
