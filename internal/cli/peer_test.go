package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/invite"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
)

// friendNode is an initialised node with its own phrase.
func friendNode(t *testing.T) *node {
	t.Helper()
	n := newNode(t)
	p, err := identity.NewPhrase()
	if err != nil {
		t.Fatal(err)
	}
	if code, _, e := n.run(p+"\n", "", "init", "--restore"); code != 0 {
		t.Fatal(e)
	}
	return n
}

func (n *node) invite(args ...string) string {
	n.t.Helper()
	code, out, errOut := n.run("", "", append([]string{"invite"}, args...)...)
	if code != 0 {
		n.t.Fatalf("invite: %d %s", code, errOut)
	}
	return strings.TrimSpace(out)
}

func (n *node) peers() []PeerJSON {
	n.t.Helper()
	code, out, errOut := n.run("", "", "peer", "list", "--json")
	if code != 0 {
		n.t.Fatalf("peer list: %s", errOut)
	}
	var ps []PeerJSON
	if err := json.Unmarshal([]byte(out), &ps); err != nil {
		n.t.Fatalf("peer list --json: %v\n%s", err, out)
	}
	return ps
}

// tunnelClient calls the control API at addr over the tunnel, dialling
// with key.
func tunnelClient(t *testing.T, key *identity.Identity, addr string) *control.Client {
	t.Helper()
	dl, err := transport.NewDialer(key, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dl.Close() })
	them, err := transport.KeyOf(addr)
	if err != nil {
		t.Fatal(err)
	}
	return &control.Client{HTTP: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dl.Dial(ctx, them, addr) },
	}}}
}

// PEER-1, PEER-2, PEER-3, LEND-8: A invites, B redeems with its own
// number, and both hold a friendship whose quota is the smaller. The
// invite carries A's address, number and retention; a replay is refused
// (PEER-2 AC) and changes nothing.
func TestInviteAndRedeemMakeFriends(t *testing.T) {
	a, b, c := friendNode(t), friendNode(t), friendNode(t)
	stop := a.serveLogged()
	defer stop()
	aID, bID := a.status().Node.ID, b.status().Node.ID

	s := a.invite("--quota", "80G", "--retention", "45d")
	inv, err := invite.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	if inv.Address != a.status().Node.Address || inv.Number != 80<<30 || inv.Retention != 45*24*time.Hour {
		t.Fatalf("invite carries %+v", inv)
	}
	// SEC-3: the secret itself is never stored.
	secret := strings.Split(s, ".")[4]
	assertNotIn(t, a, secret, string(inv.Secret[:]))

	code, out, errOut := b.run("100G\n", "", "peer", "add", s)
	if code != 0 {
		t.Fatalf("peer add: %d %s", code, errOut)
	}
	if !strings.Contains(out, "They lend you 80G and keep what you delete for 45d") ||
		!strings.Contains(out, "Quota: 80G each way") {
		t.Fatalf("peer add said:\n%s", out)
	}
	ap, bp := a.peers(), b.peers()
	if len(ap) != 1 || ap[0].ID != bID || ap[0].Quota != 80<<30 || ap[0].OwnNumber != 80<<30 || ap[0].PeerNumber != 100<<30 ||
		ap[0].OwnRetentionSeconds != 45*86400 || ap[0].PeerRetentionSeconds != 30*86400 || ap[0].Address != b.status().Node.Address {
		t.Fatalf("A's peers %+v", ap)
	}
	if len(bp) != 1 || bp[0].ID != aID || bp[0].Quota != 80<<30 || bp[0].OwnNumber != 100<<30 || bp[0].PeerNumber != 80<<30 ||
		bp[0].Address != inv.Address {
		t.Fatalf("B's peers %+v", bp)
	}
	if _, text, _ := a.run("", "", "peer", "list"); !strings.Contains(text, bID) || !strings.Contains(text, "80G") {
		t.Fatalf("peer list text:\n%s", text)
	}

	// Replay, by another node and by B.
	c.dialTimeout = 5 * time.Second
	if code, _, errOut := c.run("\n", "", "peer", "add", s); code == 0 || !strings.Contains(errOut, "already used or has expired") {
		t.Fatalf("replayed invite: %d %s", code, errOut)
	}
	if code, _, errOut := b.run("\n", "", "peer", "add", s); code == 0 || !strings.Contains(errOut, "already friends") {
		t.Fatalf("B redeemed twice: %d %s", code, errOut)
	}
	if len(a.peers()) != 1 || len(c.peers()) != 0 {
		t.Fatal("a replay made a friendship")
	}
}

// lostAnswer delivers the request, then drops the answer.
type lostAnswer struct{ next http.RoundTripper }

func (l lostAnswer) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := l.next.RoundTrip(r)
	if err == nil {
		resp.Body.Close()
	}
	return nil, errors.New("connection reset by peer")
}

// assertHealed checks that A and B hold one friendship with each other,
// with B's number as B first gave it.
func assertHealed(t *testing.T, a, b *node, bNumber int64) {
	t.Helper()
	ap, bp := a.peers(), b.peers()
	if len(ap) != 1 || ap[0].ID != b.status().Node.ID || ap[0].PeerNumber != bNumber || ap[0].OwnNumber != 80<<30 {
		t.Fatalf("A's peers %+v", ap)
	}
	if len(bp) != 1 || bp[0].ID != a.status().Node.ID || bp[0].OwnNumber != bNumber || bp[0].PeerNumber != 80<<30 {
		t.Fatalf("B's peers %+v", bp)
	}
}

// PEER-2, #51: A commits the friendship but B never sees the answer.
// Running the same peer add again repeats B's terms without asking and
// both sides end with the friendship. Another node is still refused.
func TestLostRedeemAnswerHealsOnRerun(t *testing.T) {
	a, b, c := friendNode(t), friendNode(t), friendNode(t)
	stop := a.serveLogged()
	defer stop()
	s := a.invite("--quota", "80G")

	b.redeemTransport = func(rt http.RoundTripper) http.RoundTripper { return lostAnswer{rt} }
	code, _, errOut := b.run("100G\n", "", "peer", "add", s)
	if code == 0 || !strings.Contains(errOut, "run the same `lendisk peer add` again") {
		t.Fatalf("lost answer: %d %s", code, errOut)
	}
	if len(a.peers()) != 1 || len(b.peers()) != 0 {
		t.Fatal("the dropped answer did not leave a one-sided friendship")
	}

	b.redeemTransport = nil
	code, out, errOut := b.run("", "", "peer", "add", s) // no number typed: not asked
	if code != 0 {
		t.Fatalf("re-run: %d %s", code, errOut)
	}
	if !strings.Contains(out, "Resuming") || strings.Contains(out, "What you lend them") {
		t.Fatalf("re-run said:\n%s", out)
	}
	assertHealed(t, a, b, 100<<30)

	c.dialTimeout = 10 * time.Second
	if code, _, errOut := c.run("\n", "", "peer", "add", s); code == 0 || !strings.Contains(errOut, "already used or has expired") {
		t.Fatalf("replay by another node: %d %s", code, errOut)
	}
	if len(a.peers()) != 1 || len(c.peers()) != 0 {
		t.Fatal("a replay made a friendship")
	}
}

// PEER-2, #51: B crashes after A's answer and before writing its row;
// running peer add again heals the same way.
func TestCrashBeforeFriendshipWriteHealsOnRerun(t *testing.T) {
	a, b := friendNode(t), friendNode(t)
	stop := a.serveLogged()
	defer stop()
	s := a.invite("--quota", "80G")

	b.beforeFriendship = func() error { return errors.New("killed") }
	if code, _, errOut := b.run("50G\n", "", "peer", "add", s); code == 0 {
		t.Fatalf("crash: %d %s", code, errOut)
	}
	if len(a.peers()) != 1 || len(b.peers()) != 0 {
		t.Fatal("the crash did not leave a one-sided friendship")
	}
	b.beforeFriendship = nil
	if code, _, errOut := b.run("", "", "peer", "add", s); code != 0 {
		t.Fatalf("re-run: %d %s", code, errOut)
	}
	assertHealed(t, a, b, 50<<30)
	if code, _, errOut := b.run("", "", "peer", "add", s); code == 0 || !strings.Contains(errOut, "already friends") {
		t.Fatalf("third run: %d %s", code, errOut)
	}
}

// Enter keeps the inviter's number; a bad invite is a usage error.
func TestPeerAddDefaultsAndRefusals(t *testing.T) {
	a, b := friendNode(t), friendNode(t)
	stop := a.serveLogged()
	defer stop()
	s := a.invite("--quota", "50G")
	if code, _, errOut := a.run("\n", "", "peer", "add", s); code == 0 || !strings.Contains(errOut, "your own") {
		t.Fatalf("own invite: %d %s", code, errOut)
	}
	if code, _, _ := b.run("\n", "", "peer", "add", s[:len(s)-3]); code != 2 {
		t.Fatalf("damaged invite: exit %d", code)
	}
	if code, _, _ := a.run("", "", "invite"); code != 2 {
		t.Fatal("invite without --quota")
	}
	if code, out, errOut := b.run("\n", "", "peer", "add", s); code != 0 || !strings.Contains(out, "Quota: 50G") {
		t.Fatalf("peer add: %d %s %s", code, out, errOut)
	}
}

// SEC-6: an expired invite is refused, and its row is purged.
func TestExpiredInviteRefusedAndPurged(t *testing.T) {
	a, b := friendNode(t), friendNode(t)
	stop := a.serveLogged()
	defer stop()
	a.clock = func() time.Time { return time.Now().Add(-8 * 24 * time.Hour) }
	s := a.invite("--quota", "10G") // valid 7 days from 8 days ago
	a.clock = nil
	inv, _ := invite.Parse(s)
	key, _ := inv.Key()

	b.dialTimeout = 5 * time.Second
	if code, _, errOut := b.run("\n", "", "peer", "add", s); code == 0 || !strings.Contains(errOut, "expired") {
		t.Fatalf("expired invite: %d %s", code, errOut)
	}
	if len(b.peers()) != 0 || len(a.peers()) != 0 {
		t.Fatal("an expired invite made a friendship")
	}
	a.invite("--quota", "10G") // purges first
	db, err := state.Open(t.Context(), a.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	db.QueryRow(`SELECT count(*) FROM invite WHERE invite_key = ?`, key.NodeID().String()).Scan(&n)
	if n != 0 {
		t.Fatal("the expired invite is still stored")
	}
}

// SEC-13 AC over the tunnel: a connection holding an unredeemed invite
// is refused every call but redeem, before routing.
func TestInviteConnectionMayOnlyRedeem(t *testing.T) {
	a := friendNode(t)
	stop := a.serveLogged()
	defer stop()
	inv, _ := invite.Parse(a.invite("--quota", "10G"))
	key, _ := inv.Key()
	c := tunnelClient(t, key, inv.Address)
	for _, r := range [][2]string{
		{"GET", control.VersionPath},
		{"POST", control.Prefix + "challenge"},
		{"GET", control.Prefix + "manifest"},
		{"POST", control.Prefix + "quota"},
		{"POST", control.Prefix + "undelete"},
		{"GET", control.Prefix + "list"},
		{"GET", control.RedeemPath},
		{"GET", "/"},
		{"PUT", "/sbucket"},
		{"PUT", "/sbucket/key"},
		{"GET", "/sbucket/key"},
	} {
		resp, err := c.Do(t.Context(), r[0], r[1], strings.NewReader("{}"))
		if err != nil {
			t.Fatalf("%s %s: %v", r[0], r[1], err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s with an invite key: %d, want 403", r[0], r[1], resp.StatusCode)
		}
	}
}

// PEER-10 AC: with friendships A–B and B–D, A's node has no record of D
// and D's none of A: not in the persisted state, logs, status or peer
// list. Nothing on the wire names them either: A and D never connect,
// and B's control API answers carry only B's own terms.
func TestFriendsOfFriendsUnknown(t *testing.T) {
	a, b, d := friendNode(t), friendNode(t), friendNode(t)
	// One gateway per process, so the daemons take turns.
	stopA := a.serveLogged()
	if code, _, e := b.run("\n", "", "peer", "add", a.invite("--quota", "10G")); code != 0 {
		t.Fatal(e)
	}
	logA := stopA()
	stopB := b.serveLogged()
	if code, _, e := d.run("\n", "", "peer", "add", b.invite("--quota", "20G")); code != 0 {
		t.Fatal(e)
	}
	stopB()
	if len(b.peers()) != 2 {
		t.Fatalf("B has %d friends", len(b.peers()))
	}
	// The search below finds what it looks for: B holds both.
	if len(filesHolding(b, a.status().Node.ID)) == 0 || len(filesHolding(b, d.status().Node.ID)) == 0 {
		t.Fatal("the file search does not find B's friends in B's files")
	}
	logA += a.serveLogged()()
	for _, pair := range [][2]*node{{a, d}, {d, a}} {
		self, other := pair[0], pair[1]
		st := other.status()
		pub, _ := identity.ParseNodeID(st.Node.ID)
		needles := []string{st.Node.ID, st.Node.ID[:16], st.Node.Address, string(pub[:])}
		assertNotIn(t, self, needles...)
		_, list, _ := self.run("", "", "peer", "list")
		_, js, _ := self.run("", "", "peer", "list", "--json")
		_, stat, _ := self.run("", "", "status", "--json")
		for _, out := range []string{list, js, stat} {
			for _, s := range needles {
				if strings.Contains(out, s) {
					t.Errorf("%s's output names the other node:\n%s", self.data, out)
				}
			}
		}
	}
	if strings.Contains(logA, d.status().Node.ID) {
		t.Error("A's log names D")
	}
}

// assertNotIn fails if any needle appears in any file of n's
// directories (config, identity, lendisk.db and its WAL, the store).
func assertNotIn(t *testing.T, n *node, needles ...string) {
	t.Helper()
	for _, s := range needles {
		for _, p := range filesHolding(n, s) {
			t.Errorf("%s holds %q", p, s)
		}
	}
}

// filesHolding lists the files under n's directories whose contents
// hold needle, and the files and directories whose names do.
func filesHolding(n *node, needle string) []string {
	var found []string
	root := filepath.Dir(filepath.Dir(n.config))
	filepath.WalkDir(root, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p != root && strings.Contains(de.Name(), needle) {
			found = append(found, p)
			return nil
		}
		if de.IsDir() {
			return nil
		}
		if b, _ := os.ReadFile(p); bytes.Contains(b, []byte(needle)) {
			found = append(found, p)
		}
		return nil
	})
	return found
}
