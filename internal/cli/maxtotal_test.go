package cli

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// befriendDirect adds friends of n at number each way straight into its
// lendisk.db, and returns their node IDs.
func befriendDirect(t *testing.T, n *node, number int64, count int) []string {
	t.Helper()
	db, err := state.Open(t.Context(), n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ids []string
	for range count {
		f := newID(t).NodeID().String()
		if err := db.AddFriendship(t.Context(), state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: number,
			PeerNumber: number, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, f)
	}
	return ids
}

// LEND-1 (#24 review): unset, lending.max_total is what the store's
// filesystem can hold, its size less the free-space floor (5% by
// default); a set value overrides it. `peer list` and `status` show the
// effective cap.
func TestTheDefaultLendingCapIsWhatTheStoreHolds(t *testing.T) {
	parallel(t, 0)
	n := friendNode(t)
	n.lendSpace = 100 << 30 // 95G after the floor
	b := befriendDirect(t, n, 40<<30, 2)[0]
	code, _, e := n.run("", "", "peer", "quota", b, "56G")
	if code != 1 || !strings.Contains(e, "at most 95G") || !strings.Contains(e, "free-space floor") || !strings.Contains(e, "at most 55G more") {
		t.Fatalf("a number past the store's size: %d %s", code, e)
	}
	if code, _, e := n.run("", "", "peer", "quota", b, "55G"); code != 0 {
		t.Fatalf("a number that fits the store: %s", e)
	}
	if code, _, e := n.run("", "", "peer", "quota", b, "56G", "--lending-max-total", "200G"); code != 0 {
		t.Fatalf("lending.max_total set above the store: %s", e)
	}
	_, text, _ := n.run("", "", "peer", "list")
	if !strings.Contains(text, "This node lends at most 95.0 GiB (the store's filesystem less its free-space floor); its numbers and open invites take 96.0 GiB") {
		t.Fatalf("peer list:\n%s", text)
	}
	st := n.status()
	if st.Lending == nil || st.Lending.MaxTotal != 95<<30 || st.Lending.Numbers != 96<<30 || st.Lending.From != capDisk {
		t.Fatalf("status lending %+v", st.Lending)
	}
	_, out, _ := n.run("", "", "status", "--json", "--lending-max-total", "300G")
	var set Status
	if err := json.Unmarshal([]byte(out), &set); err != nil || set.Lending == nil || set.Lending.MaxTotal != 300<<30 || set.Lending.From != capSet {
		t.Fatalf("status with lending.max_total set: %v %+v", err, set.Lending)
	}
	_, text, _ = n.run("", "", "status")
	if !strings.Contains(text, "Lending: at most 95.0 GiB") {
		t.Fatalf("status:\n%s", text)
	}
}

// LEND-1 (#24 review): `peer add` refuses a number that takes the sum
// past the cap before it dials, and records no redeem.
func TestPeerAddRefusesANumberPastTheCap(t *testing.T) {
	parallel(t, 0)
	inviter, n := friendNode(t), friendNode(t)
	befriendDirect(t, n, 50<<30, 1)
	s := inviter.invite("--quota", "10G") // not serving: the refusal comes first
	code, out, e := n.run("60G\n", "", "peer", "add", s, "--lending-max-total", "100G")
	if code != 1 || !strings.Contains(e, "at most 100G (lending.max_total)") || !strings.Contains(e, "at most 50G more") {
		t.Fatalf("peer add past the cap: %d %s%s", code, out, e)
	}
	if got := countRows(t, n, "pending_redeem"); got != 0 {
		t.Fatalf("%d redeems recorded", got)
	}
}
