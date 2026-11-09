package cli

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/sheet"
	"github.com/borje/lendisk/internal/state"
)

// sheetNode is an initialised node with two friends (rows only: nobody
// answers at their addresses) and a restic bucket at two copies.
func sheetNode(t *testing.T) (n *node, friends []string) {
	t.Helper()
	n = newNode(t)
	n.mustInit(t)
	if code, _, errOut := n.run("", "", "bucket", "add", "laptop", "--profile", "restic", "--copies", "2"); code != 0 {
		t.Fatal(errOut)
	}
	ctx := context.Background()
	db, err := state.Open(ctx, n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tag := range []string{"b", "c"} {
		p, _ := identity.NewPhrase()
		id, _ := identity.FromPhrase(p)
		f := id.NodeID().String()
		friends = append(friends, f)
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + tag + strings.Repeat("x", 150),
			OwnNumber: 1 << 30, PeerNumber: 1 << 30, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	return n, friends
}

// ID-7, S3-8 (#33): `lendisk recovery-sheet` prints the page: the node
// ID, every friend's address, the bucket with its sealed name and the
// name it is stored under at every friend, the replicas a lendisk-free
// restore needs and the command combining them, within 80 columns and
// with nothing on stderr. `--json` says the same for machines.
func TestRecoverySheetPrintsWhatARestoreNeeds(t *testing.T) {
	parallel(t, 0)
	n, friends := sheetNode(t)
	self, _ := identity.FromPhrase(testPhrase)
	sealed := self.SealedName("laptop")

	code, out, errOut := n.run("", "", "recovery-sheet")
	if code != 0 || errOut != "" {
		t.Fatalf("recovery-sheet exit %d: %s", code, errOut)
	}
	for i, l := range strings.Split(out, "\n") {
		if utf8.RuneCountInString(l) > 80 {
			t.Errorf("line %d is wider than 80 columns: %q", i+1, l)
		}
	}
	all := strings.Join(strings.Fields(out), "")
	for _, f := range friends {
		if !strings.Contains(out, f) || !strings.Contains(all, f+"Address:tc") {
			t.Errorf("friend %s or its address missing", f)
		}
	}
	text := strings.Join(strings.Fields(out), " ")
	for _, w := range []string{self.NodeID().String(), "laptop (restic, copies 2)", sealed,
		"at every friend: " + gateway.LentName(self.NodeID(), sealed), "the replica of any 1 of the 2 places",
		"Overflow targets: none", "lendisk recover <a friend's address>", "write it here by hand",
		`for r in replica-*/; do cp -R "$r". repo/; done`, "password is a separate secret"} {
		if !strings.Contains(text, w) {
			t.Errorf("sheet lacks %q:\n%s", w, out)
		}
	}

	code, js, errOut := n.run("", "", "recovery-sheet", "--json")
	if code != 0 {
		t.Fatalf("--json exit %d: %s", code, errOut)
	}
	var s sheet.Sheet
	if err := json.Unmarshal([]byte(js), &s); err != nil {
		t.Fatalf("--json: %v\n%s", err, js)
	}
	if s.Node != self.NodeID().String() || s.Places != 2 || len(s.Friends) != 2 || len(s.Overflow) != 0 ||
		len(s.Buckets) != 1 || s.Buckets[0].Sealed != sealed || s.Buckets[0].Needed != 1 || len(s.Buckets[0].At) != 2 ||
		s.Phrase != "" {
		t.Fatalf("--json: %+v", s)
	}
}

// ID-1, ID-3, ID-7 (#33): `--with-phrase` reads the phrase on stdin
// and prints it on the page, once it is shown to derive this node; it
// is never written to disk nor to stderr. Another node's phrase is
// refused.
func TestRecoverySheetWithPhraseChecksAndNeverStoresIt(t *testing.T) {
	parallel(t, 0)
	n, _ := sheetNode(t)
	code, out, errOut := n.run(testPhrase+"\n", "", "recovery-sheet", "--with-phrase")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if !strings.Contains(out, " 1. abandon") || !strings.Contains(out, "24. art") || strings.Contains(out, "____") {
		t.Fatalf("phrase not on the sheet:\n%s", out)
	}
	if strings.Contains(errOut, "abandon") {
		t.Fatalf("phrase on stderr: %s", errOut)
	}
	assertNoPhrase(t, filepath.Dir(filepath.Dir(n.config)), testPhrase)

	other, _ := identity.NewPhrase()
	code, out, errOut = n.run(other+"\n", "", "recovery-sheet", "--with-phrase")
	if code != 1 || out != "" || !strings.Contains(errOut, "is not this node's") || strings.Contains(errOut, strings.Join(strings.Fields(other)[:3], " ")) {
		t.Fatalf("another phrase: exit %d\n%s\n%s", code, out, errOut)
	}
	if code, _, errOut := n.run("", "", "recovery-sheet", "--with-phrase"); code == 0 {
		t.Fatalf("no phrase on stdin accepted: %s", errOut)
	}
}

// ID-7 (#33): once the sheet is printed, a friend's address changing
// raises recovery_sheet_stale naming the friend; printing the sheet
// again clears it.
func TestFriendsAddressChangeAlertsToRegenerateTheSheet(t *testing.T) {
	serving(t, 0)
	n, friends := sheetNode(t)
	if code, _, errOut := n.run("", "", "recovery-sheet"); code != 0 {
		t.Fatal(errOut)
	}
	ctx := context.Background()
	db, err := state.Open(ctx, n.data, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `UPDATE friendship SET address = 'tcmoved' WHERE peer_key = ?`, friends[0]); err != nil {
		t.Fatal(err)
	}
	stale := func() (state.Alert, bool) {
		as, err := db.ActiveAlerts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range as {
			if a.Kind == alert.KindRecoverySheetStale {
				return a, true
			}
		}
		return state.Alert{}, false
	}
	wait := func(what string, want bool) state.Alert {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			a, ok := stale()
			if ok == want {
				return a
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	n.tuneAlerts = func(s *alert.Service) { s.Interval = 20 * time.Millisecond }
	stop := n.serveLogged()
	a := wait("the stale sheet alert", true)
	if !strings.Contains(a.Message, "friend "+friends[0]+"'s address changed") || !strings.Contains(a.Message, "lendisk recovery-sheet") {
		t.Fatalf("alert %+v", a)
	}
	if code, _, errOut := n.run("", "", "recovery-sheet"); code != 0 {
		t.Fatal(errOut)
	}
	wait("the alert to clear", false)
	stop()
}
