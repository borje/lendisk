package sheet

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/state"
)

const phrase = "abandon abandon abandon abandon abandon abandon abandon abandon " +
	"abandon abandon abandon abandon abandon abandon abandon abandon " +
	"abandon abandon abandon abandon abandon abandon abandon art"

func self(t *testing.T) *identity.Identity {
	t.Helper()
	id, err := identity.FromPhrase(phrase)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func friendID(t *testing.T) string {
	t.Helper()
	p, _ := identity.NewPhrase()
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id.NodeID().String()
}

// longAddress stands in for a tc… address, which runs to hundreds of
// characters.
func longAddress(tag string) string {
	return "tc" + tag + strings.Repeat("0123456789abcdefghij", 20)
}

// fixture is a manifest's content with three friends, one overflow
// target (none can exist before #38, so it is made up here) and two
// buckets.
func fixture(t *testing.T, id *identity.Identity) manifest.Content {
	t.Helper()
	return manifest.Content{
		Friends: []manifest.Friend{
			{ID: friendID(t), Address: longAddress("a")},
			{ID: friendID(t), Address: longAddress("b")},
			{ID: friendID(t), Address: longAddress("c")},
		},
		Overflow: []manifest.Overflow{{Name: "b2", Endpoint: "https://s3.eu-central-003.backblazeb2.com",
			Bucket: "family-backup", Prefix: "lendisk/alice", AccessKey: "AKIAEXAMPLEKEY", SecretKey: "s3cr3t-overflow-key"}},
		Buckets: []manifest.Bucket{
			{Name: "laptop", Sealed: id.SealedName("laptop"), Copies: 2, Profile: "restic"},
			{Name: "photos", Sealed: id.SealedName("photos"), Copies: 3, Profile: "kopia"},
		},
	}
}

func text(t *testing.T, s Sheet) string {
	t.Helper()
	var b bytes.Buffer
	if err := s.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// flat is the sheet with every run of white space made one space, so a
// test can look for prose the page wraps.
func flat(sheet string) string { return strings.Join(strings.Fields(sheet), " ") }

// Joined undoes the sheet's wrapping of long strings, so a test can look
// for an address as a whole.
func joined(sheet string) string {
	var b strings.Builder
	for _, l := range strings.Split(sheet, "\n") {
		b.WriteString(strings.TrimSpace(l))
	}
	return b.String()
}

// ID-7, SEC-16 (#33): each bucket is listed with its sealed name and
// the name it is stored under at every lender, friends and overflow
// targets alike, with how many replicas a restore without lendisk needs
// (N − c + 1).
func TestEachBucketListedWithSealedAndStoredNameAtEveryLender(t *testing.T) {
	id := self(t)
	c := fixture(t, id)
	s := New(id.NodeID(), c, false)
	if s.Places != 4 {
		t.Fatalf("places %d, want 4", s.Places)
	}
	out := text(t, s)
	for _, b := range c.Buckets {
		var sb *Bucket
		for i := range s.Buckets {
			if s.Buckets[i].Name == b.Name {
				sb = &s.Buckets[i]
			}
		}
		if sb == nil || sb.Sealed != b.Sealed {
			t.Fatalf("bucket %s: %+v", b.Name, sb)
		}
		lent := gateway.LentName(id.NodeID(), b.Sealed)
		var at []string
		for _, p := range sb.At {
			at = append(at, p.Place+"="+p.Name)
		}
		want := []string{}
		for _, f := range c.Friends {
			want = append(want, f.ID+"="+lent)
		}
		want = append(want, "overflow b2=s3://family-backup/lendisk/alice/"+b.Sealed+"/")
		if !slices.Equal(at, want) {
			t.Fatalf("bucket %s stored at\n%v\nwant\n%v", b.Name, at, want)
		}
		for _, w := range []string{b.Name, b.Sealed, lent, "s3://family-backup/lendisk/alice/" + b.Sealed + "/"} {
			if !strings.Contains(out, w) {
				t.Errorf("sheet lacks %q:\n%s", w, out)
			}
		}
	}
	// laptop: 4 places, 2 copies: 3 replicas; photos: 3 copies: 2.
	if s.Buckets[0].Needed != 3 || s.Buckets[1].Needed != 2 {
		t.Fatalf("needed %d and %d, want 3 and 2", s.Buckets[0].Needed, s.Buckets[1].Needed)
	}
	for _, w := range []string{"any 3 of the 4 places", "any 2 of the 4 places", "Node ID", id.NodeID().String()} {
		if !strings.Contains(flat(out), w) {
			t.Errorf("sheet lacks %q:\n%s", w, out)
		}
	}
	all := joined(out)
	for _, f := range c.Friends {
		if !strings.Contains(all, f.ID) || !strings.Contains(all, f.Address) {
			t.Errorf("sheet lacks friend %s or its address", f.ID)
		}
	}
	// The restore commands, and the separate password.
	for _, w := range []string{"lendisk recover <", "lendisk serve", "lendisk bucket show <bucket>", "restic restore latest",
		"password is a separate secret", "is not on this sheet"} {
		if !strings.Contains(flat(out), w) {
			t.Errorf("sheet lacks %q:\n%s", w, out)
		}
	}
}

// P-5, REPL-1 AC: a lendisk-free restore needs the replicas of any
// N − c + 1 places, any one when N ≤ c, and there is nothing to combine
// with no place at all.
func TestNeededReplicasIsPlacesLessCopiesPlusOne(t *testing.T) {
	for _, tc := range []struct{ places, copies, want int }{
		{3, 2, 2}, {4, 2, 3}, {2, 2, 1}, {1, 2, 1}, {5, 1, 5}, {0, 2, 0},
	} {
		if got := needed(tc.places, tc.copies); got != tc.want {
			t.Errorf("needed(%d places, %d copies) = %d, want %d", tc.places, tc.copies, got, tc.want)
		}
	}
}

// ID-7: the page is for paper, every line within 80 columns, whether it
// carries the phrase or blanks for it, however long the addresses.
func TestSheetFitsEightyColumns(t *testing.T) {
	id := self(t)
	for _, withPhrase := range []bool{false, true} {
		for _, secrets := range []bool{false, true} {
			s := New(id.NodeID(), fixture(t, id), secrets)
			s.Printed = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
			if withPhrase {
				s.Phrase = phrase
			}
			for i, l := range strings.Split(text(t, s), "\n") {
				if n := utf8.RuneCountInString(l); n > Width {
					t.Errorf("phrase %v secrets %v: line %d is %d columns: %q", withPhrase, secrets, i+1, n, l)
				}
				if strings.ContainsRune(l, '\t') {
					t.Errorf("line %d has a tab: %q", i+1, l)
				}
			}
		}
	}
}

// ID-1, ID-3: the phrase is never stored, so the sheet has a labelled
// blank for it unless the caller gives the phrase, and then it is
// printed word by word.
func TestPhraseIsABlankUnlessGiven(t *testing.T) {
	id := self(t)
	s := New(id.NodeID(), fixture(t, id), false)
	out := text(t, s)
	if !strings.Contains(flat(out), "write it here by hand") || !strings.Contains(out, "24. ____") || strings.Contains(out, "abandon") {
		t.Fatalf("no blank for the phrase:\n%s", out)
	}
	s.Phrase = phrase
	out = text(t, s)
	if !strings.Contains(out, " 1. abandon") || !strings.Contains(out, "24. art") || strings.Contains(out, "24. ____") {
		t.Fatalf("phrase not printed:\n%s", out)
	}
	if !strings.Contains(flat(out), "Anyone holding this page holds the phrase") {
		t.Fatalf("no warning that the page now carries the phrase:\n%s", out)
	}
	// The JSON carries it only when given, too.
	b, _ := json.Marshal(New(id.NodeID(), fixture(t, id), false))
	if strings.Contains(string(b), "phrase") {
		t.Fatalf("JSON names a phrase it was not given: %s", b)
	}
}

// S3-8 (#33): an overflow target shows its endpoint, bucket and prefix
// and says its access key is needed; the keys themselves only with
// --with-secrets. With none (none exist before #38) the section says
// so.
func TestOverflowShowsEndpointAndPrefixSecretsOnlyWhenAsked(t *testing.T) {
	id := self(t)
	c := fixture(t, id)
	plain := text(t, New(id.NodeID(), c, false))
	for _, w := range []string{"b2", "https://s3.eu-central-003.backblazeb2.com", "family-backup", "lendisk/alice",
		"Its access key is needed", "--with-secrets"} {
		if !strings.Contains(flat(plain), w) {
			t.Errorf("sheet lacks %q:\n%s", w, plain)
		}
	}
	if strings.Contains(plain, "AKIAEXAMPLEKEY") || strings.Contains(plain, "s3cr3t-overflow-key") {
		t.Fatalf("keys printed without --with-secrets:\n%s", plain)
	}
	b, _ := json.Marshal(New(id.NodeID(), c, false))
	if strings.Contains(string(b), "s3cr3t-overflow-key") || strings.Contains(string(b), "AKIAEXAMPLEKEY") {
		t.Fatalf("keys in JSON without --with-secrets: %s", b)
	}
	secret := text(t, New(id.NodeID(), c, true))
	if !strings.Contains(secret, "AKIAEXAMPLEKEY") || !strings.Contains(secret, "s3cr3t-overflow-key") {
		t.Fatalf("keys not printed with --with-secrets:\n%s", secret)
	}

	c.Overflow = nil
	none := text(t, New(id.NodeID(), c, false))
	if !strings.Contains(none, "Overflow targets: none") || strings.Contains(none, "overflow b2") {
		t.Fatalf("empty overflow list:\n%s", none)
	}
	if !strings.Contains(flat(none), "any 2 of the 3 places") {
		t.Fatalf("places without overflow:\n%s", none)
	}
}

// ID-7, LEND-6, P-5 (#33): the combine command on the sheet, run as
// printed, copies every replica into one directory and leaves out the
// housekeeping directories (.lendisk/ with its tombstones, versitygw's).
func TestCombineCommandUnitesReplicasWithoutHousekeeping(t *testing.T) {
	id := self(t)
	out := text(t, New(id.NodeID(), fixture(t, id), false))
	for _, script := range []string{CombineCp, CombineRsync} {
		for _, l := range strings.Split(script, "\n") {
			if !strings.Contains(out, "    "+l) {
				t.Fatalf("the sheet does not print %q:\n%s", l, out)
			}
		}
	}
	for _, w := range []string{"lendisk export --lent", "--kopia-layout", "replica-1"} {
		if !strings.Contains(flat(out), w) {
			t.Errorf("sheet lacks %q", w)
		}
	}

	for _, d := range gateway.ReservedDirs {
		if !strings.Contains(CombineCp, "repo/"+d) || !strings.Contains(CombineRsync, "--exclude=/"+d+"/") {
			t.Errorf("the combine commands keep %s", d)
		}
	}
	for _, script := range []string{CombineCp, CombineRsync} {
		if strings.Contains(script, "rsync") {
			if _, err := exec.LookPath("rsync"); err != nil {
				t.Log("rsync not installed; its form is not run")
				continue
			}
		}
		dir := t.TempDir()
		files := map[string]string{
			"replica-1/config":                    "cfg",
			"replica-1/index/i1":                  "idx",
			"replica-1/data/00/p1":                "pack1",
			"replica-1/.lendisk/tomb/data/00/x/1": "tomb",
			"replica-2/config":                    "cfg",
			"replica-2/index/i1":                  "idx",
			"replica-2/data/01/p2":                "pack2",
			"replica-2/.sgwtmp/upload":            "partial",
			"replica-2/.vgwlocks/lock":            "",
			"replica-2/.lendisk/tomb/data/01/y/2": "tomb",
			"replica-2/snapshots/s1":              "snap",
		}
		for p, body := range files {
			full := filepath.Join(dir, p)
			if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		cmd := exec.Command("sh", "-e", "-c", script)
		cmd.Dir = dir
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", script, err, b)
		}
		var got []string
		filepath.WalkDir(filepath.Join(dir, "repo"), func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				rel, _ := filepath.Rel(filepath.Join(dir, "repo"), p)
				got = append(got, rel)
			}
			return err
		})
		want := []string{"config", "data/00/p1", "data/01/p2", "index/i1", "snapshots/s1"}
		if !slices.Equal(got, want) {
			t.Fatalf("%s gave %v, want %v", script, got, want)
		}
	}
}

func openDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.Open(context.Background(), t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ID-7 (#33): once a sheet is printed, a friend's address change, a
// friend added or removed, or a bucket added makes the live sheet
// differ, and Stale names what changed; printing again settles it. A
// node that never printed one has nothing stale.
func TestStaleNamesWhatChangedSinceTheSheetWasPrinted(t *testing.T) {
	ctx := context.Background()
	id := self(t)
	db := openDB(t)
	b := friendID(t)
	if err := db.AddFriendship(ctx, state.Friendship{Peer: b, Address: "tcB1", OwnNumber: 1 << 30, PeerNumber: 1 << 30,
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "a", SecretKey: "s",
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	stale := func() (time.Time, []string) {
		t.Helper()
		at, changes, err := Stale(ctx, db, id)
		if err != nil {
			t.Fatal(err)
		}
		return at, changes
	}
	if at, changes := stale(); !at.IsZero() || changes != nil {
		t.Fatalf("never printed: %v %v", at, changes)
	}
	print := func() {
		t.Helper()
		s, err := Build(ctx, db, id, false)
		if err != nil {
			t.Fatal(err)
		}
		s.Phrase = phrase // never recorded
		if err := Record(ctx, db, s, time.Unix(1_800_000_000, 0)); err != nil {
			t.Fatal(err)
		}
	}
	print()
	if at, changes := stale(); !at.Equal(time.Unix(1_800_000_000, 0)) || changes != nil {
		t.Fatalf("just printed: %v %v", at, changes)
	}

	if _, err := db.ExecContext(ctx, `UPDATE friendship SET address = 'tcB2' WHERE peer_key = ?`, b); err != nil {
		t.Fatal(err)
	}
	if _, changes := stale(); !slices.Equal(changes, []string{"friend " + b + "'s address changed"}) {
		t.Fatalf("after an address change: %v", changes)
	}
	print()
	if _, changes := stale(); changes != nil {
		t.Fatalf("printed again: %v", changes)
	}

	c := friendID(t)
	if err := db.AddFriendship(ctx, state.Friendship{Peer: c, Address: "tcC", OwnNumber: 1 << 30, PeerNumber: 1 << 30,
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RemoveFriendship(ctx, b, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.AddBucket(ctx, state.Bucket{Name: "photos", Profile: "kopia", Copies: 1, AccessKey: "a2", SecretKey: "s2",
		CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	_, changes := stale()
	want := []string{"friend " + c + " added", "friend " + b + " removed", "bucket photos added"}
	if !slices.Equal(changes, want) {
		t.Fatalf("changes %v, want %v", changes, want)
	}

	// Nothing recorded carries the phrase.
	v, _, _ := db.GetNode(ctx, nodeKey)
	if strings.Contains(v, "abandon") {
		t.Fatalf("the recorded sheet holds the phrase: %s", v)
	}
}
