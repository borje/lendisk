package capacity

import (
	"testing"
)

const (
	gb = int64(1) << 30
	mb = int64(1) << 20
)

func near(t *testing.T, what string, got, lo, hi int64) {
	t.Helper()
	if got < lo || got > hi {
		t.Errorf("%s = %.2f GiB, want %.2f..%.2f GiB", what, float64(got)/float64(gb), float64(lo)/float64(gb), float64(hi)/float64(gb))
	}
}

func friends(quota int64, n int) []Friend {
	var fs []Friend
	for i := range n {
		fs = append(fs, Friend{Peer: string(rune('B' + i)), Quota: quota, Size: quota})
	}
	return fs
}

// Scenario 1 (LEND-7): three friends at 80 GB, 1% metadata, two copies:
// Σ min(D, 80 − m) ≥ 2D gives D ≤ 118.5, so S ≈ 119.7 GB, "about 120".
func TestProtectableOfScenarioOne(t *testing.T) {
	p := Plan{Data: 99 * gb, Metadata: gb, Copies: 2, Friends: friends(80*gb, 3)}
	near(t, "protectable", p.Protectable(), 119*gb, 120*gb)
	if p.Alerting() {
		t.Error("100 GB of about 120 protectable alerts")
	}
}

// DESIGN §4 example 2: E with 200 GB among four friends at 100 GB. With
// the metadata counted at every place, 4 × 98 = 392 < 2 × 198: about
// 198 GB, just below E's 200.
func TestMetadataIsCountedAtEveryPlace(t *testing.T) {
	p := Plan{Data: 198 * gb, Metadata: 2 * gb, Copies: 2, Friends: friends(100*gb, 4)}
	near(t, "protectable", p.Protectable(), 197*gb, 199*gb)
	if !p.Alerting() {
		t.Error("200 GB with 198 protectable does not alert")
	}
	// No repository yet: about 1% of metadata is assumed (§4).
	empty := Plan{Copies: 2, Friends: friends(100*gb, 4)}
	near(t, "protectable with no repository yet", empty.Protectable(), 197*gb, 199*gb)
	// A repository whose tool writes no metadata (generic) has none.
	none := Plan{Data: 200 * gb, Copies: 2, Friends: friends(100*gb, 4)}
	near(t, "protectable with no metadata", none.Protectable(), 199*gb, 200*gb)
}

// DESIGN §4 example 3 (scenario 12): E with 380 GB among four friends at
// 100 GB protects just under 200; an overflow target at 450 GB covers
// the rest, its room less the metadata it holds counted in full.
func TestOverflowRoomIsIncluded(t *testing.T) {
	p := Plan{Data: 376 * gb, Metadata: 4 * gb, Copies: 2, Friends: friends(100*gb, 4)}
	near(t, "protectable", p.Protectable(), 196*gb, 199*gb)
	p.Overflow = []int64{450 * gb}
	if got := p.Protectable(); got < 380*gb {
		t.Errorf("with 450 GB of overflow: protectable %.1f GiB, want at least 380", float64(got)/float64(gb))
	}
	// A friend holds at most one copy, an overflow target any part.
	one := Plan{Data: 99 * gb, Metadata: gb, Copies: 2, Overflow: []int64{1000 * gb}, Friends: friends(1000*gb, 1)}
	near(t, "one friend and a big target", one.Protectable(), 995*gb, 1005*gb)
}

// LEND-7: the fair bound puts each friend's reported repository size in
// place of its quota.
func TestFairBoundReplacesEachQuotaWithTheFriendsSize(t *testing.T) {
	fs := friends(1000*gb, 4)
	for i := range fs {
		fs[i].Size = 100 * gb
	}
	p := Plan{Data: 376 * gb, Metadata: 4 * gb, Copies: 2, Friends: fs}
	if got := p.Protectable(); got < 1900*gb {
		t.Errorf("protectable at 1 TB quotas: %d", got)
	}
	near(t, "fair protectable", p.FairProtectable(), 196*gb, 199*gb)
	fair := Plan{Data: 376 * gb, Metadata: 4 * gb, Copies: 2, Friends: friends(100*gb, 4)}
	if p.FairProtectable() != fair.Protectable() {
		t.Errorf("fair %d, the same plan at quotas of the sizes %d", p.FairProtectable(), fair.Protectable())
	}
}

func TestFewerFriendsThanCopiesProtectNothing(t *testing.T) {
	p := Plan{Data: 10 * gb, Metadata: gb / 10, Copies: 2, Friends: friends(100*gb, 1)}
	if got := p.Protectable(); got != 0 {
		t.Errorf("protectable with one friend at two copies: %d", got)
	}
	if _, ok := p.Cover(); ok {
		t.Error("covered with one friend at two copies")
	}
	if got := (Plan{Copies: 2}).Protectable(); got != 0 {
		t.Errorf("protectable with no friends: %d", got)
	}
}

// LEND-8 AC at full scale: A, B and C friends at 50 GB each; A grows to
// 60 GB. Both friendships are named, each with a number that, set, brings
// A back under 90% of the protectable size; a friend with room is not.
func TestCoverNamesANumberPerFriendship(t *testing.T) {
	p := Plan{Data: 59400 * mb, Metadata: 600 * mb, Copies: 2, Friends: friends(50*gb, 2)}
	if !p.Alerting() || p.Protectable() >= p.Size() {
		t.Fatalf("60 GB at two 50 GB friends: protectable %d", p.Protectable())
	}
	cover, ok := p.Cover()
	if !ok || len(cover) != 2 || cover["B"] == 0 || cover["C"] == 0 {
		t.Fatalf("cover %v, %v", cover, ok)
	}
	for peer, n := range cover {
		if n > 80*gb || n < 60*gb {
			t.Errorf("%s: %d GiB, want 60..80 (80 is the AC's)", peer, n/gb)
		}
		if n%mb != 0 {
			t.Errorf("%s: %d is not a whole number of MiB", peer, n)
		}
	}
	covered := p
	covered.Friends = []Friend{{Peer: "B", Quota: cover["B"]}, {Peer: "C", Quota: cover["C"]}}
	if covered.Alerting() {
		t.Errorf("with the numbers set: protectable %d for %d", covered.Protectable(), covered.Size())
	}
	for _, f := range covered.Friends {
		if room := PlacementRoom(f.Quota, p.Metadata); room < p.Data*10/9 {
			t.Errorf("%s: placement's room %d leaves data %d no margin", f.Peer, room, p.Data)
		}
	}

	big := Plan{Data: 59400 * mb, Metadata: 600 * mb, Copies: 2,
		Friends: []Friend{{Peer: "B", Quota: 50 * gb}, {Peer: "C", Quota: 50 * gb}, {Peer: "D", Quota: 500 * gb}}}
	cover, ok = big.Cover()
	if !ok {
		t.Fatal("not coverable with D")
	}
	if _, named := cover["D"]; named {
		t.Errorf("D, with room, named: %v", cover)
	}
	small := Plan{Data: gb, Copies: 2, Friends: friends(50*gb, 2)}
	if c, ok := small.Cover(); small.Alerting() || !ok || len(c) != 0 {
		t.Errorf("1 GB at two 50 GB friends: alerting %v, cover %v, %v", small.Alerting(), c, ok)
	}
}

// LEND-7: init's suggestion is c × S plus 20% for tombstones.
func TestSuggestedMaxTotal(t *testing.T) {
	if got := SuggestMaxTotal(100*gb, 2); got != 240*gb {
		t.Errorf("100 GB at two copies: %d GiB", got/gb)
	}
	if got := SuggestMaxTotal(10*mb, 3); got != 36*mb {
		t.Errorf("10 MiB at three copies: %d", got)
	}
}

func TestRoundUp(t *testing.T) {
	for _, c := range []struct{ in, want int64 }{{0, 0}, {1, mb}, {mb, mb}, {mb + 1, 2 * mb}, {10*gb + 1, 11 * gb}, {10 * gb, 10 * gb}} {
		if got := RoundUp(c.in); got != c.want {
			t.Errorf("RoundUp(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
