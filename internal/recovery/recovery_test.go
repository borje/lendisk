package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
	"github.com/borje/lendisk/internal/transport/transporttest"
)

var quiet = slog.New(slog.DiscardHandler)

func newID(t *testing.T) *identity.Identity {
	t.Helper()
	p, _ := identity.NewPhrase()
	id, err := identity.FromPhrase(p)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func openDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.Open(context.Background(), t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// lender is a friend as recovery sees it: its node ID and an address.
type lender struct {
	id   *identity.Identity
	addr string
}

func newLender(t *testing.T, region *transporttest.Source) lender {
	t.Helper()
	id := newID(t)
	m, err := region.DERPMap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return lender{id: id, addr: transport.AddressOf(id, m.Regions[transporttest.RegionID])}
}

func friendOf(l lender) manifest.Friend {
	return manifest.Friend{ID: l.id.NodeID().String(), Address: l.addr, OwnNumber: 1 << 30, PeerNumber: 1 << 30}
}

// ID-5, #28 review: a lender may serve an older manifest, since the
// envelope binds the node and the format, not freshness. Recovery asks
// every friend the manifests list, the newest one's friends too, and
// keeps the highest version; a friend that does not answer is reported,
// not fatal.
func TestNewestManifestWinsAcrossLenders(t *testing.T) {
	src := &transporttest.Source{D: transporttest.Start(t)}
	b, c, d, e := newLender(t, src), newLender(t, src), newLender(t, src), newLender(t, src)
	served := map[string]*manifest.Manifest{
		// B, the address given, is hostile: it serves an old copy that
		// does not even name D.
		b.addr: {Seq: 3, Content: manifest.Content{Friends: []manifest.Friend{friendOf(b), friendOf(c), friendOf(e)}}},
		c.addr: {Seq: 7, Content: manifest.Content{Friends: []manifest.Friend{friendOf(b), friendOf(c), friendOf(d), friendOf(e)}}},
		d.addr: {Seq: 6},
	}
	var asked []string
	fetch := func(_ context.Context, addr string) (*manifest.Manifest, error) {
		asked = append(asked, addr)
		if m, ok := served[addr]; ok {
			return m, nil
		}
		return nil, errors.New("offline")
	}
	m, tried, err := Newest(t.Context(), b.addr, fetch)
	if err != nil || m.Seq != 7 {
		t.Fatalf("newest: %+v %v", m, err)
	}
	if len(asked) != 4 || asked[0] != b.addr {
		t.Fatalf("asked %v", asked)
	}
	for _, f := range tried {
		switch f.Peer {
		case b.id.NodeID():
			if f.Seq != 3 {
				t.Errorf("B: %+v", f)
			}
		case e.id.NodeID():
			if f.Err == nil {
				t.Errorf("E: %+v", f)
			}
		}
	}
	// The given lender is the only one known: its failure is the error.
	if _, _, err := Newest(t.Context(), e.addr, fetch); err == nil || err.Error() != "offline" {
		t.Fatalf("no lender answered: %v", err)
	}
}

// ID-5 (#29): restoring from the manifest gives every friendship back
// with its terms and the key the friend dials this node with, which the
// tunnel admits again as that friend; the address's region; a version
// that continues above the fetched one; and a list to read per friend
// and bucket. Restoring again, after a crash, changes nothing.
func TestRestoreReadmitsFriendsAndContinuesVersion(t *testing.T) {
	ctx := t.Context()
	src := &transporttest.Source{D: transporttest.Start(t)}
	self := newID(t)
	b, c := newLender(t, src), newLender(t, src)
	bDial := b.id.DialKeyFor(self.NodeID()).NodeID().String()
	region, _ := src.DERPMap(ctx)
	rawRegion, _ := json.Marshal(region.Regions[transporttest.RegionID])
	since := time.Unix(1_800_000_000, 0).UTC()
	fb := friendOf(b)
	fb.DialKey, fb.Since, fb.OwnRetentionSeconds, fb.PeerRetentionSeconds = bDial, since, 30*86400, 60*86400
	fc := friendOf(c) // from a manifest written before dial keys were in it
	m := &manifest.Manifest{Seq: 41, Content: manifest.Content{
		Settings: manifest.Settings{Address: transport.AddressOf(self, region.Regions[transporttest.RegionID]), TunnelRegion: rawRegion},
		Friends:  []manifest.Friend{fb, fc},
		Buckets:  []manifest.Bucket{{Name: "laptop", Copies: 2, Profile: "restic"}, {Name: "phone", Copies: 1, Profile: "restic"}},
	}}
	db := openDB(t)
	for range 2 {
		r, err := Restore(ctx, db, m)
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Friends) != 2 || len(r.NoDialKey) != 1 || r.NoDialKey[0] != fc.ID {
			t.Fatalf("restored %+v", r)
		}
	}
	got, err := db.GetFriendship(ctx, fb.ID)
	if err != nil || got.Address != b.addr || got.DialKey != bDial || got.OwnRetention != 30*24*time.Hour ||
		got.PeerRetention != 60*24*time.Hour || !got.CreatedAt.Equal(since) || got.Quota() != 1<<30 {
		t.Fatalf("B's friendship %+v %v", got, err)
	}
	// B dials this node with its dial key: admitted, and named as B.
	if peer, role, err := db.TunnelPeer(ctx, bDial, time.Now()); err != nil || role != state.RolePeer || peer != fb.ID {
		t.Fatalf("B's dial key resolves to %s %s %v", peer, role, err)
	}
	if r, err := transport.SavedRegion(ctx, db); err != nil || r == nil || transport.AddressOf(self, r) != m.Settings.Address {
		t.Fatalf("region %+v %v", r, err)
	}
	if v, err := db.NoteManifest(ctx, "any", time.Now()); err != nil || v.Seq != 42 {
		t.Fatalf("the next version is %+v %v, want 42", v, err)
	}
	if rs, err := db.Relists(ctx); err != nil || len(rs) != 4 {
		t.Fatalf("lists to read %+v %v", rs, err)
	}
}

// pages is a fake set of places: what each holds of a sealed bucket,
// served a key at a time, or an error while it is offline.
type pages struct {
	mu      sync.Mutex
	held    map[string][]control.ListObject
	offline map[string]bool
	sealed  string
	t       *testing.T
}

func (p *pages) page(_ context.Context, f state.Friendship, sealed, after string) (control.ListPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.offline[f.Peer] {
		return control.ListPage{}, errors.New("no answer")
	}
	if sealed != p.sealed {
		p.t.Errorf("asked for %q, not the sealed name", sealed)
	}
	for i, o := range p.held[f.Peer] {
		if o.Key > after {
			next := ""
			if i < len(p.held[f.Peer])-1 {
				next = o.Key
			}
			return control.ListPage{Objects: []control.ListObject{o}, Next: next}, nil
		}
	}
	return control.ListPage{Objects: []control.ListObject{}}, nil
}

func (p *pages) set(place string, offline bool) {
	p.mu.Lock()
	p.offline[place] = offline
	p.mu.Unlock()
}

// ID-5 (#29): placement is rebuilt from each place's list, a page at a
// time, by the bucket's profile. A place that is offline is tried again
// after a backoff, and its objects count as held where the other lists
// found them; when it returns, the daemon's lister lists it. Nothing
// local-only is recorded, and nothing is downloaded.
func TestOfflinePlaceListedWhenItReturns(t *testing.T) {
	ctx := t.Context()
	db := openDB(t)
	self := newID(t)
	now := time.Unix(1_800_000_000, 0)
	var clock sync.Mutex
	tick := func() time.Time { clock.Lock(); defer clock.Unlock(); return now }
	for _, f := range []string{"B", "C"} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 1 << 30, PeerNumber: 1 << 30,
			CreatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "A", SecretKey: "S",
		CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddRelists(ctx, []string{"B", "C"}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	h1, h2 := sum("pack"), sum("snapshot")
	objs := []control.ListObject{{Key: "config", Size: 155, SHA256: sum("config")}, {Key: "data/aa/" + h1, Size: 100, SHA256: h1},
		{Key: "locks/x", Size: 5, SHA256: sum("lock")}, {Key: "snapshots/" + h2, Size: 10, SHA256: h2}}
	p := &pages{held: map[string][]control.ListObject{"B": objs, "C": objs}, offline: map[string]bool{"C": true},
		sealed: self.SealedName("laptop"), t: t}
	l := &Lister{DB: db, Self: self, Page: p.page, Now: tick, Log: quiet, Poll: 10 * time.Millisecond}

	rs, err := l.Round(ctx)
	if err != nil || len(rs) != 2 {
		t.Fatalf("round %+v %v", rs, err)
	}
	for _, r := range rs {
		if r.Place == "B" && (r.Err != nil || r.Listed != 3) || r.Place == "C" && r.Err == nil {
			t.Fatalf("result %+v", r)
		}
	}
	held := func(place string) int {
		var n int
		db.QueryRow(`SELECT count(*) FROM placement WHERE place = ? AND state = 'acked'`, place).Scan(&n)
		return n
	}
	if held("B") != 3 || held("C") != 0 {
		t.Fatalf("held at B %d, at C %d", held("B"), held("C"))
	}
	var classes []string
	rows, _ := db.Query(`SELECT class FROM queue ORDER BY key`)
	for rows.Next() {
		var c string
		rows.Scan(&c)
		classes = append(classes, c)
	}
	rows.Close()
	if len(classes) != 3 || classes[0] != "metadata" || classes[1] != "data" || classes[2] != "snapshot" {
		t.Fatalf("classes %v", classes)
	}
	// Held at B alone for now: below copies, not lost.
	if below, _ := db.BelowCopies(ctx); below["laptop"].Objects != 3 {
		t.Fatalf("below copies %+v", below)
	}
	// Within the backoff C is not asked again.
	p.set("C", false)
	if rs, _ := l.Round(ctx); len(rs) != 0 {
		t.Fatalf("asked within the backoff: %+v", rs)
	}
	clock.Lock()
	now = now.Add(time.Minute)
	clock.Unlock()
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { l.Run(run); close(done) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if n, _ := l.Pending(ctx); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the returning place was not listed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if held("C") != 3 {
		t.Fatalf("held at C %d", held("C"))
	}
	if below, _ := db.BelowCopies(ctx); below["laptop"].Objects != 0 {
		t.Fatalf("below copies after C returned %+v", below)
	}
	// With every list read the lister waits for the daemon to stop.
	select {
	case <-done:
		t.Fatal("Run returned before its context ended")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	<-done
}

// ID-5, REPL-13 (#71): a friend that never returns is not listed for
// ever. Once it is gone past replace_after (the pool's rule: no answer
// to the daemon's sends and probes for that long), its lists are no
// longer read nor counted pending, and status says so; before that it
// is tried after each backoff. A gone friend that answers again is
// listed.
func TestAFriendGonePastReplaceAfterIsNoLongerListed(t *testing.T) {
	ctx := t.Context()
	db := openDB(t)
	self := newID(t)
	start := time.Unix(1_800_000_000, 0)
	now := start
	var clock sync.Mutex
	tick := func() time.Time { clock.Lock(); defer clock.Unlock(); return now }
	at := func(d time.Duration) { clock.Lock(); now = start.Add(d); clock.Unlock() }
	db.Clock = tick
	for _, f := range []string{"B", "C"} {
		if err := db.AddFriendship(ctx, state.Friendship{Peer: f, Address: "tc" + f, OwnNumber: 1 << 30, PeerNumber: 1 << 30,
			CreatedAt: start}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddBucket(ctx, state.Bucket{Name: "laptop", Profile: "restic", Copies: 2, AccessKey: "A", SecretKey: "S",
		CreatedAt: start}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddRelists(ctx, []string{"B", "C"}, []string{"laptop"}); err != nil {
		t.Fatal(err)
	}
	h := sum("pack")
	objs := []control.ListObject{{Key: "data/aa/" + h, Size: 100, SHA256: h}}
	p := &pages{held: map[string][]control.ListObject{"B": objs, "C": objs}, offline: map[string]bool{"C": true},
		sealed: self.SealedName("laptop"), t: t}
	var mu sync.Mutex
	asked := map[string]int{}
	askedC := func() int { mu.Lock(); defer mu.Unlock(); return asked["C"] }
	page := func(ctx context.Context, f state.Friendship, sealed, after string) (control.ListPage, error) {
		mu.Lock()
		asked[f.Peer]++
		mu.Unlock()
		return p.page(ctx, f, sealed, after)
	}
	l := &Lister{DB: db, Self: self, Page: page, Now: tick, Log: quiet}
	// The daemon's worker records C's silence (state.SetPlaceCondition).
	if err := db.SetPlaceCondition(ctx, "C", state.ConditionRetrying, "dial: no route", start); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Round(ctx); err != nil {
		t.Fatal(err)
	}
	// Six days on, C is away but not gone: tried after its backoff.
	at(6 * 24 * time.Hour)
	if rs, _ := l.Round(ctx); len(rs) != 1 || rs[0].Place != "C" || rs[0].Err == nil || askedC() != 2 {
		t.Fatalf("C before replace_after: %+v, asked %d times", rs, askedC())
	}
	if n, _ := l.Pending(ctx); n != 1 {
		t.Fatalf("pending %d", n)
	}
	// Past replace_after (7 days by default) it is gone, a month on too.
	for _, d := range []time.Duration{7*24*time.Hour + time.Minute, 30 * 24 * time.Hour} {
		at(d)
		if rs, _ := l.Round(ctx); len(rs) != 0 || askedC() != 2 {
			t.Fatalf("C gone for %v: %+v, asked %d times", d, rs, askedC())
		}
		if n, _ := l.Pending(ctx); n != 0 {
			t.Fatalf("pending %d: the daemon keeps listing a gone friend", n)
		}
	}
	if gs, err := db.GonePlaces(ctx, tick()); err != nil || len(gs) != 1 || !gs["C"].Equal(start) {
		t.Fatalf("gone places %v %v", gs, err)
	}
	if rs := mustRelists(t, db); len(rs) != 1 || rs[0].Place != "C" {
		t.Fatalf("C's list is no longer kept for its return: %+v", rs)
	}
	// C answers again: it is listed.
	p.set("C", false)
	if err := db.ProbeAnswered(ctx, "C", tick()); err != nil {
		t.Fatal(err)
	}
	if rs, _ := l.Round(ctx); len(rs) != 1 || rs[0].Err != nil || rs[0].Listed != 1 {
		t.Fatalf("C back: %+v", rs)
	}
	if n, _ := l.Pending(ctx); n != 0 {
		t.Fatalf("pending %d", n)
	}
}

func mustRelists(t *testing.T, db *state.DB) []state.Relist {
	t.Helper()
	rs, err := db.Relists(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return rs
}
