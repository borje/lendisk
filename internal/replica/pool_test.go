package replica

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

// put stores an object and captures it with the class its key has in
// the restic profile (EP-7), as the decorator does.
func (fx *fixture) put(bucket, key, class, content string) int64 {
	fx.t.Helper()
	p := filepath.Join(fx.store, bucket, filepath.FromSlash(key))
	os.MkdirAll(filepath.Dir(p), 0o700)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		fx.t.Fatal(err)
	}
	seqs, err := fx.db.BeginCaptures(fx.t.Context(), []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: class}})
	if err != nil {
		fx.t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(content))
	if _, err := fx.db.FinishPut(fx.t.Context(), seqs[0], int64(len(content)), hex.EncodeToString(sum[:])); err != nil {
		fx.t.Fatal(err)
	}
	return seqs[0]
}

func (fx *fixture) bucket(name string, copies int) {
	fx.t.Helper()
	if err := fx.db.AddBucket(fx.t.Context(), state.Bucket{Name: name, Profile: "restic", Copies: copies,
		AccessKey: "AK" + name, SecretKey: "s", CreatedAt: time.Now()}); err != nil {
		fx.t.Fatal(err)
	}
}

func (fx *fixture) holds(place, bucket, key string) bool {
	p := fx.places[place]
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.objs[bucket+"/"+key]
	return ok
}

// REPL-1 AC (#19): with N = 3 friends and copies c = 2, the replicas of
// any N − c + 1 = 2 friends together never hold a restic snapshot whose
// packs and index they do not also hold, while one friend is offline
// for a while and the others answer at their own pace. Checked at
// every object a place stores. A snapshot's contents are the index and
// the packs it lists.
func TestNoTwoFriendsEverHoldASnapshotWithoutItsPacks(t *testing.T) {
	fx := newFixture(t, "B", "C", "D")
	fx.bucket("repo", 2)
	refs := map[string][]string{} // snapshot or index -> what it references
	var world sync.Mutex
	var violations []string
	check := func() {
		names := []string{"B", "C", "D"}
		for i := range names {
			for j := i + 1; j < len(names); j++ {
				union := map[string]bool{}
				for _, n := range []string{names[i], names[j]} {
					for k := range fx.places[n].objs {
						union[k] = true
					}
				}
				for k := range union {
					if !strings.HasPrefix(k, "repo/snapshots/") {
						continue
					}
					for _, idx := range refs[k] {
						if !union[idx] {
							violations = append(violations, fmt.Sprintf("%s+%s hold %s without %s", names[i], names[j], k, idx))
						}
						for _, pack := range refs[idx] {
							if !union[pack] {
								violations = append(violations, fmt.Sprintf("%s+%s hold %s without %s", names[i], names[j], k, pack))
							}
						}
					}
				}
			}
		}
	}
	for _, p := range fx.places {
		p.world, p.check = &world, check
	}
	// D is offline at first: whatever is placed there waits.
	var offline sync.Mutex
	dOff := true
	fx.places["D"].fail = func(op, key string) error {
		offline.Lock()
		defer offline.Unlock()
		if dOff {
			return errors.New("dial: no route to host")
		}
		return nil
	}
	fx.places["C"].delay = 3 * time.Millisecond // C answers slowly
	for run := range 3 {
		var packs []string
		for i := range 4 {
			k := fmt.Sprintf("data/%d-%d", run, i)
			fx.put("repo", k, "data", "pack "+k)
			packs = append(packs, "repo/"+k)
		}
		idx := fmt.Sprintf("index/%d", run)
		refs["repo/"+idx] = packs
		fx.put("repo", idx, "metadata", "index "+idx)
		snap := fmt.Sprintf("snapshots/%d", run)
		refs["repo/"+snap] = []string{"repo/" + idx}
		fx.put("repo", snap, "snapshot", "snapshot "+snap)
	}
	fx.run()
	time.Sleep(200 * time.Millisecond)
	offline.Lock()
	dOff = false
	offline.Unlock()
	for _, n := range []string{"B", "C", "D"} {
		fx.drained(n)
	}
	world.Lock()
	defer world.Unlock()
	if len(violations) > 0 {
		t.Fatalf("%d violations, first: %s", len(violations), violations[0])
	}
	for _, n := range []string{"B", "C", "D"} {
		for k := range refs {
			if _, ok := fx.places[n].objs[k]; !ok {
				t.Fatalf("%s lacks metadata %s", n, k)
			}
		}
	}
}

// REPL-1 (#19, #20 review): a friend that refuses every object (a
// hostile BadDigest) does not hold back the other friends' metadata:
// a refused row is not waited for.
func TestARefusingFriendDoesNotStallTheBarrier(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.bucket("repo", 2)
	fx.places["C"].fail = func(op, key string) error { return ErrRejected }
	fx.put("repo", "data/1", "data", "pack")
	fx.put("repo", "index/1", "metadata", "index")
	fx.put("repo", "snapshots/1", "snapshot", "snapshot")
	fx.run()
	fx.drained("B")
	fx.drained("C")
	for _, k := range []string{"data/1", "index/1", "snapshots/1"} {
		if !fx.holds("B", "repo", k) {
			t.Fatalf("B lacks %s", k)
		}
	}
}

// REPL-1 AC, second half (#19): with too little room for `copies`,
// every place with room still receives its data and metadata.
func TestTooLittleRoomStillReplicatesWhatFits(t *testing.T) {
	fx := newFixture(t, "B", "C")
	fx.bucket("repo", 2)
	if _, err := fx.db.Exec(`UPDATE friendship SET own_number = 10, peer_number = 10 WHERE peer_key = 'C'`); err != nil {
		t.Fatal(err)
	}
	fx.put("repo", "data/1", "data", "a pack that does not fit at C")
	fx.put("repo", "index/1", "metadata", "index")
	fx.run()
	fx.drained("B")
	fx.drained("C")
	for _, want := range []struct {
		place, key string
		held       bool
	}{{"B", "data/1", true}, {"B", "index/1", true}, {"C", "data/1", false}, {"C", "index/1", true}} {
		if got := fx.holds(want.place, "repo", want.key); got != want.held {
			t.Fatalf("%s holds %s: %v", want.place, want.key, got)
		}
	}
}

// §3.4 (#19): the worker re-routes unsent data from a friend failing
// for longer than reroute_after to another friend with room, so the
// metadata barrier does not wait on it.
func TestWorkerReroutesFromAnUnreachableFriend(t *testing.T) {
	fx := newFixture(t, "B", "C", "D")
	fx.bucket("repo", 2)
	fx.db.RerouteAfter = 100 * time.Millisecond
	fx.w.RerouteEvery = 20 * time.Millisecond
	fx.places["C"].fail = func(op, key string) error { return errors.New("dial: no route to host") }
	d := fx.put("repo", "data/1", "data", "pack") // B and C: ties by node ID
	fx.put("repo", "index/1", "metadata", "index")
	fx.run()
	waitFor(t, "data/1 at D and index/1 at B and D", func() bool {
		return fx.holds("D", "repo", "data/1") && fx.holds("B", "repo", "index/1") && fx.holds("D", "repo", "index/1")
	})
	var places []string
	rows, err := fx.db.Query(`SELECT place FROM placement WHERE seq = ? ORDER BY place`, d)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var p string
		rows.Scan(&p)
		places = append(places, p)
	}
	rows.Close()
	if !slices.Equal(places, []string{"B", "D"}) {
		t.Fatalf("data/1 placed at %v", places)
	}
}

// §3.4 (#19 review): a row its sender is sending is not re-routed: when
// the send lands, the place's row records the copy, and a later delete
// reaches that place, so no copy is left that nothing deletes.
func TestARowBeingSentIsNotRerouted(t *testing.T) {
	fx := newFixture(t, "B", "C", "D")
	fx.bucket("repo", 2)
	fx.db.RerouteAfter = time.Millisecond
	fx.w.RerouteEvery = 5 * time.Millisecond
	hold := make(chan struct{})
	fx.places["C"].hold = hold
	d := fx.put("repo", "data/1", "data", "pack") // B and C: ties by node ID
	fx.run()
	waitFor(t, "C's send in flight", func() bool {
		p := fx.places["C"]
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.inFlight == 1
	})
	// C looks unreachable while its send is in flight.
	if err := fx.db.SetPlaceCondition(t.Context(), "C", state.ConditionRetrying, "dial: timeout",
		time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // many reroute passes
	close(hold)
	fx.drained("C")
	fx.drained("B")
	if !fx.holds("C", "repo", "data/1") {
		t.Fatal("C's send did not land")
	}
	var state string
	if err := fx.db.QueryRow(`SELECT state FROM placement WHERE place = 'C' AND seq = ?`, d).Scan(&state); err != nil || state != "acked" {
		t.Fatalf("C's row of data/1: %q %v", state, err)
	}
	fx.remove("repo", "data/1")
	for _, n := range []string{"B", "C", "D"} {
		fx.drained(n)
	}
	if fx.holds("C", "repo", "data/1") {
		t.Fatal("C keeps a copy of data/1 that no delete reached")
	}
}
