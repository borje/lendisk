package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/state"
)

const week = 7 * 24 * time.Hour

// fakeNow is a clock a test moves.
type fakeNow struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeNow) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeNow) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// sampler is a bit-rot sampler on the running gateway, on clock c,
// recording every object it checks in checked.
func (h *harness) sampler(s SampleSettings, c *fakeNow, checked *[]string) *sampler {
	h.t.Helper()
	l := h.gw.running.Load()
	d, ok := l.Next.(*Decorator)
	if !ok {
		h.t.Fatal("no decorator")
	}
	var mu sync.Mutex
	return &sampler{d: d, l: l, storeDir: h.store, SampleSettings: s, log: h.gw.Log, now: c.now, checked: func(b, k string) {
		mu.Lock()
		defer mu.Unlock()
		if checked != nil {
			*checked = append(*checked, b+"/"+k)
		}
	}}
}

func (h *harness) step(s *sampler) {
	h.t.Helper()
	if err := s.step(context.Background()); err != nil {
		h.t.Fatal(err)
	}
}

// rot flips a byte of key's object in bucket's directory, keeping its
// inode, size and modification time: what a failing disk does.
func (h *harness) rot(bucket, key string) {
	h.t.Helper()
	p := filepath.Join(h.store, bucket, filepath.FromSlash(key))
	fi, err := os.Stat(p)
	if err != nil {
		h.t.Fatal(err)
	}
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		h.t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, 3); err != nil {
		h.t.Fatal(err)
	}
	b[0] ^= 0x40
	if _, err := f.WriteAt(b, 3); err != nil {
		h.t.Fatal(err)
	}
	f.Close()
	if err := os.Chtimes(p, fi.ModTime(), fi.ModTime()); err != nil {
		h.t.Fatal(err)
	}
}

// edit writes other bytes of the same size into key's file in place,
// as a user's tool would: a later modification time.
func (h *harness) edit(bucket, key string, body []byte) {
	h.t.Helper()
	p := filepath.Join(h.store, bucket, filepath.FromSlash(key))
	fi, err := os.Stat(p)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(p, body, 0o600); err != nil {
		h.t.Fatal(err)
	}
	// A filesystem's timestamps are coarse (a few ms on ext4): move the
	// modification time on as a later edit would see it.
	if after, _ := os.Stat(p); after != nil && after.ModTime().Equal(fi.ModTime()) {
		if err := os.Chtimes(p, fi.ModTime(), fi.ModTime().Add(time.Millisecond)); err != nil {
			h.t.Fatal(err)
		}
	}
}

// ack records that place acknowledged laptop's key as its latest row.
func (h *harness) ack(place, key string) {
	h.t.Helper()
	if _, err := h.db.Exec(`INSERT OR REPLACE INTO placement (seq, place, bucket, key, op, state, sha256, acked_at)
		SELECT seq, ?, bucket, key, 'put', 'acked', sha256, 0 FROM queue
		WHERE bucket = 'laptop' AND key = ? AND state = 'done' ORDER BY seq DESC LIMIT 1`, place, key); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) rots() []state.Rot {
	h.t.Helper()
	rs, err := h.db.Rots(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	return rs
}

// VER-5 (#31): the sample is a share of the stored bytes a week: at 1%
// a week it hashes a hundredth of what the store holds after a week,
// own and lent alike, and nothing before any time has passed. The sweep
// takes objects in turn, so every object is checked once before any is
// checked twice, the cycle wraps, and its place survives a restart. A
// sweep's first cycle starts at a bucket the injected randomness picks.
func TestSampleCoversTheStoreInTurnAtTheConfiguredRate(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic", "photos": "restic"})
	h.self = newIdentity(t)
	friend := newIdentity(t)
	h.befriend(friend)
	h.serve(false)
	body := bytes.Repeat([]byte("x"), 1000)
	var keys []string
	for i := range 40 {
		keys = append(keys, fmt.Sprintf("data/%02d", i))
	}
	for _, b := range []string{"laptop", "photos"} {
		c := h.client(b)
		for _, k := range keys {
			if _, err := c.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(k),
				Body: bytes.NewReader(body)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	fc := h.pairClient(friend)
	sealed := friend.SealedName("laptop")
	if _, err := fc.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(sealed)}); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if _, err := fc.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(sealed), Key: aws.String(k),
			Body: bytes.NewReader(body), Metadata: hashMeta(body)}); err != nil {
			t.Fatal(err)
		}
	}
	lent := LentName(friend.NodeID(), sealed)
	stored, err := h.db.StoredBytes(t.Context())
	if err != nil || stored < 120*1000 {
		t.Fatalf("stored %d %v", stored, err)
	}

	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	var checked []string
	// Randomness picking 1 starts the first cycle at the second bucket
	// in order: lent-… sorts after laptop.
	s := h.sampler(SampleSettings{Percent: 1, Random: bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 1})}, clock, &checked)
	h.step(s)
	if len(checked) != 0 {
		t.Fatalf("checked %v with no time passed", checked)
	}
	clock.add(week)
	h.step(s)
	// 1% of the stored bytes (lent ones with their overhead) in objects
	// of 1000 bytes; the last object may run over.
	if n, want := len(checked), int(math.Ceil(float64(stored)/100/1000)); n != want || checked[0] != lent+"/data/00" {
		t.Fatalf("a week at 1%% checked %v; want %d objects from %s/data/00 (stored %d)", checked, want, lent, stored)
	}
	first := len(checked)

	// At 100% half a week owes more than the store holds: the step takes
	// the rest of the cycle and on, in order, until it is back where it
	// began, so every object is checked once and none twice.
	s.Percent = 100
	checked = nil
	clock.add(week / 2)
	h.step(s)
	seen := map[string]int{}
	for _, c := range checked {
		seen[c]++
	}
	for _, b := range []string{"laptop", lent, "photos"} {
		for _, k := range keys {
			if seen[b+"/"+k] != 1 {
				t.Fatalf("%s/%s checked %d times in a cycle: %v", b, k, seen[b+"/"+k], checked)
			}
		}
	}
	if len(checked) != 3*len(keys) || checked[0] != fmt.Sprintf("%s/data/%02d", lent, first) {
		t.Fatalf("checked %d objects in one cycle; want %d", len(checked), 3*len(keys))
	}
	sw, ok, err := h.db.Sweep(t.Context())
	if err != nil || !ok || sw.Cycles != 1 || sw.CheckedAt.IsZero() {
		t.Fatalf("sweep %+v %v %v", sw, ok, err)
	}

	// A restarted sampler goes on from where the sweep was, and owes at
	// its first step what passed since the last one (#31 review): a
	// daemon restarted more often than it steps still samples.
	last := checked[len(checked)-1]
	checked = nil
	clock.add(week)
	s = h.sampler(SampleSettings{Percent: 1}, clock, &checked)
	h.step(s)
	if next := fmt.Sprintf("%s/data/%02d", lent, first); len(checked) == 0 || checked[0] != next {
		t.Fatalf("after %s a restarted sampler checked %v; want %s first", last, checked, next)
	}
}

// VER-5 (#31): the sample's reads go through the disk pacing
// (store.bitrot_read_limit): 64 KiB at 16 KiB a second takes the paced
// reader's four seconds, on its clock, not the wall's.
func TestSampleReadsArePaced(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	body := bytes.Repeat([]byte("p"), 128<<10)
	if _, err := h.client("laptop").PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("data/00"), Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	var slept atomic.Int64
	pc := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	p := pace.New(pc.now)
	p.Sleep = func(ctx context.Context, d time.Duration) error { slept.Add(int64(d)); pc.add(d); return nil }
	p.Set(pace.Settings{Scrub: 16 << 10})
	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	s := h.sampler(SampleSettings{Percent: 100, Pace: p.Scrub}, clock, nil)
	h.step(s)
	clock.add(week)
	h.step(s)
	// 128 KiB: the first burst of 64 KiB is free, the rest takes 4 s.
	if d := time.Duration(slept.Load()); d < 3*time.Second || d > 5*time.Second {
		t.Fatalf("128 KiB at 16 KiB/s slept %v", d)
	}
}

// VER-5, EP-10 (#31): a local object whose bytes no longer hash to the
// version the index holds, in the very file this node verified, is
// alerted and replaced by a verified copy fetched through the
// read-through from a friend that acknowledged that version; nothing is
// enqueued, no friend is asked to change anything, and the store holds
// the right bytes again.
func TestRottenLocalObjectIsAlertedRemovedAndRefetched(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	good := bytes.Repeat([]byte("good pack "), 100)
	ff := &fakeFetch{held: map[string][]byte{"C data/01": good}}
	h.fetch = ff
	var logs syncBuffer
	h.log = slogTo(&logs)
	h.serve(false)
	c := h.client("laptop")
	for _, k := range []string{"data/00", "data/01"} {
		if _, err := c.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"), Key: aws.String(k),
			Body: bytes.NewReader(good)}); err != nil {
			t.Fatal(err)
		}
		h.ack("B", k)
	}
	h.ack("C", "data/01")
	rows, _ := h.queueRows()
	h.rot("laptop", "data/01")

	var alerted atomic.Int32
	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	s := h.sampler(SampleSettings{Percent: 100, Alerted: func() { alerted.Add(1) }}, clock, nil)
	h.step(s)
	clock.add(week)
	h.step(s)

	rs := h.rots()
	if len(rs) != 1 || rs[0].Bucket != "laptop" || rs[0].Key != "data/01" || rs[0].SHA256 != sum(good) ||
		rs[0].Found == sum(good) || rs[0].Refetch || rs[0].RepairedAt.IsZero() || rs[0].Lent() {
		t.Fatalf("rots %+v", rs)
	}
	if b, ok := h.stored("data/01"); !ok || !bytes.Equal(b, good) {
		t.Fatalf("store holds %q", b)
	}
	if f := ff.fetched(); len(f) == 0 || !strings.HasSuffix(f[0], " data/01") {
		t.Fatalf("fetched %v", f)
	}
	if alerted.Load() == 0 || !strings.Contains(logs.String(), "alert=true") || !strings.Contains(logs.String(), "data/01") {
		t.Fatalf("not alerted (%d):\n%s", alerted.Load(), logs.String())
	}
	if n, _ := h.queueRows(); n != rows {
		t.Fatalf("queue rows %d; want %d: the repair enqueued something", n, rows)
	}
	if v, err := h.db.OwnVersion(t.Context(), "laptop", "data/01"); err != nil || !v.Held() || v.Stamp == "" {
		t.Fatalf("the re-fetched object is not held and verified: %+v %v", v, err)
	}
}

// VER-5, EP-14 (#31): a repair never leaves the key missing, so the
// store reconciler never takes it for a delete. With the only friend
// holding the version offline, then serving forged bytes, the rotten
// copy stays where it is (a failed fetch replaces nothing) and a
// reconcile pass enqueues nothing; once the friend serves the version,
// the next sample step replaces it.
func TestARepairNeverLeavesTheKeyMissing(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	good := bytes.Repeat([]byte("pack "), 300)
	ff := &fakeFetch{held: map[string][]byte{}}
	h.fetch = ff
	h.serve(false)
	h.putAll("laptop", dataKeys(3)...)
	if _, err := h.client("laptop").PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("data/09"), Body: bytes.NewReader(good)}); err != nil {
		t.Fatal(err)
	}
	h.ack("B", "data/09")
	h.rot("laptop", "data/09")

	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	s := h.sampler(SampleSettings{Percent: 100}, clock, nil)
	h.step(s)
	clock.add(week)
	h.step(s)
	rs := h.rots()
	if len(rs) != 1 || !rs[0].Refetch || !rs[0].RepairedAt.IsZero() || rs[0].Detail == "" {
		t.Fatalf("rots %+v", rs)
	}
	rotten, ok := h.stored("data/09")
	if !ok || bytes.Equal(rotten, good) {
		t.Fatal("the rotten copy is not where it was")
	}
	// DeleteLimit 100 would enqueue any delete the pass took it for.
	h.pass(h.reconciler(ReconcileSettings{DeleteLimit: 100}))
	if n := h.deletes("laptop"); n != 0 {
		t.Fatalf("the reconciler enqueued %d deletes", n)
	}
	if got := h.reconciled("laptop"); len(got) != 0 {
		t.Fatalf("the reconciler enqueued %v", got)
	}
	// Forged bytes of the right size fail verification midway: the
	// rotten copy stays.
	forged := bytes.Repeat([]byte("PACK "), 300)
	ff.mu.Lock()
	ff.held["B data/09"] = forged
	ff.mu.Unlock()
	h.step(s)
	if b, _ := h.stored("data/09"); !bytes.Equal(b, rotten) {
		t.Fatalf("a failed fetch changed the store: %q", b)
	}

	ff.mu.Lock()
	ff.held["B data/09"] = good
	ff.mu.Unlock()
	h.step(s)
	if rs := h.rots(); len(rs) != 1 || rs[0].RepairedAt.IsZero() {
		t.Fatalf("not repaired: %+v", rs)
	}
	if b, ok := h.stored("data/09"); !ok || !bytes.Equal(b, good) {
		t.Fatalf("store holds %q", b)
	}
	h.pass(h.reconciler(ReconcileSettings{DeleteLimit: 100}))
	if n := h.deletes("laptop"); n != 0 {
		t.Fatalf("the reconciler enqueued %d deletes", n)
	}
}

// VER-5 (#31): a rotten object no friend holds (not replicated yet) is
// kept as it is, since its bytes are all there is, and alerted loudly.
func TestRotNoFriendHoldsIsKeptAndAlerted(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.fetch = &fakeFetch{held: map[string][]byte{}}
	var logs syncBuffer
	h.log = slogTo(&logs)
	h.serve(false)
	h.putAll("laptop", "data/00")
	h.rot("laptop", "data/00")
	rotten, _ := h.stored("data/00")
	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	s := h.sampler(SampleSettings{Percent: 100}, clock, nil)
	h.step(s)
	clock.add(week)
	h.step(s)
	rs := h.rots()
	if len(rs) != 1 || rs[0].Refetch || !rs[0].RepairedAt.IsZero() || !strings.Contains(rs[0].Detail, "no friend") {
		t.Fatalf("rots %+v", rs)
	}
	if b, ok := h.stored("data/00"); !ok || !bytes.Equal(b, rotten) {
		t.Fatal("the only copy was not kept")
	}
	if !strings.Contains(logs.String(), "no friend holds") {
		t.Fatalf("not alerted loudly:\n%s", logs.String())
	}
	// Written again by the backup tool, the same bytes: another file, so
	// the rot is over.
	h.putAll("laptop", "data/00")
	h.step(s)
	if rs := h.rots(); len(rs) != 1 || rs[0].RepairedAt.IsZero() {
		t.Fatalf("after a rewrite: %+v", rs)
	}
}

// VER-5 (#31): keys the sample must leave alone: a key EP-11 held back
// (its bytes are the user's, replicated nowhere) and a key with a write
// in flight are skipped, and a key recovered and never read is not in
// the store at all.
func TestHeldBackAndBusyKeysAreSkipped(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.fetch = &fakeFetch{held: map[string][]byte{}}
	h.serve(false)
	h.putAll("laptop", "data/00", "data/01")
	h.ack("B", "data/00")
	if _, err := h.db.Exec(`INSERT INTO friendship (peer_key, created_at) VALUES ('B', 0)`); err != nil {
		t.Fatal(err)
	}
	// data/00 is written with other bytes: EP-11 holds it back.
	if _, err := h.client("laptop").PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("laptop"),
		Key: aws.String("data/00"), Body: strings.NewReader("other bytes")}); err != nil {
		t.Fatal(err)
	}
	if v, _ := h.db.OwnVersion(t.Context(), "laptop", "data/00"); !v.Changed {
		t.Fatalf("not held back: %+v", v)
	}
	h.rot("laptop", "data/00")
	// data/01 has a write in flight.
	if _, err := h.db.BeginCaptures(t.Context(), []state.Capture{{Bucket: "laptop", Key: "data/01", Op: state.OpPut,
		Class: "data"}}); err != nil {
		t.Fatal(err)
	}
	h.rot("laptop", "data/01")
	var checked []string
	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	s := h.sampler(SampleSettings{Percent: 100}, clock, &checked)
	h.step(s)
	clock.add(week)
	h.step(s)
	if rs := h.rots(); len(rs) != 0 {
		t.Fatalf("rots %+v", rs)
	}
	if _, ok := h.stored("data/00"); !ok {
		t.Fatal("removed a held-back key")
	}
}

// VER-5, #88 (#31): an object changed in place outside the endpoint
// (its file is no longer the one this node verified: a later
// modification time) is no rot: it is the user's write, enqueued as
// one, and EP-11 holds it back as it would a capture, since B
// acknowledged the old bytes.
func TestAnObjectChangedInPlaceIsAWriteNotRot(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.fetch = &fakeFetch{held: map[string][]byte{}}
	h.serve(false)
	h.putAll("laptop", "data/00")
	if _, err := h.db.Exec(`INSERT INTO friendship (peer_key, created_at) VALUES ('B', 0)`); err != nil {
		t.Fatal(err)
	}
	h.ack("B", "data/00")
	h.edit("laptop", "data/00", []byte("EDITED of data/00"))
	edited, _ := h.stored("data/00")
	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	s := h.sampler(SampleSettings{Percent: 100}, clock, nil)
	h.step(s)
	clock.add(week)
	h.step(s)
	if rs := h.rots(); len(rs) != 0 {
		t.Fatalf("an edit taken for rot: %+v", rs)
	}
	if b, ok := h.stored("data/00"); !ok || !bytes.Equal(b, edited) {
		t.Fatal("the edit was undone")
	}
	if got := h.reconciled("laptop"); got["data/00"] != state.OpPut {
		t.Fatalf("reconciled %v; want the edit enqueued as a put", got)
	}
	v, err := h.db.OwnVersion(t.Context(), "laptop", "data/00")
	if err != nil || v.SHA256 != sum(edited) || !v.Changed || !v.Held() {
		t.Fatalf("version %+v %v; want the edit, held back by EP-11", v, err)
	}
}

// #88 (#31): the store reconciler re-hashes, paced, an indexed object
// whose file is no longer the one this node verified, and enqueues it
// as a write when its bytes changed (EP-11 then applies); a copy of the
// same bytes back over it is hashed once and recorded; a chmod is no
// edit and hashes nothing.
func TestReconcilerRehashesObjectsChangedInPlace(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	h.putAll("laptop", "data/00", "data/01", "data/02")
	h.edit("laptop", "data/00", []byte("edited in place"))
	if err := os.Chmod(filepath.Join(h.store, "laptop", "data", "01"), 0o640); err != nil {
		t.Fatal(err)
	}
	same, _ := h.stored("data/02")
	h.edit("laptop", "data/02", same)
	var paced atomic.Int32
	r := h.reconciler(ReconcileSettings{Pace: func(_ context.Context, rd io.Reader) io.Reader { paced.Add(1); return rd }})
	h.pass(r)
	if n := paced.Load(); n != 2 {
		t.Fatalf("hashed %d objects through the pacing; want data/00 and data/02", n)
	}
	got := h.reconciled("laptop")
	if len(got) != 1 || got["data/00"] != state.OpPut {
		t.Fatalf("reconciled %v; want data/00's edit as a put", got)
	}
	if v, _ := h.db.OwnVersion(t.Context(), "laptop", "data/00"); v.SHA256 != sum([]byte("edited in place")) {
		t.Fatalf("version %+v", v)
	}
	h.pass(r)
	if got := h.reconciled("laptop"); len(got) != 1 || paced.Load() != 2 {
		t.Fatalf("a second pass reconciled %v, hashed %d", got, paced.Load()-2)
	}
}

// VER-5, LEND-4 (#31): a friend's object whose bytes no longer hash to
// the hash it was stored with is alerted on the lender and left as it
// is: the lender cannot repair a friend's data. The record ends when
// the friend replaces or deletes the object.
func TestRottenLentObjectIsAlertedOnly(t *testing.T) {
	h := newHarness(t, nil)
	h.self = newIdentity(t)
	friend := newIdentity(t)
	h.befriend(friend)
	var logs syncBuffer
	h.log = slogTo(&logs)
	h.serve(false)
	fc := h.pairClient(friend)
	sealed := friend.SealedName("laptop")
	if _, err := fc.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(sealed)}); err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("friend's pack "), 50)
	for _, k := range []string{"data/00", "data/01"} {
		if _, err := fc.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(sealed), Key: aws.String(k),
			Body: bytes.NewReader(body), Metadata: hashMeta(body)}); err != nil {
			t.Fatal(err)
		}
	}
	lent := LentName(friend.NodeID(), sealed)
	h.rot(lent, "data/01")
	rotten, _ := os.ReadFile(filepath.Join(h.store, lent, "data", "01"))

	var alerted atomic.Int32
	clock := &fakeNow{t: time.Unix(1_800_000_000, 0)}
	s := h.sampler(SampleSettings{Percent: 100, Alerted: func() { alerted.Add(1) }}, clock, nil)
	h.step(s)
	clock.add(week)
	h.step(s)
	rs := h.rots()
	if len(rs) != 1 || !rs[0].Lent() || rs[0].Borrower != friend.NodeID().String() || rs[0].Sealed != sealed ||
		rs[0].Bucket != lent || rs[0].Key != "data/01" || rs[0].SHA256 != sum(body) || rs[0].Found != sum(rotten) {
		t.Fatalf("rots %+v", rs)
	}
	if b, _ := os.ReadFile(filepath.Join(h.store, lent, "data", "01")); !bytes.Equal(b, rotten) {
		t.Fatal("the lender changed a friend's object")
	}
	if alerted.Load() == 0 || !strings.Contains(logs.String(), "alert=true") {
		t.Fatalf("not alerted:\n%s", logs.String())
	}
	// The friend's next read of it fails its own verification; it
	// rewrites the key.
	if _, err := fc.DeleteObject(t.Context(), &s3.DeleteObjectInput{Bucket: aws.String(sealed), Key: aws.String("data/01")}); err != nil {
		t.Fatal(err)
	}
	h.step(s)
	if rs := h.rots(); len(rs) != 0 {
		t.Fatalf("after the friend deleted it: %+v", rs)
	}
}

func slogTo(w io.Writer) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }
