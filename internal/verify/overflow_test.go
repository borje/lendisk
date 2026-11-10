package verify

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

const targetPlace = "overflow:g"

// provider is an overflow target as the verifier sees it: the bytes it
// serves and what it says it stores, by key.
type provider struct {
	mu       sync.Mutex
	objs     map[string][]byte
	etag     map[string]string // overrides the MD5 ETag
	checksum map[string]string // a stored SHA-256, when set
	multi    map[string]bool   // stored in parts
	got      int64             // bytes served by Get
	stats    int
	// versioning is what Versioning answers ("" is enabled);
	// versioningErr, if set, is its error instead. versioningReads
	// counts the reads.
	versioning      string
	versioningErr   error
	versioningReads int
}

func (p *provider) Versioning(context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.versioningReads++
	if p.versioningErr != nil {
		return "", p.versioningErr
	}
	return cmp.Or(p.versioning, state.VersioningEnabled), nil
}

func (p *provider) setVersioning(v string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.versioning, p.versioningErr = v, err
}

func (p *provider) reads() int { p.mu.Lock(); defer p.mu.Unlock(); return p.versioningReads }

func (p *provider) Get(_ context.Context, _, key string) (io.ReadCloser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.objs[key]
	if !ok {
		return nil, replica.ErrNoSuchKey
	}
	p.got += int64(len(b))
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (p *provider) Stat(_ context.Context, _, key string) (replica.ObjectStat, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats++
	b, ok := p.objs[key]
	if !ok {
		return replica.ObjectStat{}, false, nil
	}
	sum, m := sha256.Sum256(b), md5.Sum(b)
	st := replica.ObjectStat{Size: int64(len(b)), SHA256: hex.EncodeToString(sum[:]), ETag: hex.EncodeToString(m[:]),
		ChecksumSHA256: p.checksum[key]}
	if e, ok := p.etag[key]; ok {
		st.ETag = e
	}
	if p.multi[key] {
		// Neither the ETag nor a composite checksum is of the bytes.
		st.Multipart, st.Composite, st.ETag = true, true, "0123456789abcdef0123456789abcdef-3"
		st.ChecksumSHA256 = ""
	}
	return st, true, nil
}

func (p *provider) served() int64 { p.mu.Lock(); defer p.mu.Unlock(); return p.got }

// targetNode is newNode with an overflow target g, without friends'
// copies: objects objects of laptop, each size bytes, acknowledged at
// g, which holds them, and in the node's store.
func targetNode(t *testing.T, objects, size int) (*node, *provider) {
	t.Helper()
	n := newNode(t, 0)
	ctx := t.Context()
	if err := n.db.AddOverflow(ctx, state.Overflow{Name: "g", Endpoint: "http://g", Bucket: "offsite", MaxBytes: 1 << 40,
		EnvPrefix: "X_", Retention: time.Hour, Versioning: state.VersioningEnabled, AddedAt: n.now}); err != nil {
		t.Fatal(err)
	}
	pr := &provider{objs: map[string][]byte{}, etag: map[string]string{}, checksum: map[string]string{}, multi: map[string]bool{}}
	for i := range objects {
		body := make([]byte, size)
		rand.Read(body)
		key := "data/" + string(rune('a'+i))
		p := filepath.Join(n.store, "laptop", filepath.FromSlash(key))
		os.MkdirAll(filepath.Dir(p), 0o700)
		if err := os.WriteFile(p, body, 0o600); err != nil {
			t.Fatal(err)
		}
		seqs, err := n.db.BeginCaptures(ctx, []state.Capture{{Bucket: "laptop", Key: key, Op: state.OpPut, Class: "data"}})
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(body)
		if _, err := n.db.FinishPut(ctx, seqs[0], int64(len(body)), hex.EncodeToString(sum[:])); err != nil {
			t.Fatal(err)
		}
		if _, err := n.db.ExecContext(ctx, `INSERT INTO placement (seq, place, bucket, key, op, state, sha256, acked_at, size)
			VALUES (?, ?, 'laptop', ?, 'put', 'acked', ?, ?, ?)`, seqs[0], targetPlace, key, hex.EncodeToString(sum[:]),
			n.now.Unix(), len(body)+state.ObjectOverhead); err != nil {
			t.Fatal(err)
		}
		pr.objs[key] = body
	}
	n.v.Overflow = func(state.Overflow) (Target, error) { return pr, nil }
	return n, pr
}

func (n *node) targetRecord() state.ReplicaCheck {
	n.t.Helper()
	cs, err := n.db.ReplicaChecks(n.t.Context())
	if err != nil {
		n.t.Fatal(err)
	}
	for _, c := range cs {
		if c.Place == targetPlace {
			return c
		}
	}
	n.t.Fatalf("no record of the target in %+v", cs)
	return state.ReplicaCheck{}
}

// S3-6 (#39): an overflow target's replica is due daily like a
// friend's, and checked by what the provider stores: lendisk's hash and
// the provider's stored SHA-256 against the index, and a single-part
// MD5 ETag against the MD5 of the local copy. A mismatch of either
// marks the replica unverified and alerts.
func TestATargetIsCheckedDailyByTheProvidersStoredChecksums(t *testing.T) {
	n, pr := targetNode(t, 4, 2000)
	due, err := n.db.DueReplicas(t.Context(), n.now, DefaultEvery, DefaultRetry)
	if err != nil || len(due) != 1 || due[0].Place != targetPlace {
		t.Fatalf("due %+v, %v", due, err)
	}
	if o := n.v.Check(t.Context(), targetPlace, "laptop", false); o.Outcome != state.VerifyPassed || o.Checked != 4 {
		t.Fatalf("outcome %+v", o)
	}
	if pr.served() != 0 {
		t.Fatal("a daily check downloaded objects")
	}
	if c := n.targetRecord(); c.VerifiedAt != n.now || c.Unverified() {
		t.Fatalf("record %+v", c)
	}
	// An ETag that is not the MD5 of the local copy: failed.
	pr.etag["data/b"] = "00000000000000000000000000000000"
	o := n.v.Check(t.Context(), targetPlace, "laptop", false)
	if o.Outcome != state.VerifyFailed || n.alerts.Load() != 1 || !n.targetRecord().Unverified() {
		t.Fatalf("a wrong ETag: %+v, %d alerts", o, n.alerts.Load())
	}
	// A stored SHA-256 is compared with the index directly, and an ETag
	// of a multipart object is not compared.
	n2, pr2 := targetNode(t, 2, 2000)
	pr2.etag["data/a"] = "abc-2"
	sum := sha256.Sum256(pr2.objs["data/b"])
	pr2.checksum["data/b"] = hex.EncodeToString(sum[:])
	if o := n2.v.Check(t.Context(), targetPlace, "laptop", false); o.Outcome != state.VerifyPassed || o.Checked != 2 {
		t.Fatalf("outcome %+v", o)
	}
	pr2.checksum["data/b"] = hex.EncodeToString(make([]byte, 32))
	if o := n2.v.Check(t.Context(), targetPlace, "laptop", false); o.Outcome != state.VerifyFailed {
		t.Fatalf("a wrong stored SHA-256: %+v", o)
	}
	// An object the target lost: failed.
	n3, pr3 := targetNode(t, 1, 100)
	delete(pr3.objs, "data/a")
	if o := n3.v.Check(t.Context(), targetPlace, "laptop", false); o.Outcome != state.VerifyFailed {
		t.Fatalf("a lost object: %+v", o)
	}
}

// VER-2 (#39): the sample downloads SamplePercent of the bytes a target
// holds each week, through the download limits (REPL-7), and hashes it
// against the index; a step's debt survives as its record.
func TestTheSampleDownloadsAShareOfTheTargetsBytesAWeek(t *testing.T) {
	n, pr := targetNode(t, 10, 100_000) // 1 MB
	n.v.SamplePercent = 10              // 100 kB a week
	p := pace.New(n.clock)
	var slept time.Duration
	p.Sleep = func(_ context.Context, d time.Duration) error { slept += d; return nil }
	p.Set(pace.Settings{Down: 16 << 10}) // replication.download_limit
	n.v.Pace = p
	n.v.sampleTargets(t.Context()) // starts the clock
	if pr.served() != 0 {
		t.Fatal("downloaded at the first step")
	}
	n.advance(5 * time.Minute)
	n.v.sampleTargets(t.Context())
	if pr.served() != 0 {
		t.Fatal("stepped before SampleEvery")
	}
	n.advance(week - 5*time.Minute)
	n.v.sampleTargets(t.Context())
	if got := pr.served(); got < 100_000 || got > 200_000 {
		t.Fatalf("a week's sample downloaded %d bytes, want 100 kB (the last object may run over)", got)
	}
	if slept == 0 {
		t.Fatal("the sample's downloads were not paced by the download limit")
	}
	if c := n.targetRecord(); c.VerifiedAt != n.now {
		t.Fatalf("record %+v", c)
	}
	_, owed, _, _ := n.db.SampleStep(t.Context(), targetPlace)
	if owed > 0 {
		t.Fatalf("still owes %d", owed)
	}
}

// VER-2 (#39): a sample whose bytes do not hash to the index marks the
// replica unverified and alerts.
func TestASampleThatDoesNotMatchTheIndexAlerts(t *testing.T) {
	n, pr := targetNode(t, 3, 1000)
	n.v.SamplePercent = 100
	for k := range pr.objs {
		pr.objs[k] = bytes.Repeat([]byte("x"), 1000) // rotted at the provider
	}
	n.v.sampleTargets(t.Context())
	n.advance(week)
	n.v.sampleTargets(t.Context())
	if c := n.targetRecord(); !c.Unverified() || n.alerts.Load() == 0 {
		t.Fatalf("record %+v, %d alerts", c, n.alerts.Load())
	}
}

// VER-3 (#39): `lendisk verify --full` at a target downloads every
// object of the replica and hashes it.
func TestAFullCheckOfATargetDownloadsEveryObject(t *testing.T) {
	n, pr := targetNode(t, 5, 1000)
	if o := n.v.Check(t.Context(), targetPlace, "laptop", true); o.Outcome != state.VerifyPassed || o.Checked != 5 || pr.served() != 5000 {
		t.Fatalf("full: %+v, %d bytes", o, pr.served())
	}
	pr.objs["data/c"] = bytes.Repeat([]byte("y"), 1000)
	if o := n.v.Check(t.Context(), targetPlace, "laptop", true); o.Outcome != state.VerifyFailed {
		t.Fatalf("full over rotted bytes: %+v", o)
	}
}

// #39 review: a multipart object's ETag and composite checksum are of
// its parts; a check that compared them would fail every such object
// and drain the replica. They are not compared.
func TestAMultipartObjectsChecksumsAreNotCompared(t *testing.T) {
	n, pr := targetNode(t, 2, 2000)
	pr.multi["data/a"], pr.multi["data/b"] = true, true
	if o := n.v.Check(t.Context(), targetPlace, "laptop", false); o.Outcome != state.VerifyPassed || o.Checked != 2 {
		t.Fatalf("outcome %+v", o)
	}
}

// #39 review: once a target's replica is unverified, the sample takes
// nothing more of it, and a ranged pass does not make it look freshly
// verified; a full pass clears the mark.
func TestAnUnverifiedTargetIsNotSampledNorRefreshed(t *testing.T) {
	n, pr := targetNode(t, 3, 1000)
	ctx := t.Context()
	n.v.SamplePercent = 100
	if o := n.v.Check(ctx, targetPlace, "laptop", false); o.Outcome != state.VerifyPassed {
		t.Fatalf("outcome %+v", o)
	}
	passed := n.targetRecord().VerifiedAt
	if err := n.db.RecordUnverified(ctx, targetPlace, "laptop", "lost one", n.now); err != nil {
		t.Fatal(err)
	}
	n.v.sampleTargets(ctx)
	n.advance(week)
	n.v.sampleTargets(ctx)
	if pr.served() != 0 {
		t.Fatalf("sampled an unverified replica: %d bytes", pr.served())
	}
	if o := n.v.Check(ctx, targetPlace, "laptop", false); o.Outcome != state.VerifyPassed {
		t.Fatalf("outcome %+v", o)
	}
	if c := n.targetRecord(); !c.Unverified() || c.VerifiedAt != passed {
		t.Fatalf("a ranged pass refreshed an unverified replica: %+v", c)
	}
	if o := n.v.Check(ctx, targetPlace, "laptop", true); o.Outcome != state.VerifyPassed {
		t.Fatalf("full %+v", o)
	}
	if c := n.targetRecord(); c.Unverified() || c.VerifiedAt != n.now {
		t.Fatalf("after a full pass %+v", c)
	}
}
