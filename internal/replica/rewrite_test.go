package replica

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/state"
)

func hashOf(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// immutablePlace is a place that keeps LEND-4 as a lender does: a key
// holding other bytes takes a put only as a rewrite marked with its
// hash (#74). It records each put's mark.
type immutablePlace struct {
	*place
	mu    sync.Mutex
	marks []string // key=mark of each put, in order
	// lose, if set, is called after a put is stored; an error it returns
	// is answered instead, as if the answer were lost.
	lose func(key string) error
}

func (p *immutablePlace) Put(ctx context.Context, o Object) error {
	p.place.mu.Lock()
	held, ok := p.objs[o.Bucket+"/"+o.Key]
	p.place.mu.Unlock()
	if ok && hashOf(string(held)) != o.SHA256 && hashOf(string(held)) != o.Replaces {
		return ErrImmutable
	}
	p.mu.Lock()
	p.marks = append(p.marks, o.Key+"="+o.Replaces)
	p.mu.Unlock()
	if err := p.place.Put(ctx, o); err != nil {
		return err
	}
	if p.lose != nil {
		return p.lose(o.Key)
	}
	return nil
}

// held is what the place holds at bucket/key.
func (p *immutablePlace) held(key string) string {
	p.place.mu.Lock()
	defer p.place.mu.Unlock()
	return string(p.objs[key])
}

func (p *immutablePlace) sentMarks() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.marks...)
}

func (p *immutablePlace) Head(ctx context.Context, bucket, key string) (string, bool, error) {
	p.place.mu.Lock()
	defer p.place.mu.Unlock()
	b, ok := p.objs[bucket+"/"+key]
	if !ok {
		return "", false, nil
	}
	return hashOf(string(b)), true, nil
}

// writeAs stores an object and captures it as the decorator does for a
// key of the given class, rewritable or not.
func (fx *fixture) writeAs(bucket, key, content, class string, rewritable bool) int64 {
	fx.t.Helper()
	p := filepath.Join(fx.store, bucket, filepath.FromSlash(key))
	os.MkdirAll(filepath.Dir(p), 0o700)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		fx.t.Fatal(err)
	}
	seqs, err := fx.db.BeginCaptures(fx.t.Context(), []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut,
		Class: class, Rewritable: rewritable}})
	if err != nil {
		fx.t.Fatal(err)
	}
	if _, err := fx.db.FinishPut(fx.t.Context(), seqs[0], int64(len(content)), hashOf(content)); err != nil {
		fx.t.Fatal(err)
	}
	return seqs[0]
}

func (fx *fixture) refusedAt(place string) int64 {
	fx.t.Helper()
	st, err := fx.db.PlaceStats(fx.t.Context(), time.Now())
	if err != nil {
		fx.t.Fatal(err)
	}
	return st[place].Refused
}

// EP-7, EP-11, LEND-4 (#74): a changed version of a rewritable key is
// sent to the place as a rewrite marked with the hash the place
// acknowledged, which the place stores: no refusal and no alert. The
// first version goes unmarked. A changed key that is not rewritable is
// refused by the place as before (EP-11 until #22): place_refused.
func TestRewritableKeyIsSentAsARewrite(t *testing.T) {
	fx := newFixture(t, "B")
	b := &immutablePlace{place: fx.places["B"]}
	fx.w.Connect = func(f state.Friendship) (Replica, error) { return b, nil }
	fx.run()
	fx.writeAs("vault", "kopia.maintenance", "schedule one", "metadata", true)
	fx.drained("B")
	fx.writeAs("vault", "kopia.maintenance", "schedule two", "metadata", true)
	fx.drained("B")
	fx.writeAs("vault", "kopia.maintenance", "schedule three", "metadata", true)
	fx.drained("B")
	if got := b.held("vault/kopia.maintenance"); got != "schedule three" {
		t.Fatalf("B holds %q", got)
	}
	want := []string{"kopia.maintenance=", "kopia.maintenance=" + hashOf("schedule one"), "kopia.maintenance=" + hashOf("schedule two")}
	if strings.Join(b.sentMarks(), " ") != strings.Join(want, " ") {
		t.Fatalf("marks %v, want %v", b.sentMarks(), want)
	}
	if n := fx.refusedAt("B"); n != 0 || fx.alerted.Load() != 0 {
		t.Fatalf("refused %d, alerts %d", n, fx.alerted.Load())
	}
	if h, ok, _ := fx.db.AckedHash(t.Context(), "B", "vault", "kopia.maintenance"); !ok || h != hashOf("schedule three") {
		t.Fatalf("acked %q %v", h, ok)
	}

	if cs, _ := fx.db.ChangedKeys(t.Context()); len(cs) != 0 {
		t.Fatalf("a rewritable key reported changed: %+v", cs)
	}

	// Not rewritable: held back at capture (EP-11, #22), never sent; B
	// keeps the old version, and the change is reported.
	fx.writeAs("vault", "n0123", "index one", "snapshot", false)
	fx.drained("B")
	fx.writeAs("vault", "n0123", "index two", "snapshot", false)
	fx.drained("B")
	if got := b.held("vault/n0123"); got != "index one" {
		t.Fatalf("B holds %q of a key that is not rewritable", got)
	}
	if n := len(b.sentMarks()); n != 4 {
		t.Fatalf("%d puts sent, want 4: the changed index was sent", n)
	}
	if cs, _ := fx.db.ChangedKeys(t.Context()); len(cs) != 1 || cs[0].Key != "n0123" ||
		cs[0].Acknowledged != hashOf("index one") || cs[0].SHA256 != hashOf("index two") {
		t.Fatalf("changed keys %+v", cs)
	}
}

// EP-11, LEND-4 (#22): a place may hold a key that is not rewritable
// with bytes the index does not show it acknowledged (a send whose
// answer was lost, then superseded; or another friend's version after
// recover). Its 412 is the immutability alert: the worker asks the place
// what it holds and records the refusal as a changed key with that hash,
// not as place_refused, and moves on. A place already holding the row's
// bytes has it: an acknowledgement.
func TestALendersImmutabilityRefusalIsReportedAsAChangedKey(t *testing.T) {
	fx := newFixture(t, "B")
	b := &immutablePlace{place: fx.places["B"]}
	fx.w.Connect = func(f state.Friendship) (Replica, error) { return b, nil }
	b.place.mu.Lock()
	b.objs["laptop/data/theirs"] = []byte("their bytes")
	b.objs["laptop/data/same"] = []byte("same bytes")
	b.place.mu.Unlock()
	fx.write("laptop", "data/theirs", "our bytes")
	fx.write("laptop", "data/same", "same bytes")
	fx.write("laptop", "data/fine", "fine")
	fx.run()
	fx.drained("B")
	if got := b.held("laptop/data/theirs"); got != "their bytes" {
		t.Fatalf("B holds %q", got)
	}
	cs, err := fx.db.ChangedKeys(t.Context())
	if err != nil || len(cs) != 1 || cs[0].Key != "data/theirs" || cs[0].SHA256 != hashOf("our bytes") ||
		cs[0].Places["B"] != hashOf("their bytes") || cs[0].Acknowledged != hashOf("their bytes") {
		t.Fatalf("changed keys %+v %v", cs, err)
	}
	if r, _ := fx.db.RefusedAtPlaces(t.Context()); r["B"] != 0 {
		t.Fatalf("recorded as place_refused: %v", r)
	}
	if n := fx.refusedAt("B"); n != 1 || fx.alerted.Load() == 0 {
		t.Fatalf("refused %d, alerts %d", n, fx.alerted.Load())
	}
	if h, ok, _ := fx.db.AckedHash(t.Context(), "B", "laptop", "data/same"); !ok || h != hashOf("same bytes") {
		t.Fatalf("data/same acked %q %v", h, ok)
	}
	if c := fx.condition("B"); c.Condition != state.ConditionOK {
		t.Fatalf("condition %+v", c)
	}
}

// #74: the place may hold a version the node's index does not show it
// acknowledged: a rewrite stored there whose answer was lost, and which
// a later write superseded before the retry. The marked rewrite is then
// refused; the worker asks the place what it holds and, the key being
// rewritable, sends the rewrite again marked with that hash. A place
// that already holds the bytes is an acknowledgement.
func TestRewriteAfterALostAnswerAsksThePlace(t *testing.T) {
	fx := newFixture(t, "B")
	b := &immutablePlace{place: fx.places["B"]}
	fx.w.Connect = func(f state.Friendship) (Replica, error) { return b, nil }
	fx.writeAs("vault", "config", "config one", "metadata", true)
	fx.run()
	fx.drained("B")
	var once sync.Once
	b.lose = func(key string) error {
		var err error
		once.Do(func() {
			// Stored, the answer lost; meanwhile a third version is
			// captured, which supersedes the second.
			fx.writeAs("vault", "config", "config three", "metadata", true)
			err = errors.New("connection reset by peer")
		})
		return err
	}
	fx.writeAs("vault", "config", "config two", "metadata", true)
	fx.drained("B")
	if got := b.held("vault/config"); got != "config three" {
		t.Fatalf("B holds %q", got)
	}
	marks := b.sentMarks()
	if last := marks[len(marks)-1]; last != "config="+hashOf("config two") {
		t.Fatalf("marks %v: the last rewrite should replace what B holds", marks)
	}
	if n := fx.refusedAt("B"); n != 0 || fx.alerted.Load() != 0 {
		t.Fatalf("refused %d, alerts %d", n, fx.alerted.Load())
	}
}

// #74: Friend.Put sends a rewrite's mark as x-amz-meta-lendisk-replaces,
// and no mark otherwise; Friend.Head reads the hash a key is stored
// with, and a key the friend lacks is not an error.
func TestFriendSendsTheRewriteMark(t *testing.T) {
	self := newID(t)
	rec := &recorder{}
	f, _ := tunnelLender(t, self, rec)
	if err := put(t, f, "vault", "kopia.maintenance", []byte("one")); err != nil {
		t.Fatal(err)
	}
	body := []byte("two")
	if err := f.Put(t.Context(), Object{Bucket: "vault", Key: "kopia.maintenance", Size: 3, SHA256: hashOf("two"),
		Replaces: hashOf("one"), Body: strings.NewReader(string(body))}); err != nil {
		t.Fatal(err)
	}
	reqs := rec.all()
	if got := reqs[1].Header.Get("X-Amz-Meta-Lendisk-Replaces"); got != "" {
		t.Errorf("a first put carries a mark %q", got)
	}
	if got := reqs[2].Header.Get("X-Amz-Meta-Lendisk-Replaces"); got != hashOf("one") {
		t.Errorf("the rewrite's mark %q", got)
	}
	if a := reqs[2].Header.Get("Authorization"); !strings.Contains(strings.ToLower(a), "x-amz-meta-lendisk-replaces") {
		t.Errorf("the mark is not signed: %s", a)
	}

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && strings.HasSuffix(r.URL.Path, "/held"):
			w.Header().Set("X-Amz-Meta-Lendisk-Sha256", hashOf("held"))
			w.Header().Set("ETag", `"0123"`)
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	g, _ := tunnelLender(t, newID(t), h)
	if got, ok, err := g.Head(t.Context(), "vault", "held"); err != nil || !ok || got != hashOf("held") {
		t.Fatalf("head of a held key: %q %v %v", got, ok, err)
	}
	if _, ok, err := g.Head(t.Context(), "vault", "absent"); err != nil || ok {
		t.Fatalf("head of an absent key: %v %v", ok, err)
	}
}
