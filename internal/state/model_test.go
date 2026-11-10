package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The removal model test (#103): random sequences of what a pool lives
// through, each step driven through the real code (captures, the
// worker's send as replica.Worker does it, Replace, Reroute, AckPlacement,
// seeds, undelete, overflow targets and their tombs), checking after
// every step the invariants mayRemove keeps (removal.go, DESIGN §3.4)
// against what the model believes each place holds:
//
//   - I1: a data copy acknowledged before the step and no longer after
//     it (dropping, drained, gone) leaves its live object with at least
//     `copies` acknowledged copies at intact places; so does a dropping
//     copy's delete as it is sent and as it is acknowledged (drained,
//     #104);
//   - I2: no metadata object a target held or had queued is dropped or
//     cancelled there while the target holds, or can still undelete,
//     data of its bucket: a live copy, a delete it has queued but not
//     sent, a held tomb, a denied delete, or a delete marker inside its
//     window (the model's own view, not holdsLiveData's);
//   - every acknowledged copy of a live object is one the model sent;
//   - liveness, after the steps (settle, #108): made quiescent with
//     friends holding everything, the pool empties every target within
//     settleRounds rounds.
//
// The steps are what the model can see happen: a put row's state
// changes are logged by triggers on placement (model_change), and the
// checks look at each step's changes.
//
// A failure prints the seed, the step and the steps before it; the run
// replays with LENDISK_MODEL_SEED and LENDISK_MODEL_STEPS, and
// LENDISK_MODEL_LOG prints every step of a run that passes.
// LENDISK_MODEL_SEEDS=n runs seeds 1 to n instead of CI's three, and
// LENDISK_MODEL_STEPS more steps each: a longer run than CI's.

// What a place holds of one version, as the model believes it.
const (
	mLive        = iota // landed; no delete of it sent
	mTomb               // deleted at a client-side target: its tomb (row tomb) keeps it for the window
	mDrainTomb          // dropped by re-placement at a client-side target: its tomb keeps it, but no undelete
	mMarker             // deleted at a versioned target: the version is kept until until
	mDrainMarker        // dropped by re-placement at a versioned target
	mDenied             // its delete refused for want of permission: it stays (tomb row tomb)
	mKept               // at a target removed with keep_data: it stays, unmanaged
	mFriendTomb         // deleted at a friend: the lender's tomb keeps it for the window
)

type mver struct {
	seq         int64
	bucket, key string
	data        bool
	kind        int
	tomb        int64     // the placement row a tomb holds (mTomb, mDrainTomb, mDenied)
	until       time.Time // mMarker
}

// mplace is a friend or an overflow target.
type mplace struct {
	name      string
	target    bool
	answering bool // it answers: the worker sends to it
	refuses   bool
	removing  bool
	keepData  bool
	out       bool // the friendship ended, or the target's removal finished
	held      []*mver
}

// mkey is a key ever written, and its latest done write.
type mkey struct {
	bucket, key string
	data        bool
	live        bool
	seq         int64
	size        int64
	sha         string
}

// maxLiveData is how many live data objects the repository keeps at
// most: a put beyond it prunes instead.
const (
	maxLiveData     = 40
	maxLiveMetadata = 12
	// targetRetention is every overflow target's --retention.
	targetRetention = 72 * time.Hour
)

type removalModel struct {
	t       *testing.T
	p       *pool
	r       *rand.Rand
	seed    uint64
	log     []string
	places  []*mplace
	byName  map[string]*mplace
	buckets []string
	keys    []*mkey
	n       int    // names and versions handed out
	step    int    // the step running
	what    string // what it did, for the log
	// small: the friends are small (odd seeds).
	small bool
	// exempt: places whose rows the step removes as a named exception
	// (I3), not by the removal rule.
	exempt map[string]bool
	// calm: no send fails and no delete is refused (settle's liveness
	// phase).
	calm bool
}

func TestRemovalModel(t *testing.T) {
	steps := 2000
	// Each of these seeds, on the code before #103, caught all four of
	// the earlier removal bugs reintroduced (the mutation check in the
	// #103 commit); one small pool (odd) and two of every size. On the
	// code before #104, each caught a dropping copy's delete sent while
	// it left its object short (seed 6 at step 490, 8 at 1337, 5 at
	// 1956).
	seeds := []uint64{5, 6, 8}
	if s := os.Getenv("LENDISK_MODEL_STEPS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("LENDISK_MODEL_STEPS=%q: %v", s, err)
		}
		steps = n
	}
	if s := os.Getenv("LENDISK_MODEL_SEEDS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("LENDISK_MODEL_SEEDS=%q: %v", s, err)
		}
		seeds = nil
		for i := range n {
			seeds = append(seeds, uint64(i+1))
		}
	}
	if s := os.Getenv("LENDISK_MODEL_SEED"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("LENDISK_MODEL_SEED=%q: %v", s, err)
		}
		seeds = []uint64{n}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel()
			runModel(t, seed, steps)
		})
	}
}

func runModel(t *testing.T, seed uint64, steps int) {
	m := &removalModel{t: t, p: newPool(t), r: rand.New(rand.NewPCG(seed, 103)), seed: seed, byName: map[string]*mplace{}}
	m.p.db.ReplaceAfter = 7 * 24 * time.Hour
	// Every change of a put row's state, for the checks after each step.
	for _, q := range []string{
		`CREATE TABLE model_change (id INTEGER PRIMARY KEY, place TEXT, seq INTEGER, old TEXT, new TEXT)`,
		`CREATE TRIGGER model_insert AFTER INSERT ON placement WHEN NEW.op = 'put' BEGIN
			INSERT INTO model_change (place, seq, new) VALUES (NEW.place, NEW.seq, NEW.state); END`,
		`CREATE TRIGGER model_update AFTER UPDATE OF state ON placement WHEN NEW.op = 'put' AND OLD.state IS NOT NEW.state BEGIN
			INSERT INTO model_change (place, seq, old, new) VALUES (NEW.place, NEW.seq, OLD.state, NEW.state); END`,
		`CREATE TRIGGER model_delete AFTER DELETE ON placement WHEN OLD.op = 'put' BEGIN
			INSERT INTO model_change (place, seq, old) VALUES (OLD.place, OLD.seq, OLD.state); END`,
	} {
		mustExec(t, m.p.db, q)
	}
	// Odd seeds run a pool whose friends are small, with both kinds of
	// overflow target from the start: targets hold copies most of the
	// time, so their removal, drain-back and retention windows come up
	// often. Even seeds run friends of every size.
	m.small = seed%2 == 1
	for range 3 {
		m.addFriend()
	}
	if m.small {
		m.addTargetOf(VersioningEnabled)
		m.addTargetOf(VersioningOff)
	}
	for _, b := range []string{"a", "b"} {
		m.p.bucket(b, 2)
		m.buckets = append(m.buckets, b)
	}
	type action struct {
		weight int
		do     func()
	}
	actions := []action{
		{10, m.putData},
		{4, m.putMetadata},
		{3, m.prune},
		{1, m.pruneAll},
		{1, m.deleteMetadata},
		{2, m.reput},
		{16, m.send},
		{3, m.sendAll},
		{7, m.replace},
		{2, m.reroute},
		{2, m.addFriend},
		{1, m.removeFriend},
		{2, m.goGone},
		{2, m.loseHolder},
		{2, m.goUnreachable},
		{1, m.refuse},
		{4, m.answer},
		{1, m.unverify},
		{1, m.verify},
		{2, m.quota},
		{1, m.copies},
		{2, m.addTarget},
		{1, m.versioning},
		{2, m.removeTarget},
		{2, m.finishRemovals},
		{1, m.forceRemoveTarget},
		{4, m.tombs},
		{4, m.tick},
		{1, m.startSeed},
		{1, m.confirmSeed},
		{1, m.undelete},
	}
	total := 0
	for _, a := range actions {
		total += a.weight
	}
	for m.step = 1; m.step <= steps; m.step++ {
		m.what, m.exempt = "", map[string]bool{}
		k := m.r.IntN(total)
		for _, a := range actions {
			if k < a.weight {
				a.do()
				break
			}
			k -= a.weight
		}
		m.log = append(m.log, fmt.Sprintf("%d %s: %s", m.step, m.p.now.UTC().Format("01-02 15:04"), m.what))
		m.check()
		if t.Failed() {
			return
		}
	}
	m.settle()
	if os.Getenv("LENDISK_MODEL_LOG") != "" {
		t.Log(strings.Join(m.log, "\n"))
	}
}

// fail reports a violation with the steps that led to it.
func (m *removalModel) fail(format string, args ...any) {
	m.t.Helper()
	from := max(len(m.log)-80, 0)
	m.t.Fatalf("seed %d, step %d: %s\nreplay: LENDISK_MODEL_SEED=%d LENDISK_MODEL_STEPS=%d go test ./internal/state -run TestRemovalModel\nsteps %d to %d:\n%s",
		m.seed, m.step, fmt.Sprintf(format, args...), m.seed, m.step, from+1, len(m.log), strings.Join(m.log[from:], "\n"))
}

func (m *removalModel) did(format string, args ...any) {
	if m.what != "" {
		m.what += "; "
	}
	m.what += fmt.Sprintf(format, args...)
}

func (m *removalModel) must(err error) {
	m.t.Helper()
	if err != nil {
		m.fail("%s: %v", m.what, err)
	}
}

func (m *removalModel) pick(n int) int { return m.r.IntN(n) }

func (m *removalModel) units(lo, hi float64) float64 { return lo + m.r.Float64()*(hi-lo) }

// inPool lists the places still in the pool, optionally targets or
// friends only.
func (m *removalModel) inPool(filter func(*mplace) bool) []*mplace {
	var out []*mplace
	for _, pl := range m.places {
		if !pl.out && (filter == nil || filter(pl)) {
			out = append(out, pl)
		}
	}
	return out
}

func friends(pl *mplace) bool { return !pl.target }
func targets(pl *mplace) bool { return pl.target }

// The pool's changes.

func (m *removalModel) addFriend() {
	if m.small {
		m.addFriendOf(float64(3 + m.pick(10)))
		return
	}
	m.addFriendOf(float64(4 + m.pick(30)))
}

func (m *removalModel) addFriendOf(q float64) {
	if len(m.inPool(friends)) >= 5 {
		m.did("no friend added: five already")
		return
	}
	m.n++
	name := fmt.Sprintf("F%02d", m.n)
	m.p.friend(name, q)
	pl := &mplace{name: name, answering: true}
	m.places = append(m.places, pl)
	m.byName[name] = pl
	m.did("friend %s added, quota %.0f", name, q)
}

func (m *removalModel) somePlace(filter func(*mplace) bool) *mplace {
	ps := m.inPool(filter)
	if len(ps) == 0 {
		return nil
	}
	return ps[m.pick(len(ps))]
}

func (m *removalModel) removeFriend() {
	pl := m.somePlace(friends)
	if pl == nil || len(m.inPool(friends)) <= 1 {
		m.did("no friend removed")
		return
	}
	_, err := m.p.db.RemoveFriendship(context.Background(), pl.name, false, m.p.now)
	m.must(err)
	pl.out, pl.answering = true, false
	m.exempt[pl.name] = true // I3: a friendship ended
	m.did("friend %s removed", pl.name)
}

func (m *removalModel) setCondition(pl *mplace, cond string, since time.Time) {
	m.p.condition(pl.name, cond, since, since)
}

func (m *removalModel) goGone() {
	pl := m.somePlace(nil)
	if pl == nil {
		return
	}
	m.p.gone(pl.name)
	pl.answering, pl.refuses = false, false
	m.did("%s gone past replace_after", pl.name)
}

// loseHolder takes away, while a re-placed copy waits for its
// acknowledgement, another place holding the object: gone, refusing
// this node, or its replica of the bucket unverified. That is the
// window in which dropping the copy it replaces would leave the object
// short. Half the time it looks only at copies replacing one at an
// overflow target (one being removed, or drain-back).
func (m *removalModel) loseHolder() {
	rows, err := m.p.db.Query(`SELECT DISTINCT h.place, h.bucket FROM placement r JOIN placement h ON h.seq = r.seq
		WHERE r.state = 'queued' AND r.replaces IS NOT NULL AND h.state = 'acked' AND h.place != r.place
		AND h.place != r.replaces AND (? OR r.replaces GLOB '`+OverflowPrefix+`*') ORDER BY h.place, h.bucket`, m.pick(2) == 0)
	m.must(err)
	type holder struct{ place, bucket string }
	var hs []holder
	for rows.Next() {
		var h holder
		m.must(rows.Scan(&h.place, &h.bucket))
		if pl := m.byName[h.place]; pl != nil && !pl.out {
			hs = append(hs, h)
		}
	}
	rows.Close()
	m.must(rows.Err())
	if len(hs) == 0 {
		m.did("no copy waits for its replacement")
		return
	}
	h := hs[m.pick(len(hs))]
	pl := m.byName[h.place]
	switch m.pick(4) {
	case 0:
		m.must(m.p.db.RecordUnverified(context.Background(), pl.name, h.bucket, "wrong answer", m.p.now))
		m.did("%s's replica of %s unverified while a copy waits", pl.name, h.bucket)
	case 1:
		m.must(m.p.db.SetPlaceCondition(context.Background(), pl.name, ConditionRemoved, "refuses", m.p.now))
		pl.answering, pl.refuses = false, true
		m.did("%s refuses this node while a copy waits", pl.name)
	default:
		m.p.gone(pl.name)
		pl.answering, pl.refuses = false, false
		m.did("%s gone while a copy waits", pl.name)
	}
}

func (m *removalModel) goUnreachable() {
	pl := m.somePlace(nil)
	if pl == nil {
		return
	}
	m.must(m.p.db.SetPlaceCondition(context.Background(), pl.name, ConditionRetrying, "down", m.p.now))
	pl.answering, pl.refuses = false, false
	m.did("%s unreachable", pl.name)
}

func (m *removalModel) refuse() {
	pl := m.somePlace(friends)
	if pl == nil {
		return
	}
	m.must(m.p.db.SetPlaceCondition(context.Background(), pl.name, ConditionRemoved, "refuses", m.p.now))
	pl.answering, pl.refuses = false, true
	m.did("%s refuses this node", pl.name)
}

func (m *removalModel) answer() {
	pl := m.somePlace(func(pl *mplace) bool { return !pl.answering })
	if pl == nil {
		m.did("every place answers")
		return
	}
	m.must(m.p.db.SetPlaceCondition(context.Background(), pl.name, ConditionOK, "", m.p.now))
	pl.answering, pl.refuses = true, false
	m.did("%s answers again", pl.name)
}

func (m *removalModel) unverify() {
	pl := m.somePlace(nil)
	if pl == nil {
		return
	}
	b := m.buckets[m.pick(len(m.buckets))]
	m.must(m.p.db.RecordUnverified(context.Background(), pl.name, b, "wrong answer", m.p.now))
	m.did("%s's replica of %s unverified", pl.name, b)
}

func (m *removalModel) verify() {
	pl := m.somePlace(nil)
	if pl == nil {
		return
	}
	b := m.buckets[m.pick(len(m.buckets))]
	m.must(m.p.db.RecordVerified(context.Background(), pl.name, b, true, m.p.now))
	m.did("%s's replica of %s verified in full", pl.name, b)
}

func (m *removalModel) quota() {
	pl := m.somePlace(friends)
	if pl == nil {
		return
	}
	q := float64(2 + m.pick(50))
	_, err := m.p.db.Exec(`UPDATE friendship SET own_number = ?1, peer_number = ?1 WHERE peer_key = ?2`,
		int64(q*float64(unit)), pl.name)
	m.must(err)
	m.did("%s's quota now %.0f", pl.name, q)
}

func (m *removalModel) copies() {
	b := m.buckets[m.pick(len(m.buckets))]
	c := 1 + m.pick(3)
	m.must(m.p.db.SetCopies(context.Background(), b, c))
	m.did("%s's copies now %d", b, c)
}

func (m *removalModel) addTarget() {
	v := VersioningOff
	if m.pick(2) == 0 {
		v = VersioningEnabled
	}
	m.addTargetOf(v)
}

func (m *removalModel) addTargetOf(v string) {
	if len(m.inPool(targets)) >= 2 {
		m.did("no target added: two already")
		return
	}
	m.n++
	name := fmt.Sprintf("t%02d", m.n)
	size := float64(20 + m.pick(100))
	o := Overflow{Name: name, Endpoint: "https://s3.example", Bucket: "b", Prefix: "lendisk",
		MaxBytes: int64(size * float64(unit)), EnvPrefix: "LENDISK_OVERFLOW_X_", Retention: targetRetention,
		Versioning: v, AddedAt: m.p.now}
	m.must(m.p.db.AddOverflow(context.Background(), o))
	pl := &mplace{name: o.Place(), target: true, answering: true}
	m.places = append(m.places, pl)
	m.byName[pl.name] = pl
	m.did("target %s added, %.0f units, versioning %s", pl.name, size, v)
}

func (m *removalModel) versioning() {
	pl := m.somePlace(targets)
	if pl == nil {
		return
	}
	name, _ := OverflowName(pl.name)
	o, err := m.p.db.GetOverflow(context.Background(), name)
	m.must(err)
	v := VersioningEnabled
	if o.Versioning == VersioningEnabled {
		v = VersioningSuspended
	}
	m.p.now = m.p.now.Add(time.Minute) // a change is known to the second
	_, err = m.p.db.RecordOverflowVersioning(context.Background(), name, v, m.p.now)
	m.must(err)
	m.did("%s's versioning now %s", pl.name, v)
}

func (m *removalModel) removeTarget() {
	pl := m.somePlace(func(pl *mplace) bool { return pl.target && !pl.removing })
	if pl == nil {
		return
	}
	keep := m.pick(3) == 0
	name, _ := OverflowName(pl.name)
	// A user removing a target makes room for its copies first.
	m.addFriendOf(float64(20 + m.pick(30)))
	m.must(m.p.db.RemoveOverflow(context.Background(), name, keep, m.p.now))
	pl.removing = true
	// Its unsent puts leave its FIFO: the user removed it (#96).
	m.exempt[pl.name] = true
	if keep {
		// I3: overflow remove --keep-data. What it holds stays there,
		// out of lendisk's hands.
		pl.keepData = true
		for _, v := range pl.held {
			v.kind = mKept
		}
	}
	m.did("target %s removed, keep_data %t", pl.name, keep)
}

// forceRemoveTarget is `overflow remove --force` (#102): the target is
// dropped at once and nothing more is deleted there. Not accepted, it
// is refused while a live copy there is one its object needs, which the
// checks after the step hold it to (I1); accepted, it is I3's named
// exception.
func (m *removalModel) forceRemoveTarget() {
	pl := m.somePlace(targets)
	if pl == nil {
		return
	}
	accept := m.pick(4) == 0
	name, _ := OverflowName(pl.name)
	loss, _, err := m.p.db.ForceRemoveOverflow(context.Background(), name, accept, m.p.now)
	if errors.Is(err, ErrWouldLose) {
		m.did("target %s not forced out: %d objects need it", pl.name, loss.Objects)
		return
	}
	m.must(err)
	pl.out, pl.answering, pl.removing = true, false, true
	if accept {
		m.exempt[pl.name] = true
	}
	m.did("target %s forced out, loss accepted %t (%d objects)", pl.name, accept, loss.Objects)
}

func (m *removalModel) finishRemovals() {
	done, err := m.p.db.FinishOverflowRemovals(context.Background())
	m.must(err)
	var names []string
	for _, r := range done {
		names = append(names, r.Name)
		pl := m.byName[OverflowPlace(r.Name)]
		pl.out, pl.answering = true, false
		m.exempt[pl.name] = true
	}
	m.did("removals finished: %v", names)
}

// tick lets time pass, and the worker re-route as it does every minute.
func (m *removalModel) tick() {
	d := time.Duration(10+m.pick(12*60)) * time.Minute
	m.p.now = m.p.now.Add(d)
	m.did("time +%s", d)
	m.reroute()
}

// The repository's writes.

func (m *removalModel) capture(bucket, key, op string, data bool) int64 {
	class := "metadata"
	if data {
		class = "data"
	}
	seqs, err := m.p.db.BeginCaptures(context.Background(), []Capture{{Bucket: bucket, Key: key, Op: op, Class: class,
		Rewritable: !data}})
	m.must(err)
	return seqs[0]
}

func (m *removalModel) write(k *mkey, units float64) {
	m.n++
	k.size = int64(units*float64(unit)) - ObjectOverhead
	k.sha = fmt.Sprintf("v%d", m.n)
	seq := m.capture(k.bucket, k.key, OpPut, k.data)
	_, err := m.p.db.FinishPut(context.Background(), seq, k.size, k.sha)
	m.must(err)
	k.seq, k.live = seq, true
}

func (m *removalModel) putData() {
	if len(m.liveKeys(true, "")) >= maxLiveData {
		m.prune() // the repository stays small, so each step stays fast
		return
	}
	m.n++
	k := &mkey{bucket: m.buckets[m.pick(len(m.buckets))], key: fmt.Sprintf("data/%d", m.n), data: true}
	m.keys = append(m.keys, k)
	m.write(k, m.units(0.5, 4))
	m.did("put %s/%s (%d)", k.bucket, k.key, k.seq)
}

func (m *removalModel) putMetadata() {
	var k *mkey
	var ks []*mkey
	for _, x := range m.keys {
		if !x.data && x.live {
			ks = append(ks, x)
		}
	}
	if len(ks) >= maxLiveMetadata || len(ks) > 0 && m.pick(2) == 0 {
		k = ks[m.pick(len(ks))]
	} else {
		m.n++
		k = &mkey{bucket: m.buckets[m.pick(len(m.buckets))], key: fmt.Sprintf("index/%d", m.n)}
		m.keys = append(m.keys, k)
	}
	m.write(k, m.units(0.05, 0.3))
	m.did("put metadata %s/%s (%d)", k.bucket, k.key, k.seq)
}

func (m *removalModel) liveKeys(data bool, bucket string) []*mkey {
	var out []*mkey
	for _, k := range m.keys {
		if k.live && k.data == data && (bucket == "" || k.bucket == bucket) {
			out = append(out, k)
		}
	}
	return out
}

func (m *removalModel) deleteKeys(ks []*mkey) {
	var cs []Capture
	for _, k := range ks {
		cs = append(cs, Capture{Bucket: k.bucket, Key: k.key, Op: OpDelete, Class: map[bool]string{true: "data", false: "metadata"}[k.data]})
	}
	seqs, err := m.p.db.BeginCaptures(context.Background(), cs)
	m.must(err)
	m.must(m.p.db.FinishDelete(context.Background(), seqs...))
	for i, k := range ks {
		k.live, k.seq = false, seqs[i]
	}
}

// prune deletes up to three data keys of a bucket at once, as restic
// prune does.
func (m *removalModel) prune() { m.pruneSome(3) }

// pruneAll deletes every data key of a bucket at once (a repository
// forgotten whole), which leaves the targets that held it idle.
func (m *removalModel) pruneAll() { m.pruneSome(maxLiveData + 1) }

func (m *removalModel) pruneSome(most int) {
	b := m.buckets[m.pick(len(m.buckets))]
	ks := m.liveKeys(true, b)
	if len(ks) == 0 {
		m.did("nothing to prune in %s", b)
		return
	}
	m.r.Shuffle(len(ks), func(i, j int) { ks[i], ks[j] = ks[j], ks[i] })
	if most < len(ks) {
		ks = ks[:1+m.pick(most)]
	}
	m.deleteKeys(ks)
	var names []string
	for _, k := range ks {
		names = append(names, k.key)
	}
	m.did("pruned %s: %v", b, names)
}

func (m *removalModel) deleteMetadata() {
	ks := m.liveKeys(false, "")
	if len(ks) <= 2 {
		m.did("no metadata deleted")
		return
	}
	k := ks[m.pick(len(ks))]
	m.deleteKeys([]*mkey{k})
	m.did("deleted metadata %s/%s", k.bucket, k.key)
}

func (m *removalModel) reput() {
	var ks []*mkey
	for _, k := range m.keys {
		if k.data && !k.live {
			ks = append(ks, k)
		}
	}
	if len(ks) == 0 {
		m.did("nothing to write again")
		return
	}
	k := ks[m.pick(len(ks))]
	m.write(k, m.units(0.5, 4))
	m.did("wrote %s/%s again (%d)", k.bucket, k.key, k.seq)
}

// The worker.

// send runs up to six rows of one answering place's FIFO.
func (m *removalModel) send() {
	pl := m.somePlace(func(pl *mplace) bool { return pl.answering })
	if pl == nil {
		m.did("nobody answers")
		return
	}
	m.did("send to %s:", pl.name)
	m.sendTo(pl, 1+m.pick(6))
}

// sendAll runs every answering place's FIFO in turn, a row each, until
// each is empty or held, or 24 rows went: the pool working for a while,
// so the metadata barrier clears as it does in a daemon.
func (m *removalModel) sendAll() {
	m.did("send to all:")
	active := m.inPool(func(pl *mplace) bool { return pl.answering })
	for n := 0; n < 24 && len(active) > 0; {
		var next []*mplace
		for _, pl := range active {
			if k := m.sendTo(pl, 1); k > 0 {
				n += k
				next = append(next, pl)
			}
		}
		active = next
	}
}

// sendTo runs up to n rows of pl's FIFO as the worker does
// (replica.Worker.run, send): the barrier, a superseded put cancelled, a
// delete at a target held as a tomb, sent as a marker or refused for
// want of permission; a send fails one time in ten. It returns how many
// rows it took.
func (m *removalModel) sendTo(pl *mplace, n int) int {
	ctx := context.Background()
	took := 0
	for range n {
		p, ok, err := m.p.db.NextPlacement(ctx, pl.name)
		m.must(err)
		if !ok {
			return took
		}
		if p.Class != "data" || p.Op == OpDelete {
			held, err := m.p.db.Held(ctx, p.Seq)
			m.must(err)
			if held {
				m.did("%s: %d held by the barrier", pl.name, p.Seq)
				return took
			}
		}
		took++
		if p.Op == OpPut {
			sup, err := m.p.db.Superseded(ctx, p.Seq)
			m.must(err)
			if sup {
				m.must(m.p.db.CancelPlacement(ctx, pl.name, p.Seq))
				m.did("%s: %d superseded", pl.name, p.Seq)
				continue
			}
		}
		started, err := m.p.db.StartPlacement(ctx, pl.name, p.Seq, m.p.now)
		m.must(err)
		if !started {
			continue
		}
		if p.State == PlacementDropping {
			m.checkSend(pl, p)
		}
		if !m.calm && m.pick(10) == 0 {
			m.must(m.p.db.SetPlaceCondition(ctx, pl.name, ConditionRetrying, "blip", m.p.now))
			m.did("%s: %d %s failed", pl.name, p.Seq, p.Op)
			return took
		}
		sha := ""
		if p.Op == OpPut {
			sha = p.SHA256
			if pl.target {
				// The put replaces the key's current version there: the
				// one it overwrites is nothing lendisk deletes or
				// undeletes there any more.
				pl.held = slices.DeleteFunc(pl.held, func(v *mver) bool {
					return v.kind == mLive && v.bucket == p.Bucket && v.key == p.Key
				})
			}
			pl.held = append(pl.held, &mver{seq: p.Seq, bucket: p.Bucket, key: p.Key, data: p.Class == "data", kind: mLive})
			m.did("%s: %d put %s", pl.name, p.Seq, p.Key)
		} else {
			m.deleted(pl, p)
		}
		m.must(m.p.db.AckPlacement(ctx, pl.name, p.Seq, sha, m.p.now))
		m.must(m.p.db.SetPlaceCondition(ctx, pl.name, ConditionOK, "", m.p.now))
	}
	return took
}

// deleted sends delete p to pl (a user's delete, or re-placement's drop
// of copy p.Seq) and records what pl keeps of it.
func (m *removalModel) deleted(pl *mplace, p Placement) {
	ctx := context.Background()
	drop := p.State == PlacementDropping
	var vs []*mver // what the delete takes off
	for _, v := range pl.held {
		if v.bucket == p.Bucket && v.key == p.Key && (drop && v.seq == p.Seq || !drop && v.kind == mLive) {
			vs = append(vs, v)
		}
	}
	set := func(kind int, tomb int64, until time.Time) {
		for _, v := range vs {
			v.kind, v.tomb, v.until = kind, tomb, until
		}
	}
	if !pl.target {
		set(mFriendTomb, 0, time.Time{})
		m.did("%d delete %s (drop %t)", p.Seq, p.Key, drop)
		return
	}
	held, err := m.p.db.HoldOverflowDelete(ctx, pl.name, p.Seq, m.p.now)
	m.must(err)
	switch {
	case pl.keepData:
		set(mKept, 0, time.Time{})
		m.did("%d delete %s acknowledged unsent (keep_data)", p.Seq, p.Key)
	case held && drop:
		set(mDrainTomb, p.Seq, time.Time{})
		m.did("%d drop %s held as a tomb", p.Seq, p.Key)
	case held:
		set(mTomb, p.Seq, time.Time{})
		m.did("%d delete %s held as a tomb", p.Seq, p.Key)
	case !m.calm && m.pick(8) == 0:
		m.must(m.p.db.DenyOverflowDelete(ctx, pl.name, p.Seq, m.p.now))
		set(mDenied, p.Seq, time.Time{})
		m.did("%d delete %s denied", p.Seq, p.Key)
	case drop:
		set(mDrainMarker, 0, m.p.now.Add(targetRetention))
		m.did("%d drop %s sent as a marker", p.Seq, p.Key)
	default:
		set(mMarker, 0, m.p.now.Add(targetRetention))
		m.did("%d delete %s sent as a marker", p.Seq, p.Key)
	}
}

// tombs issues or refuses each target's due tombs (replica.issueTombs).
func (m *removalModel) tombs() {
	m.p.now = m.p.now.Add(time.Duration(m.pick(6*60)) * time.Minute)
	m.did("tombs at %s", m.p.now.UTC().Format("01-02 15:04"))
	m.issueTombs()
}

// issueTombs issues or refuses each answering target's tombs due now
// (replica.issueTombs).
func (m *removalModel) issueTombs() {
	ctx := context.Background()
	for _, pl := range m.inPool(targets) {
		if !pl.answering {
			continue
		}
		ts, err := m.p.db.DueOverflowTombs(ctx, pl.name, m.p.now, 100)
		m.must(err)
		for _, tb := range ts {
			if !m.calm && m.pick(5) == 0 {
				m.must(m.p.db.DenyOverflowDelete(ctx, pl.name, tb.Seq, m.p.now))
				for _, v := range pl.held {
					if v.tomb == tb.Seq && (v.kind == mTomb || v.kind == mDrainTomb || v.kind == mDenied) {
						v.kind = mDenied
					}
				}
				m.did("%s denied %d", pl.name, tb.Seq)
				continue
			}
			m.must(m.p.db.DropOverflowTomb(ctx, pl.name, tb.Seq))
			pl.held = slices.DeleteFunc(pl.held, func(v *mver) bool {
				return v.tomb == tb.Seq && (v.kind == mTomb || v.kind == mDrainTomb || v.kind == mDenied)
			})
			m.did("%s issued %d", pl.name, tb.Seq)
		}
	}
}

func (m *removalModel) replace() {
	s, err := m.p.db.Replace(context.Background(), nil, nil)
	m.must(err)
	m.did("pass %+v", s)
}

func (m *removalModel) reroute() {
	moved, dropped, err := m.p.db.Reroute(context.Background(), nil, nil)
	m.must(err)
	m.did("reroute: %d moved, %d dropped", moved, dropped)
}

// Seeds and undelete.

func (m *removalModel) startSeed() {
	pl := m.somePlace(friends)
	if pl == nil {
		return
	}
	b := m.buckets[m.pick(len(m.buckets))]
	var through int64
	m.must(m.p.db.QueryRow(`SELECT coalesce(max(seq), 0) FROM queue`).Scan(&through))
	m.must(m.p.db.StartSeed(context.Background(), pl.name, b, through, m.p.now))
	m.did("seed of %s to %s through %d started", b, pl.name, through)
}

// confirmSeed confirms a pending seed: the friend imported the disk,
// and lists what it holds.
func (m *removalModel) confirmSeed() {
	ctx := context.Background()
	ss, err := m.p.db.Seeds(ctx)
	m.must(err)
	var pending []Seed
	for _, s := range ss {
		if s.DoneAt.IsZero() && m.byName[s.Place] != nil && !m.byName[s.Place].out {
			pending = append(pending, s)
		}
	}
	if len(pending) == 0 {
		m.did("no seed pending")
		return
	}
	s := pending[m.pick(len(pending))]
	type row struct {
		seq      int64
		key, sha string
		data     bool
	}
	var rs []row
	rows, err := m.p.db.Query(`SELECT p.seq, p.key, q.sha256, q.class = 'data' FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.bucket = ? AND p.op = 'put' AND p.state = 'queued' AND p.seq <= ? ORDER BY p.seq`,
		s.Place, s.Bucket, s.Through)
	m.must(err)
	for rows.Next() {
		var r row
		m.must(rows.Scan(&r.seq, &r.key, &r.sha, &r.data))
		rs = append(rs, r)
	}
	rows.Close()
	var listed []SeedListed
	for _, r := range rs {
		listed = append(listed, SeedListed{Key: r.key, SHA256: r.sha})
	}
	n, err := m.p.db.ConfirmSeeded(ctx, s.Place, s.Bucket, s.Through, listed, m.p.now)
	m.must(err)
	pl := m.byName[s.Place]
	for _, r := range rs {
		var st string
		m.must(m.p.db.QueryRow(`SELECT state FROM placement WHERE place = ? AND seq = ?`, s.Place, r.seq).Scan(&st))
		if st == PlacementAcked {
			pl.held = append(pl.held, &mver{seq: r.seq, bucket: s.Bucket, key: r.key, data: r.data, kind: mLive})
		}
	}
	m.must(m.p.db.EndSeed(ctx, s.Place, s.Bucket, SeedConfirmed, m.p.now))
	m.did("seed of %s at %s confirmed: %d acknowledged", s.Bucket, s.Place, n)
}

// undelete brings a deleted data key back from a friend's tomb (RET-3).
func (m *removalModel) undelete() {
	ctx := context.Background()
	type cand struct {
		pl *mplace
		v  *mver
		k  *mkey
	}
	var cs []cand
	for _, k := range m.keys {
		if !k.data || k.live {
			continue
		}
		for _, pl := range m.inPool(friends) {
			for _, v := range pl.held {
				if v.kind == mFriendTomb && v.bucket == k.bucket && v.key == k.key {
					cs = append(cs, cand{pl, v, k})
				}
			}
		}
	}
	if len(cs) == 0 {
		m.did("nothing to undelete")
		return
	}
	c := cs[m.pick(len(cs))]
	var size int64
	var sha string
	m.must(m.p.db.QueryRow(`SELECT coalesce(size, 0), sha256 FROM queue WHERE seq = ?`, c.v.seq).Scan(&size, &sha))
	id, err := m.p.db.RequestUndelete(ctx, c.k.bucket, time.Time{}, false, m.p.now)
	m.must(err)
	m.must(m.p.db.PreviewUndelete(ctx, id, "{}", []string{c.k.key}, m.p.now))
	m.must(m.p.db.ConfirmUndelete(ctx, id, m.p.now))
	res, err := m.p.db.RecordRestored(ctx, c.pl.name, c.k.bucket, id, []Restored{{Key: c.k.key, Size: size, SHA256: sha, Class: "data"}})
	m.must(err)
	m.must(m.p.db.FinishUndelete(ctx, id, false, "", m.p.now))
	if res.Recorded == 1 {
		var seq int64
		m.must(m.p.db.QueryRow(`SELECT max(seq) FROM queue WHERE bucket = ? AND key = ?`, c.k.bucket, c.k.key).Scan(&seq))
		c.k.live, c.k.seq, c.k.size, c.k.sha = true, seq, size, sha
		c.pl.held = slices.DeleteFunc(c.pl.held, func(v *mver) bool { return v == c.v })
		c.pl.held = append(c.pl.held, &mver{seq: seq, bucket: c.k.bucket, key: c.k.key, data: true, kind: mLive})
	}
	m.did("undelete of %s/%s from %s: %+v", c.k.bucket, c.k.key, c.pl.name, res)
}

// Liveness (#108).

// settleRounds bounds the rounds settle gives the pool to empty its
// targets: a round per day is past the targets' retention (three days)
// and a denied delete's retry (one) after four, and drain-back, the
// metadata drops behind it and the tombs behind those take a round
// each.
const settleRounds = 8

// settle checks liveness once the random steps are done (#108): the
// pool is made quiescent, with friends holding everything (every place
// in the pool answers, refuses nothing, is not gone, its replicas
// verified; no seed pending; at least as many friends as any bucket's
// copies, each with room for the whole repository), and from here no
// send fails and no delete is refused. Then at most settleRounds rounds
// of what a daemon does run, each a day after the last: a pass, every
// FIFO sent until it is empty or held, the tombs due issued, the
// removals finished. Each round is checked as a step is (I1, I2). After
// them every target still in the pool holds nothing: no key whose
// latest row there is a put, no delete still to send, no tomb, no use
// counted (what status shows), and nothing the model believes it holds
// but versions behind delete markers.
func (m *removalModel) settle() {
	ctx := context.Background()
	m.calm = true
	m.step++
	m.what, m.exempt = "settle:", map[string]bool{}
	for len(m.inPool(friends)) < 3 {
		m.addFriendOf(1000)
	}
	for _, pl := range m.inPool(friends) {
		_, err := m.p.db.Exec(`UPDATE friendship SET own_number = ?1, peer_number = ?1 WHERE peer_key = ?2`,
			1000*unit, pl.name)
		m.must(err)
	}
	for {
		before := m.what
		m.confirmSeed()
		if strings.HasSuffix(m.what, "no seed pending") {
			break
		}
		if m.what == before {
			m.fail("confirming a seed changed nothing")
		}
	}
	m.log = append(m.log, fmt.Sprintf("%d %s: %s", m.step, m.p.now.UTC().Format("01-02 15:04"), m.what))
	m.check()
	for round := 1; round <= settleRounds; round++ {
		m.step++
		m.what, m.exempt = fmt.Sprintf("settle round %d:", round), map[string]bool{}
		m.p.now = m.p.now.Add(25 * time.Hour)
		for _, pl := range m.inPool(nil) {
			m.must(m.p.db.SetPlaceCondition(ctx, pl.name, ConditionOK, "", m.p.now))
			pl.answering, pl.refuses = true, false
			for _, b := range m.buckets {
				m.must(m.p.db.RecordVerified(ctx, pl.name, b, true, m.p.now))
			}
		}
		m.replace()
		for sweep := 0; sweep < 100; sweep++ {
			took := 0
			for _, pl := range m.inPool(func(pl *mplace) bool { return pl.answering }) {
				took += m.sendTo(pl, 1000)
			}
			if took == 0 {
				break
			}
		}
		m.issueTombs()
		m.finishRemovals()
		m.log = append(m.log, fmt.Sprintf("%d %s: %s", m.step, m.p.now.UTC().Format("01-02 15:04"), m.what))
		m.check()
		if m.t.Failed() {
			return
		}
	}
	for _, pl := range m.inPool(targets) {
		// What it holds or will: each key whose latest row there is a
		// put not deleted, and each delete still to send.
		var rows, tombs int
		m.must(m.p.db.QueryRow(`SELECT count(*) FROM placement p WHERE p.place = ?
			AND (p.op = 'put' AND p.state IN ('queued', 'acked', 'dropping') AND NOT EXISTS (SELECT 1 FROM placement l
				WHERE l.place = p.place AND l.bucket = p.bucket AND l.key = p.key AND l.seq > p.seq)
			OR p.op = 'delete' AND p.state = 'queued')`, pl.name).Scan(&rows))
		m.must(m.p.db.QueryRow(`SELECT count(*) FROM overflow_tomb WHERE place = ?`, pl.name).Scan(&tombs))
		var held []string
		for _, v := range pl.held {
			if v.kind != mMarker && v.kind != mDrainMarker {
				held = append(held, fmt.Sprintf("%s/%s (%d) kind %d", v.bucket, v.key, v.seq, v.kind))
			}
		}
		use := m.p.use(pl.name)
		if rows != 0 || tombs != 0 || use != 0 || len(held) != 0 {
			all, err := column(ctx, m.p.db, `SELECT p.bucket || '/' || p.key || ' (' || p.seq || ') ' || q.class || ' ' || p.op || ' ' || p.state
				FROM placement p JOIN queue q ON q.seq = p.seq
				WHERE p.place = ? AND p.state IN ('queued', 'acked', 'dropping') ORDER BY p.seq`, pl.name)
			m.must(err)
			m.fail("liveness: %s not empty after %d quiescent rounds: %d rows, %d tombs, use %d, holding %v\n%s",
				pl.name, settleRounds, rows, tombs, use, held, strings.Join(all, "\n"))
		}
	}
}

// The checks.

type rowKey struct {
	place string
	seq   int64
}

// rowInfo is what the checks read of a changed row's object.
type rowInfo struct {
	bucket, key string
	data, live  bool
}

// change is a put row's state before and after a step: "" where it
// had no row.
type change struct {
	rowKey
	was, now string
}

// changes reads and clears the step's changes of put rows, one per row,
// in sequence and place order.
func (m *removalModel) changes() []change {
	rows, err := m.p.db.Query(`SELECT place, seq, coalesce(old, ''), coalesce(new, '') FROM model_change ORDER BY id`)
	m.must(err)
	byRow := map[rowKey]*change{}
	var out []*change
	for rows.Next() {
		var c change
		m.must(rows.Scan(&c.place, &c.seq, &c.was, &c.now))
		if o := byRow[c.rowKey]; o != nil {
			o.now = c.now
			continue
		}
		byRow[c.rowKey] = &c
		out = append(out, &c)
	}
	rows.Close()
	m.must(rows.Err())
	_, err = m.p.db.Exec(`DELETE FROM model_change`)
	m.must(err)
	var cs []change
	for _, c := range out {
		if c.was != c.now {
			cs = append(cs, *c)
		}
	}
	slices.SortFunc(cs, func(a, b change) int {
		if a.seq != b.seq {
			return int(a.seq - b.seq)
		}
		return strings.Compare(a.place, b.place)
	})
	return cs
}

// infos reads the objects of rows seqs: bucket, key, class, and
// whether each is live (its key's latest done write).
func (m *removalModel) infos(seqs []int64) map[int64]rowInfo {
	list, err := json.Marshal(seqs)
	m.must(err)
	rows, err := m.p.db.Query(`SELECT q.seq, q.bucket, q.key, q.class = 'data', q.state = 'done' AND `+liveQ+`
		FROM queue q WHERE q.seq IN (SELECT value FROM json_each(?))`, string(list))
	m.must(err)
	defer rows.Close()
	out := map[int64]rowInfo{}
	for rows.Next() {
		var seq int64
		var r rowInfo
		m.must(rows.Scan(&seq, &r.bucket, &r.key, &r.data, &r.live))
		out[seq] = r
	}
	m.must(rows.Err())
	return out
}

// intactCopies counts row seq's acknowledged copies at the places
// intact holds true.
func (m *removalModel) intactCopies(seq int64, intact map[string]bool) int {
	acked, err := column(context.Background(), m.p.db, `SELECT place FROM placement WHERE seq = ? AND state = 'acked'`, seq)
	m.must(err)
	n := 0
	for _, a := range acked {
		if intact[a] {
			n++
		}
	}
	return n
}

// checkSend checks I1 as the delete of pl's dropping copy p is sent
// (#104), whether or not the send then fails: the live object keeps
// `copies` acknowledged copies at intact places without it.
func (m *removalModel) checkSend(pl *mplace, p Placement) {
	m.t.Helper()
	r := m.infos([]int64{p.Seq})[p.Seq]
	if !r.data || !r.live {
		return
	}
	bk, err := m.p.db.GetBucket(context.Background(), p.Bucket)
	m.must(err)
	if n := m.intactCopies(p.Seq, m.intact(p.Bucket)); n < bk.Copies {
		m.log = append(m.log, fmt.Sprintf("%d %s: %s", m.step, m.p.now.UTC().Format("01-02 15:04"), m.what))
		m.fail("I1: %s's delete of its copy of %s/%s (%d) sent, leaving %d acknowledged copies at intact places, copies %d",
			pl.name, p.Bucket, p.Key, p.Seq, n, bk.Copies)
	}
}

// intact reports, per place, whether it is intact for bucket (I1): in
// the pool, not refusing this node, not gone past replace_after, not a
// target being removed, its replica of the bucket not unverified.
func (m *removalModel) intact(bucket string) map[string]bool {
	ctx := context.Background()
	ps, err := poolPlaces(ctx, m.p.db)
	m.must(err)
	unverified, err := unverifiedAt(ctx, m.p.db, bucket)
	m.must(err)
	out := map[string]bool{}
	for _, p := range ps {
		out[p.peer] = p.cond != ConditionRemoved && !p.gone(m.p.db, m.p.now) && !p.removing && !unverified[p.peer]
	}
	return out
}

// holdsData reports whether the model believes pl holds, or can still
// undelete, data of bucket (I2), or has a copy of a live data object of
// it queued there. A copy re-placement is deleting there (dropping)
// whose object was deleted since holds nothing an undelete restores
// there: its delete goes as a drop, whose tomb undelete leaves (the
// object's other copies bring it back), as for any drained copy.
func (m *removalModel) holdsData(pl *mplace, bucket string) bool {
	for _, v := range pl.held {
		if !v.data || v.bucket != bucket {
			continue
		}
		switch v.kind {
		case mLive:
			var leaving bool
			m.must(m.p.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM placement p JOIN queue q ON q.seq = p.seq
				WHERE p.place = ? AND p.seq = ? AND p.state = 'dropping' AND NOT `+liveQ+`)`, pl.name, v.seq).Scan(&leaving))
			if !leaving {
				return true
			}
		case mTomb, mDenied:
			return true
		case mMarker:
			if v.until.After(m.p.now) {
				return true
			}
		}
	}
	var queued bool
	m.must(m.p.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.bucket = ? AND p.state = 'queued' AND p.op = 'put' AND q.class = 'data' AND `+liveQ+`)`,
		pl.name, bucket).Scan(&queued))
	return queued
}

// describe lists what the model believes pl holds of bucket's data.
func (m *removalModel) describe(pl *mplace, bucket string) string {
	kinds := []string{"live", "tomb", "drained tomb", "marker", "drained marker", "denied", "kept", "friend tomb"}
	var out []string
	for _, v := range pl.held {
		if v.data && v.bucket == bucket {
			d := fmt.Sprintf("%s (%d) %s", v.key, v.seq, kinds[v.kind])
			if v.kind == mTomb || v.kind == mDrainTomb || v.kind == mDenied {
				d += fmt.Sprintf(" row %d", v.tomb)
			}
			if v.kind == mMarker || v.kind == mDrainMarker {
				d += " until " + v.until.UTC().Format("01-02 15:04")
			}
			out = append(out, d)
		}
	}
	rows, err := m.p.db.Query(`SELECT p.key || ' (' || p.seq || ') ' || p.op || ' ' || p.state FROM placement p JOIN queue q ON q.seq = p.seq
		WHERE p.place = ? AND p.bucket = ? AND q.class = 'data' ORDER BY p.seq`, pl.name, bucket)
	m.must(err)
	defer rows.Close()
	var db []string
	for rows.Next() {
		var r string
		m.must(rows.Scan(&r))
		db = append(db, r)
	}
	return strings.Join(out, ", ") + "; its data rows: " + strings.Join(db, ", ")
}

// check looks at the step's changes of put rows: I1, I2, and every copy
// acknowledged in the step one the model sent.
func (m *removalModel) check() {
	m.t.Helper()
	changed := m.changes()
	if len(changed) == 0 {
		return
	}
	copies := map[string]int{}
	for _, b := range m.buckets {
		bk, err := m.p.db.GetBucket(context.Background(), b)
		m.must(err)
		copies[b] = bk.Copies
	}
	intact := map[string]map[string]bool{}
	var seqs []int64
	for _, c := range changed {
		seqs = append(seqs, c.seq)
	}
	info := m.infos(seqs)
	for _, c := range changed {
		k, was, st := c.rowKey, c.was, c.now
		if st == "" {
			st = "gone"
		}
		r := info[k.seq]
		if st == PlacementAcked && r.live {
			// Every acknowledged copy of a live object is one the model
			// sent.
			found := false
			if pl := m.byName[k.place]; pl != nil {
				for _, v := range pl.held {
					found = found || v.seq == k.seq
				}
			}
			if !found {
				m.fail("%s acknowledged %s/%s (%d), which the model never sent there", k.place, r.bucket, r.key, k.seq)
			}
		}
		if m.exempt[k.place] || !r.live {
			continue
		}
		switch {
		case r.data && (was == PlacementAcked || was == PlacementDropping && st == PlacementDrained):
			// I1, also as a dropping copy's delete is acknowledged: it
			// was sent in this step (#104).
			if intact[r.bucket] == nil {
				intact[r.bucket] = m.intact(r.bucket)
			}
			if n := m.intactCopies(k.seq, intact[r.bucket]); n < copies[r.bucket] {
				m.fail("I1: %s's copy of %s/%s (%d) went from %s to %s, leaving %d acknowledged copies at intact places, copies %d",
					k.place, r.bucket, r.key, k.seq, was, st, n, copies[r.bucket])
			}
		case !r.data && (was == PlacementAcked || was == PlacementQueued) && st != PlacementAcked && st != PlacementQueued:
			// I2.
			pl := m.byName[k.place]
			if pl == nil || !pl.target || pl.keepData || pl.out {
				continue
			}
			if m.holdsData(pl, r.bucket) {
				m.fail("I2: %s's metadata %s/%s (%d) went from %s to %s while it holds data of %s: %s",
					k.place, r.bucket, r.key, k.seq, was, st, r.bucket, m.describe(pl, r.bucket))
			}
		}
	}
}
