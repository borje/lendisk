// Package verify is the borrower's side of verification (VER-1, VER-2,
// VER-3, VER-4, S3-6; DESIGN §3.6, §3.11): the daily challenge of each
// friend's replica of each bucket, the checks of overflow targets by
// their stored checksums and a weekly downloaded sample (overflow.go),
// and the checks `lendisk verify` asks the daemon for.
//
// A challenge names one of the node's objects that the friend
// acknowledged (state.DB.ChallengeableAt), by the bucket's sealed name
// (SEC-16), a random byte range of it and a fresh random nonce; the
// friend answers HMAC-SHA256 keyed with the nonce over those bytes as it
// holds them (control.ChallengePath), and the node computes the same
// over its own copy. A full check asks for every acknowledged object,
// whole, so the friend reads every byte while none crosses the tunnel.
//
// Outcomes, recorded in lendisk.db:
//   - every answer right: the replica is verified now (a full check also
//     clears an unverified mark);
//   - an answer that the friend does not hold the bytes, or a wrong
//     HMAC over bytes the node's own copy still holds as captured: a
//     failed proof. The replica is marked unverified, Alerted is called,
//     and re-placement moves its copies (state, REPL-13);
//   - no answer (a network error, a refusal, a node without the call):
//     nothing is decided; the replica is due again an hour later;
//   - an object whose local copy is gone (a recovered node, EP-10) or no
//     longer matches its capture hash (VER-5's business) is skipped: it
//     cannot tell the friend's fault.
//
// Rules a caller must keep:
//   - Randomness and the clock are fields (Random, Now): tests set them.
//   - The verifier reads the store and lendisk.db; it never writes the
//     store. It dials friends through Dial, the daemon's tunnel, so a
//     one-shot command asks it through lendisk.db (state.RequestVerify)
//     rather than dialling with the node's keys itself.
package verify

import (
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/pace"
	"github.com/borje/lendisk/internal/state"
)

const (
	// DefaultEvery is how often each replica is challenged (VER-1).
	DefaultEvery = 24 * time.Hour
	// DefaultRetry is how soon a replica whose challenge got no answer
	// is challenged again.
	DefaultRetry = time.Hour
	// DefaultPoll is how often Run looks for due replicas and requests.
	DefaultPoll = 5 * time.Second
	// DefaultSamples is how many distinct objects one daily challenge of
	// a replica names: a friend that lost half of them is caught with
	// probability 1 − 2⁻⁸ each day.
	DefaultSamples = 8
	// DefaultRange is the longest byte range a daily challenge names.
	DefaultRange = 64 << 10
	// callTimeout bounds one ranged challenge; a whole-object one gets
	// a second more per MiB.
	callTimeout = 30 * time.Second
	// maxWait is how long a full check waits out the friend's pacing
	// before one object, beyond what the largest object it read before
	// takes at WholeRate.
	maxWait = 5 * time.Minute
	// DefaultWholeRate is the slowest pace of a friend's whole-object
	// reads a full check waits for: a quarter of the lender's default
	// (control.DefaultWholeRate).
	DefaultWholeRate = 1 << 20
	// pageSize is how many objects a full check reads from lendisk.db at
	// once.
	pageSize = 256
)

// DialFunc opens a connection to a friend's tunnel port
// (transport.Transport.Dial).
type DialFunc func(ctx context.Context, peer identity.NodeID, addr string) (net.Conn, error)

// Verifier challenges friends' replicas (package comment).
type Verifier struct {
	DB       *state.DB
	StoreDir string // the posix store: an object is <StoreDir>/<bucket>/<key>
	Self     *identity.Identity
	// Dial reaches a friend's tunnel port; Call uses it when nil.
	Dial DialFunc
	// Call makes one challenge of friend f; nil means the control API
	// over Dial. Tests set it.
	Call func(ctx context.Context, f state.Friendship, req control.ChallengeRequest) (control.ChallengeResponse, error)
	// Random is the source of nonces and of the objects and ranges
	// challenged; nil means crypto/rand.
	Random io.Reader
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Every, Retry and Poll: DefaultEvery, DefaultRetry and DefaultPoll
	// when zero.
	Every, Retry, Poll time.Duration
	// Samples and Range: DefaultSamples and DefaultRange when zero.
	Samples int
	Range   int64
	// WholeRate is DefaultWholeRate when zero (bytes per second).
	WholeRate int64
	// Sleep waits out the friend's pacing; nil means a timer. Tests
	// advance their clock in it.
	Sleep func(ctx context.Context, d time.Duration) error
	// Alerted, if set, is called after a replica is marked unverified,
	// so the alert service reads it at once. It must not block.
	Alerted func()
	// Overflow builds an overflow target's client (VER-2, S3-6, #39):
	// the daemon's reads the credential from the environment, as the
	// worker's does. Nil leaves overflow targets unchecked.
	Overflow func(o state.Overflow) (Target, error)
	// Pace paces the sample's downloads by the download limits
	// (REPL-7); nil paces nothing.
	Pace *pace.Pacer
	// SamplePercent is the share of each overflow target's bytes
	// downloaded a week (DefaultSamplePercent when zero); SampleEvery
	// how often the sample steps (DefaultSampleEvery when zero).
	SamplePercent float64
	SampleEvery   time.Duration
	Log           *slog.Logger

	once sync.Once
	kick chan struct{}
	// tried is when each overflow target's versioning read last got no
	// answer (versioning.go).
	triedMu sync.Mutex
	tried   map[string]time.Time
}

func or[T int | int64 | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

func (v *Verifier) now() time.Time {
	if v.Now == nil {
		return time.Now()
	}
	return v.Now()
}

func (v *Verifier) random() io.Reader {
	if v.Random == nil {
		return cryptorand.Reader
	}
	return v.Random
}

func (v *Verifier) init() { v.once.Do(func() { v.kick = make(chan struct{}, 1) }) }

// Kick asks Run for a pass now (a request was recorded). It never
// blocks.
func (v *Verifier) Kick() {
	v.init()
	select {
	case v.kick <- struct{}{}:
	default:
	}
}

// Run challenges due replicas and runs requested checks until ctx ends.
func (v *Verifier) Run(ctx context.Context) {
	v.init()
	t := time.NewTicker(or(v.Poll, DefaultPoll))
	defer t.Stop()
	for {
		v.pass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-v.kick:
		}
	}
}

// pass runs every pending request, then every due replica's challenge.
func (v *Verifier) pass(ctx context.Context) {
	reqs, err := v.DB.PendingVerifies(ctx)
	if err != nil {
		if ctx.Err() == nil {
			v.Log.Error("verification: reading the requested checks failed", "err", err)
		}
		return
	}
	for _, r := range reqs {
		o := v.Check(ctx, r.Place, r.Bucket, r.Full)
		if ctx.Err() != nil {
			return
		}
		if err := v.DB.FinishVerify(ctx, r.ID, o, v.now()); err != nil {
			v.Log.Error("verification: recording a requested check failed", "err", err)
		}
	}
	due, err := v.DB.DueReplicas(ctx, v.now(), or(v.Every, DefaultEvery), or(v.Retry, DefaultRetry))
	if err != nil {
		if ctx.Err() == nil {
			v.Log.Error("verification: reading the replicas due failed", "err", err)
		}
		return
	}
	for _, k := range due {
		v.Check(ctx, k.Place, k.Bucket, false)
		if ctx.Err() != nil {
			return
		}
	}
	v.sampleTargets(ctx)
	v.recheckVersioning(ctx)
}

// Check challenges place's replica of bucket now, ranged (VER-1) or full
// (VER-3), records the outcome, and returns it.
func (v *Verifier) Check(ctx context.Context, place, bucket string, full bool) state.VerifyOutcome {
	log := v.Log.With("peer", place, "bucket", bucket, "full", full)
	o, err := v.check(ctx, place, bucket, full)
	now := v.now()
	var rerr error
	switch {
	case ctx.Err() != nil:
		return state.VerifyOutcome{Outcome: state.VerifyError, Detail: "stopped"}
	case err != nil:
		o = state.VerifyOutcome{Outcome: state.VerifyError, Checked: o.Checked, Skipped: o.Skipped, Detail: err.Error()}
		log.Warn("verification: the challenge got no answer; nothing is decided, and it is tried again within the hour", "err", err)
		rerr = v.DB.RecordCheckError(ctx, place, bucket, err.Error(), now)
	case o.Outcome == state.VerifyFailed:
		log.Error("verification failed: the friend does not hold what it acknowledged; its replica is unverified and its copies move to other friends",
			"alert", true, "detail", o.Detail)
		rerr = v.DB.RecordUnverified(ctx, place, bucket, o.Detail, now)
		if rerr == nil && v.Alerted != nil {
			v.Alerted()
		}
	case o.Outcome == state.VerifyNothing:
		log.Info("verification: nothing this node can check at the friend (no local copy to compare with)", "skipped", o.Skipped)
		rerr = v.DB.RecordCheckError(ctx, place, bucket, "nothing to check: no object held there has a local copy", now)
	default:
		log.Info("verification passed", "objects", o.Checked, "skipped", o.Skipped)
		rerr = v.DB.RecordVerified(ctx, place, bucket, full, now)
	}
	if rerr != nil {
		log.Error("verification: recording the outcome failed", "err", rerr)
	}
	return o
}

// errNoAnswer marks a challenge that decided nothing.
var errNoAnswer = errors.New("no answer")

// check runs the challenges. An error is no answer; a failed proof is an
// outcome.
func (v *Verifier) check(ctx context.Context, place, bucket string, full bool) (state.VerifyOutcome, error) {
	var o state.VerifyOutcome
	if _, ok := state.OverflowName(place); ok {
		return v.checkTarget(ctx, place, bucket, full)
	}
	f, err := v.DB.GetFriendship(ctx, place)
	if err != nil {
		return o, fmt.Errorf("the friendship: %w", err)
	}
	call, done := v.caller(f)
	defer done()
	var largest int64 // the largest object read whole so far: the friend's debt for it is waited out
	try := func(c state.Challengeable, whole bool) (bool, error) {
		wait := maxWait + time.Duration(float64(largest)/float64(or(v.WholeRate, DefaultWholeRate))*float64(time.Second))
		if whole {
			largest = max(largest, c.Size)
		}
		failed, skipped, err := v.challenge(ctx, call, f, bucket, c, whole, wait)
		switch {
		case err != nil:
			return false, err
		case skipped:
			o.Skipped++
		case failed != "":
			o.Checked++
			o.Outcome, o.Detail = state.VerifyFailed, failed
			return true, nil
		default:
			o.Checked++
		}
		return false, nil
	}
	if full {
		var after int64
		for {
			page, err := v.DB.ChallengeablePage(ctx, place, bucket, after, pageSize)
			if err != nil {
				return o, err
			}
			if len(page) == 0 {
				break
			}
			for _, c := range page {
				after = c.Seq
				if stop, err := try(c, true); stop || err != nil {
					return o, err
				}
			}
		}
	} else {
		n, err := v.DB.ChallengeableCount(ctx, place, bucket)
		if err != nil {
			return o, err
		}
		picks, err := v.pick(n, or(v.Samples, DefaultSamples))
		if err != nil {
			return o, err
		}
		for _, i := range picks {
			c, err := v.DB.ChallengeableAt(ctx, place, bucket, i)
			if errors.Is(err, state.ErrNotFound) {
				continue // deleted or written again since the count
			}
			if err != nil {
				return o, err
			}
			if stop, err := try(c, false); stop || err != nil {
				return o, err
			}
		}
	}
	switch {
	case o.Checked > 0:
		o.Outcome = state.VerifyPassed
	default:
		o.Outcome = state.VerifyNothing
	}
	return o, nil
}

// rng returns a generator seeded from Random.
func (v *Verifier) rng() (*rand.Rand, error) {
	var seed [32]byte
	if _, err := io.ReadFull(v.random(), seed[:]); err != nil {
		return nil, err
	}
	return rand.New(rand.NewChaCha8(seed)), nil
}

// pick returns k distinct indices below n at random, all of them when n
// is k or fewer.
func (v *Verifier) pick(n int64, k int) ([]int64, error) {
	if n <= int64(k) {
		out := make([]int64, n)
		for i := range out {
			out[i] = int64(i)
		}
		return out, nil
	}
	r, err := v.rng()
	if err != nil {
		return nil, err
	}
	seen := map[int64]bool{}
	var out []int64
	for len(out) < k {
		i := r.Int64N(n)
		if !seen[i] {
			seen[i] = true
			out = append(out, i)
		}
	}
	return out, nil
}

type callFunc func(ctx context.Context, req control.ChallengeRequest) (control.ChallengeResponse, error)

// caller returns how to challenge f, and what to call when done.
func (v *Verifier) caller(f state.Friendship) (callFunc, func()) {
	if v.Call != nil {
		return func(ctx context.Context, req control.ChallengeRequest) (control.ChallengeResponse, error) {
			return v.Call(ctx, f, req)
		}, func() {}
	}
	peer, perr := identity.ParseNodeID(f.Peer)
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		if perr != nil {
			return nil, perr
		}
		return v.Dial(ctx, peer, f.Address)
	}}
	cl := &control.Client{HTTP: &http.Client{Transport: tr}}
	return cl.Challenge, tr.CloseIdleConnections
}

// challenge challenges f for object c of bucket, ranged or whole,
// waiting out the friend's pacing of whole-object reads for at most
// wait. It returns what failed ("" when the answer was right), or
// skipped when the local copy cannot tell or the object changed while
// it was challenged, or an error when nothing was decided.
func (v *Verifier) challenge(ctx context.Context, call callFunc, f state.Friendship, bucket string, c state.Challengeable,
	whole bool, wait time.Duration) (failed string, skipped bool, err error) {
	nonce := make([]byte, 32)
	if _, err := io.ReadFull(v.random(), nonce); err != nil {
		return "", false, err
	}
	req := control.ChallengeRequest{Bucket: v.Self.SealedName(bucket), Key: c.Key, Nonce: nonce, Whole: whole}
	timeout := callTimeout
	if whole {
		timeout += time.Duration(c.Size>>20) * time.Second
	} else {
		r, err := v.rng()
		if err != nil {
			return "", false, err
		}
		req.Offset = r.Int64N(c.Size)
		req.Length = 1 + r.Int64N(min(c.Size-req.Offset, or(v.Range, DefaultRange)))
	}
	path := filepath.Join(v.StoreDir, bucket, filepath.FromSlash(c.Key))
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return "", true, nil // no local copy to compare with
	}
	// The friend is asked first, and the local copy read after its
	// answer: what the friend held is compared with what this node holds
	// then, and any wait for the friend's pacing comes before both.
	var resp control.ChallengeResponse
	waited := time.Duration(0)
	for {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		resp, err = call(cctx, req)
		cancel()
		var ce *control.CallError
		if whole && errors.As(err, &ce) && ce.Status == http.StatusServiceUnavailable && waited+ce.RetryAfter <= wait {
			// A full check is paced by the friend (control's whole-object
			// budget): wait as long as it says, within wait.
			d := max(ce.RetryAfter, time.Second)
			waited += d
			if err := v.sleep(ctx, d); err != nil {
				return "", false, err
			}
			continue
		}
		break
	}
	what := fmt.Sprintf("bytes %d to %d of %s", req.Offset, req.Offset+req.Length-1, c.Key)
	if whole {
		what = c.Key
	}
	failed = fmt.Sprintf("friend %s answered for other bytes than it acknowledged at %s", f.Peer, what)
	switch {
	case control.FailedProof(err):
		failed = fmt.Sprintf("friend %s does not hold %s, which it acknowledged: %v", f.Peer, what, err)
	case err != nil:
		return "", false, fmt.Errorf("%w: challenging %s: %w", errNoAnswer, what, err)
	default:
		want, ok, err := localMAC(path, req.Nonce, req.Offset, req.Length, whole)
		if err != nil {
			return "", false, fmt.Errorf("reading this node's copy of %s: %w", c.Key, err)
		}
		if !ok {
			return "", true, nil // gone or shorter since: written again or deleted
		}
		if hmac.Equal(resp.HMAC, want) {
			return "", false, nil
		}
		// A wrong answer: unless this node's own copy changed since
		// capture (VER-5's to find), the friend holds other bytes.
		if same, err := matches(path, c.SHA256); err != nil {
			return "", false, fmt.Errorf("re-reading this node's copy of %s: %w", c.Key, err)
		} else if !same {
			v.Log.Warn("verification: this node's copy of an object no longer matches its capture hash; the friend's answer cannot be judged",
				"bucket", bucket, "key", c.Key)
			return "", true, nil
		}
	}
	// Before a failure is recorded, the object must still be the one the
	// friend holds: a delete, a rewrite or re-placement's drop that
	// reached it while the challenge was in flight explains its answer.
	still, err := v.DB.StillChallengeable(ctx, f.Peer, c.Seq)
	if err != nil {
		return "", false, err
	}
	if !still {
		v.Log.Info("verification: the object changed while it was challenged; skipped", "bucket", bucket, "key", c.Key)
		return "", true, nil
	}
	return failed, false, nil
}

// sleep waits d, or less if ctx ends.
func (v *Verifier) sleep(ctx context.Context, d time.Duration) error {
	if v.Sleep != nil {
		return v.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// localMAC is the challenge's HMAC over the local copy at path; ok is
// false when there is none, or it is shorter than the range.
func localMAC(path string, nonce []byte, offset, length int64, whole bool) (mac []byte, ok bool, err error) {
	fh, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer fh.Close()
	m := hmac.New(sha256.New, nonce)
	var src io.Reader = fh
	if !whole {
		src = io.NewSectionReader(fh, offset, length)
	}
	n, err := io.Copy(m, src)
	if err != nil {
		return nil, false, err
	}
	if !whole && n != length {
		return nil, false, nil
	}
	return m.Sum(nil), true, nil
}

// matches reports whether the file at path hashes to sha (hex).
func matches(path, sha string) (bool, error) {
	fh, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer fh.Close()
	h := sha256.New()
	if _, err := io.Copy(h, fh); err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == sha, nil
}
