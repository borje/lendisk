package verify

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/state"
)

// Overflow targets (VER-2, S3-6, VER-3, #39). A provider cannot answer
// a challenge, so a target's replicas are checked two ways:
//
//   - Daily, like a friend's, by the provider's stored checksums
//     (S3-6): DefaultSamples random objects of the replica are read with
//     a HEAD. lendisk's hash in the object's metadata and the provider's
//     stored SHA-256 of the whole object, where it keeps one, must be
//     the index's; where it keeps none and the ETag is the MD5 of a
//     single-part object, the ETag must be the MD5 of the local copy
//     (skipped, as for a challenge, when the local copy is gone or no
//     longer matches its capture hash). A multipart object's ETag and
//     composite checksum are never compared. This trusts the provider's word
//     for its checksums, which S3-6 accepts.
//   - Each week, by downloads (VER-2): SamplePercent of the bytes each
//     target holds is downloaded whole, at random, through Pace.Fetch
//     (REPL-7's download limits), hashed and compared with the index.
//     Stepped every SampleEvery; what a step owes is kept in lendisk.db.
//
// `lendisk verify --full` at a target downloads every object of the
// replica and hashes it (VER-3). Outcomes are recorded as a friend's:
// a missing object or other bytes marks the replica unverified, which
// alerts and drains its copies (REPL-13); an error decides nothing.

const (
	// DefaultSamplePercent is the share of a target's bytes downloaded
	// each week (VER-2).
	DefaultSamplePercent = 0.1
	// DefaultSampleEvery is how often the sample steps.
	DefaultSampleEvery = 10 * time.Minute
	week               = 7 * 24 * time.Hour
)

// Target is an overflow target as verification reads it:
// replica.Client is one.
type Target interface {
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	Stat(ctx context.Context, bucket, key string) (replica.ObjectStat, bool, error)
	// Versioning reads the bucket's versioning as state.Versioning…
	// (versioning.go).
	Versioning(ctx context.Context) (string, error)
}

// target builds place's client.
func (v *Verifier) target(ctx context.Context, place string) (Target, error) {
	name, _ := state.OverflowName(place)
	if v.Overflow == nil {
		return nil, errors.New("overflow targets are not checked here")
	}
	o, err := v.DB.GetOverflow(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("overflow target %s: %w", name, err)
	}
	return v.Overflow(o)
}

// checkTarget is check for an overflow target.
func (v *Verifier) checkTarget(ctx context.Context, place, bucket string, full bool) (state.VerifyOutcome, error) {
	var o state.VerifyOutcome
	t, err := v.target(ctx, place)
	if err != nil {
		return o, err
	}
	try := func(c state.Challengeable, whole bool) (bool, error) {
		var failed string
		var skipped bool
		var err error
		if whole {
			failed, skipped, err = v.download(ctx, t, place, bucket, c)
		} else {
			failed, skipped, err = v.stat(ctx, t, place, bucket, c)
		}
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
			if err != nil || len(page) == 0 {
				if err != nil {
					return o, err
				}
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
				continue
			}
			if err != nil {
				return o, err
			}
			if stop, err := try(c, false); stop || err != nil {
				return o, err
			}
		}
	}
	if o.Checked > 0 {
		o.Outcome = state.VerifyPassed
	} else {
		o.Outcome = state.VerifyNothing
	}
	return o, nil
}

// confirm returns failed once the object is still the one the target
// acknowledged; skipped when a delete, a rewrite or re-placement's drop
// reached it meanwhile, which explains the answer (#27 review).
func (v *Verifier) confirm(ctx context.Context, place, bucket string, c state.Challengeable, failed string) (string, bool, error) {
	still, err := v.DB.StillChallengeable(ctx, place, c.Seq)
	if err != nil {
		return "", false, err
	}
	if !still {
		v.Log.Info("verification: the object changed while it was checked; skipped", "place", place, "bucket", bucket, "key", c.Key)
		return "", true, nil
	}
	return failed, false, nil
}

// stat compares what the target stores about c with the index and the
// local copy (S3-6).
func (v *Verifier) stat(ctx context.Context, t Target, place, bucket string, c state.Challengeable) (failed string, skipped bool, err error) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	st, ok, err := t.Stat(cctx, bucket, c.Key)
	cancel()
	switch {
	case err != nil:
		return "", false, fmt.Errorf("%w: asking %s for %s: %w", errNoAnswer, place, c.Key, err)
	case !ok:
		return v.confirm(ctx, place, bucket, c, fmt.Sprintf("overflow target %s does not hold %s, which it acknowledged", place, c.Key))
	case st.Size != c.Size:
		return v.confirm(ctx, place, bucket, c, fmt.Sprintf("overflow target %s holds %s with %d bytes, not %d", place, c.Key, st.Size, c.Size))
	case st.SHA256 != "" && st.SHA256 != c.SHA256:
		return v.confirm(ctx, place, bucket, c, fmt.Sprintf("overflow target %s holds another version of %s (%s)", place, c.Key, st.SHA256))
	case st.Multipart || st.Composite:
		// A multipart object's ETag and composite checksum are of its
		// parts: comparing them would be a false mismatch, and one
		// drains the whole replica (#39 review).
		return "", false, nil
	case st.ChecksumSHA256 != "":
		if st.ChecksumSHA256 != c.SHA256 {
			return v.confirm(ctx, place, bucket, c, fmt.Sprintf("the provider's stored SHA-256 of %s at %s is %s, not %s",
				c.Key, place, st.ChecksumSHA256, c.SHA256))
		}
		return "", false, nil
	case st.MD5ETag():
		md5hex, ok, err := localMD5(filepath.Join(v.StoreDir, bucket, filepath.FromSlash(c.Key)), c.SHA256)
		if err != nil {
			return "", false, fmt.Errorf("reading this node's copy of %s: %w", c.Key, err)
		}
		if !ok {
			return "", true, nil // no local copy as captured to compare with
		}
		if md5hex != st.ETag {
			return v.confirm(ctx, place, bucket, c, fmt.Sprintf("the provider's ETag of %s at %s is %s, not the MD5 of this node's copy (%s)",
				c.Key, place, st.ETag, md5hex))
		}
	}
	return "", false, nil
}

// download reads c whole from the target through the download limits
// (VER-2, REPL-7) and hashes it against the index.
func (v *Verifier) download(ctx context.Context, t Target, place, bucket string, c state.Challengeable) (failed string, skipped bool, err error) {
	body, err := t.Get(ctx, bucket, c.Key)
	if replica.NotFound(err) {
		return v.confirm(ctx, place, bucket, c, fmt.Sprintf("overflow target %s does not hold %s, which it acknowledged", place, c.Key))
	}
	if err != nil {
		return "", false, fmt.Errorf("%w: downloading %s from %s: %w", errNoAnswer, c.Key, place, err)
	}
	defer body.Close()
	h := sha256.New()
	n, err := io.Copy(h, v.Pace.Fetch(ctx, place, body))
	if err != nil {
		return "", false, fmt.Errorf("%w: downloading %s from %s: %w", errNoAnswer, c.Key, place, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); n != c.Size || got != c.SHA256 {
		return v.confirm(ctx, place, bucket, c, fmt.Sprintf("overflow target %s served %d bytes of %s hashing to %s, not the %d acknowledged with %s",
			place, n, c.Key, got, c.Size, c.SHA256))
	}
	return "", false, nil
}

// localMD5 is the MD5 of the file at path, in hex; ok is false when it
// is gone or no longer hashes to sha (VER-5's to find).
func localMD5(path, sha string) (string, bool, error) {
	fh, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer fh.Close()
	m, s := md5.New(), sha256.New()
	if _, err := io.Copy(io.MultiWriter(m, s), fh); err != nil {
		return "", false, err
	}
	if hex.EncodeToString(s.Sum(nil)) != sha {
		return "", false, nil
	}
	return hex.EncodeToString(m.Sum(nil)), true, nil
}

// sampleTargets steps every overflow target's download sample (VER-2).
func (v *Verifier) sampleTargets(ctx context.Context) {
	if v.Overflow == nil {
		return
	}
	targets, err := v.DB.Overflows(ctx)
	if err != nil {
		if ctx.Err() == nil {
			v.Log.Error("verification: reading the overflow targets failed", "err", err)
		}
		return
	}
	for _, o := range targets {
		if ctx.Err() != nil {
			return
		}
		v.sampleStep(ctx, o)
	}
}

// sampleStep owes SamplePercent of o's bytes a week for the time since
// its last step, and downloads random objects until it has paid; the
// last may run over, and an empty one counts a byte. A clock set back
// restarts the count; what a step that got no answer still owes is
// kept for the next.
func (v *Verifier) sampleStep(ctx context.Context, o state.Overflow) {
	place, now := o.Place(), v.now()
	log := v.Log.With("place", place)
	at, owed, ok, err := v.DB.SampleStep(ctx, place)
	if err != nil {
		log.Error("verification: reading the overflow sample failed", "err", err)
		return
	}
	if !ok || now.Before(at) {
		if err := v.DB.SetSampleStep(ctx, place, now, 0); err != nil {
			log.Error("verification: recording the overflow sample failed", "err", err)
		}
		return
	}
	if now.Sub(at) < or(v.SampleEvery, DefaultSampleEvery) {
		return
	}
	n, bytes, err := v.DB.Sampleable(ctx, place)
	if err != nil {
		log.Error("verification: reading what the overflow target holds failed", "err", err)
		return
	}
	pct := v.SamplePercent
	if pct <= 0 {
		pct = DefaultSamplePercent
	}
	owed = min(owed+int64(float64(bytes)*pct/100*float64(now.Sub(at))/float64(week)), bytes)
	defer func() {
		if err := v.DB.SetSampleStep(context.WithoutCancel(ctx), place, now, owed); err != nil {
			log.Error("verification: recording the overflow sample failed", "err", err)
		}
	}()
	if owed <= 0 || n == 0 {
		return
	}
	t, err := v.Overflow(o)
	if err != nil {
		log.Warn("verification: cannot build the overflow target's client; the sample waits", "err", err)
		return
	}
	r, err := v.rng()
	if err != nil {
		return
	}
	for owed > 0 && ctx.Err() == nil {
		s, err := v.DB.SampleableAt(ctx, place, r.Int64N(n))
		if errors.Is(err, state.ErrNotFound) {
			return // fewer since the count: the next step
		}
		if err != nil {
			log.Error("verification: reading the overflow sample failed", "err", err)
			return
		}
		failed, skipped, err := v.download(ctx, t, place, s.Bucket, s.Challengeable)
		bl := log.With("bucket", s.Bucket, "key", s.Key)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			bl.Warn("verification: the overflow sample got no answer; nothing is decided, and it goes on at the next step", "err", err)
			if err := v.DB.RecordCheckError(ctx, place, s.Bucket, err.Error(), v.now()); err != nil {
				bl.Error("verification: recording the outcome failed", "err", err)
			}
			return
		}
		owed -= max(s.Size, 1)
		switch {
		case skipped:
		case failed != "":
			bl.Error("verification failed: the overflow target does not hold what it acknowledged; its replica is unverified and its copies move elsewhere",
				"alert", true, "detail", failed)
			if err := v.DB.RecordUnverified(ctx, place, s.Bucket, failed, v.now()); err != nil {
				bl.Error("verification: recording the outcome failed", "err", err)
			} else if v.Alerted != nil {
				v.Alerted()
			}
		default:
			bl.Debug("verification: an overflow sample matched the index", "bytes", s.Size)
			if err := v.DB.RecordVerified(ctx, place, s.Bucket, false, v.now()); err != nil {
				bl.Error("verification: recording the outcome failed", "err", err)
			}
		}
	}
}
