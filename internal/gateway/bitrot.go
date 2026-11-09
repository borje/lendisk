package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// The bit-rot sampler (VER-5, #31). It sweeps the store, own buckets and
// friends' replicas alike, one object after another in bucket then key
// order, and hashes each against what the index holds:
//
//   - an own bucket's key against its latest done put (the queue row's
//     SHA-256), if the key is held locally, has no write in flight and is
//     not held back by EP-11;
//   - a friend's object against the hash it was stored with (LEND-4,
//     x-amz-meta-lendisk-sha256 in its xattrs).
//
// It hashes, at each step (every Every), what it owes: Percent of the
// stored bytes (state.StoredBytes) for each week passed on its clock,
// never more than one cycle of the store in a step; the reads go through
// Pace (the disk's read limit, store.bitrot_read_limit). Its place
// (state.Sweep) survives a restart, so every object is checked once a
// cycle, the one checked longest ago first; the first cycle starts at a
// bucket Random picks.
//
// A mismatch is checked again holding the key's lock. An own object
// whose file changed (ctime) after this node last knew it (queue.local)
// is an edit outside the endpoint, not rot: it is enqueued as a write
// (#88). Otherwise it is rot, alerted (state.Rot, Alerted) and, when a
// friend holds the captured version, removed (queue.local cleared first,
// so the store reconciler does not take its absence for a delete, EP-14)
// and fetched again through the read-through (EP-10); with no friend
// holding it, kept as it is. A friend's rotten object is alerted and
// left alone: the lender cannot repair it, and the friend's challenges
// (VER-1) and the read-through's verification judge it there.

// DefaultSamplePercent is SampleSettings.Percent when zero: VER-5's
// least, 0.1% of the stored bytes a week.
const DefaultSamplePercent = 0.1

// DefaultSampleEvery is SampleSettings.Every when zero.
const DefaultSampleEvery = 10 * time.Minute

// samplePage is the keys one listing of the sweep reads.
const samplePage = 256

const sampleWeek = 7 * 24 * time.Hour

// SampleSettings configure the bit-rot sampler (VER-5).
type SampleSettings struct {
	// Percent is the share of the stored bytes hashed a week; zero is
	// DefaultSamplePercent.
	Percent float64
	// Every is the time between steps; zero is DefaultSampleEvery.
	Every time.Duration
	// Pace paces each read of the store (pace.Pacer.Scrub); nil reads
	// unpaced.
	Pace func(ctx context.Context, r io.Reader) io.Reader
	// Random picks where the first sweep starts; nil is crypto/rand.
	Random io.Reader
	// Alerted, if set, is called when a rotten object is found or its
	// record changes.
	Alerted func()
}

type sampler struct {
	d        *Decorator
	l        *Lending
	storeDir string
	SampleSettings
	log *slog.Logger
	now func() time.Time
	// checked, if set, is called with each object the sweep visits.
	// Tests only.
	checked func(bucket, key string)

	owed float64   // bytes the sample owes
	last time.Time // the last step
}

// storeBucket is a bucket in the store: an own one, or a friend's
// (borrower and sealed set).
type storeBucket struct{ name, borrower, sealed string }

func (b storeBucket) lent() bool { return b.borrower != "" }

// sampled is an object the sweep visits.
type sampled struct {
	bucket storeBucket
	key    string
	size   int64
}

func (s *sampler) percent() float64 {
	if s.Percent > 0 {
		return s.Percent
	}
	return DefaultSamplePercent
}

func (s *sampler) alerted() {
	if s.Alerted != nil {
		s.Alerted()
	}
}

// loop runs a step at once and every Every until ctx ends.
func (s *sampler) loop(ctx context.Context) {
	every := s.Every
	if every <= 0 {
		every = DefaultSampleEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := s.step(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("bit-rot: a sample step failed; trying again at the next", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// step follows up what earlier steps found, then hashes what the sample
// owes since the last step.
func (s *sampler) step(ctx context.Context) error {
	now := s.now()
	if err := s.followUp(ctx, now); err != nil {
		return err
	}
	if s.last.IsZero() {
		s.last = now
		return nil
	}
	elapsed := now.Sub(s.last)
	s.last = now
	if elapsed <= 0 {
		return nil
	}
	stored, err := s.d.DB.StoredBytes(ctx)
	if err != nil {
		return err
	}
	s.owed = min(s.owed+s.percent()/100*float64(stored)*elapsed.Seconds()/sampleWeek.Seconds(), float64(stored))
	if s.owed <= 0 {
		return nil
	}
	buckets, err := s.buckets(ctx)
	if err != nil || len(buckets) == 0 {
		return err
	}
	sw, ok, err := s.d.DB.Sweep(ctx)
	if err != nil {
		return err
	}
	if !ok {
		sw = state.Sweep{Bucket: buckets[s.pick(len(buckets))].name, CycleAt: now}
	}
	start := [2]string{sw.Bucket, sw.Key}
	w := &sweepWalk{s: s, buckets: buckets, at: start}
	wrapped := false
	for s.owed > 0 {
		o, wrap, ok, err := w.next(ctx)
		if err != nil || !ok {
			return err
		}
		if wrap {
			if wrapped {
				return nil
			}
			wrapped = true
			sw.Cycles++
			sw.CycleAt, sw.Objects, sw.Bytes = now, 0, 0
		}
		// Back past where the step began: the cycle is done for now.
		if wrapped && after([2]string{o.bucket.name, o.key}, start) {
			return s.d.DB.SaveSweep(ctx, sw)
		}
		if err := s.check(ctx, o); err != nil {
			return err
		}
		s.owed -= float64(max(o.size, 1))
		sw.Bucket, sw.Key = o.bucket.name, o.key
		sw.Objects++
		sw.Bytes += o.size
		sw.CheckedAt = now
		if err := s.d.DB.SaveSweep(ctx, sw); err != nil {
			return err
		}
	}
	return nil
}

// after reports whether position a sorts after b.
func after(a, b [2]string) bool { return a[0] > b[0] || a[0] == b[0] && a[1] > b[1] }

// pick returns a random index below n.
func (s *sampler) pick(n int) int {
	src := s.Random
	if src == nil {
		src = rand.Reader
	}
	var b [8]byte
	if _, err := io.ReadFull(src, b[:]); err != nil {
		return 0
	}
	return int(binary.BigEndian.Uint64(b[:]) % uint64(n))
}

// buckets are the store's buckets the sweep visits, by name: the own
// ones and every friend's.
func (s *sampler) buckets(ctx context.Context) ([]storeBucket, error) {
	own, err := s.d.DB.Buckets(ctx)
	if err != nil {
		return nil, err
	}
	lent, err := s.d.DB.AllLentBuckets(ctx)
	if err != nil {
		return nil, err
	}
	var out []storeBucket
	for _, b := range own {
		out = append(out, storeBucket{name: b.Name})
	}
	for _, b := range lent {
		id, err := identity.ParseNodeID(b.Peer)
		if err != nil {
			continue
		}
		out = append(out, storeBucket{name: LentName(id, b.Sealed), borrower: b.Peer, sealed: b.Sealed})
	}
	slices.SortFunc(out, func(a, b storeBucket) int { return strings.Compare(a.name, b.name) })
	return out, nil
}

// sweepWalk lists the store from a position, a page at a time.
type sweepWalk struct {
	s       *sampler
	buckets []storeBucket
	at      [2]string // the next object sorts after this
	buf     []sampled
}

// next returns the object after the walk's position, in bucket then
// listing order, going on at the first bucket after the last (wrap). ok
// is false when the store holds no object at all.
func (w *sweepWalk) next(ctx context.Context) (o sampled, wrap, ok bool, err error) {
	if len(w.buf) > 0 {
		o, w.buf = w.buf[0], w.buf[1:]
		w.at = [2]string{o.bucket.name, o.key}
		return o, false, true, nil
	}
	for {
		i := sort.Search(len(w.buckets), func(i int) bool { return w.buckets[i].name >= w.at[0] })
		if i == len(w.buckets) {
			if wrap {
				return o, false, false, nil // round the store, nothing in it
			}
			wrap, w.at = true, [2]string{w.buckets[0].name, ""}
			continue
		}
		b := w.buckets[i]
		from := ""
		if b.name == w.at[0] {
			from = w.at[1]
		}
		page, err := w.s.list(ctx, b, from)
		if err != nil {
			return o, false, false, err
		}
		if len(page) == 0 {
			w.at = [2]string{b.name + "\x00", ""} // the next bucket
			continue
		}
		o, w.buf = page[0], page[1:]
		w.at = [2]string{o.bucket.name, o.key}
		return o, wrap, true, nil
	}
}

// list returns the next objects of bucket b after key from, skipping
// lendisk's and the store's housekeeping; none at its end.
func (s *sampler) list(ctx context.Context, b storeBucket, from string) ([]sampled, error) {
	page := int32(samplePage)
	for {
		// posix reads StartAfter (which must be set) only beside a
		// continuation token, which is its last key: the same marker.
		marker := from
		in := &s3.ListObjectsV2Input{Bucket: &b.name, MaxKeys: &page, StartAfter: &marker}
		if from != "" {
			in.ContinuationToken = &marker
		}
		res, err := s.d.Backend.ListObjectsV2(ctx, in)
		if noBucket(err) || notFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		var out []sampled
		for _, o := range res.Contents {
			k := deref(o.Key)
			if k == "" || housekeeping(k) || strings.HasSuffix(k, "/") || (b.lent() && checkKey(k) != nil) {
				continue
			}
			var size int64
			if o.Size != nil {
				size = *o.Size
			}
			out = append(out, sampled{bucket: b, key: k, size: size})
		}
		if len(out) > 0 {
			return out, nil
		}
		if res.IsTruncated == nil || !*res.IsTruncated || res.NextContinuationToken == nil {
			return nil, nil
		}
		from = *res.NextContinuationToken
	}
}

// hash reads an object of the store whole, paced, and returns its size
// and SHA-256.
func (s *sampler) hash(ctx context.Context, bucket, key string) (int64, string, error) {
	out, err := s.d.Backend.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return 0, "", err
	}
	defer out.Body.Close()
	var r io.Reader = out.Body
	if s.Pace != nil {
		r = s.Pace(ctx, r)
	}
	h := sha256.New()
	n, err := io.Copy(h, ctxReader{ctx, r})
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// check hashes one object against the index. Only the database failing
// is an error; an object that cannot be read is logged and passed.
func (s *sampler) check(ctx context.Context, o sampled) error {
	if s.checked != nil {
		s.checked(o.bucket.name, o.key)
	}
	if o.bucket.lent() {
		return s.checkLent(ctx, o)
	}
	return s.checkOwn(ctx, o)
}

// checkOwn checks an own bucket's object against the key's version.
func (s *sampler) checkOwn(ctx context.Context, o sampled) error {
	bucket, key := o.bucket.name, o.key
	if class, err := s.d.classify(&bucket, &key); err != nil || !captured(class) {
		return nil // local-only keys have no version to hold to
	}
	v, err := s.d.DB.OwnVersion(ctx, bucket, key)
	if err != nil {
		return err
	}
	if !v.Held() || v.Pending || v.Changed {
		return nil
	}
	n, sum, err := s.hash(ctx, bucket, key)
	if err != nil {
		if !notFound(err) && ctx.Err() == nil {
			s.log.Warn("bit-rot: reading an object to check it failed", "bucket", bucket, "key", key, "err", err)
		}
		return ctx.Err()
	}
	if n == v.Size && sum == v.SHA256 {
		return nil
	}
	return s.mismatchOwn(ctx, bucket, key, v)
}

// mismatchOwn decides, holding the key's lock, what an own object that
// hashed wrong is: an edit outside the endpoint (enqueued as a write),
// rot (alerted, then removed and fetched again from a friend holding the
// captured version, or kept when none does), or a misread (nothing).
func (s *sampler) mismatchOwn(ctx context.Context, bucket, key string, seen state.OwnVersion) error {
	unlock, err := s.d.keys.lock(ctx, bucket, key)
	if err != nil {
		return err
	}
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	v, err := s.d.DB.OwnVersion(ctx, bucket, key)
	if err != nil {
		return err
	}
	if v.Seq != seen.Seq || !v.Held() || v.Pending || v.Changed {
		return nil // written, deleted or held back since
	}
	fi, err := os.Lstat(filepath.Join(s.storeDir, bucket, filepath.FromSlash(key)))
	if err != nil {
		return nil // gone since: the reconciler's business
	}
	edited := changeTime(fi).Unix() > v.Local.Unix()
	n, sum, err := s.hash(ctx, bucket, key)
	if err != nil {
		if !notFound(err) && ctx.Err() == nil {
			s.log.Warn("bit-rot: reading an object to check it failed", "bucket", bucket, "key", key, "err", err)
		}
		return ctx.Err()
	}
	if n == v.Size && sum == v.SHA256 {
		return s.resolve(ctx, bucket, key, "the object hashes right again")
	}
	if edited {
		// #88: the file changed after this node last knew it, so the
		// bytes are the user's: a write, which EP-11 judges as any other.
		s.log.Info("bit-rot: an object was changed in place outside the endpoint; enqueued as a write, not taken for rot",
			"bucket", bucket, "key", key, "sha256", sum)
		return s.d.captureChanged(ctx, bucket, key, n, sum)
	}
	now := s.now()
	r := state.Rot{Bucket: bucket, Key: key, Seq: v.Seq, LocalAt: v.Local, SHA256: v.SHA256, Found: sum, Size: v.Size,
		FoundAt: now}
	src, ok, err := s.d.DB.Source(ctx, bucket, key)
	if err != nil {
		return err
	}
	if !ok || src.SHA256 != v.SHA256 || len(src.Places) == 0 || s.d.Fetch == nil {
		r.Detail = "no friend holds the version captured, so the rotten object is kept: it is all there is"
		s.log.Error("bit-rot: an object in the store no longer matches the hash it was captured with, and no friend holds that version; it is kept as it is. Check the disk",
			"alert", true, "bucket", bucket, "key", key, "sha256", v.SHA256, "found", sum)
		if err := s.d.DB.RecordRot(ctx, r); err != nil {
			return err
		}
		s.alerted()
		return nil
	}
	s.log.Error("bit-rot: an object in the store no longer matches the hash it was captured with; it is removed and fetched again from a friend that holds it. Check the disk",
		"alert", true, "bucket", bucket, "key", key, "sha256", v.SHA256, "found", sum, "friends", len(src.Places))
	// EP-14: not held locally before it leaves the store, so the store
	// reconciler does not take its absence for the user's delete.
	if err := s.d.DB.ClearLocal(ctx, v.Seq); err != nil {
		return err
	}
	if _, err := s.d.Backend.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil && !notFound(err) {
		r.Detail = "removing the rotten object failed (" + err.Error() + "); it is kept"
		s.log.Error("bit-rot: removing a rotten object failed; it is kept", "bucket", bucket, "key", key, "err", err)
		if err := s.d.DB.MarkLocal(ctx, bucket, key, v.SHA256); err != nil {
			return err
		}
		if err := s.d.DB.RecordRot(ctx, r); err != nil {
			return err
		}
		s.alerted()
		return nil
	}
	r.RemovedAt = now
	if err := s.d.DB.RecordRot(ctx, r); err != nil {
		return err
	}
	s.alerted()
	unlock()
	locked = false
	return s.refetch(ctx, r)
}

// refetch fetches a removed rotten object again through the
// read-through (EP-10), which takes the key's lock, and records the
// outcome. A failure is left for the next step, and a read fetches it
// meanwhile.
func (s *sampler) refetch(ctx context.Context, r state.Rot) error {
	bucket, key := r.Bucket, r.Key
	err := s.d.readThrough(ctx, &bucket, &key)
	if err != nil && !errors.Is(err, errUnknown) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		detail := "removed, and no friend holding it could serve it yet (" + err.Error() + "); a read fetches it, and the sampler tries again"
		if r.Detail != detail {
			r.Detail = detail
			s.log.Warn("bit-rot: fetching a rotten object again failed; trying again at the next step", "bucket", bucket,
				"key", key, "err", err)
			return s.d.DB.RecordRot(ctx, r)
		}
		return nil
	}
	v, verr := s.d.DB.OwnVersion(ctx, bucket, key)
	if verr != nil {
		return verr
	}
	r.RepairedAt, r.Detail = s.now(), "fetched again from a friend"
	if v.Seq != r.Seq {
		r.Detail = "written or deleted since"
	}
	s.log.Info("bit-rot: a rotten object is whole again", "bucket", bucket, "key", key, "how", r.Detail)
	if err := s.d.DB.RecordRot(ctx, r); err != nil {
		return err
	}
	s.alerted()
	return nil
}

// resolve marks an own key's open rot record repaired, if it has one.
func (s *sampler) resolve(ctx context.Context, bucket, key, how string) error {
	rs, err := s.d.DB.Rots(ctx)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.Bucket == bucket && r.Key == key && !r.Lent() && r.RepairedAt.IsZero() {
			r.RepairedAt, r.Detail = s.now(), how
			if err := s.d.DB.RecordRot(ctx, r); err != nil {
				return err
			}
			s.alerted()
		}
	}
	return nil
}

// followUp goes over what earlier steps found: an own object removed and
// not fetched again is fetched; one kept is looked at again once a
// friend holds its version (after recovery or an undelete) or the
// version was stored again (an identical re-upload); one written or
// deleted since is over, and a repaired record is dropped after
// state.RotShown. A friend's record ends when the friend replaced or
// deleted the object.
func (s *sampler) followUp(ctx context.Context, now time.Time) error {
	rs, err := s.d.DB.Rots(ctx)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.Lent() {
			if err := s.followLent(ctx, r); err != nil {
				return err
			}
			continue
		}
		if !r.RepairedAt.IsZero() {
			if now.Sub(r.RepairedAt) > state.RotShown {
				if err := s.d.DB.DropRot(ctx, r.Bucket, r.Key); err != nil {
					return err
				}
			}
			continue
		}
		v, err := s.d.DB.OwnVersion(ctx, r.Bucket, r.Key)
		if err != nil {
			return err
		}
		switch {
		case v.Seq != r.Seq:
			r.RepairedAt, r.Detail = now, "written or deleted since"
			if err := s.d.DB.RecordRot(ctx, r); err != nil {
				return err
			}
			s.alerted()
		case !r.RemovedAt.IsZero():
			if err := s.refetch(ctx, r); err != nil {
				return err
			}
		case v.Held() && !v.Pending && !v.Changed:
			again := !v.Local.Equal(r.LocalAt)
			if !again {
				src, ok, err := s.d.DB.Source(ctx, r.Bucket, r.Key)
				if err != nil {
					return err
				}
				again = ok && src.SHA256 == r.SHA256 && len(src.Places) > 0
			}
			if again {
				if err := s.mismatchOwn(ctx, r.Bucket, r.Key, v); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// checkLent checks a friend's object against the hash it was stored
// with; a mismatch, checked again holding the key's lock, is recorded
// and alerted. The object is left as it is.
func (s *sampler) checkLent(ctx context.Context, o sampled) error {
	bucket, key := o.bucket.name, o.key
	want, ok := s.lentHash(ctx, bucket, key)
	if !ok {
		return ctx.Err()
	}
	n, sum, err := s.hash(ctx, bucket, key)
	if err != nil {
		if !notFound(err) && ctx.Err() == nil {
			s.log.Warn("bit-rot: reading a friend's object to check it failed", "bucket", bucket, "key", key, "err", err)
		}
		return ctx.Err()
	}
	if sum == want {
		return nil
	}
	// A put, rewrite or delete of the key holds its lock until the
	// store answers: under it, the object is the one stored.
	unlock, err := s.l.keys.lock(ctx, bucket, key)
	if err != nil {
		return err
	}
	defer unlock()
	if want, ok = s.lentHash(ctx, bucket, key); !ok {
		return ctx.Err()
	}
	if n, sum, err = s.hash(ctx, bucket, key); err != nil || sum == want {
		return ctx.Err()
	}
	s.log.Error("bit-rot: a friend's object here no longer matches the hash it was stored with; this node cannot repair it and leaves it as it is, and the friend's challenges find it. Check the disk",
		"alert", true, "friend", o.bucket.borrower, "bucket", bucket, "key", key, "sha256", want, "found", sum, "bytes", n)
	if err := s.d.DB.RecordRot(ctx, state.Rot{Bucket: bucket, Key: key, Borrower: o.bucket.borrower, Sealed: o.bucket.sealed,
		SHA256: want, Found: sum, Size: n, FoundAt: s.now()}); err != nil {
		return err
	}
	s.alerted()
	return nil
}

// lentHash is the hash a friend's object was stored with; ok is false
// when the object is gone or carries none.
func (s *sampler) lentHash(ctx context.Context, bucket, key string) (string, bool) {
	head, err := s.d.Backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		if !notFound(err) && ctx.Err() == nil {
			s.log.Warn("bit-rot: reading a friend's object's attributes failed", "bucket", bucket, "key", key, "err", err)
		}
		return "", false
	}
	h := strings.ToLower(head.Metadata[metaSHA256])
	return h, h != ""
}

// followLent drops a friend's rot record once the object is gone or
// holds another version.
func (s *sampler) followLent(ctx context.Context, r state.Rot) error {
	head, err := s.d.Backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &r.Bucket, Key: &r.Key})
	switch {
	case notFound(err) || noBucket(err):
	case err != nil:
		return nil // looked at again at the next step
	case strings.ToLower(head.Metadata[metaSHA256]) == r.SHA256:
		return nil
	}
	if err := s.d.DB.DropRot(ctx, r.Bucket, r.Key); err != nil {
		return err
	}
	s.alerted()
	return nil
}
