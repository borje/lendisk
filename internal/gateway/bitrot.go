package gateway

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"io"
	"log/slog"
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
// stored bytes (state.StoredBytes) for each week passed on its clock
// since the last step, never more than one cycle of the store in a step;
// the reads go through Pace (the disk's read limit,
// store.bitrot_read_limit). Its place, its last step and what it owes
// (state.Sweep) survive a restart, so every object is checked once a
// cycle, the one checked longest ago first, however often the daemon
// restarts; the first cycle starts at a bucket Random picks.
//
// Rot is told from an edit by the file's identity, never by a clock:
// queue.stamp is the inode, size and modification time of the file whose
// bytes this node wrote or hashed to the row's hash. Other bytes in the
// file of that inode and modification time are rot, whatever its size
// (a lost tail is damage); in another file (an edit, or bytes never
// verified, stamp empty) they are the user's write, enqueued as one
// (#88), except an unverified file of a rewritable key whose version a
// friend holds, which EP-11 would not hold back: that is repaired as
// rot. A mismatch is read twice, holding no lock; the row and the
// file are checked again under the key's lock before anything is done.
// Rot is recorded and alerted (state.Rot, Alerted) and, when a friend
// holds the captured version, replaced in place by a verified copy
// through the read-through's fetch (EP-10): the rotten copy stays until
// a verified one takes its place, so the key is never missing and the
// store reconciler sees nothing. With no friend holding it, it is kept.
// A friend's rotten object is alerted and left alone: the lender cannot
// repair it, and the friend's challenges (VER-1) and the read-through's
// verification judge it there.

// unverifiedDetail begins the record of a rewritable key's unverified
// file found differing from the version a friend holds.
const unverifiedDetail = "an unverified file differed from the version a friend holds"

// sameFile reports whether two file identities (fileStamp) are of one
// file not written since: the same inode and modification time. Their
// sizes may differ: a file that lost its tail (fsck) without a write
// moving its modification time is rot, not an edit (#31 review).
func sameFile(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	pa, pb := strings.Split(a, ":"), strings.Split(b, ":")
	return len(pa) == 3 && len(pb) == 3 && pa[0] == pb[0] && pa[2] == pb[2]
}

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
	// beforeLock, if set, runs when a mismatch has been read twice and
	// its key's lock is not yet taken. Tests only.
	beforeLock func()
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
// owes since the last step, which is recorded with the sweep.
func (s *sampler) step(ctx context.Context) error {
	now := s.now()
	if err := s.followUp(ctx, now); err != nil {
		return err
	}
	buckets, err := s.buckets(ctx)
	if err != nil {
		return err
	}
	sw, ok, err := s.d.DB.Sweep(ctx)
	if err != nil {
		return err
	}
	if !ok {
		// The first step ever: the sweep starts now, owing nothing.
		sw = state.Sweep{CycleAt: now, SteppedAt: now}
		if len(buckets) > 0 {
			sw.Bucket = buckets[s.pick(len(buckets))].name
		}
		return s.d.DB.SaveSweep(ctx, sw)
	}
	if elapsed := now.Sub(sw.SteppedAt); elapsed > 0 {
		stored, err := s.d.DB.StoredBytes(ctx)
		if err != nil {
			return err
		}
		sw.Owed = min(sw.Owed+s.percent()/100*float64(stored)*elapsed.Seconds()/sampleWeek.Seconds(), float64(stored))
	}
	sw.SteppedAt = now // a clock set back starts counting again from here
	if sw.Owed <= 0 || len(buckets) == 0 {
		return s.d.DB.SaveSweep(ctx, sw)
	}
	start := [2]string{sw.Bucket, sw.Key}
	w := &sweepWalk{s: s, buckets: buckets, at: start}
	wrapped := false
	for sw.Owed > 0 {
		o, wrap, ok, err := w.next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if wrap {
			if wrapped {
				break
			}
			wrapped = true
			sw.Cycles++
			sw.CycleAt, sw.Objects, sw.Bytes = now, 0, 0
		}
		// Back past where the step began: the cycle is done for now.
		if wrapped && after([2]string{o.bucket.name, o.key}, start) {
			break
		}
		if err := s.check(ctx, o); err != nil {
			return err
		}
		sw.Owed -= float64(max(o.size, 1))
		sw.Bucket, sw.Key = o.bucket.name, o.key
		sw.Objects++
		sw.Bytes += o.size
		sw.CheckedAt = now
		if err := s.d.DB.SaveSweep(ctx, sw); err != nil {
			return err
		}
	}
	return s.d.DB.SaveSweep(ctx, sw)
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

// read hashes a stored object, paced, and returns it with the identity
// of its file; ok is false when it is gone, cannot be read or changed
// while it was read (the next cycle looks again).
func (s *sampler) read(ctx context.Context, bucket, key string) (n int64, sum, stamp string, ok bool) {
	n, sum, stamp, err := s.d.hashStamped(ctx, bucket, key, s.Pace)
	if err != nil {
		if !notFound(err) && ctx.Err() == nil {
			s.log.Warn("bit-rot: reading an object to check it failed", "bucket", bucket, "key", key, "err", err)
		}
		return 0, "", "", false
	}
	return n, sum, stamp, stamp != ""
}

// checkOwn checks an own bucket's object against the key's version: the
// right bytes record the file's identity if it was not recorded; other
// bytes, read the same twice, are judged under the key's lock
// (mismatchOwn).
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
	n, sum, stamp, ok := s.read(ctx, bucket, key)
	if !ok {
		return ctx.Err()
	}
	if n == v.Size && sum == v.SHA256 {
		if stamp == v.Stamp {
			return nil
		}
		return s.verified(ctx, bucket, key, v, stamp)
	}
	// Read again, still holding no lock: a misread, or a file still
	// being written, is no verdict.
	n2, sum2, stamp2, ok := s.read(ctx, bucket, key)
	if !ok || stamp2 != stamp || n2 != n || sum2 != sum {
		return ctx.Err()
	}
	if s.beforeLock != nil {
		s.beforeLock()
	}
	return s.mismatchOwn(ctx, bucket, key, v, n, sum, stamp)
}

// verified records, under the key's lock, the identity of the file whose
// bytes were just found to be the key's version: from now on other
// bytes in it are rot (VER-5).
func (s *sampler) verified(ctx context.Context, bucket, key string, seen state.OwnVersion, stamp string) error {
	unlock, err := s.d.keys.lock(ctx, bucket, key)
	if err != nil {
		return err
	}
	defer unlock()
	v, err := s.d.DB.OwnVersion(ctx, bucket, key)
	if err != nil || v.Seq != seen.Seq || v.Stamp != seen.Stamp || s.d.stampOf(bucket, key) != stamp {
		return err
	}
	return s.d.DB.MarkLocal(ctx, bucket, key, v.SHA256, stamp)
}

// mismatchOwn decides, holding the key's lock, what an own object whose
// bytes (n, sum, read from the file of identity stamp) are not the key's
// version is, once the row and the file are still the ones read: rot
// when the file is the one this node verified (queue.stamp), otherwise
// the user's write (an edit outside the endpoint, or bytes never
// verified), enqueued as one (#88).
func (s *sampler) mismatchOwn(ctx context.Context, bucket, key string, seen state.OwnVersion, n int64, sum, stamp string) error {
	unlock, err := s.d.keys.lock(ctx, bucket, key)
	if err != nil {
		return err
	}
	defer unlock()
	v, err := s.d.DB.OwnVersion(ctx, bucket, key)
	if err != nil {
		return err
	}
	if v.Seq != seen.Seq || v.Stamp != seen.Stamp || !v.Held() || v.Pending || v.Changed || s.d.stampOf(bucket, key) != stamp {
		return nil // written, deleted, held back or changed since it was read
	}
	if !sameFile(v.Stamp, stamp) && !s.d.still(bucket, key, s.now(), DefaultSettle) {
		// Written to here within the settle time: maybe half a write
		// still going on (#88), neither a version nor rot. The store
		// reconciler or a later sample takes it once it is still.
		return nil
	}
	src, ok, err := s.d.DB.Source(ctx, bucket, key)
	if err != nil {
		return err
	}
	held := ok && src.SHA256 == v.SHA256 && len(src.Places) > 0 && s.d.Fetch != nil
	unverified := false
	if !sameFile(v.Stamp, stamp) {
		k, err := s.d.kind(&bucket, &key)
		if err != nil {
			return err
		}
		// An unverified file of a rewritable key whose version a friend
		// holds is not the user's write: EP-11 would not hold it back, and
		// it would replicate over the friend's copy (a damaged store
		// copied back onto a recovered node). It is repaired as rot.
		if v.Stamp != "" || !k.rewritable || !held {
			how := "an object was changed in place outside the endpoint"
			if v.Stamp == "" {
				how = "an object this node never verified holds other bytes than its version"
			}
			s.log.Info("bit-rot: "+how+"; enqueued as a write, not taken for rot", "bucket", bucket, "key", key, "sha256", sum)
			return s.d.captureChanged(ctx, bucket, key, n, sum, stamp)
		}
		unverified = true
	}
	r := state.Rot{Bucket: bucket, Key: key, Seq: v.Seq, Stamp: stamp, SHA256: v.SHA256, Found: sum, Size: v.Size,
		FoundAt: s.now()}
	if unverified {
		r.Detail = unverifiedDetail
		s.log.Error("bit-rot: a rewritable key's file this node never verified differs from the version a friend holds; it is not replicated, and the friend's verified copy replaces it. If you wrote it on purpose, write it again through the endpoint",
			"alert", true, "bucket", bucket, "key", key, "sha256", v.SHA256, "found", sum, "friends", len(src.Places))
		r.Refetch = true
		if err := s.d.DB.RecordRot(ctx, r); err != nil {
			return err
		}
		s.alerted()
		return s.replace(ctx, r, src)
	}
	if !held {
		r.Detail = "no friend holds the version captured, so the rotten object is kept: it is all there is"
		s.log.Error("bit-rot: an object in the store no longer matches the hash it was captured with, and no friend holds that version; it is kept as it is. Check the disk",
			"alert", true, "bucket", bucket, "key", key, "sha256", v.SHA256, "found", sum)
		if err := s.d.DB.RecordRot(ctx, r); err != nil {
			return err
		}
		s.alerted()
		return nil
	}
	s.log.Error("bit-rot: an object in the store no longer matches the hash it was captured with; a verified copy from a friend that holds it replaces it. Check the disk",
		"alert", true, "bucket", bucket, "key", key, "sha256", v.SHA256, "found", sum, "friends", len(src.Places))
	r.Refetch = true
	if err := s.d.DB.RecordRot(ctx, r); err != nil {
		return err
	}
	s.alerted()
	return s.replace(ctx, r, src)
}

// replace fetches a rotten own object's version from a friend that
// holds it, verified as it passes, over the rotten copy (fetchAny): the
// key is never missing, so the store reconciler sees nothing, and the
// rotten copy stays until a verified one takes its place. The caller
// holds the key's lock. A failure is tried again at the next step.
func (s *sampler) replace(ctx context.Context, r state.Rot, src state.Source) error {
	if err := s.d.fetchAny(ctx, r.Bucket, r.Key, src); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		detail := "no friend holding its version could serve it yet; the rotten copy stays until one does, and the sampler tries again"
		if strings.HasPrefix(r.Detail, unverifiedDetail) {
			detail = unverifiedDetail + "; " + detail
		}
		if r.Detail != detail {
			r.Detail = detail
			s.log.Warn("bit-rot: fetching a rotten object's version failed; trying again at the next step", "bucket", r.Bucket,
				"key", r.Key)
			return s.d.DB.RecordRot(ctx, r)
		}
		return nil
	}
	how := "replaced by a verified copy from a friend"
	if strings.HasPrefix(r.Detail, unverifiedDetail) {
		how = unverifiedDetail + "; " + how
	}
	r.Refetch, r.RepairedAt, r.Detail = false, s.now(), how
	s.log.Info("bit-rot: a rotten object is whole again", "bucket", r.Bucket, "key", r.Key)
	if err := s.d.DB.RecordRot(ctx, r); err != nil {
		return err
	}
	s.alerted()
	return nil
}

// repair takes a recorded rotten own object up again under the key's
// lock: if the row and the file are still the rotten ones and a friend
// holds the version, it is replaced.
func (s *sampler) repair(ctx context.Context, r state.Rot) error {
	unlock, err := s.d.keys.lock(ctx, r.Bucket, r.Key)
	if err != nil {
		return err
	}
	defer unlock()
	v, err := s.d.DB.OwnVersion(ctx, r.Bucket, r.Key)
	if err != nil || v.Seq != r.Seq || v.Pending || !sameFile(s.d.stampOf(r.Bucket, r.Key), r.Stamp) {
		return err // followUp's next step sees what changed
	}
	src, ok, err := s.d.DB.Source(ctx, r.Bucket, r.Key)
	if err != nil || !ok || src.SHA256 != r.SHA256 || len(src.Places) == 0 || s.d.Fetch == nil {
		return err
	}
	if !r.Refetch {
		r.Refetch = true
		if err := s.d.DB.RecordRot(ctx, r); err != nil {
			return err
		}
	}
	return s.replace(ctx, r, src)
}

// followUp goes over what earlier steps found: an own object a friend
// holds is fetched again (one kept is taken up once a friend holds its
// version, after recovery or an undelete); one written, deleted or
// stored again since, or whose file was replaced, is over; a repaired
// record is dropped after state.RotShown. A friend's record ends when
// the friend replaced or deleted the object.
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
		if v.Pending {
			continue
		}
		if v.Seq != r.Seq || !sameFile(s.d.stampOf(r.Bucket, r.Key), r.Stamp) {
			// The key was written or deleted, or its file replaced: the
			// sweep judges what is there now.
			r.Refetch, r.RepairedAt, r.Detail = false, now, "written, deleted or stored again since"
			if err := s.d.DB.RecordRot(ctx, r); err != nil {
				return err
			}
			s.alerted()
			continue
		}
		if err := s.repair(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// checkLent checks a friend's object against the hash it was stored
// with; a mismatch read the same twice, holding no lock, is checked to
// be the same object under the key's lock, then recorded and alerted.
// The object is left as it is.
func (s *sampler) checkLent(ctx context.Context, o sampled) error {
	bucket, key := o.bucket.name, o.key
	want, ok := s.lentHash(ctx, bucket, key)
	if !ok {
		return ctx.Err()
	}
	n, sum, stamp, ok := s.read(ctx, bucket, key)
	if !ok || sum == want {
		return ctx.Err()
	}
	if n2, sum2, stamp2, ok := s.read(ctx, bucket, key); !ok || n2 != n || sum2 != sum || stamp2 != stamp {
		return ctx.Err()
	}
	// A put, rewrite or delete of the key holds its lock until the
	// store answers: under it, the object is the one read, or another.
	unlock, err := s.l.keys.lock(ctx, bucket, key)
	if err != nil {
		return err
	}
	defer unlock()
	if now, ok := s.lentHash(ctx, bucket, key); !ok || now != want || s.d.stampOf(bucket, key) != stamp {
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
