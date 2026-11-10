package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/state"
)

// The store reconciler (EP-14, #8). At start and every Every it lists
// each own bucket in the store a page at a time (the posix backend under
// the decorator, so neither the read-through nor the index merge of
// ListObjectsV2 is in the way) and walks the index beside it, a page at
// a time, in key order:
//
//   - an object the index does not hold (no done put is the key's latest
//     write) is a write that reached the store outside the endpoint: it is
//     hashed and enqueued as a put in its class, as a capture would be
//     (EP-7 local-only keys excepted, EP-11 and rewritable keys applied
//     by FinishPut);
//   - an indexed key whose object is gone is a delete only if the key is
//     held locally (queue.local): this node knew its object to be in its
//     own store. A recovered node's store is empty by design (ID-5,
//     ID-6) and fills through the read-through, so a key it never read
//     is not held and its absence deletes nothing;
//   - an indexed key whose object is listed is marked held (not
//     verified: queue.stamp stays empty); one whose file is no longer the
//     one this node verified (queue.stamp: inode, size, modification
//     time) is hashed again, paced, and enqueued as a put if its bytes
//     changed: an edit in place outside the endpoint, which EP-11 judges
//     as any write (#88). The same bytes record the file's identity.
//
// A file is hashed for either only once it has been still for Settle
// (#88): its change time (st_ctime, which no copy tool sets back) is
// Settle old by the node's clock, or a pass saw it with the same
// identity and change time at least Settle before. A file still being
// copied in, or an edit half done, is left, and its bucket is passed
// again once it may have settled; a file that changes while it is
// hashed is left the same way. Truncated bytes are never a version, so
// they never replicate over the friends' good copies.
//
// Guards against mass deletion (a delete goes to every place, where
// lenders keep it only as a tombstone for the retention window):
//
//   - the store marker, after Syncthing's .stfolder: the first pass
//     writes <store>/.lendisk/store-marker and records it in lendisk.db;
//     from then on a store without the marker (unmounted, replaced) is
//     not reconciled at all, MarkerMissingNode says where it should be,
//     and the store_marker_missing alert asks the user to check the
//     mount and recreate the file;
//   - a bucket whose directory is gone (NoSuchBucket) is skipped;
//   - the delete limit, after Unison's confirmbigdeletes and rsync's
//     --max-delete: a pass that finds more than DeleteLimit percent of a
//     bucket's locally held objects missing enqueues none of them, holds
//     them (state.FinishReconcile) and alerts, until the user allows at
//     least as many (`lendisk bucket reconcile <bucket> --allow-deletes N`);
//     the deletes are enqueued by the first pass after that, which the
//     loop runs within Poll.
//
// Every write and delete is decided under the key's lock (keyLocks), so
// a capture or a read-through of the key is never raced, and is checked
// again there: a key with a pending row is the capture's (in flight, or
// left for ResolveStranded) and is skipped; a delete needs the store's
// HEAD to say the key is gone and the key's latest row to be the one
// the walk saw. Passes run one at a time.

// StoreMarker is the store marker's name in the store's housekeeping
// directory.
const StoreMarker = "store-marker"

// MarkerMissingNode is the node-table key holding the store marker's
// path while it is missing ("" or unset otherwise); markerMadeNode
// records that the marker was made.
const (
	MarkerMissingNode = state.NodeMarkerMissing
	markerMadeNode    = "store_marker"
)

// DefaultDeleteLimit is ReconcileSettings.DeleteLimit when zero.
const DefaultDeleteLimit = 10

// ReconcileSettings configure the store reconciler (EP-14).
type ReconcileSettings struct {
	// Every is the time between passes; zero means a day.
	Every time.Duration
	// DeleteLimit is the percentage of a bucket's locally held objects
	// above which a pass's deletes are held for confirmation; zero means
	// DefaultDeleteLimit, 100 never holds.
	DeleteLimit int
	// Poll is how often allowances (`bucket reconcile --allow-deletes`)
	// are looked for; zero means a minute.
	Poll time.Duration
	// Alerted, if set, is called when a pass held deletes back or found
	// the store marker missing.
	Alerted func()
	// Page is the keys per listing page; zero means MaxListPage.
	Page int
	// Pace paces the reads of objects hashed again (pace.Pacer.Scrub);
	// nil reads unpaced.
	Pace func(ctx context.Context, r io.Reader) io.Reader
	// Settle is how long a file written outside the endpoint must have
	// been still before it is hashed and enqueued (#88); zero means
	// DefaultSettle.
	Settle time.Duration
}

// DefaultSettle is ReconcileSettings.Settle when zero, and how long the
// bit-rot sampler leaves a file written outside the endpoint before it
// judges it. A copy that pauses for longer is taken as it is.
const DefaultSettle = 10 * time.Minute

// maxSightings bounds the files a bucket's passes watch settle (a
// restore of the whole store changes every file at once). A file past
// it settles by its change time alone.
const maxSightings = 10_000

// sighting is a file left to settle: its identity (fileStamp and change
// time) and when a pass first saw it so.
type sighting struct {
	id string
	at time.Time
}

type reconciler struct {
	d        *Decorator
	storeDir string
	ReconcileSettings
	log *slog.Logger
	now func() time.Time

	mu sync.Mutex // one pass at a time
	// listed, if set, is called with each listing's page size. Tests
	// only.
	listed func(n int32)
	// beforeLock, if set, runs when a re-hash has read an object and has
	// not yet taken its key's lock. Tests only.
	beforeLock func()

	// Files left to settle (#88), kept by the pass that holds mu: seen
	// is each bucket's sightings from its last pass, next the running
	// pass's and waiting how many it left; due is when a bucket with
	// files left is passed again.
	seen    map[string]map[string]sighting
	next    map[string]sighting
	waiting int
	due     map[string]time.Time
}

func (r *reconciler) settle() time.Duration {
	if r.Settle > 0 {
		return r.Settle
	}
	return DefaultSettle
}

// settled reports whether key's file in bucket has been still for the
// settle time, and returns its identity (fileStamp), which the bytes
// hashed must come from. A file that has not is recorded (sighting) and
// its bucket passed again once it may have. A key with no regular file
// to stat (or a store whose files are not read) is "", true: the read
// decides.
func (r *reconciler) settled(bucket, key string) (string, bool) {
	fi, ok := r.d.fileOf(bucket, key)
	if !ok {
		return "", true
	}
	stamp, changed := fileStamp(fi), changeTime(fi)
	now, settle := r.now(), r.settle()
	if !changed.After(now.Add(-settle)) {
		return stamp, true
	}
	id := fmt.Sprintf("%s:%d", stamp, changed.UnixNano())
	s, seen := r.seen[bucket][key]
	if seen && s.id == id && !now.Before(s.at.Add(settle)) {
		return stamp, true // changed ahead of the clock, and watched still
	}
	if !seen || s.id != id {
		s = sighting{id: id, at: now}
	}
	if r.next != nil && len(r.next) < maxSightings {
		r.next[key] = s
	}
	since := s.at
	if changed.Before(since) {
		since = changed
	}
	r.later(bucket, since.Add(settle))
	return stamp, false
}

// later has bucket passed again at at, or sooner if it already is due.
func (r *reconciler) later(bucket string, at time.Time) {
	r.waiting++
	if r.due == nil {
		r.due = map[string]time.Time{}
	}
	if d, ok := r.due[bucket]; !ok || at.Before(d) {
		r.due[bucket] = at
	}
}

// dueBuckets returns the buckets with files left to settle that may have
// settled by now.
func (r *reconciler) dueBuckets() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var bs []string
	now := r.now()
	for b, at := range r.due {
		if !now.Before(at) {
			bs = append(bs, b)
		}
	}
	slices.Sort(bs)
	return bs
}

func (r *reconciler) page() int {
	if r.Page > 0 {
		return r.Page
	}
	return MaxListPage
}

func (r *reconciler) limit() int64 {
	if r.DeleteLimit > 0 {
		return int64(r.DeleteLimit)
	}
	return DefaultDeleteLimit
}

// loop runs a pass at once and every Every, and between them a pass of
// the buckets the user allowed deletes for, until ctx ends.
func (r *reconciler) loop(ctx context.Context) {
	every, poll := r.Every, r.Poll
	if every <= 0 {
		every = 24 * time.Hour
	}
	if poll <= 0 {
		poll = time.Minute
	}
	var next time.Time
	for {
		if !time.Now().Before(next) {
			next = time.Now().Add(every)
			if err := r.pass(ctx, nil); err != nil && ctx.Err() == nil {
				r.log.Error("reconcile: a pass failed; trying again at the next", "err", err)
			}
		} else if bs, err := r.d.DB.AllowedBuckets(ctx); err != nil {
			if ctx.Err() == nil {
				r.log.Error("reconcile: reading the allowed deletes failed", "err", err)
			}
		} else if bs = append(bs, r.dueBuckets()...); len(bs) > 0 {
			if err := r.pass(ctx, bs); err != nil && ctx.Err() == nil {
				r.log.Error("reconcile: a pass failed; trying again at the next", "err", err)
			}
		}
		wait := min(poll, time.Until(next))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// pass reconciles every own bucket, or only those named.
func (r *reconciler) pass(ctx context.Context, only []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ok, err := r.marker(ctx); err != nil || !ok {
		return err
	}
	bs, err := r.d.DB.Buckets(ctx)
	if err != nil {
		return err
	}
	var first error
	for _, b := range bs {
		if only != nil && !slices.Contains(only, b.Name) {
			continue
		}
		if err := r.bucket(ctx, b.Name); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.log.Error("reconcile: reconciling a bucket failed", "bucket", b.Name, "err", err)
			if first == nil {
				first = err
			}
		}
	}
	return first
}

// marker makes the store marker on the first pass, and afterwards
// reports whether it is there.
func (r *reconciler) marker(ctx context.Context) (bool, error) {
	path := filepath.Join(r.storeDir, HousekeepingDir, StoreMarker)
	_, made, err := r.d.DB.GetNode(ctx, markerMadeNode)
	if err != nil {
		return false, err
	}
	if !made {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return false, fmt.Errorf("making the store marker: %w", err)
		}
		if err := os.WriteFile(path, []byte("lendisk's store marker: while this file is missing, lendisk does not reconcile the store (EP-14)\n"), 0o600); err != nil {
			return false, fmt.Errorf("making the store marker: %w", err)
		}
		return true, r.d.DB.SetNode(ctx, markerMadeNode, r.now().UTC().Format(time.RFC3339))
	}
	prev, _, err := r.d.DB.GetNode(ctx, MarkerMissingNode)
	if err != nil {
		return false, err
	}
	switch _, err := os.Stat(path); {
	case errors.Is(err, fs.ErrNotExist):
		if prev != path {
			if err := r.d.DB.SetNode(ctx, MarkerMissingNode, path); err != nil {
				return false, err
			}
			r.log.Error("reconcile: the store marker is missing, so the store may be unmounted or replaced; nothing is reconciled until it is back",
				"alert", true, "marker", path)
			if r.Alerted != nil {
				r.Alerted()
			}
		}
		return false, nil
	case err != nil:
		return false, fmt.Errorf("reading the store marker: %w", err)
	}
	if prev != "" {
		if err := r.d.DB.SetNode(ctx, MarkerMissingNode, ""); err != nil {
			return false, err
		}
		r.log.Info("reconcile: the store marker is back", "marker", path)
		if r.Alerted != nil {
			r.Alerted()
		}
	}
	return true, nil
}

// housekeeping reports whether key is lendisk's or versitygw's own,
// never a backup tool's.
func housekeeping(key string) bool {
	return strings.HasPrefix(key, HousekeepingDir+"/") || strings.HasPrefix(key, ".sgwtmp/")
}

// visit is what a walk does with each key: present in the store and
// indexed, in the store only, indexed only.
type visit struct {
	present   func(k state.IndexedKey) error
	unindexed func(key string) error
	missing   func(k state.IndexedKey) error
}

// noBucket reports whether the store answered that the bucket is not
// there; posix answers so with an s3err.BucketError.
func noBucket(err error) bool {
	var be s3err.BucketError
	var ae s3err.APIError
	return (errors.As(err, &be) && be.Code == "NoSuchBucket") || (errors.As(err, &ae) && ae.Code == "NoSuchBucket")
}

// errChanged: a file changed while it was hashed.
var errChanged = errors.New("the file changed while it was read")

// errNoBucketDir: the bucket's directory is not in the store.
var errNoBucketDir = errors.New("the bucket's directory is not in the store")

// walk compares bucket's listing in the store with the index, a page of
// each at a time. A store page covers the index keys up to its largest
// key, so neither side is ever held whole. The store's order need not be
// the index's: whatever a walk calls is checked again before it acts.
func (r *reconciler) walk(ctx context.Context, bucket string, v visit) error {
	page := int32(r.page())
	var token *string
	start, lo := "", ""
	for {
		if r.listed != nil {
			r.listed(page)
		}
		res, err := r.d.Backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, ContinuationToken: token,
			StartAfter: &start, MaxKeys: &page})
		if noBucket(err) {
			return errNoBucketDir
		}
		if err != nil {
			return err
		}
		var keys []string
		hi := ""
		for _, o := range res.Contents {
			if o.Key == nil {
				continue
			}
			hi = max(hi, *o.Key)
			if !housekeeping(*o.Key) {
				keys = append(keys, *o.Key)
			}
		}
		more := res.IsTruncated != nil && *res.IsTruncated && res.NextContinuationToken != nil
		if more && hi == "" {
			hi = *res.NextContinuationToken
		}
		if !more {
			hi = "" // the last page covers every index key after lo
		}
		slices.Sort(keys)
		i, after := 0, lo
		for {
			idx, err := r.d.DB.IndexedKeys(ctx, bucket, after, hi, int(page))
			if err != nil {
				return err
			}
			for _, k := range idx {
				for ; i < len(keys) && keys[i] < k.Key; i++ {
					if err := v.unindexed(keys[i]); err != nil {
						return err
					}
				}
				if i < len(keys) && keys[i] == k.Key {
					i++
					err = v.present(k)
				} else {
					err = v.missing(k)
				}
				if err != nil {
					return err
				}
			}
			if len(idx) < int(page) {
				break
			}
			after = idx[len(idx)-1].Key
		}
		for ; i < len(keys); i++ {
			if err := v.unindexed(keys[i]); err != nil {
				return err
			}
		}
		if !more {
			return nil
		}
		lo, token = hi, res.NextContinuationToken
	}
}

// DeleteWindow is how far back the reconciler's own deletes count
// against the delete limit, unless an allowance was used since.
const DeleteWindow = 7 * 24 * time.Hour

// bucket reconciles one own bucket: writes and marks in a first walk,
// counting the held keys whose objects are missing. If those deletes,
// with the reconciler's own deletes of the bucket in its window (since
// the last used allowance, within DeleteWindow), stay within the delete
// limit, or a valid allowance covers them, a second walk enqueues them,
// at most as many as the first walk counted: whatever else it finds
// missing is held. Otherwise all are held back with an alert. An
// allowance is used only for the hold it was given for, and is dropped
// by a pass that does not finish.
func (r *reconciler) bucket(ctx context.Context, bucket string) error {
	delete(r.due, bucket)
	r.next, r.waiting = map[string]sighting{}, 0
	defer func() {
		if r.seen == nil {
			r.seen = map[string]map[string]sighting{}
		}
		if len(r.next) > 0 {
			r.seen[bucket] = r.next
		} else {
			delete(r.seen, bucket)
		}
		if r.waiting > 0 {
			r.log.Info("reconcile: files written to outside the endpoint within the settle time are left until they are still",
				"bucket", bucket, "files", r.waiting, "settle", r.settle())
		}
		r.next, r.waiting = nil, 0
	}()
	st, err := r.d.DB.ReconcileState(ctx, bucket)
	if err != nil {
		return err
	}
	finished := false
	defer func() {
		if !finished && st.Allowed > 0 {
			if err := r.d.DB.DropAllowance(context.Background(), bucket); err != nil {
				r.log.Error("reconcile: dropping an allowance after an unfinished pass failed", "bucket", bucket, "err", err)
				return
			}
			r.log.Warn("reconcile: the pass did not finish, so the deletes allowed are dropped; allow them again once the hold is shown",
				"bucket", bucket)
		}
	}()
	var allowance int64
	switch {
	case st.AllowanceValid():
		allowance = st.Allowed
	case st.Allowed > 0:
		if err := r.d.DB.DropAllowance(ctx, bucket); err != nil {
			return err
		}
		r.log.Warn("reconcile: the deletes allowed were for another hold; dropped", "bucket", bucket, "allowed", st.Allowed)
		st.Allowed = 0
	}
	var held, gone, writes int64
	var mark []int64
	err = r.walk(ctx, bucket, visit{
		present: func(k state.IndexedKey) error {
			held++
			if !k.Local {
				mark = append(mark, k.Seq)
				if len(mark) >= r.page() {
					err := r.d.DB.MarkLocalSeqs(ctx, mark...)
					mark = mark[:0]
					return err
				}
				return nil
			}
			// A verified key whose file is another one now was edited
			// (#88); an unverified one is the sampler's to verify.
			// A file of the same inode and modification time but another
			// size lost bytes without a write: rot, the sampler's.
			if cur := r.d.stampOf(bucket, k.Key); k.Stamp != "" && cur != k.Stamp && !sameFile(cur, k.Stamp) {
				ok, err := r.rehash(ctx, bucket, k)
				if ok {
					writes++
				}
				return err
			}
			return nil
		},
		unindexed: func(key string) error {
			ok, err := r.write(ctx, bucket, key)
			if ok {
				writes++
			}
			return err
		},
		missing: func(k state.IndexedKey) error {
			if k.Local {
				held++
				gone++
			}
			return nil
		},
	})
	if err == nil && len(mark) > 0 {
		err = r.d.DB.MarkLocalSeqs(ctx, mark...)
	}
	if errors.Is(err, errNoBucketDir) {
		r.log.Warn("reconcile: the bucket's directory is not in the store; nothing of it is reconciled", "bucket", bucket)
		return nil
	}
	if err != nil {
		return err
	}
	if writes > 0 {
		r.log.Info("reconcile: objects found in the store outside the endpoint enqueued as writes", "bucket", bucket, "writes", writes)
	}
	recent, err := r.d.DB.RecentDeletes(ctx, bucket, r.now().Add(-DeleteWindow))
	if err != nil {
		return err
	}
	within := (recent+gone)*100 <= r.limit()*(held+recent)
	allowed := allowance > 0 && gone <= allowance
	if gone > 0 && !within && !allowed {
		finished = true
		return r.hold(ctx, bucket, gone, held, recent, st.Allowed)
	}
	var deletes, over int64
	if gone > 0 {
		err := r.walk(ctx, bucket, visit{
			present:   func(state.IndexedKey) error { return nil },
			unindexed: func(string) error { return nil },
			missing: func(k state.IndexedKey) error {
				if !k.Local {
					return nil
				}
				if deletes >= gone { // more missing than the first walk counted
					over++
					return nil
				}
				ok, err := r.delete(ctx, bucket, k)
				if ok {
					deletes++
				}
				return err
			},
		})
		if errors.Is(err, errNoBucketDir) {
			r.log.Warn("reconcile: the bucket's directory went away during the pass", "bucket", bucket, "deletes", deletes)
			return nil
		}
		if err != nil {
			return err
		}
		r.log.Info("reconcile: objects removed from the store outside the endpoint enqueued as deletes", "bucket", bucket, "deletes", deletes)
		if allowed && !within {
			if err := r.d.DB.StartDeleteWindow(ctx, bucket); err != nil {
				return err
			}
		}
	}
	finished = true
	if over > 0 {
		return r.hold(ctx, bucket, over, held, recent, st.Allowed)
	}
	if err := r.d.DB.FinishReconcile(ctx, bucket, r.now(), 0, 0, st.Allowed); err != nil {
		return err
	}
	if st.Held > 0 && r.Alerted != nil {
		r.Alerted() // the hold ended
	}
	return nil
}

// hold records n deletes of bucket held back (of held objects held
// locally) and alerts; consumed is the allowance the pass read.
func (r *reconciler) hold(ctx context.Context, bucket string, n, held, recent, consumed int64) error {
	r.log.Error("reconcile: many objects this node held are missing from the store; their deletes are held back until you allow them",
		"alert", true, "bucket", bucket, "missing", n, "held", held, "recent_deletes", recent, "limit_percent", r.limit(),
		"allow", fmt.Sprintf("lendisk bucket reconcile %s --allow-deletes %d", bucket, n))
	if err := r.d.DB.FinishReconcile(ctx, bucket, r.now(), n, held, consumed); err != nil {
		return err
	}
	if r.Alerted != nil {
		r.Alerted()
	}
	return nil
}

// write enqueues key's object, found in the store and not in the index,
// as a put in its class, read once to hash it. It reports whether a row
// was queued.
func (r *reconciler) write(ctx context.Context, bucket, key string) (bool, error) {
	k, err := r.d.kind(&bucket, &key)
	if err != nil || !captured(k.class) {
		return false, err
	}
	want, ok := r.settled(bucket, key)
	if !ok {
		return false, nil
	}
	unlock, err := r.d.keys.lock(ctx, bucket, key)
	if err != nil {
		return false, err
	}
	defer unlock()
	now, err := r.d.DB.KeyNow(ctx, bucket, key)
	if err != nil || now.Pending {
		return false, err // a capture's, in flight or left for ResolveStranded
	}
	if now.Indexed() {
		if !now.Local {
			return false, r.d.DB.MarkLocalSeqs(ctx, now.Seq)
		}
		return false, nil
	}
	if _, err := r.d.Backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); missing(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	seqs, err := r.d.DB.BeginCaptures(ctx, []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: string(k.class),
		Rewritable: k.rewritable, Reconciled: true}})
	if err != nil {
		return false, err
	}
	n, sum, stamp, err := r.d.hashStamped(ctx, bucket, key, r.Pace)
	if err == nil && want != "" && stamp != want {
		// Written to since it settled, or while it was read: the bytes
		// may be half a write. Left until it is still again.
		r.later(bucket, r.now().Add(r.settle()))
		err = errChanged
	}
	if err != nil {
		if derr := r.d.DB.DropCaptures(context.Background(), seqs...); derr != nil {
			r.d.strand(seqs...)
		}
		if notFound(err) || errors.Is(err, errChanged) {
			return false, nil
		}
		return false, err
	}
	dup, err := r.d.DB.FinishPut(ctx, seqs[0], n, sum)
	if err != nil {
		r.d.strand(seqs...)
		return false, err
	}
	if stamp != "" {
		r.d.markHeld(ctx, bucket, key, sum, stamp)
	}
	r.d.noteFormat(bucket, key, k.format)
	return !dup, nil
}

// rehash hashes again a held key's object whose file is no longer the
// one this node verified (its identity, queue.stamp: another size,
// modification time or inode), #88. The read is paced (Pace) and holds
// no lock; under the key's lock the row and the file are checked to be
// the ones hashed, then other bytes are enqueued as a put, an edit
// outside the endpoint, and the same bytes (a copy back, a touch) record
// the file's identity. It reports whether a row was queued.
func (r *reconciler) rehash(ctx context.Context, bucket string, k state.IndexedKey) (bool, error) {
	if class, err := r.d.classify(&bucket, &k.Key); err != nil || !captured(class) {
		return false, err
	}
	want, ok := r.settled(bucket, k.Key)
	if !ok {
		return false, nil
	}
	n, sum, stamp, err := r.d.hashStamped(ctx, bucket, k.Key, r.Pace)
	if notFound(err) {
		return false, nil
	}
	if err == nil && (stamp == "" || stamp != want) {
		// Changing while read, or since it settled: left until it is
		// still again.
		r.later(bucket, r.now().Add(r.settle()))
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if r.beforeLock != nil {
		r.beforeLock()
	}
	unlock, err := r.d.keys.lock(ctx, bucket, k.Key)
	if err != nil {
		return false, err
	}
	defer unlock()
	v, err := r.d.DB.OwnVersion(ctx, bucket, k.Key)
	if err != nil || v.Seq != k.Seq || !v.Held() || v.Pending || v.Stamp != k.Stamp || r.d.stampOf(bucket, k.Key) != stamp {
		return false, err
	}
	if n == v.Size && sum == v.SHA256 {
		return false, r.d.DB.MarkLocal(ctx, bucket, k.Key, sum, stamp)
	}
	r.log.Info("reconcile: an object was changed in place outside the endpoint; enqueued as a write", "bucket", bucket,
		"key", k.Key, "sha256", sum)
	return true, r.d.captureChanged(ctx, bucket, k.Key, n, sum, stamp)
}

// captureChanged enqueues an indexed key's object, changed in place
// outside the endpoint, as a put of the bytes the caller hashed (n,
// sum) from the file of identity stamp, in its class; FinishPut applies
// EP-11 as to any capture. The caller holds the key's lock. A crash
// between the row and its finish leaves a pending row with no token,
// which startup settles as landed.
func (d *Decorator) captureChanged(ctx context.Context, bucket, key string, n int64, sum, stamp string) error {
	k, err := d.kind(&bucket, &key)
	if err != nil || !captured(k.class) {
		return err
	}
	seqs, err := d.DB.BeginCaptures(ctx, []state.Capture{{Bucket: bucket, Key: key, Op: state.OpPut, Class: string(k.class),
		Rewritable: k.rewritable, Reconciled: true}})
	if err != nil {
		return err
	}
	if _, err := d.DB.FinishPut(ctx, seqs[0], n, sum); err != nil {
		d.strand(seqs...)
		return err
	}
	d.markHeld(ctx, bucket, key, sum, stamp)
	d.noteFormat(bucket, key, k.format)
	return nil
}

// delete enqueues the delete of a held key whose object is gone from
// the store, if under its lock the store still lacks it and the key's
// latest row is still the one the walk saw, with no row pending. It
// reports whether a row was queued.
func (r *reconciler) delete(ctx context.Context, bucket string, k state.IndexedKey) (bool, error) {
	class, err := r.d.classify(&bucket, &k.Key)
	if err != nil || !captured(class) {
		return false, err
	}
	unlock, err := r.d.keys.lock(ctx, bucket, k.Key)
	if err != nil {
		return false, err
	}
	defer unlock()
	now, err := r.d.DB.KeyNow(ctx, bucket, k.Key)
	if err != nil || now.Pending || !now.Indexed() || !now.Local || now.Seq != k.Seq {
		return false, err
	}
	if _, err := r.d.Backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &k.Key}); err == nil {
		return false, nil
	} else if !missing(err) {
		return false, err
	}
	// Something at the path the store does not answer for (a directory
	// where the object was, an object whose attributes were lost) is no
	// delete: it is left, and logged.
	if _, err := os.Lstat(filepath.Join(r.storeDir, bucket, filepath.FromSlash(k.Key))); err == nil {
		r.log.Warn("reconcile: the store does not answer for a key, but something is at its path; not a delete",
			"bucket", bucket, "key", k.Key)
		return false, nil
	} else if !errors.Is(err, fs.ErrNotExist) && !errors.Is(err, syscall.ENOTDIR) {
		return false, err
	}
	seqs, err := r.d.DB.BeginCaptures(ctx, []state.Capture{{Bucket: bucket, Key: k.Key, Op: state.OpDelete, Class: string(class),
		Reconciled: true}})
	if err != nil {
		return false, err
	}
	if err := r.d.DB.FinishDelete(ctx, seqs...); err != nil {
		r.d.strand(seqs...)
		return false, err
	}
	return true, nil
}
