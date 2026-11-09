package gateway

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/state"
)

// The read-through (EP-10, ID-6, #30). A GET or HEAD of an own bucket's
// key the local store lacks (every key after `recover`) fetches the
// version that counts from a place that acknowledged holding it,
// verifies its SHA-256 against the recorded hash, stores it in the
// local store directly through the backend (so it is neither captured
// nor replicated again: those places hold it already), and then serves
// the request from the store. A ranged GET fetches the whole object
// first and then serves the range from the store: restic's packs are
// about 16 MiB, so the simple way costs little, and the object is then
// local for the next range. Only what is read is fetched.
//
// Listing an own bucket adds the keys the index holds and the store
// lacks (state.LiveKeys), so a recovered repository lists its snapshots,
// indexes and keys before any of them is read.

// Fetcher opens an object's bytes at a place: replica.Fetcher for
// friends. The read-through verifies what it reads; the place's word is
// not taken for it.
type Fetcher interface {
	Fetch(ctx context.Context, place, bucket, key string) (io.ReadCloser, error)
}

var (
	// errUnknown: the index knows no version of the key; the store's own
	// answer (not found) stands.
	errUnknown = errors.New("no version of the key is known")
	// errMismatch: a place served bytes that are not the version it
	// acknowledged: the place's fault, and the next place is tried.
	errMismatch = errors.New("the bytes do not match the hash the place acknowledged")
	// errNotFetched is the client's answer when no place holding the
	// object could serve it.
	errNotFetched = s3err.APIError{Code: "ServiceUnavailable", HTTPStatusCode: 503,
		Description: "lendisk: this object is not in the local store yet and no friend holding it could serve it now; " +
			"try again when one is reachable (lendisk status shows the friends)"}
)

// errKeyConflict refuses a recovered key whose friends hold different
// versions, none of which counts (ID-5, state.KeyConflict).
func errKeyConflict(k state.RecoveredKey) s3err.APIError {
	return s3err.APIError{Code: "KeyConflict", HTTPStatusCode: 409,
		Description: fmt.Sprintf("lendisk: since recovery, friends hold different versions of this key (%s); "+
			"see lendisk status and pick one with: lendisk bucket resolve %s %s <friend>", k.Reason, k.Bucket, k.Key)}
}

// missing reports whether the store answered that the key is not there
// (not that the bucket is not).
func missing(err error) bool {
	var ae s3err.APIError
	return errors.As(err, &ae) && ae.Code != "NoSuchBucket" && (ae.Code == "NoSuchKey" || ae.HTTPStatusCode == 404)
}

// GetObject serves a key the store lacks through the read-through.
func (d *Decorator) GetObject(ctx context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	out, err := d.Backend.GetObject(ctx, in)
	if !missing(err) {
		return out, err
	}
	if rerr := d.readThrough(ctx, in.Bucket, in.Key); rerr != nil {
		if errors.Is(rerr, errUnknown) {
			return out, err
		}
		return nil, rerr
	}
	return d.Backend.GetObject(ctx, in)
}

// HeadObject serves a key the store lacks through the read-through.
func (d *Decorator) HeadObject(ctx context.Context, in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	out, err := d.Backend.HeadObject(ctx, in)
	if !missing(err) {
		return out, err
	}
	if rerr := d.readThrough(ctx, in.Bucket, in.Key); rerr != nil {
		if errors.Is(rerr, errUnknown) {
			return out, err
		}
		return nil, rerr
	}
	return d.Backend.HeadObject(ctx, in)
}

// Fill puts the version that counts of an own bucket's key into the
// store through the read-through, for replication of an object the store
// lacks (after `recover`, ID-5): a copy re-placement places at a new
// friend is read from a friend that holds it, verified against the
// recorded hash, never sent unverified (REPL-13, #26 review). filled is
// true when the store holds it now; false with no error when no friend
// is recorded holding that version, so its absence is the store's.
// Another error may pass: no friend holding it answered.
func (g *Gateway) Fill(ctx context.Context, bucket, key string) (filled bool, err error) {
	l := g.running.Load()
	if l == nil {
		return false, errNotServing
	}
	d, ok := l.Next.(*Decorator)
	if !ok {
		return false, errNotServing
	}
	if src, ok, err := d.DB.Source(ctx, bucket, key); err != nil {
		return false, err
	} else if !ok || len(src.Places) == 0 {
		return false, nil // no friend holds it: the store's loss stands
	}
	switch err := d.readThrough(ctx, &bucket, &key); {
	case err == nil:
		return true, nil
	case errors.Is(err, errUnknown):
		return false, nil
	default:
		return false, err
	}
}

// readThrough puts the version that counts of an own bucket's key into
// the store, or says why not: errUnknown when the index knows none (the
// caller's not-found stands), or an s3err.APIError for the client. It
// holds the key's lock while it fetches, so concurrent misses of one key
// fetch once (the others find it stored) and a write of the key waits
// for it.
func (d *Decorator) readThrough(ctx context.Context, bucketp, keyp *string) error {
	class, err := d.classify(bucketp, keyp)
	if err != nil {
		return err
	}
	if !captured(class) {
		return errUnknown
	}
	bucket, key := *bucketp, *keyp
	if k, conflict, err := d.DB.KeyConflict(ctx, bucket, key); err != nil {
		d.Log.Error("read-through: reading the index failed", "bucket", bucket, "key", key, "err", err)
		return errInternal
	} else if conflict {
		return errKeyConflict(k)
	}
	if _, ok, err := d.DB.Source(ctx, bucket, key); err != nil {
		d.Log.Error("read-through: reading the index failed", "bucket", bucket, "key", key, "err", err)
		return errInternal
	} else if !ok {
		return errUnknown
	}
	unlock, err := d.keys.lock(ctx, bucket, key)
	if err != nil {
		return errInternal // the client went away
	}
	defer unlock()
	// Another reader fetched it while this one waited, or a write
	// landed: the store answers.
	if _, err := d.Backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); !missing(err) {
		return err
	}
	// Read again under the lock: a delete may have landed meanwhile.
	src, ok, err := d.DB.Source(ctx, bucket, key)
	if err != nil {
		d.Log.Error("read-through: reading the index failed", "bucket", bucket, "key", key, "err", err)
		return errInternal
	}
	if !ok {
		return errUnknown
	}
	if d.Fetch == nil || len(src.Places) == 0 {
		d.Log.Warn("read-through: the object is not in the store and no friend is recorded holding it",
			"bucket", bucket, "key", key, "sha256", src.SHA256)
		return errNotFetched
	}
	return d.fetchAny(ctx, bucket, key, src)
}

// fetchAny stores src's version of key from the first place holding it
// that serves it, verified as it passes, most recently reachable first,
// over whatever the store holds there: posix replaces an object only
// once the new one is complete, so a rotten copy stays until a verified
// one takes its place (VER-5). The caller holds the key's lock. It
// returns errNotFetched when no place could serve it, or errInternal.
func (d *Decorator) fetchAny(ctx context.Context, bucket, key string, src state.Source) error {
	for _, p := range d.fetchOrder(src.Places) {
		start := time.Now()
		err := d.fetchFrom(ctx, p, bucket, key, src)
		if err == nil {
			d.reached(p, true)
			// The store holds the version now, in a file this node wrote
			// verified: its loss from here on is a delete (EP-14), and
			// other bytes in that file are rot (VER-5). A failed mark is
			// left to the next pass.
			d.markHeld(ctx, bucket, key, src.SHA256, d.stampOf(bucket, key))
			d.Log.Info("read-through: fetched an object from a friend", "bucket", bucket, "key", key, "place", p,
				"bytes", src.Size, "took", time.Since(start).Round(time.Millisecond))
			return nil
		}
		if ctx.Err() != nil {
			return errInternal // the client went away
		}
		var store storeError
		if errors.As(err, &store) {
			d.Log.Error("read-through: the local store refused a fetched object", "bucket", bucket, "key", key, "err", err)
			return errInternal
		}
		d.reached(p, false)
		if errors.Is(err, errMismatch) {
			d.Log.Warn("read-through: a friend served bytes that are not the version it acknowledged; "+
				"they were not stored, and the next friend holding it is tried", "bucket", bucket, "key", key, "place", p,
				"sha256", src.SHA256, "err", err)
			continue
		}
		d.Log.Warn("read-through: fetching from a friend failed; trying the next", "bucket", bucket, "key", key,
			"place", p, "err", err)
	}
	return errNotFetched
}

// storeError is the local store's failure to take a fetched object: no
// other place would do better.
type storeError struct{ err error }

func (e storeError) Error() string { return "the local store: " + e.err.Error() }
func (e storeError) Unwrap() error { return e.err }

// fetchFrom streams key from place into the store, verified as it
// passes: bytes that are not the recorded version (too many, too few, or
// another hash) fail the read before the store commits the object.
func (d *Decorator) fetchFrom(ctx context.Context, place, bucket, key string, src state.Source) error {
	body, err := d.Fetch.Fetch(ctx, place, bucket, key)
	if err != nil {
		return err
	}
	defer body.Close()
	v := &verifying{r: body, h: sha256.New(), size: src.Size, want: src.SHA256}
	size := src.Size
	_, err = d.Backend.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: v, ContentLength: &size})
	switch {
	case v.err != nil:
		if err == nil {
			d.removeFetched(bucket, key)
		}
		return v.err
	case v.readErr != nil:
		return v.readErr
	case err != nil:
		return storeError{err}
	}
	if err := v.final(); err != nil {
		// The store took the object without reading it to its end (an
		// empty object): it is not the version, so it goes again.
		d.removeFetched(bucket, key)
		return err
	}
	return nil
}

// removeFetched deletes an object the store took although it failed
// verification; the deletion is not captured.
func (d *Decorator) removeFetched(bucket, key string) {
	if _, err := d.Backend.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: &bucket, Key: &key}); err != nil {
		d.Log.Error("read-through: removing an object that failed verification failed", "bucket", bucket, "key", key, "err", err)
	}
}

// verifying passes a place's body to the store, failing the read as soon
// as the bytes cannot be the version recorded: more than its size, fewer
// at the end, or another SHA-256 once its size has passed.
type verifying struct {
	r       io.Reader
	h       hash.Hash
	n, size int64
	want    string
	checked bool
	err     error // the bytes are not the version (errMismatch)
	readErr error // the place's body failed
}

func (v *verifying) Read(p []byte) (int, error) {
	if v.err != nil {
		return 0, v.err
	}
	n, err := v.r.Read(p)
	v.h.Write(p[:n])
	v.n += int64(n)
	switch {
	case v.n > v.size:
		v.err = fmt.Errorf("%w: more than the %d bytes acknowledged", errMismatch, v.size)
	case v.n == v.size && !v.checked:
		v.err = v.final()
	case err == io.EOF && v.n < v.size:
		v.err = fmt.Errorf("%w: %d bytes of the %d acknowledged", errMismatch, v.n, v.size)
	case err != nil && err != io.EOF:
		v.readErr = err
	}
	if v.err != nil {
		return n, v.err
	}
	return n, err
}

// final checks the hash of what passed, once its size has.
func (v *verifying) final() error {
	if v.checked {
		return v.err
	}
	v.checked = true
	if v.n != v.size {
		return fmt.Errorf("%w: %d bytes of the %d acknowledged", errMismatch, v.n, v.size)
	}
	if got := hex.EncodeToString(v.h.Sum(nil)); got != v.want {
		return fmt.Errorf("%w: SHA-256 %s", errMismatch, got)
	}
	return nil
}

// reach is what the read-through saw of a place: its last successful
// fetch and its last failed one.
type reach struct{ ok, failed time.Time }

// reached records a fetch's outcome at place.
func (d *Decorator) reached(place string, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.reach == nil {
		d.reach = map[string]reach{}
	}
	r := d.reach[place]
	if ok {
		r.ok = time.Now()
	} else {
		r.failed = time.Now()
	}
	d.reach[place] = r
}

// fetchOrder is the order places are tried in: most recently reachable
// first, by the later of replication's last acknowledgement and the
// read-through's last fetch; a place whose last fetch failed goes after
// every other, so an offline friend costs one dial timeout, not one per
// object.
func (d *Decorator) fetchOrder(ps []state.SourcePlace) []string {
	type cand struct {
		place  string
		last   time.Time
		failed bool
	}
	d.mu.Lock()
	cs := make([]cand, len(ps))
	for i, p := range ps {
		r := d.reach[p.Place]
		last := p.LastOK
		if r.ok.After(last) {
			last = r.ok
		}
		cs[i] = cand{p.Place, last, r.failed.After(r.ok)}
	}
	d.mu.Unlock()
	slices.SortStableFunc(cs, func(a, b cand) int {
		if a.failed != b.failed {
			if a.failed {
				return 1
			}
			return -1
		}
		return b.last.Compare(a.last)
	})
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.place
	}
	return out
}

// ListObjectsV2 lists an own bucket with the keys the index holds and
// the store lacks: what a recovered node has not read yet. Both sides are
// read in key order from the same marker and merged, so pages and
// continuation work as on the store alone.
func (d *Decorator) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	res, err := d.Backend.ListObjectsV2(ctx, in)
	if err != nil || d.DB == nil || in.Bucket == nil || in.MaxKeys == nil || *in.MaxKeys <= 0 {
		return res, err
	}
	if _, err := d.DB.GetBucket(ctx, *in.Bucket); errors.Is(err, state.ErrNotFound) {
		return res, nil
	} else if err != nil {
		d.Log.Error("list: reading the bucket failed", "bucket", *in.Bucket, "err", err)
		return res, errInternal
	}
	prefix, delim := deref(in.Prefix), deref(in.Delimiter)
	// posix's marker: the continuation token, or the later of it and
	// StartAfter; StartAfter alone it ignores, and so does this.
	marker := ""
	if in.ContinuationToken != nil {
		marker = max(deref(in.StartAfter), *in.ContinuationToken)
	}
	limit := int(*in.MaxKeys)
	idx, err := d.indexItems(ctx, *in.Bucket, prefix, delim, marker, limit)
	if err != nil {
		d.Log.Error("list: reading the index failed", "bucket", *in.Bucket, "err", err)
		return res, errInternal
	}
	if len(idx) == 0 {
		return res, nil
	}
	local := make([]listItem, 0, len(res.Contents)+len(res.CommonPrefixes))
	for _, o := range res.Contents {
		local = append(local, listItem{name: deref(o.Key), obj: o})
	}
	for _, p := range res.CommonPrefixes {
		local = append(local, listItem{name: deref(p.Prefix), prefix: true})
	}
	slices.SortFunc(local, func(a, b listItem) int { return cmp.Compare(a.name, b.name) })
	merged := mergeItems(local, idx)
	truncated := res.IsTruncated != nil && *res.IsTruncated
	if len(merged) > limit {
		merged, truncated = merged[:limit], true
	}
	res.Contents, res.CommonPrefixes = nil, nil
	for _, it := range merged {
		if it.prefix {
			res.CommonPrefixes = append(res.CommonPrefixes, types.CommonPrefix{Prefix: &it.name})
		} else {
			res.Contents = append(res.Contents, it.obj)
		}
	}
	n := int32(len(merged))
	res.KeyCount, res.IsTruncated = &n, &truncated
	res.NextContinuationToken = nil
	if truncated {
		last := merged[len(merged)-1].name
		res.NextContinuationToken = &last
	}
	return res, nil
}

// listItem is an entry of a list page: an object or a common prefix.
type listItem struct {
	name   string
	prefix bool
	obj    s3response.Object
}

// indexItems returns up to limit+1 entries the index has for a list
// page: its live keys after marker, those under a delimiter folded into
// their common prefix, as the store folds them.
func (d *Decorator) indexItems(ctx context.Context, bucket, prefix, delim, marker string, limit int) ([]listItem, error) {
	var out []listItem
	after := marker
	for len(out) <= limit {
		ks, err := d.DB.LiveKeys(ctx, bucket, prefix, after, limit+1)
		if err != nil {
			return nil, err
		}
		folded := false
		for _, k := range ks {
			after = k.Key
			it := listItem{name: k.Key}
			if delim != "" {
				if i := strings.Index(k.Key[len(prefix):], delim); i >= 0 {
					it = listItem{name: k.Key[:len(prefix)+i+len(delim)], prefix: true}
				}
			}
			if it.name > marker {
				if !it.prefix {
					key, size := k.Key, k.Size
					mod := k.Modified
					it.obj = s3response.Object{Key: &key, Size: &size, LastModified: &mod, StorageClass: types.ObjectStorageClassStandard}
				}
				out = append(out, it)
			}
			if it.prefix {
				// Skip the rest of the prefix: every key in it folds into it.
				after, folded = it.name+"\xff", true
				break
			}
			if len(out) > limit {
				break
			}
		}
		if !folded && len(ks) <= limit {
			break
		}
	}
	return out, nil
}

// mergeItems merges two lists in name order, the store's entry winning
// a name both have.
func mergeItems(local, idx []listItem) []listItem {
	out := make([]listItem, 0, len(local)+len(idx))
	i, j := 0, 0
	for i < len(local) || j < len(idx) {
		switch {
		case j == len(idx) || (i < len(local) && local[i].name < idx[j].name):
			out = append(out, local[i])
			i++
		case i == len(local) || idx[j].name < local[i].name:
			out = append(out, idx[j])
			j++
		default:
			out = append(out, local[i])
			i++
			j++
		}
	}
	return out
}
