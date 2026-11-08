package gateway

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

// MaxListPage is the most keys one page of the NET-7 list carries.
const MaxListPage = 1000

// MaxObjectSize is the largest object S3 stores (5 TiB): a listed size
// above it is no object a lender holds.
const MaxObjectSize = 5 << 40

// CheckKey reports whether key is one a borrower may name (SEC-5): what
// a list from a lender may carry, too.
func CheckKey(key string) error { return checkKey(key) }

// MaxReplaced is the most replaced versions one listed key carries
// (LentObject.Replaced), the newest; recovery ignores a key listed with
// more.
const MaxReplaced = 1000

// LentObject is one key of a borrower's bucket as this lender holds it
// (NET-7 list): its size, the SHA-256 it was stored with (REPL-10,
// LEND-4) and, if it is tombstoned (RET-1), when.
type LentObject struct {
	Key          string
	Size         int64
	SHA256       string    // hex; "" if the object carries none
	TombstonedAt time.Time // zero while live
	// Replaced are, for a live key, the hashes of the versions the
	// borrower's rewrites replaced here and whose tombs are kept, newest
	// first, at most MaxReplaced (#74): recovery reads them as evidence
	// of which version is newer where friends disagree.
	Replaced []string
}

// ErrNoLentBucket means the borrower has no bucket of that sealed name
// here.
var ErrNoLentBucket = errors.New("no bucket of yours by that name here")

// LentIndex is the lender's index of what it holds for each borrower
// (NET-7 list, ID-5, REPL-8). Live keys come from the store itself:
// every object a borrower stores carries its hash as
// x-amz-meta-lendisk-sha256 (COMPAT-4), so they need no table and
// survive the loss of lendisk.db with the store. Tombstoned keys come
// from the tombstone table, merged in key order. A borrower names its buckets by sealed
// name and reaches only those lent_bucket records as its own (PEER-8),
// so the caller must pass the borrower the request is answered for,
// never one a request names.
type LentIndex struct {
	Store backend.Backend
	DB    *state.DB

	// afterStoreList runs between the read of the store and the read of
	// the tombstones. Tests only.
	afterStoreList func()
}

// List returns up to limit keys of borrower's bucket sealed after the
// key after, in key order, and the key the next page starts after: ""
// when none follows. A tombstoned key is listed with its newest
// tombstone, unless it is live again; a key whose newest tombstone is a
// rewrite's is looked up in the store again (the rewrite may have
// published since the store was read) and, while the rewrite is still
// in progress (pending), listed live as the version it still holds. Keys of lendisk's own and the
// store's housekeeping (SEC-5's reserved prefixes) are not listed.
func (x *LentIndex) List(ctx context.Context, borrower identity.NodeID, sealed, after string, limit int) ([]LentObject, string, error) {
	if limit < 1 || limit > MaxListPage {
		return nil, "", errors.New("a list page holds 1 to 1000 keys")
	}
	if !identity.IsSealedName(sealed) {
		return nil, "", ErrNoLentBucket
	}
	ok, err := x.DB.HasLentBucket(ctx, borrower.String(), sealed)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", ErrNoLentBucket
	}
	bucket := LentName(borrower, sealed)
	page := int32(limit)
	// posix reads StartAfter only beside a continuation token, which is
	// its last key: the same marker.
	res, err := x.Store.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, StartAfter: &after,
		ContinuationToken: &after, MaxKeys: &page})
	if notFound(err) {
		// Recorded but not in the store: it holds nothing live.
		res, err = s3response.ListObjectsV2Result{}, nil
	}
	if err != nil {
		return nil, "", err
	}
	if x.afterStoreList != nil {
		x.afterStoreList()
	}
	tombs, err := x.DB.TombstonedKeys(ctx, borrower.String(), sealed, after, limit)
	if err != nil {
		return nil, "", err
	}
	// Each source is read up to limit keys; past the last key of a
	// source that has more, the other cannot be merged yet, so the page
	// ends there (bound).
	bound, bounded := "", false
	lower := func(k string) {
		if !bounded || k < bound {
			bound, bounded = k, true
		}
	}
	if res.IsTruncated != nil && *res.IsTruncated && len(res.Contents) > 0 {
		lower(deref(res.Contents[len(res.Contents)-1].Key))
	}
	if len(tombs) == limit {
		lower(tombs[len(tombs)-1].Key)
	}
	out := make([]LentObject, 0, limit)
	live := map[string]bool{}
	for _, o := range res.Contents {
		key := deref(o.Key)
		if bounded && key > bound || strings.HasSuffix(key, "/") || checkKey(key) != nil {
			continue
		}
		h, err := x.Store.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
		if notFound(err) {
			continue // deleted since the listing
		}
		if err != nil {
			return nil, "", err
		}
		lo := LentObject{Key: key, SHA256: strings.ToLower(h.Metadata[metaSHA256])}
		if h.ContentLength != nil {
			lo.Size = *h.ContentLength
		}
		live[key] = true
		out = append(out, lo)
	}
	for _, t := range tombs {
		if bounded && t.Key > bound || live[t.Key] {
			continue // a key live again is listed live
		}
		if t.Rewrite {
			// A rewrite's tomb as the newest, with the key not in the
			// store's listing (#74): the rewrite may have published since
			// the store was read; else it is between its move and its
			// publication, and the key still holds the version it
			// replaces. Either way the key is live, not deleted.
			h, err := x.Store.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &t.Key})
			switch {
			case err == nil:
				lo := LentObject{Key: t.Key, SHA256: strings.ToLower(h.Metadata[metaSHA256])}
				if h.ContentLength != nil {
					lo.Size = *h.ContentLength
				}
				live[t.Key] = true
				out = append(out, lo)
				continue
			case !notFound(err):
				return nil, "", err
			case t.Pending:
				live[t.Key] = true
				out = append(out, LentObject{Key: t.Key, Size: t.Size, SHA256: t.SHA256})
				continue
			}
		}
		out = append(out, LentObject{Key: t.Key, Size: t.Size, SHA256: t.SHA256, TombstonedAt: t.DeletedAt})
	}
	if len(out) > 0 {
		upto := out[0].Key
		for _, o := range out {
			upto = max(upto, o.Key)
		}
		rs, err := x.DB.ReplacedVersions(ctx, borrower.String(), sealed, after, upto)
		if err != nil {
			return nil, "", err
		}
		for i := range out {
			if r := rs[out[i].Key]; len(r) > 0 && out[i].TombstonedAt.IsZero() {
				out[i].Replaced = r[:min(len(r), MaxReplaced)]
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) > limit {
		return out[:limit], out[limit-1].Key, nil
	}
	return out, bound, nil
}
