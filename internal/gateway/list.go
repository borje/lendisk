package gateway

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"

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

// LentObject is one key of a borrower's bucket as this lender holds it
// (NET-7 list): its size, the SHA-256 it was stored with (REPL-10,
// LEND-4) and, once tombstones exist (#17), when it was tombstoned.
type LentObject struct {
	Key          string
	Size         int64
	SHA256       string    // hex; "" if the object carries none
	TombstonedAt time.Time // zero while live
}

// ErrNoLentBucket means the borrower has no bucket of that sealed name
// here.
var ErrNoLentBucket = errors.New("no bucket of yours by that name here")

// LentIndex is the lender's index of what it holds for each borrower
// (NET-7 list, ID-5, REPL-8). It is the store itself: every object a
// borrower stores carries its hash as x-amz-meta-lendisk-sha256
// (COMPAT-4), so the index needs no table and survives the loss of
// lendisk.db with the store. A borrower names its buckets by sealed
// name and reaches only those lent_bucket records as its own (PEER-8),
// so the caller must pass the borrower the request is answered for,
// never one a request names.
type LentIndex struct {
	Store backend.Backend
	DB    *state.DB
}

// List returns up to limit keys of borrower's bucket sealed after the
// key after, in key order, and the key the next page starts after: ""
// when none follows. Keys of lendisk's
// own and the store's housekeeping (SEC-5's reserved prefixes) are not
// listed.
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
		// Recorded but not in the store: it holds nothing.
		return []LentObject{}, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	out := make([]LentObject, 0, len(res.Contents))
	next := ""
	for _, o := range res.Contents {
		key := deref(o.Key)
		next = key
		if strings.HasSuffix(key, "/") || checkKey(key) != nil {
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
		out = append(out, lo)
	}
	if res.IsTruncated == nil || !*res.IsTruncated {
		next = ""
	}
	return out, next, nil
}
