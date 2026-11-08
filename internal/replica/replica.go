// Package replica moves a node's captured objects to the places in its
// pool (D12, DESIGN §3.4): the replica client, built on minio-go, and the
// worker that drains each place's FIFO from the store (REPL-1, REPL-3,
// REPL-9, REPL-10).
//
// Rules a caller must keep:
//   - This is the only package that imports minio-go (D12). Everything
//     else talks to a place through Replica.
//   - A client is built per place: for a friend, NewFriend, which dials
//     through the tunnel, signs with the derived per-pair credential
//     (NET-7a) and names each bucket by its sealed name (SEC-16). The
//     secret is derived on demand and never written anywhere.
//   - Clients never retry (minio-go's MaxRetries is 1) and never use
//     minio-go's signed trailer (Q-Y.2); the worker owns retries.
//   - The worker only reads the store and lendisk.db; the endpoint never
//     waits for it (REPL-9).
package replica

import (
	"context"
	"errors"
	"io"
)

// MetaSHA256 is the user metadata key carrying an object's whole-object
// SHA-256 in hex (REPL-10), sent as x-amz-meta-lendisk-sha256.
const MetaSHA256 = "lendisk-sha256"

// MetaReplaces is the user metadata key that marks a put as a rewrite
// (LEND-4, EP-7, #74): the SHA-256, in hex, of the version the place
// holds and the put replaces, sent as x-amz-meta-lendisk-replaces. A
// lender stores a marked put only if the key holds that version, which
// it keeps as a tombstone for the retention window (RET-1).
const MetaReplaces = "lendisk-replaces"

// MaxPutSize is the largest object one S3 PUT carries (5 GiB). The
// worker refuses a larger one until multipart replication (#23).
const MaxPutSize = 5 << 30

// Object is one object to send: the queue row's facts and the store's
// bytes, read once as they are sent.
type Object struct {
	Bucket, Key string // the node's own bucket name; the client seals it
	Size        int64
	SHA256      string // hex, from the capture (EP-3)
	// Replaces, if set, marks the put as a rewrite of the version with
	// this hash (MetaReplaces): only a key its profile names rewritable
	// is sent so (EP-7, #74).
	Replaces string
	Body     io.Reader
}

// Replica is a place in the pool, as the worker uses it.
type Replica interface {
	// Put stores the object, verified against its SHA-256 by the place
	// before it answers.
	Put(ctx context.Context, o Object) error
	// Delete removes a key; a key the place lacks is already deleted.
	Delete(ctx context.Context, bucket, key string) error
	// Head returns the hash the place stores a key with; ok is false
	// when it holds none. The worker asks it only when a rewrite is
	// refused (#74), and acts on the answer only by marking a rewrite.
	Head(ctx context.Context, bucket, key string) (sha256 string, ok bool, err error)
}

// A place's refusals, as the worker acts on them (§3.4 Errors). Any
// other error is retried with backoff.
var (
	// ErrQuota: the friend refused for quota or its free-space floor
	// (507 QuotaExceeded, LEND-2). The place is paused for quota.
	ErrQuota = errors.New("paused for quota")
	// ErrRemoved: the friend refuses this node as not a friend (a 403
	// AccessDenied saying state.NoRelationship, or InvalidAccessKeyId;
	// PEER-5, SEC-15).
	ErrRemoved = errors.New("removed by the friend")
	// ErrImmutable: the friend holds the key with another hash (412,
	// LEND-4) and the put was not a rewrite of it; an immutability alert
	// (EP-11), unless the key is rewritable (#74).
	ErrImmutable = errors.New("immutability alert")
	// ErrRejected: the friend found the bytes do not match the hash
	// (BadDigest, LEND-4). The worker re-hashes the store's copy to
	// tell whose fault it is: a copy that changed after capture is
	// refused for every place, one that still matches only at this
	// place (#57).
	ErrRejected = errors.New("hash rejected by the place")
)
