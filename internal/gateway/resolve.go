package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/state"
)

// ResolvePending settles the queue rows a crash left pending (EP-3),
// against the store, before the gateway serves anything:
//
//   - a put is done if the object on disk carries the row's capture token
//     (PutObject) or was assembled from the row's upload (multipart);
//     else it never took effect and the row is removed. A put that took
//     effect is hashed by reading the object once;
//   - a delete is done if the key is gone, else removed.
//
// It returns how many rows it marked done and removed.
func (d *Decorator) ResolvePending(ctx context.Context) (done, dropped int, err error) {
	if d.DB == nil {
		return 0, 0, nil
	}
	pending, err := d.DB.PendingCaptures(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range pending {
		ok, err := d.resolve(ctx, e)
		if err != nil {
			return done, dropped, fmt.Errorf("resolving queue row %d (%s/%s): %w", e.Seq, e.Bucket, e.Key, err)
		}
		if ok {
			done++
		} else {
			dropped++
		}
	}
	return done, dropped, nil
}

// resolve settles one pending row against the store, as ResolvePending
// does; done is false when the row was removed.
func (d *Decorator) resolve(ctx context.Context, e state.Entry) (done bool, err error) {
	landed, err := d.landed(ctx, e)
	if err != nil {
		return false, err
	}
	if !landed {
		return false, d.DB.DropCaptures(ctx, e.Seq)
	}
	if e.Op == state.OpDelete {
		return true, d.DB.FinishDelete(ctx, e.Seq)
	}
	size, sum, err := d.hashStored(ctx, e.Bucket, e.Key)
	if err == nil {
		_, err = d.DB.FinishPut(ctx, e.Seq, size, sum)
	}
	return true, err
}

// strand records rows whose request has ended without completing or
// removing them (the state database refused the write, or a multipart
// hash failed). ResolveStranded settles them while the gateway runs,
// rather than leaving them pending until the next start.
func (d *Decorator) strand(seqs ...int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stranded == nil {
		d.stranded = map[int64]bool{}
	}
	for _, s := range seqs {
		d.stranded[s] = true
	}
}

// ResolveStranded settles the stranded rows (EP-3) as ResolvePending
// settles a crash's. A row that cannot be settled yet stays stranded for
// the next call.
func (d *Decorator) ResolveStranded(ctx context.Context) {
	d.mu.Lock()
	seqs := slices.Sorted(maps.Keys(d.stranded))
	d.mu.Unlock()
	for _, seq := range seqs {
		e, ok, err := d.DB.PendingCapture(ctx, seq)
		if err == nil && ok {
			// A request writing the key now would race the read of the
			// store: leave the row for the next call.
			unlock, free := d.keys.tryLock(e.Bucket, e.Key)
			if !free {
				continue
			}
			if e, ok, err = d.DB.PendingCapture(ctx, seq); err == nil && ok {
				_, err = d.resolve(ctx, e)
			}
			unlock()
		}
		if err != nil {
			if ctx.Err() == nil {
				d.Log.Warn("capture: settling a stranded queue row failed; retrying later", "seq", seq, "err", err)
			}
			continue
		}
		d.mu.Lock()
		delete(d.stranded, seq)
		d.mu.Unlock()
	}
}

// resolveLoop calls ResolveStranded every interval until ctx ends.
func (d *Decorator) resolveLoop(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.ResolveStranded(ctx)
		}
	}
}

// landed reports whether a pending row's backend call took effect.
func (d *Decorator) landed(ctx context.Context, e state.Entry) (bool, error) {
	head, err := d.Backend.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &e.Bucket, Key: &e.Key})
	if notFound(err) {
		return e.Op == state.OpDelete, nil
	}
	if err != nil {
		return false, err
	}
	switch {
	case e.Op == state.OpDelete:
		return false, nil
	case e.Upload != "":
		id, err := storeUpload(d.Backend, e.Bucket, e.Key)
		return id == e.Upload, err
	default:
		return head.Metadata[MetaCapture] == e.Token, nil
	}
}

// hashStored reads a stored object once and returns its size and SHA-256.
func (d *Decorator) hashStored(ctx context.Context, bucket, key string) (int64, string, error) {
	out, err := d.Backend.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return 0, "", err
	}
	defer out.Body.Close()
	h := sha256.New()
	n, err := io.Copy(h, ctxReader{ctx, out.Body})
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// mpMetaAttr is the attribute in which posix records, on an object it
// assembled from a multipart upload, the upload's ID (and part sizes):
// versitygw v1.10.0 backend/posix/posix.go, mpMetaKey, written by
// CompleteMultipartUpload and checked there to answer a retried
// completion. TestPendingCompletionMatchedByUploadNotETag pins it.
const mpMetaAttr = "mp-metadata"

// storeUpload returns the ID of the multipart upload that assembled
// key's object, or "" if it was stored another way (a PutObject drops
// the attribute) or is gone. It matches a pending completion's row to
// its own upload, where an ETag would also match an earlier upload of
// the same parts (#59). be is the posix store, or wraps it with an
// Unwrap method.
func storeUpload(be backend.Backend, bucket, key string) (string, error) {
	for {
		switch b := be.(type) {
		case *posix.Posix:
			v, err := meta.XattrMeta{}.RetrieveAttribute(nil, b.BucketPath(bucket), key, mpMetaAttr)
			if errors.Is(err, meta.ErrNoSuchKey) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
				return "", nil
			}
			if err != nil {
				return "", fmt.Errorf("reading the upload ID of %s/%s: %w", bucket, key, err)
			}
			m, err := backend.UnmarshalMpUploadMetadata(v, false)
			if err != nil {
				return "", fmt.Errorf("reading the upload ID of %s/%s: %w", bucket, key, err)
			}
			return m.UploadID, nil
		case interface{ Unwrap() backend.Backend }:
			be = b.Unwrap()
		default:
			return "", fmt.Errorf("the store (%T) records no multipart upload IDs", be)
		}
	}
}

func notFound(err error) bool {
	var ae s3err.APIError
	if errors.As(err, &ae) {
		return ae.Code == "NoSuchKey" || ae.Code == "NoSuchBucket" || ae.HTTPStatusCode == 404
	}
	return false
}
