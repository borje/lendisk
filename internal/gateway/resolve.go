package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
// A completion's row whose object's upload ID cannot be read is logged
// and left pending and stranded, for ResolveStranded to retry (#62); it
// does not stop the start. It returns how many rows it marked done and
// removed.
func (d *Decorator) ResolvePending(ctx context.Context) (done, dropped int, err error) {
	if d.DB == nil {
		return 0, 0, nil
	}
	pending, err := d.DB.PendingCaptures(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range pending {
		ok, err := d.resolve(ctx, e, nil)
		if errors.Is(err, errUploadUnreadable) && ctx.Err() == nil {
			d.Log.Warn("capture: a completed upload's row cannot be matched to the object in the store; leaving it pending and retrying while serving",
				"seq", e.Seq, "bucket", e.Bucket, "key", e.Key, "upload", e.Upload, "err", err)
			d.strand(e.Seq)
			continue
		}
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
// does; done is false when the row was removed. landed, if set, is
// called once the row is known to have taken effect, before the object
// is hashed.
func (d *Decorator) resolve(ctx context.Context, e state.Entry, landed func()) (done bool, err error) {
	ok, err := d.landed(ctx, e)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, d.DB.DropCaptures(ctx, e.Seq)
	}
	if landed != nil {
		landed()
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
			var busy bool
			if busy, err = d.settleStranded(ctx, e); busy {
				continue
			}
		}
		if err != nil {
			if ctx.Err() == nil {
				d.Log.Warn("capture: settling a stranded queue row failed; retrying later",
					"seq", seq, "bucket", e.Bucket, "key", e.Key, "err", err)
			}
			continue
		}
		d.mu.Lock()
		delete(d.stranded, seq)
		d.mu.Unlock()
	}
}

// settleStranded settles one stranded row under its key's lock; busy
// means a request holds the key (or claims the row's upload) and the row
// is left for the next round. A completion's row is settled holding the
// upload's claim, which is settled as stored as soon as the object is
// known to have landed, before the hash: a client's retry of the
// completion meanwhile waits for that answer, a HEAD and an attribute
// read, never for the hash (#62).
func (d *Decorator) settleStranded(ctx context.Context, e state.Entry) (busy bool, err error) {
	var landed func()
	if e.Upload != "" {
		id := uploadClaim(e.Bucket, e.Key, e.Upload)
		f, mine := d.claim(id)
		if !mine {
			return true, nil // a retry of the completion is settling it
		}
		defer d.unclaim(id, f, nil) // after the key's lock is released
		landed = func() { f.settle(true) }
	}
	// A request writing the key now would race the read of the store.
	unlock, free := d.keys.tryLock(e.Bucket, e.Key)
	if !free {
		return true, nil
	}
	defer unlock()
	e, ok, err := d.DB.PendingCapture(ctx, e.Seq)
	if err != nil || !ok {
		return false, err
	}
	_, err = d.resolve(ctx, e, landed)
	return false, err
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

// landed reports whether a pending row's backend call took effect. A
// completion's row is matched by the upload ID posix records on the
// object. If the object carries none but has the row's ETag (the
// attribute was lost, as when a store is copied without extended
// attributes), it is taken as landed and logged: the object holds the
// bytes the row's parts give. An attribute that cannot be read is an
// error wrapping errUploadUnreadable: the row is undecided.
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
		if d.Uploads == nil {
			return false, fmt.Errorf("%w: the store (%T) records no multipart upload IDs", errUploadUnreadable, d.Backend)
		}
		id, err := d.Uploads.UploadID(e.Bucket, e.Key)
		if err != nil || id != "" {
			return id == e.Upload, err
		}
		if e.ETag != "" && strings.Trim(deref(head.ETag), `"`) == strings.Trim(e.ETag, `"`) {
			d.Log.Warn("capture: a stored object has a completed upload's ETag but no upload ID; taking it as that upload's",
				"seq", e.Seq, "bucket", e.Bucket, "key", e.Key, "upload", e.Upload, "etag", e.ETag)
			return true, nil
		}
		return false, nil
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

func notFound(err error) bool {
	var ae s3err.APIError
	if errors.As(err, &ae) {
		return ae.Code == "NoSuchKey" || ae.Code == "NoSuchBucket" || ae.HTTPStatusCode == 404
	}
	return false
}
