package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
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
			d.unmatchable(e, err)
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
	size, sum, stamp, err := d.hashStamped(ctx, e.Bucket, e.Key, nil)
	if err == nil {
		_, err = d.DB.FinishPut(ctx, e.Seq, size, sum)
	}
	if err == nil && stamp != "" {
		d.markHeld(ctx, e.Bucket, e.Key, sum, stamp)
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

// unmatchable reports a completion's row that cannot be matched to the
// object in the store (errUploadUnreadable), once per row while the
// daemon runs: it is retried every round and may never settle (#62).
// It is marked in lendisk.db (MarkUnmatchable), so status counts it as
// stuck and an alert names it until it settles (#20).
func (d *Decorator) unmatchable(e state.Entry, err error) {
	d.mu.Lock()
	seen := d.unmatched[e.Seq]
	if d.unmatched == nil {
		d.unmatched = map[int64]bool{}
	}
	d.unmatched[e.Seq] = true
	d.mu.Unlock()
	if seen {
		return
	}
	// In lendisk.db too, for status and the upload_unmatchable alert: the
	// row holds its bucket's metadata until it settles (#20).
	if merr := d.DB.MarkUnmatchable(context.Background(), e.Seq, err.Error()); merr != nil {
		d.Log.Error("capture: recording an unmatchable row failed", "seq", e.Seq, "err", merr)
	}
	d.Log.Warn("capture: a completed upload's row cannot be matched to the object in the store; leaving it pending and retrying while serving, reported once",
		"seq", e.Seq, "bucket", e.Bucket, "key", e.Key, "upload", e.Upload, "err", err)
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
		if errors.Is(err, errUploadUnreadable) && ctx.Err() == nil {
			d.unmatchable(e, err)
			continue
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
		delete(d.unmatched, seq)
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
// object, never by ETag alone: an upload of the same parts gives the same
// ETag (#59, #62). An object with no upload ID and a plain (PutObject)
// ETag replaced the upload's, which did not take effect here. An
// attribute that cannot be read, or an object with no upload ID whose
// ETag is a multipart one or missing (its attributes were lost, as when
// a store is copied without extended attributes), is an error wrapping
// errUploadUnreadable: the row is undecided.
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
		// No upload ID: a PutObject's object (a plain MD5 ETag) replaced
		// the upload's, or the object lost its attributes. An ETag never
		// stands in for the ID: an upload of the same parts gives it too.
		// A plain ETag has no '-' only while posix's DataIntegrityEtag is
		// off (OpenBackend); with it a PutObject's ETag contains one.
		if etag := strings.Trim(deref(head.ETag), `"`); etag == "" || strings.Contains(etag, "-") {
			return false, fmt.Errorf("%w: %s/%s: the object has no upload ID (ETag %q); its extended attributes may have been lost: restore them, or write the key again",
				errUploadUnreadable, e.Bucket, e.Key, etag)
		}
		return false, nil
	default:
		return head.Metadata[MetaCapture] == e.Token, nil
	}
}

// hashStored reads a stored object once and returns its size and SHA-256.
func (d *Decorator) hashStored(ctx context.Context, bucket, key string) (int64, string, error) {
	return d.hashPaced(ctx, bucket, key, nil)
}

// hashPaced reads a stored object once, each read through pace if set
// (the disk's read limit, VER-5), and returns its size and SHA-256.
func (d *Decorator) hashPaced(ctx context.Context, bucket, key string, pace func(context.Context, io.Reader) io.Reader) (int64, string, error) {
	out, err := d.Backend.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return 0, "", err
	}
	defer out.Body.Close()
	var r io.Reader = out.Body
	if pace != nil {
		r = pace(ctx, r)
	}
	h := sha256.New()
	n, err := io.Copy(h, ctxReader{ctx, r})
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// hashStamped hashes a stored object as hashPaced does and returns its
// file's identity (fileStamp) if the file did not change while it was
// read, "" otherwise: the identity those bytes belong to (VER-5).
func (d *Decorator) hashStamped(ctx context.Context, bucket, key string, pace func(context.Context, io.Reader) io.Reader) (n int64, sum, stamp string, err error) {
	before := d.stampOf(bucket, key)
	if n, sum, err = d.hashPaced(ctx, bucket, key, pace); err != nil {
		return 0, "", "", err
	}
	if after := d.stampOf(bucket, key); after == before {
		stamp = after
	}
	return n, sum, stamp, nil
}

// filePaths names the file of a stored object: *Store.
type filePaths interface {
	Path(bucket, key string) string
}

// stampOf is the identity of the file holding key's object in bucket
// (fileStamp), "" if there is none or it cannot be read.
func (d *Decorator) stampOf(bucket, key string) string {
	fp, ok := d.Uploads.(filePaths)
	if !ok {
		return ""
	}
	fi, err := os.Lstat(fp.Path(bucket, key))
	if err != nil || !fi.Mode().IsRegular() {
		return ""
	}
	return fileStamp(fi)
}

// markHeld marks key's version of hash sum held locally, with stamp the
// identity of the file this node wrote or hashed it from ("" for none).
// A failure is logged: the key stays unverified, which is safe.
func (d *Decorator) markHeld(ctx context.Context, bucket, key, sum, stamp string) {
	if err := d.DB.MarkLocal(ctx, bucket, key, sum, stamp); err != nil {
		d.Log.Warn("capture: recording an object's file failed; it stays unverified", "bucket", bucket, "key", key, "err", err)
	}
}

func notFound(err error) bool {
	var ae s3err.APIError
	if errors.As(err, &ae) {
		return ae.Code == "NoSuchKey" || ae.Code == "NoSuchBucket" || ae.HTTPStatusCode == 404
	}
	return false
}
