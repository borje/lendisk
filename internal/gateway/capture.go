package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"maps"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// MetaCapture is the user metadata key every captured PutObject stores
// with the object: the token of its queue row, which tells startup whether
// the object on disk is the one a pending row was written for (EP-3).
const MetaCapture = "lendisk-capture"

// Decorator wraps the store's backend (D4). It captures every successful
// write and delete of an own bucket's key into the replication queue
// (EP-2, EP-3), classified by the bucket's tool profile (EP-7); quota
// (#16) and tombstones (#17) are added as further overriding methods.
// Buckets that are not the node's own (lent-*) pass through.
//
// Capture is a pending row written before the backend call, marked done
// with the SHA-256 taken from the request stream as it passes (REPL-10),
// or removed when the call fails; ResolvePending settles rows a crash
// left. Bodies are never buffered or read twice.
//
// Overrides must return unwrapped s3err.APIError values for client-visible
// refusals (Q-I.5).
type Decorator struct {
	backend.Backend
	// DB holds the queue; nil passes everything through.
	DB       *state.DB
	Profiles profile.Set
	Log      *slog.Logger
	// AfterStore, if set, is called after the store accepted a captured
	// request and before its row is completed: the crash window startup
	// settles. Tests only.
	AfterStore func()
}

var (
	errInternal       = s3err.GetAPIError(s3err.ErrInternalError)
	errCopyNotOffered = s3err.APIError{Code: "NotImplemented", HTTPStatusCode: 501,
		Description: "lendisk: server-side copy is not offered on a backed-up bucket; upload the object instead"}
)

// classify returns the class of an own bucket's key, or "" when the
// bucket is not the node's own or capture is off. An error means the
// state database could not be read; the request must fail rather than go
// uncaptured.
func (d *Decorator) classify(bucket, key *string) (profile.Class, error) {
	if d.DB == nil || bucket == nil || key == nil {
		return "", nil
	}
	b, err := d.DB.GetBucket(context.Background(), *bucket)
	if errors.Is(err, state.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		d.Log.Error("capture: reading the bucket's profile failed; refusing the request", "bucket", *bucket, "err", err)
		return "", errInternal
	}
	return d.Profiles.Get(b.Profile).Classify(*key), nil
}

// captured reports whether a class is queued: an own bucket's key that
// is not local-only (EP-7).
func captured(c profile.Class) bool { return c != "" && c != profile.LocalOnly }

// PutObject captures a successful put (EP-2) with the hash of its stream.
func (d *Decorator) PutObject(ctx context.Context, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil || !captured(class) {
		if err != nil {
			return s3response.PutObjectOutput{}, err
		}
		return d.Backend.PutObject(ctx, in)
	}
	token := randomHex(8)
	in.Metadata = maps.Clone(in.Metadata)
	if in.Metadata == nil {
		in.Metadata = map[string]string{}
	}
	in.Metadata[MetaCapture] = token
	seq, err := d.begin(state.Capture{Bucket: *in.Bucket, Key: *in.Key, Op: state.OpPut, Class: string(class), Token: token})
	if err != nil {
		return s3response.PutObjectOutput{}, err
	}
	var h *hashReader
	in.Body, h = hashing(in.Body)
	out, err := d.Backend.PutObject(ctx, in)
	if err != nil {
		d.drop(seq)
		return out, err
	}
	d.stored()
	if _, err := d.DB.FinishPut(context.Background(), seq, h.n, h.hex()); err != nil {
		// The object is stored and the row stays pending: startup
		// resolves it. The client sees a retryable error.
		d.Log.Error("capture: completing a queue row failed", "bucket", *in.Bucket, "key", *in.Key, "seq", seq, "err", err)
		return s3response.PutObjectOutput{}, errInternal
	}
	return out, nil
}

// UploadPart records the hash of each part of an own bucket's upload as
// it is written; a part is not itself queued (EP-2).
func (d *Decorator) UploadPart(ctx context.Context, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil {
		return nil, err
	}
	if !captured(class) || in.UploadId == nil || in.PartNumber == nil {
		return d.Backend.UploadPart(ctx, in)
	}
	cp := *in
	var h *hashReader
	cp.Body, h = hashing(in.Body)
	out, err := d.Backend.UploadPart(ctx, &cp)
	if err != nil {
		return out, err
	}
	d.stored()
	if err := d.DB.SaveUploadPart(context.Background(), *in.UploadId,
		state.Part{Number: *in.PartNumber, Size: h.n, SHA256: h.hex()}); err != nil {
		d.Log.Error("capture: saving a part hash failed", "bucket", *in.Bucket, "key", *in.Key, "err", err)
		return nil, errInternal
	}
	return out, nil
}

// CompleteMultipartUpload captures a completed upload (EP-2). Its hash is
// the composite of the part hashes taken as the parts were written
// (§3.3): SHA-256 over the parts' SHA-256 digests in order, with the part
// count, as "<hex>-<n>".
func (d *Decorator) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	if !captured(class) || in.UploadId == nil || in.MultipartUpload == nil {
		return d.Backend.CompleteMultipartUpload(ctx, in)
	}
	bg := context.Background()
	uploadID := *in.UploadId
	// A retry of a completion that was already captured completes again
	// in the store (posix answers it from the finished object) but is not
	// queued twice.
	if done, err := d.DB.UploadCaptured(bg, *in.Bucket, *in.Key, uploadID); err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", errInternal
	} else if done {
		return d.Backend.CompleteMultipartUpload(ctx, in)
	}
	etag, err := backend.ComputeMultipartETagFromPartETags(uploadID, in.MultipartUpload.Parts)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	have, err := d.DB.UploadParts(bg, uploadID)
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", errInternal
	}
	parts := make([]state.Part, 0, len(in.MultipartUpload.Parts))
	var size int64
	for _, p := range in.MultipartUpload.Parts {
		hp, ok := have[*p.PartNumber]
		if !ok {
			// No part the client has an ETag for lacks a hash: the hash
			// is saved before UploadPart answers. Let the store judge.
			return s3response.CompleteMultipartUploadResult{}, "", s3err.GetInvalidPartErr(uploadID, *p.PartNumber, deref(p.ETag))
		}
		parts = append(parts, hp)
		size += hp.Size
	}
	seq, err := d.begin(state.Capture{Bucket: *in.Bucket, Key: *in.Key, Op: state.OpPut, Class: string(class),
		ETag: etag, UploadID: uploadID, Parts: parts})
	if err != nil {
		return s3response.CompleteMultipartUploadResult{}, "", err
	}
	res, ver, err := d.Backend.CompleteMultipartUpload(ctx, in)
	if err != nil {
		d.drop(seq)
		return res, ver, err
	}
	d.stored()
	if _, err := d.DB.FinishPut(bg, seq, size, compositeHash(parts)); err != nil {
		d.Log.Error("capture: completing a queue row failed", "bucket", *in.Bucket, "key", *in.Key, "seq", seq, "err", err)
		return s3response.CompleteMultipartUploadResult{}, "", errInternal
	}
	return res, ver, nil
}

// AbortMultipartUpload forgets the upload's part hashes; nothing is
// queued (EP-2).
func (d *Decorator) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput) error {
	if err := d.Backend.AbortMultipartUpload(ctx, in); err != nil {
		return err
	}
	if d.DB != nil && in.UploadId != nil {
		if err := d.DB.DropUpload(context.Background(), *in.UploadId); err != nil {
			d.Log.Warn("capture: forgetting an aborted upload's parts failed", "upload", *in.UploadId, "err", err)
		}
	}
	return nil
}

// DeleteObject captures a successful delete (EP-2).
func (d *Decorator) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil {
		return nil, err
	}
	if !captured(class) {
		return d.Backend.DeleteObject(ctx, in)
	}
	seq, err := d.begin(state.Capture{Bucket: *in.Bucket, Key: *in.Key, Op: state.OpDelete, Class: string(class)})
	if err != nil {
		return nil, err
	}
	out, err := d.Backend.DeleteObject(ctx, in)
	if err != nil {
		d.drop(seq)
		return out, err
	}
	d.stored()
	if err := d.DB.FinishDelete(context.Background(), seq); err != nil {
		d.Log.Error("capture: completing a queue row failed", "bucket", *in.Bucket, "key", *in.Key, "seq", seq, "err", err)
		return nil, errInternal
	}
	return out, nil
}

// DeleteObjects captures each successfully deleted entry (EP-2).
func (d *Decorator) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	if d.DB == nil || in.Delete == nil {
		return d.Backend.DeleteObjects(ctx, in)
	}
	var cs []state.Capture
	for _, o := range in.Delete.Objects {
		class, err := d.classify(in.Bucket, o.Key)
		if err != nil {
			return s3response.DeleteResult{}, err
		}
		if captured(class) {
			cs = append(cs, state.Capture{Bucket: *in.Bucket, Key: *o.Key, Op: state.OpDelete, Class: string(class)})
		}
	}
	if len(cs) == 0 {
		return d.Backend.DeleteObjects(ctx, in)
	}
	bg := context.Background()
	seqs, err := d.DB.BeginCaptures(bg, cs)
	if err != nil {
		d.Log.Error("capture: writing queue rows failed; refusing the request", "bucket", *in.Bucket, "err", err)
		return s3response.DeleteResult{}, errInternal
	}
	res, err := d.Backend.DeleteObjects(ctx, in)
	if err != nil {
		d.drop(seqs...)
		return res, err
	}
	deleted := map[string]bool{}
	for _, o := range res.Deleted {
		if o.Key != nil {
			deleted[*o.Key] = true
		}
	}
	var done, failed []int64
	for i, c := range cs {
		if deleted[c.Key] {
			done = append(done, seqs[i])
		} else {
			failed = append(failed, seqs[i])
		}
	}
	d.stored()
	d.drop(failed...)
	if err := d.DB.FinishDelete(bg, done...); err != nil {
		d.Log.Error("capture: completing queue rows failed", "bucket", *in.Bucket, "err", err)
		return s3response.DeleteResult{}, errInternal
	}
	return res, nil
}

// CopyObject is refused on own buckets: the copy's bytes never pass the
// decorator, so it could not be hashed from a stream (Q-H, §3.3). No
// backup tool lendisk serves needs it.
func (d *Decorator) CopyObject(ctx context.Context, in s3response.CopyObjectInput) (s3response.CopyObjectOutput, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil {
		return s3response.CopyObjectOutput{}, err
	}
	if class != "" {
		return s3response.CopyObjectOutput{}, errCopyNotOffered
	}
	return d.Backend.CopyObject(ctx, in)
}

// UploadPartCopy is refused on own buckets, as CopyObject is.
func (d *Decorator) UploadPartCopy(ctx context.Context, in *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	class, err := d.classify(in.Bucket, in.Key)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}
	if class != "" {
		return s3response.CopyPartResult{}, errCopyNotOffered
	}
	return d.Backend.UploadPartCopy(ctx, in)
}

func (d *Decorator) stored() {
	if d.AfterStore != nil {
		d.AfterStore()
	}
}

func (d *Decorator) begin(c state.Capture) (int64, error) {
	seqs, err := d.DB.BeginCaptures(context.Background(), []state.Capture{c})
	if err != nil {
		d.Log.Error("capture: writing a queue row failed; refusing the request", "bucket", c.Bucket, "key", c.Key, "err", err)
		return 0, errInternal
	}
	return seqs[0], nil
}

func (d *Decorator) drop(seqs ...int64) {
	if err := d.DB.DropCaptures(context.Background(), seqs...); err != nil {
		// Left pending, the rows are resolved at the next start.
		d.Log.Warn("capture: removing the rows of a failed request failed", "seqs", seqs, "err", err)
	}
}

// compositeHash is a multipart object's hash: SHA-256 over the binary
// SHA-256 of each part in order, hex, then "-" and the part count.
func compositeHash(parts []state.Part) string {
	h := sha256.New()
	for _, p := range parts {
		b, _ := hex.DecodeString(p.SHA256)
		h.Write(b)
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(parts))
}

// hashReader hashes what is read through it, so the hash is taken as the
// stream passes to the store.
type hashReader struct {
	r io.Reader
	h hash.Hash
	n int64
}

func (h *hashReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		h.h.Write(p[:n])
		h.n += int64(n)
	}
	return n, err
}

func (h *hashReader) hex() string { return hex.EncodeToString(h.h.Sum(nil)) }

// checksumHashReader keeps versitygw's ChecksumReader visible through the
// wrapper: posix asks the body for a trailing checksum by type assertion.
type checksumHashReader struct {
	*hashReader
	cr middlewares.ChecksumReader
}

func (c checksumHashReader) Algorithm() string { return c.cr.Algorithm() }
func (c checksumHashReader) Checksum() string  { return c.cr.Checksum() }

// hashing wraps body; a nil body reads as empty.
func hashing(body io.Reader) (io.Reader, *hashReader) {
	if body == nil {
		body = eofReader{}
	}
	h := &hashReader{r: body, h: sha256.New()}
	if cr, ok := body.(middlewares.ChecksumReader); ok {
		return checksumHashReader{h, cr}, h
	}
	return h, h
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
