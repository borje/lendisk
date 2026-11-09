package replica

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// The Friend's multipart calls (REPL-6, Q-Y.1): minio-go's low-level
// NewMultipartUpload, PutObjectPart, ListObjectParts and
// CompleteMultipartUpload. The upload is created with the whole
// object's SHA-256 as x-amz-meta-lendisk-sha256, which the lender
// checks against the assembled object (LEND-4), and with
// x-amz-checksum-algorithm SHA256; each part carries its own SHA-256 in
// an unsigned trailer, taken as the part streams, so each byte is read
// once (Q-Y.2). A rewrite (#74) carries its mark at the create.

var _ Multipart = (*Friend)(nil)

const checksumHeader = "X-Amz-Checksum-Sha256"

// Create begins an upload of o at the friend.
func (f *Friend) Create(ctx context.Context, o Object) (string, error) {
	if sum, err := hex.DecodeString(o.SHA256); err != nil || len(sum) != 32 {
		return "", fmt.Errorf("object %s has no SHA-256 to send (%q)", o.Key, o.SHA256)
	}
	sealed := f.seal(o.Bucket)
	ctx, cancel := context.WithTimeout(ctx, f.stall)
	defer cancel()
	if err := f.ensureBucket(ctx, sealed); err != nil {
		return "", err
	}
	meta := map[string]string{MetaSHA256: o.SHA256, "X-Amz-Checksum-Algorithm": "SHA256"}
	if o.Replaces != "" {
		meta[MetaReplaces] = o.Replaces
	}
	id, err := f.core.NewMultipartUpload(ctx, sealed, o.Key, minio.PutObjectOptions{UserMetadata: meta})
	if err != nil {
		return "", classify(fmt.Errorf("beginning the upload of %s: %w", o.Key, err))
	}
	return id, nil
}

// Parts lists the parts the friend holds for the upload.
func (f *Friend) Parts(ctx context.Context, bucket, key, uploadID string) (map[int]string, error) {
	ctx, cancel := context.WithTimeout(ctx, f.stall)
	defer cancel()
	sealed := f.seal(bucket)
	out := map[int]string{}
	marker := 0
	for {
		res, err := f.core.ListObjectParts(ctx, sealed, key, uploadID, marker, 1000)
		if err != nil {
			return nil, uploadErr(fmt.Errorf("listing the parts of %s: %w", key, err))
		}
		for _, p := range res.ObjectParts {
			out[p.PartNumber] = p.ETag
		}
		if !res.IsTruncated || res.NextPartNumberMarker <= marker {
			return out, nil
		}
		marker = res.NextPartNumberMarker
	}
}

// PutPart sends part n with an unsigned payload and its SHA-256 as an
// unsigned trailer. A part that moves no bytes for the stall timeout
// fails.
func (f *Friend) PutPart(ctx context.Context, bucket, key, uploadID string, n int, body io.Reader, size int64) (Part, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	trailer := http.Header{}
	// The signer sizes the request from the placeholder, so it is as
	// long as the final value (Q-Y.2).
	trailer.Set(checksumHeader, base64.StdEncoding.EncodeToString(make([]byte, sha256.Size)))
	h := &trailerHasher{r: body, h: sha256.New(), trailer: trailer}
	ctx, abandon, ended := abandoning(ctx, cancel)
	wd := newWatchdog(h, f.stall, abandon)
	defer wd.stop()
	p, err := f.core.PutObjectPart(ctx, f.seal(bucket), key, uploadID, n, wd, size, minio.PutObjectPartOptions{
		DisableContentSha256: true,
		Trailer:              trailer,
	})
	ended()
	if err != nil {
		if wd.stalled() {
			return Part{}, fmt.Errorf("sending part %d of %s: no progress for %v: %w", n, key, f.stall, err)
		}
		return Part{}, uploadErr(fmt.Errorf("sending part %d of %s: %w", n, key, err))
	}
	return Part{Number: n, ETag: p.ETag, SHA256: hex.EncodeToString(h.h.Sum(nil)), Size: size}, nil
}

// Complete assembles the parts at the friend, which verifies the whole
// object against the hash given at Create (LEND-4).
func (f *Friend) Complete(ctx context.Context, bucket, key, uploadID string, parts []Part) error {
	cps := make([]minio.CompletePart, 0, len(parts))
	for _, p := range parts {
		cp := minio.CompletePart{PartNumber: p.Number, ETag: p.ETag}
		if sum, err := hex.DecodeString(p.SHA256); err == nil && len(sum) == sha256.Size {
			cp.ChecksumSHA256 = base64.StdEncoding.EncodeToString(sum)
		}
		cps = append(cps, cp)
	}
	// The lender reads the assembled object once to verify it: allow it a
	// stall period plus a second per MiB.
	var size int64
	for _, p := range parts {
		size += p.Size
	}
	ctx, cancel := context.WithTimeout(ctx, f.stall+timePerMiB(size))
	defer cancel()
	if _, err := f.core.CompleteMultipartUpload(ctx, f.seal(bucket), key, uploadID, cps, minio.PutObjectOptions{}); err != nil {
		return uploadErr(fmt.Errorf("completing the upload of %s: %w", key, err))
	}
	return nil
}

// Abort drops the upload at the friend.
func (f *Friend) Abort(ctx context.Context, bucket, key, uploadID string) error {
	ctx, cancel := context.WithTimeout(ctx, f.stall)
	defer cancel()
	if err := f.core.AbortMultipartUpload(ctx, f.seal(bucket), key, uploadID); err != nil {
		return uploadErr(fmt.Errorf("aborting the upload of %s: %w", key, err))
	}
	return nil
}

// uploadErr classifies err, with NoSuchUpload as ErrNoSuchUpload.
func uploadErr(err error) error {
	var er minio.ErrorResponse
	if errors.As(err, &er) && (er.Code == "NoSuchUpload" || strings.EqualFold(er.Code, "NoSuchUpload")) {
		return fmt.Errorf("%w: %w", ErrNoSuchUpload, err)
	}
	return classify(err)
}

func timePerMiB(size int64) time.Duration { return time.Duration(size>>20) * time.Second }

// trailerHasher hashes what passes and writes the digest into the
// trailer at EOF, which the client sends after the last chunk.
type trailerHasher struct {
	r       io.Reader
	h       hash.Hash
	trailer http.Header
}

func (t *trailerHasher) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.h.Write(p[:n])
	if err == io.EOF {
		t.trailer.Set(checksumHeader, base64.StdEncoding.EncodeToString(t.h.Sum(nil)))
	}
	return n, err
}
