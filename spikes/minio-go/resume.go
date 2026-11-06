// Package main is a throwaway spike (issue #4): the replica client's
// multipart resume loop, rate limiter and timetable on minio-go's
// low-level calls, against a versitygw "friend" and a Garage container.
// It answers DESIGN.md §5 Q-Y items 1, 2 and 4.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Target is one place: what the replica client is built from.
type Target struct {
	Name     string // "friend", "garage"
	Endpoint string // host:port
	Region   string
	Access   string
	Secret   string
	Bucket   string
	Secure   bool
	// Checksum is the per-part request checksum: "" none; "sha256" a
	// SHA-256 computed while streaming and sent as a trailer (one read);
	// "sha256-header" a SHA-256 computed first and sent as a header (two
	// reads of the part, for comparison).
	Checksum string
	// UnsignedPayload sets DisableContentSha256: over plain HTTP minio-go
	// otherwise signs every 64 KiB chunk (aws-chunked).
	UnsignedPayload bool
	Transport       http.RoundTripper
}

func newCore(t Target) (*minio.Core, error) {
	return minio.NewCore(t.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(t.Access, t.Secret, ""),
		Secure:       t.Secure,
		Region:       t.Region,
		BucketLookup: minio.BucketLookupPath,
		Transport:    t.Transport,
		// The worker owns retries (one part at a time); minio-go would
		// only retry seekable bodies anyway, and ours is not.
		MaxRetries: 1,
	})
}

// State is what survives a SIGKILL: in lendisk it is the queue row
// (REPL-6). Here it is a small JSON file replaced atomically.
type State struct {
	UploadID string               `json:"upload_id"`
	Parts    []minio.CompletePart `json:"parts"`
}

func loadState(path string) (State, error) {
	var s State
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}

// saveState writes to a temp file, fsyncs and renames, so a kill at any
// instant leaves either the old or the new state.
func saveState(path string, s State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Job is one object to upload in parts.
type Job struct {
	Source    string // local file (the store object)
	Key       string
	PartSize  int64
	SHA256Hex string // REPL-10's hash, from the queue row
	StatePath string
}

// Events lets the caller observe the loop (the test drives SIGKILL off
// these; the CLI prints them).
type Events struct {
	Resumed   func(uploadID string, have []int)
	PartStart func(n int, off, size int64)
	PartDone  func(n int, etag string)
}

// ErrWindowClosed: the timetable closed; the state is saved and the
// next call continues at the next part.
var ErrWindowClosed = errors.New("replication window closed")

// Upload runs the resume loop: create or reuse the upload, check the
// stored parts against ListObjectParts, send the missing parts one at a
// time through the limiter (checking the window before each), save the
// state after each, complete. It reads each byte of a part once per
// attempt; parts already acknowledged are never read again.
func Upload(ctx context.Context, core *minio.Core, t Target, j Job, l *Limits, w Window, ev Events) (minio.UploadInfo, error) {
	st, err := loadState(j.StatePath)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	f, err := os.Open(j.Source)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return minio.UploadInfo{}, err
	}
	nParts := int((fi.Size() + j.PartSize - 1) / j.PartSize)

	if st.UploadID != "" {
		have, err := checkParts(ctx, core, t, j.Key, st)
		if err != nil {
			var er minio.ErrorResponse
			if errors.As(err, &er) && er.Code == "NoSuchUpload" {
				st = State{} // aborted or expired at the target: start over
			} else {
				return minio.UploadInfo{}, err
			}
		} else {
			st.Parts = have
			if ev.Resumed != nil {
				ev.Resumed(st.UploadID, partNumbers(have))
			}
		}
	}
	if st.UploadID == "" {
		opts := minio.PutObjectOptions{
			UserMetadata: map[string]string{"lendisk-sha256": j.SHA256Hex},
		}
		if t.Checksum != "" {
			// Composite (per-part) SHA-256: S3 has no full-object SHA-256
			// for multipart, only for CRC types.
			opts.UserMetadata["X-Amz-Checksum-Algorithm"] = "SHA256"
		}
		id, err := core.NewMultipartUpload(ctx, t.Bucket, j.Key, opts)
		if err != nil {
			return minio.UploadInfo{}, fmt.Errorf("create multipart upload: %w", err)
		}
		st = State{UploadID: id}
		if err := saveState(j.StatePath, st); err != nil {
			return minio.UploadInfo{}, err
		}
	}

	done := map[int]bool{}
	for _, p := range st.Parts {
		done[p.PartNumber] = true
	}
	for n := 1; n <= nParts; n++ {
		if done[n] {
			continue
		}
		if !w.Open(l.now()) {
			return minio.UploadInfo{}, ErrWindowClosed
		}
		off := int64(n-1) * j.PartSize
		size := min(j.PartSize, fi.Size()-off)
		if ev.PartStart != nil {
			ev.PartStart(n, off, size)
		}
		cp, err := putPart(ctx, core, t, j.Key, st.UploadID, n, io.NewSectionReader(f, off, size), size, l)
		if err != nil {
			return minio.UploadInfo{}, fmt.Errorf("part %d: %w", n, err)
		}
		st.Parts = append(st.Parts, cp)
		if err := saveState(j.StatePath, st); err != nil {
			return minio.UploadInfo{}, err
		}
		if ev.PartDone != nil {
			ev.PartDone(n, cp.ETag)
		}
	}
	sortParts(st.Parts)
	info, err := core.CompleteMultipartUpload(ctx, t.Bucket, j.Key, st.UploadID, st.Parts, minio.PutObjectOptions{})
	if err != nil {
		return info, fmt.Errorf("complete: %w", err)
	}
	return info, os.Remove(j.StatePath)
}

// checkParts lists the open upload's parts and keeps the stored ones
// whose ETag matches. A part the target has but the state lacks (killed
// between the response and the save) is sent again; that is harmless.
func checkParts(ctx context.Context, core *minio.Core, t Target, key string, st State) ([]minio.CompletePart, error) {
	at := map[int]string{}
	marker := 0
	for {
		res, err := core.ListObjectParts(ctx, t.Bucket, key, st.UploadID, marker, 1000)
		if err != nil {
			return nil, err
		}
		for _, p := range res.ObjectParts {
			at[p.PartNumber] = p.ETag
		}
		if !res.IsTruncated {
			break
		}
		marker = res.NextPartNumberMarker
	}
	var keep []minio.CompletePart
	for _, p := range st.Parts {
		if e, ok := at[p.PartNumber]; ok && trimETag(e) == trimETag(p.ETag) {
			keep = append(keep, p)
		}
	}
	return keep, nil
}

// putPart sends one part. The body is the section of the store file,
// wrapped by the limiter and, for "sha256", a hasher that fills the
// trailer at EOF: one read of the bytes, no buffering.
func putPart(ctx context.Context, core *minio.Core, t Target, key, uploadID string, n int, r *io.SectionReader, size int64, l *Limits) (minio.CompletePart, error) {
	body := io.Reader(l.Reader(ctx, r))
	opts := minio.PutObjectPartOptions{DisableContentSha256: t.UnsignedPayload}
	switch t.Checksum {
	case "sha256-header":
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(r, 0, size)); err != nil {
			return minio.CompletePart{}, err
		}
		opts.CustomHeader = http.Header{}
		opts.CustomHeader.Set("X-Amz-Checksum-Sha256", base64.StdEncoding.EncodeToString(h.Sum(nil)))
	case "sha256":
		trailer := http.Header{}
		// The streaming signer sizes Content-Length from the placeholder,
		// so it must be as long as the final value (44 base64 chars).
		trailer.Set("X-Amz-Checksum-Sha256", base64.StdEncoding.EncodeToString(make([]byte, 32)))
		body = &trailerHasher{r: body, h: sha256.New(), trailer: trailer, key: "X-Amz-Checksum-Sha256"}
		opts.Trailer = trailer
	}
	p, err := core.PutObjectPart(ctx, t.Bucket, key, uploadID, n, body, size, opts)
	if err != nil {
		return minio.CompletePart{}, err
	}
	return minio.CompletePart{PartNumber: n, ETag: p.ETag, ChecksumSHA256: p.ChecksumSHA256}, nil
}

// trailerHasher hashes what passes and writes the digest into the
// trailer at EOF, which the signer sends after the last chunk.
type trailerHasher struct {
	r       io.Reader
	h       hash.Hash
	trailer http.Header
	key     string
}

func (t *trailerHasher) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.h.Write(p[:n])
	if err == io.EOF {
		t.trailer.Set(t.key, base64.StdEncoding.EncodeToString(t.h.Sum(nil)))
	}
	return n, err
}

func partNumbers(ps []minio.CompletePart) []int {
	var out []int
	for _, p := range ps {
		out = append(out, p.PartNumber)
	}
	return out
}

func sortParts(ps []minio.CompletePart) {
	for i := 1; i < len(ps); i++ {
		for k := i; k > 0 && ps[k].PartNumber < ps[k-1].PartNumber; k-- {
			ps[k], ps[k-1] = ps[k-1], ps[k]
		}
	}
}

func trimETag(e string) string {
	if len(e) >= 2 && e[0] == '"' && e[len(e)-1] == '"' {
		return e[1 : len(e)-1]
	}
	return e
}
