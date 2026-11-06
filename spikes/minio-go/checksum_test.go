package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ---- Q-Y.2: request checksums ----

// recorder notes the checksum-relevant headers of every PUT/POST.
type recorder struct {
	mu   sync.Mutex
	seen []string
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPut || req.Method == http.MethodPost {
		var hs []string
		for k, v := range req.Header {
			lk := strings.ToLower(k)
			if strings.HasPrefix(lk, "x-amz-checksum") || strings.HasPrefix(lk, "x-amz-sdk-checksum") ||
				lk == "x-amz-content-sha256" || lk == "x-amz-trailer" || lk == "content-md5" ||
				lk == "content-encoding" {
				val := v[0]
				if len(val) > 40 && lk != "x-amz-content-sha256" {
					val = val[:12] + "…"
				}
				if lk == "x-amz-content-sha256" && len(val) == 64 {
					val = "<hex sha256 of body>"
				}
				hs = append(hs, lk+"="+val)
			}
		}
		sort.Strings(hs)
		q := req.URL.Query()
		what := "PutObject"
		switch {
		case q.Has("uploads"):
			what = "CreateMultipartUpload"
		case q.Has("partNumber"):
			what = "UploadPart"
		case q.Has("uploadId"):
			what = "CompleteMultipartUpload"
		}
		r.mu.Lock()
		r.seen = append(r.seen, what+" "+strings.Join(hs, " "))
		r.mu.Unlock()
	}
	return http.DefaultTransport.RoundTrip(req)
}

func (r *recorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.seen
	r.seen = nil
	return out
}

func verdict(err error) string {
	if err == nil {
		return "accepted"
	}
	er := minio.ToErrorResponse(err)
	return fmt.Sprintf("rejected %d %s: %s", er.StatusCode, er.Code, er.Message)
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// TestMinioGoDefaults: what the high-level PutObject sends with default
// Options and with TrailingHeaders on, single-part and multipart, and
// whether each target takes it.
func TestMinioGoDefaults(t *testing.T) {
	eachTarget(t, func(t *testing.T, tg Target) {
		for _, trailing := range []bool{false, true} {
			for _, size := range []int{1 * MiB, 12 * MiB} {
				rec := &recorder{}
				c, err := minio.New(tg.Endpoint, &minio.Options{
					Creds:           credentials.NewStaticV4(tg.Access, tg.Secret, ""),
					Region:          tg.Region,
					BucketLookup:    minio.BucketLookupPath,
					Transport:       rec,
					TrailingHeaders: trailing,
				})
				if err != nil {
					t.Fatal(err)
				}
				key := fmt.Sprintf("defaults/%v/%d", trailing, size)
				_, err = c.PutObject(context.Background(), tg.Bucket, key, bytes.NewReader(randBytes(size)), int64(size),
					minio.PutObjectOptions{PartSize: 5 * MiB})
				t.Logf("RESULT %s TrailingHeaders=%v size=%dMiB: %s", tg.Name, trailing, size/MiB, verdict(err))
				for _, l := range dedupe(rec.take()) {
					t.Logf("    sent: %s", l)
				}
			}
		}
	})
}

func dedupe(ls []string) []string {
	var out []string
	for i, l := range ls {
		if i == 0 || l != ls[i-1] {
			out = append(out, l)
		}
	}
	return out
}

// TestSHA256RequestChecksum: lendisk's own calls. For each target, each
// payload mode (signed aws-chunked, as over plain HTTP in the tunnel;
// unsigned, as minio-go does over HTTPS) and each way of sending a
// SHA-256: whether a correct value is accepted and a wrong one refused.
func TestSHA256RequestChecksum(t *testing.T) {
	eachTarget(t, func(t *testing.T, tg Target) {
		for _, unsigned := range []bool{false, true} {
			payload := map[bool]string{false: "signed-chunks", true: "unsigned"}[unsigned]
			rec := &recorder{}
			tg := tg
			tg.UnsignedPayload = unsigned
			tg.Transport = rec
			core, err := newCore(tg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			data := randBytes(5*MiB + 1234)
			sum := sha256.Sum256(data)
			good := base64.StdEncoding.EncodeToString(sum[:])
			bad := base64.StdEncoding.EncodeToString(make([]byte, 32))

			// Single PUT with the whole object's SHA-256 as a header.
			for _, v := range []struct{ name, val string }{{"correct", good}, {"wrong", bad}} {
				_, err := core.PutObject(ctx, tg.Bucket, "cs/single-"+payload+"-"+v.name, bytes.NewReader(data), int64(len(data)), "", "",
					minio.PutObjectOptions{
						DisableContentSha256: unsigned,
						UserMetadata:         map[string]string{"X-Amz-Checksum-Sha256": v.val},
					})
				t.Logf("RESULT %s %s single PUT, sha256 header %s: %s", tg.Name, payload, v.name, verdict(err))
			}

			// Multipart, one part, each mode, correct then wrong.
			for _, mode := range []string{"", "sha256", "sha256-header"} {
				tm := tg
				tm.Checksum = mode
				key := "cs/mp-" + payload + "-" + mode
				id, err := core.NewMultipartUpload(ctx, tg.Bucket, key, minio.PutObjectOptions{
					UserMetadata: map[string]string{"X-Amz-Checksum-Algorithm": "SHA256"},
				})
				if mode == "" {
					id, err = core.NewMultipartUpload(ctx, tg.Bucket, key, minio.PutObjectOptions{})
				}
				if err != nil {
					t.Fatalf("create: %v", err)
				}
				name := map[string]string{"": "none", "sha256": "sha256 trailer", "sha256-header": "sha256 header"}[mode]
				cp, err := putPart(ctx, core, tm, key, id, 1, io.NewSectionReader(bytes.NewReader(data), 0, int64(len(data))), int64(len(data)), nil)
				t.Logf("RESULT %s %s UploadPart, %s correct: %s", tg.Name, payload, name, verdict(err))
				if err == nil {
					_, err = core.CompleteMultipartUpload(ctx, tg.Bucket, key, id, []minio.CompletePart{cp}, minio.PutObjectOptions{})
					t.Logf("RESULT %s %s   CompleteMultipartUpload: %s", tg.Name, payload, verdict(err))
				}
				if mode == "" {
					continue
				}
				// The same with a wrong value.
				id2, err := core.NewMultipartUpload(ctx, tg.Bucket, key+"-wrong", minio.PutObjectOptions{
					UserMetadata: map[string]string{"X-Amz-Checksum-Algorithm": "SHA256"},
				})
				if err != nil {
					t.Fatal(err)
				}
				opts := minio.PutObjectPartOptions{DisableContentSha256: unsigned}
				if mode == "sha256" {
					opts.Trailer = http.Header{"X-Amz-Checksum-Sha256": {bad}} // placeholder never filled
				} else {
					opts.CustomHeader = http.Header{"X-Amz-Checksum-Sha256": {bad}}
				}
				_, err = core.PutObjectPart(ctx, tg.Bucket, key+"-wrong", id2, 1, bytes.NewReader(data), int64(len(data)), opts)
				t.Logf("RESULT %s %s UploadPart, %s wrong: %s", tg.Name, payload, name, verdict(err))
				core.AbortMultipartUpload(ctx, tg.Bucket, key+"-wrong", id2)
			}
			for _, l := range dedupe(rec.take()) {
				t.Logf("    sent: %s", l)
			}
		}
	})
}
