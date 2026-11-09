package replica

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

// Undelete and verification at an overflow target (S3-4, S3-6, RET-3,
// VER-2, #39): what the provider's versions and stored checksums say,
// read through the same client.

// Deleted is a key deleted at a versioned overflow target: its newest
// version is a delete marker, made at DeletedAt, and VersionID is the
// newest version before that marker, which undelete makes current
// again (S3-4).
type Deleted struct {
	Key       string // the node's own key, below <prefix>/<sealed name>/
	DeletedAt time.Time
	VersionID string
	Size      int64
	SHA256    string // the version's x-amz-meta-lendisk-sha256; "" when it has none
}

// ListDeleted lists the keys of bucket at the target whose newest
// version is a delete marker made at or after since, each with the
// newest version before the marker, by key. A key whose earlier
// versions the lifecycle rule has purged has nothing to bring back and
// is left out. Each listed version is read once (a HEAD) for its hash.
func (f *Client) ListDeleted(ctx context.Context, bucket string, since time.Time) ([]Deleted, error) {
	b, pfx := f.where(bucket, "")
	// A provider need not list a key's versions together (RustFS lists
	// every delete marker of a page before its versions), so they are
	// gathered by key; the provider flags the current one (IsLatest).
	vers := map[string][]minio.ObjectInfo{}
	for o := range f.core.Client.ListObjects(ctx, b, minio.ListObjectsOptions{Prefix: pfx, Recursive: true, WithVersions: true}) {
		if o.Err != nil {
			var er minio.ErrorResponse
			if errors.As(o.Err, &er) && er.Code == "NoSuchBucket" {
				return nil, nil
			}
			return nil, f.classify(fmt.Errorf("listing the versions under %s: %w", pfx, o.Err))
		}
		k := strings.TrimPrefix(o.Key, pfx)
		vers[k] = append(vers[k], o)
	}
	var out []Deleted
	for _, key := range slices.Sorted(maps.Keys(vers)) {
		// Newest first; at equal times the current version (IsLatest),
		// then the order the provider listed them in, which S3 defines
		// as newest first.
		vs := vers[key]
		slices.SortStableFunc(vs, func(a, b minio.ObjectInfo) int {
			if c := b.LastModified.Compare(a.LastModified); c != 0 {
				return c
			}
			switch {
			case a.IsLatest && !b.IsLatest:
				return -1
			case b.IsLatest && !a.IsLatest:
				return 1
			}
			return 0
		})
		latest := vs[0]
		if !latest.IsLatest || !latest.IsDeleteMarker || latest.LastModified.Before(since) {
			continue
		}
		// The version just before the marker. Another marker there means
		// the key was already deleted then; none means the lifecycle rule
		// purged it: either way nothing comes back (#39 review).
		if len(vs) < 2 || vs[1].IsDeleteMarker {
			continue
		}
		prev := &vs[1]
		info, err := f.core.StatObject(ctx, b, prev.Key, minio.StatObjectOptions{VersionID: prev.VersionID})
		if err != nil {
			return nil, f.classify(fmt.Errorf("reading version %s of %s: %w", prev.VersionID, key, err))
		}
		out = append(out, Deleted{Key: key, DeletedAt: latest.LastModified, VersionID: prev.VersionID, Size: prev.Size,
			SHA256: strings.ToLower(info.Metadata.Get("X-Amz-Meta-" + MetaSHA256))})
	}
	return out, nil
}

// Restore makes version d.VersionID of d.Key current again by copying it
// onto the same key at the provider (S3-4): the delete marker stays,
// for the lifecycle rule, since S3-5's credential cannot remove
// versions. An object over 5 GiB is copied in parts. The copy keeps the
// version's bytes and its x-amz-meta-lendisk-sha256.
func (f *Client) Restore(ctx context.Context, bucket string, d Deleted) error {
	b, k := f.where(bucket, d.Key)
	src := minio.CopySrcOptions{Bucket: b, Object: k, VersionID: d.VersionID}
	var err error
	if d.Size <= MaxPutSize {
		_, err = f.core.Client.CopyObject(ctx, minio.CopyDestOptions{Bucket: b, Object: k}, src)
	} else {
		_, err = f.core.Client.ComposeObject(ctx, minio.CopyDestOptions{Bucket: b, Object: k, ReplaceMetadata: true,
			UserMetadata: map[string]string{MetaSHA256: d.SHA256}}, src)
	}
	if err != nil {
		return f.classify(fmt.Errorf("copying version %s of %s back: %w", d.VersionID, d.Key, err))
	}
	return nil
}

// ListLive lists the live keys of bucket at the target, by key.
func (f *Client) ListLive(ctx context.Context, bucket string) ([]string, error) {
	b, pfx := f.where(bucket, "")
	var out []string
	for o := range f.core.Client.ListObjects(ctx, b, minio.ListObjectsOptions{Prefix: pfx, Recursive: true}) {
		if o.Err != nil {
			var er minio.ErrorResponse
			if errors.As(o.Err, &er) && er.Code == "NoSuchBucket" {
				return nil, nil
			}
			return nil, f.classify(fmt.Errorf("listing %s: %w", pfx, o.Err))
		}
		out = append(out, strings.TrimPrefix(o.Key, pfx))
	}
	return out, nil
}

// ErrNoSuchKey is a place's answer that it holds no object at a key, for
// replicas other than Client (test doubles); NotFound recognises it.
var ErrNoSuchKey = errors.New("no such key at the place")

// NotFound reports whether err is a place's answer that it holds no
// object at the key (a 404 other than for the bucket).
func NotFound(err error) bool {
	var er minio.ErrorResponse
	return errors.Is(err, ErrNoSuchKey) ||
		errors.As(err, &er) && er.Code != "NoSuchBucket" && (er.Code == "NoSuchKey" || er.StatusCode == http.StatusNotFound)
}

// ObjectStat is what a place stores about an object (S3-6): lendisk's
// own hash, and the provider's checksums.
type ObjectStat struct {
	Size   int64
	SHA256 string // x-amz-meta-lendisk-sha256, hex; "" when absent
	// ChecksumSHA256 is the provider's stored SHA-256 of the whole
	// object, hex, when it keeps one (an object sent in one PUT with
	// its checksum, S3-3); "" otherwise, a multipart object's composite
	// included.
	ChecksumSHA256 string
	// ETag is the provider's ETag without quotes; Multipart when it is
	// a multipart object's ("<hash>-<parts>"), which is no MD5 of the
	// bytes.
	ETag      string
	Multipart bool
	// Composite: the provider's checksum is of the parts, not of the
	// whole object, so it is no SHA-256 of the bytes.
	Composite bool
}

// MD5ETag reports whether the ETag is a hex MD5 of the bytes: 32 hex
// digits of a single-part object. A provider may give other ETags
// (encrypted objects, other hashes), which are not compared.
func (s ObjectStat) MD5ETag() bool {
	if s.Multipart || len(s.ETag) != 32 {
		return false
	}
	_, err := hex.DecodeString(s.ETag)
	return err == nil
}

// Stat reads what the place stores about key of bucket, with the
// provider's checksums; ok is false when it holds no object there.
func (f *Client) Stat(ctx context.Context, bucket, key string) (ObjectStat, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, f.stall)
	defer cancel()
	b, k := f.where(bucket, key)
	opts := minio.StatObjectOptions{}
	opts.Checksum = true
	info, err := f.core.StatObject(ctx, b, k, opts)
	var er minio.ErrorResponse
	if errors.As(err, &er) && (er.Code == "NoSuchKey" || er.Code == "NoSuchBucket" || er.StatusCode == http.StatusNotFound) {
		return ObjectStat{}, false, nil
	}
	if err != nil {
		return ObjectStat{}, false, f.classify(fmt.Errorf("asking for %s: %w", key, err))
	}
	s := ObjectStat{Size: info.Size, SHA256: strings.ToLower(info.Metadata.Get("X-Amz-Meta-" + MetaSHA256)),
		ETag: strings.Trim(info.ETag, `"`)}
	s.Multipart = strings.Contains(s.ETag, "-")
	s.Composite = strings.Contains(info.ChecksumSHA256, "-") || strings.EqualFold(info.ChecksumMode, "COMPOSITE")
	if c := info.ChecksumSHA256; c != "" && !s.Composite {
		if raw, err := base64.StdEncoding.DecodeString(c); err == nil && len(raw) == 32 {
			s.ChecksumSHA256 = hex.EncodeToString(raw)
		}
	}
	return s, true, nil
}
