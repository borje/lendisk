package gateway

import (
	"errors"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
)

// Store is the posix store the gateway serves (D15), with the metadata
// storer it was opened with, so capture reads the attributes posix
// writes through the same storer and bucket paths (EP-3, #62).
type Store struct {
	backend.Backend // the *posix.Posix
	posix           *posix.Posix
	meta            meta.MetadataStorer
}

// OpenBackend opens the posix store on dir. AbsolutePaths is required:
// without it posix.New changes the process's working directory (D15).
// DataIntegrityEtag stays off: resolve's landed tells a PutObject's ETag
// from a multipart one by the '-' (#62).
func OpenBackend(dir string) (*Store, error) {
	m := meta.XattrMeta{}
	p, err := posix.New(dir, m, posix.PosixOpts{AbsolutePaths: true})
	if err != nil {
		return nil, fmt.Errorf("opening store %s: %w", dir, err)
	}
	return &Store{Backend: p, posix: p, meta: m}, nil
}

// UploadIDs reads which multipart upload assembled a stored object.
// *Store implements it; the decorator is given it explicitly, so a
// wrapper between the decorator and the store cannot hide it.
type UploadIDs interface {
	// UploadID returns the ID of the upload that assembled key's object,
	// "" if the object carries none (a PutObject drops it) or is gone,
	// or an error wrapping errUploadUnreadable if it carries one that
	// cannot be read.
	UploadID(bucket, key string) (string, error)
}

// errUploadUnreadable marks an upload-ID attribute that exists (or may)
// but cannot be read: the row it would settle is undecided, not lost.
var errUploadUnreadable = errors.New("the store's upload ID cannot be read")

// mpMetaAttr is the attribute in which posix records, on an object it
// assembled from a multipart upload, the upload's ID (and part sizes):
// versitygw v1.10.0 backend/posix/posix.go, mpMetaKey, written by
// CompleteMultipartUpload and checked there to answer a retried
// completion. TestPendingCompletionMatchedByUploadNotETag pins it.
const mpMetaAttr = "mp-metadata"

// UploadID matches a pending completion's row to its own upload, where
// an ETag would also match an earlier upload of the same parts (#59).
func (s *Store) UploadID(bucket, key string) (string, error) {
	v, err := s.meta.RetrieveAttribute(nil, s.posix.BucketPath(bucket), key, mpMetaAttr)
	if errors.Is(err, meta.ErrNoSuchKey) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("%w: %s/%s: %w", errUploadUnreadable, bucket, key, err)
	}
	m, err := backend.UnmarshalMpUploadMetadata(v, false)
	if err != nil {
		return "", fmt.Errorf("%w: %s/%s: %w", errUploadUnreadable, bucket, key, err)
	}
	if m.UploadID == "" {
		return "", fmt.Errorf("%w: %s/%s: the attribute names no upload", errUploadUnreadable, bucket, key)
	}
	return m.UploadID, nil
}
