package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/borje/lendisk/internal/identity"
)

// VER-1 (#27): the challenge reads a borrower's object as the borrower
// would through the lender's S3 surface, through Lending: a range, or
// the whole object, of its own bucket by sealed name. Another friend's
// bucket, a tomb or any reserved key is unreachable, and an object it
// does not hold, or a range past its end, is told apart from a failure
// to read.
func TestLentIndexReadsABorrowersOwnBytesOnly(t *testing.T) {
	h := newHarness(t, nil)
	h.self = newIdentity(t)
	b, c := newIdentity(t), newIdentity(t)
	h.befriend(b)
	h.befriend(c)
	stop := h.serve(false)
	defer stop()
	ctx := context.Background()
	cl := h.pairClient(b)
	sealed := b.SealedName("laptop")
	if _, err := cl.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(sealed)}); err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("0123456789"), 100)
	if _, err := cl.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(sealed), Key: aws.String("data/ab/pack"),
		Body: bytes.NewReader(body), Metadata: hashMeta(body)}); err != nil {
		t.Fatal(err)
	}
	tomb := filepath.Join(h.store, LentName(b.NodeID(), sealed), ".lendisk", "tomb", "data", "ab", "pack", "1")
	os.MkdirAll(filepath.Dir(tomb), 0o700)
	if err := os.WriteFile(tomb, []byte("tomb"), 0o600); err != nil {
		t.Fatal(err)
	}
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	idx := &LentIndex{Store: be, DB: h.db}
	read := func(borrower *identity.Identity, sealed, key string, off, n int64) ([]byte, error) {
		t.Helper()
		r, err := idx.Read(ctx, borrower.NodeID(), sealed, key, off, n)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	}
	if got, err := read(b, sealed, "data/ab/pack", 10, 25); err != nil || !bytes.Equal(got, body[10:35]) {
		t.Fatalf("a range: %q %v", got, err)
	}
	if got, err := read(b, sealed, "data/ab/pack", 0, -1); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("the whole object: %d bytes %v", len(got), err)
	}
	for name, c := range map[string]struct {
		who    *identity.Identity
		sealed string
		key    string
		off, n int64
		want   error
	}{
		"another friend's bucket": {c, sealed, "data/ab/pack", 0, 10, ErrNoLentBucket},
		"no such bucket":          {b, b.SealedName("photos"), "data/ab/pack", 0, 10, ErrNoLentBucket},
		"a name not sealed":       {b, "laptop", "data/ab/pack", 0, 10, ErrNoLentBucket},
		"no such key":             {b, sealed, "data/ab/gone", 0, 10, ErrNoLentKey},
		"the tomb":                {b, sealed, ".lendisk/tomb/data/ab/pack/1", 0, 4, ErrNoLentKey},
		"past the end":            {b, sealed, "data/ab/pack", 5000, 10, ErrBadRange},
	} {
		got, err := read(c.who, c.sealed, c.key, c.off, c.n)
		if !errors.Is(err, c.want) || got != nil {
			t.Errorf("%s: %q %v, want %v", name, got, err, c.want)
		}
	}
}
