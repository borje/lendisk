package gateway

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// NET-7 list, ID-5 (#29): the lender pages a borrower's bucket from its
// own store: each key with its size and the hash it was stored with,
// in key order, resuming after a key. A borrower names only its own
// buckets, by sealed name; another friend's is no bucket at all.
func TestLentIndexPagesABorrowersBucket(t *testing.T) {
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
	want := map[string][]byte{}
	for i, k := range []string{"config", "data/ab/pack", "index/1"} {
		body := bytes.Repeat([]byte{byte('a' + i)}, 10*(i+1))
		want[k] = body
		if _, err := cl.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(sealed), Key: aws.String(k),
			Body: bytes.NewReader(body), Metadata: hashMeta(body)}); err != nil {
			t.Fatal(err)
		}
	}
	be, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	idx := &LentIndex{Store: be, DB: h.db}

	// What lendisk keeps beside a borrower's objects in its bucket
	// (tombstones, #17) is no key of the borrower's: not listed (SEC-5).
	for _, k := range []string{".lendisk/tomb/config", ".LENDISK/x"} {
		p := filepath.Join(h.store, LentName(b.NodeID(), sealed), filepath.FromSlash(k))
		os.MkdirAll(filepath.Dir(p), 0o700)
		if err := os.WriteFile(p, []byte("tomb"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var got []LentObject
	after := ""
	for pages := 0; ; pages++ {
		page, next, err := idx.List(ctx, b.NodeID(), sealed, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > 2 || pages > 3 {
			t.Fatalf("page of %d", len(page))
		}
		got = append(got, page...)
		if next == "" {
			break
		}
		after = next
	}
	if len(got) != len(want) {
		t.Fatalf("listed %+v", got)
	}
	for i, o := range got {
		if i > 0 && got[i-1].Key >= o.Key {
			t.Fatalf("not in key order: %+v", got)
		}
		body := want[o.Key]
		if o.Size != int64(len(body)) || o.SHA256 != sum(body) || !o.TombstonedAt.IsZero() {
			t.Errorf("%s: %+v", o.Key, o)
		}
	}

	for name, call := range map[string]func() error{
		"another friend's bucket": func() error { _, _, err := idx.List(ctx, c.NodeID(), sealed, "", 10); return err },
		"a bucket never made":     func() error { _, _, err := idx.List(ctx, b.NodeID(), b.SealedName("phone"), "", 10); return err },
		"a store name": func() error {
			_, _, err := idx.List(ctx, b.NodeID(), LentName(b.NodeID(), sealed), "", 10)
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrNoLentBucket) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, _, err := idx.List(ctx, b.NodeID(), sealed, "", 0); err == nil {
		t.Error("a page of no keys was served")
	}
}
