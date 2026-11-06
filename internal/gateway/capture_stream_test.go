package gateway

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/profile"
	"github.com/borje/lendisk/internal/state"
)

// noReadBack is the store with every object read refused and counted:
// a capture that reads an object back from the store fails, and the
// count shows it tried.
type noReadBack struct {
	backend.Backend
	reads atomic.Int32
}

func (n *noReadBack) GetObject(context.Context, *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	n.reads.Add(1)
	return nil, errors.New("test: object read refused")
}

// EP-3, REPL-10 (#6): a single PUT is hashed from the request stream as
// it passes into the store; capture never reads the object back.
func TestPutCaptureHashesTheStreamWithoutReadingItBack(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	posix, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	be := &noReadBack{Backend: posix}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: posix, DB: h.db, Profiles: set, Log: quiet}
	body := bytes.Repeat([]byte("stream"), 200_000)
	n := int64(len(body))
	if _, err := d.PutObject(context.Background(), s3response.PutObjectInput{Bucket: aws.String("files"),
		Key: aws.String("doc"), ContentLength: &n, Body: bytes.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	if r := be.reads.Load(); r != 0 {
		t.Errorf("capture read the object back %d times", r)
	}
	es := h.entries("files")
	if len(es) != 1 || es[0].State != state.StateDone || es[0].SHA256 != sum(body) || es[0].Size != n {
		t.Fatalf("queue %+v, want one done row with the body's hash and size", es)
	}
}

// partialDelete is the store refusing some entries of a multi-delete:
// those are reported in the result's errors and left in place, the
// others deleted.
type partialDelete struct {
	backend.Backend
	refuse map[string]bool
}

func (p *partialDelete) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	var pass []types.ObjectIdentifier
	var refused []types.Error
	for _, o := range in.Delete.Objects {
		if p.refuse[*o.Key] {
			refused = append(refused, types.Error{Key: o.Key, Code: aws.String("AccessDenied"), Message: aws.String("test")})
		} else {
			pass = append(pass, o)
		}
	}
	out := *in
	out.Delete = &types.Delete{Objects: pass}
	res, err := p.Backend.DeleteObjects(ctx, &out)
	res.Error = append(res.Error, refused...)
	return res, err
}

// EP-2: a multi-delete in which some entries fail captures only the
// entries the store deleted; the failed ones leave no row, pending or
// done, and their objects stay.
func TestPartialMultiDeleteCapturesOnlyTheDeleted(t *testing.T) {
	h := newHarness(t, map[string]string{"files": "generic"})
	ctx := context.Background()
	posix, err := OpenBackend(h.store)
	if err != nil {
		t.Fatal(err)
	}
	be := &partialDelete{Backend: posix, refuse: map[string]bool{"b": true, "d": true}}
	set, _ := profile.Load("")
	d := &Decorator{Backend: be, Uploads: posix, DB: h.db, Profiles: set, Log: quiet}
	bucket := aws.String("files")
	keys := []string{"a", "b", "c", "d"}
	var objs []types.ObjectIdentifier
	for _, k := range keys {
		n := int64(1)
		if _, err := d.PutObject(ctx, s3response.PutObjectInput{Bucket: bucket, Key: aws.String(k), ContentLength: &n,
			Body: strings.NewReader(k)}); err != nil {
			t.Fatal(err)
		}
		objs = append(objs, types.ObjectIdentifier{Key: aws.String(k)})
	}
	res, err := d.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: bucket, Delete: &types.Delete{Objects: objs}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deleted) != 2 || len(res.Error) != 2 {
		t.Fatalf("result: %d deleted, %d failed; want 2 and 2", len(res.Deleted), len(res.Error))
	}
	var deletes []string
	for _, e := range h.entries("files") {
		if e.State != state.StateDone {
			t.Errorf("row %d (%s %s) left %s", e.Seq, e.Op, e.Key, e.State)
		}
		if e.Op == state.OpDelete {
			deletes = append(deletes, e.Key)
		}
	}
	if strings.Join(deletes, ",") != "a,c" {
		t.Errorf("delete rows for %v, want a and c", deletes)
	}
	if p, _ := h.db.PendingCaptures(ctx); len(p) != 0 {
		t.Errorf("pending rows left: %+v", p)
	}
	for _, k := range []string{"b", "d"} {
		if _, err := posix.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: aws.String(k)}); err != nil {
			t.Errorf("refused %s is gone from the store: %v", k, err)
		}
	}
}
