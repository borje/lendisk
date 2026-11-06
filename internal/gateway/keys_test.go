package gateway

import (
	"bufio"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"

	"github.com/borje/lendisk/internal/state"
)

// unsupported answers everything NotImplemented and records a select.
type unsupported struct {
	backend.BackendUnsupported
	selected bool
}

func (u *unsupported) SelectObjectContent(ctx context.Context, in *s3.SelectObjectContentInput) func(*bufio.Writer) {
	u.selected = true
	return func(*bufio.Writer) {}
}

// SEC-5: every Lending method that names an object refuses a borrower's
// reserved or malformed key before the store sees it. A method added
// later that takes a key fails this test until it is listed here.
func TestEveryKeyTakingMethodValidatesTheKey(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	b := newIdentity(t)
	peer, sealed := b.NodeID().String(), b.SealedName("laptop")
	if err := h.db.AddFriendship(ctx, state.Friendship{Peer: peer, CreatedAt: time.Now(), OwnNumber: 1 << 30, PeerNumber: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	if err := h.db.AddLentBucket(ctx, peer, sealed, time.Now()); err != nil {
		t.Fatal(err)
	}
	next := &unsupported{}
	l := &Lending{Next: next, DB: h.db, StoreDir: h.store, Log: quiet}
	bctx := context.WithValue(ctx, "account", auth.Account{Access: peer}) //nolint:staticcheck // versitygw's key
	bk := aws.String(sealed)
	for _, key := range []string{".lendisk/tomb/x", ".LENDISK/x", "a/../b"} {
		k := aws.String(key)
		id := aws.String("up")
		one := int32(1)
		calls := map[string]func() error{
			"PutObject": func() error {
				_, err := l.PutObject(bctx, s3response.PutObjectInput{Bucket: bk, Key: k, Metadata: hashMeta(nil)})
				return err
			},
			"HeadObject": func() error { _, err := l.HeadObject(bctx, &s3.HeadObjectInput{Bucket: bk, Key: k}); return err },
			"GetObject":  func() error { _, err := l.GetObject(bctx, &s3.GetObjectInput{Bucket: bk, Key: k}); return err },
			"GetObjectAcl": func() error {
				_, err := l.GetObjectAcl(bctx, &s3.GetObjectAclInput{Bucket: bk, Key: k})
				return err
			},
			"GetObjectAttributes": func() error {
				_, err := l.GetObjectAttributes(bctx, &s3.GetObjectAttributesInput{Bucket: bk, Key: k})
				return err
			},
			"CopyObject": func() error {
				_, err := l.CopyObject(bctx, s3response.CopyObjectInput{Bucket: bk, Key: k})
				return err
			},
			"DeleteObject": func() error {
				_, err := l.DeleteObject(bctx, &s3.DeleteObjectInput{Bucket: bk, Key: k})
				return err
			},
			"DeleteObjects": func() error {
				_, err := l.DeleteObjects(bctx, &s3.DeleteObjectsInput{Bucket: bk, Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: k}}}})
				return err
			},
			"PutObjectAcl": func() error { return l.PutObjectAcl(bctx, &s3.PutObjectAclInput{Bucket: bk, Key: k}) },
			"CreateMultipartUpload": func() error {
				_, err := l.CreateMultipartUpload(bctx, s3response.CreateMultipartUploadInput{Bucket: bk, Key: k, Metadata: hashMeta(nil)})
				return err
			},
			"CompleteMultipartUpload": func() error {
				_, _, err := l.CompleteMultipartUpload(bctx, &s3.CompleteMultipartUploadInput{Bucket: bk, Key: k, UploadId: id,
					MultipartUpload: &types.CompletedMultipartUpload{}})
				return err
			},
			"AbortMultipartUpload": func() error {
				return l.AbortMultipartUpload(bctx, &s3.AbortMultipartUploadInput{Bucket: bk, Key: k, UploadId: id})
			},
			"ListParts": func() error {
				_, err := l.ListParts(bctx, &s3.ListPartsInput{Bucket: bk, Key: k, UploadId: id})
				return err
			},
			"UploadPart": func() error {
				_, err := l.UploadPart(bctx, &s3.UploadPartInput{Bucket: bk, Key: k, UploadId: id, PartNumber: &one})
				return err
			},
			"UploadPartCopy": func() error {
				_, err := l.UploadPartCopy(bctx, &s3.UploadPartCopyInput{Bucket: bk, Key: k, UploadId: id, PartNumber: &one})
				return err
			},
			"RestoreObject": func() error { return l.RestoreObject(bctx, &s3.RestoreObjectInput{Bucket: bk, Key: k}) },
			"SelectObjectContent": func() error {
				next.selected = false
				l.SelectObjectContent(bctx, &s3.SelectObjectContentInput{Bucket: bk, Key: k})
				if next.selected {
					return s3err.GetAPIError(s3err.ErrNotImplemented)
				}
				return errBadKey
			},
			"GetObjectTagging": func() error { _, err := l.GetObjectTagging(bctx, sealed, key, ""); return err },
			"PutObjectTagging": func() error {
				return l.PutObjectTagging(bctx, sealed, key, "", map[string]string{"a": "b"})
			},
			"DeleteObjectTagging": func() error { return l.DeleteObjectTagging(bctx, sealed, key, "") },
			"PutObjectRetention":  func() error { return l.PutObjectRetention(bctx, sealed, key, "", nil) },
			"GetObjectRetention":  func() error { _, err := l.GetObjectRetention(bctx, sealed, key, ""); return err },
			"PutObjectLegalHold":  func() error { return l.PutObjectLegalHold(bctx, sealed, key, "", true) },
			"GetObjectLegalHold":  func() error { _, err := l.GetObjectLegalHold(bctx, sealed, key, ""); return err },
			"ChangeBucketOwner":   func() error { return l.ChangeBucketOwner(bctx, sealed, "someone") },
		}
		for name, call := range calls {
			err := call()
			ae, ok := err.(s3err.APIError)
			if !ok || ae.Code != "InvalidArgument" && ae.Code != "AccessDenied" {
				t.Errorf("%s with %q: %v; want it refused before the store", name, key, err)
			}
		}
		if key != ".lendisk/tomb/x" {
			continue
		}
		lt := reflect.TypeOf(l)
		ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
		for i := 0; i < lt.NumMethod(); i++ {
			m := lt.Method(i)
			mt := m.Type
			if mt.NumIn() < 3 || mt.In(1) != ctxType {
				continue
			}
			str := reflect.TypeOf("")
			takesKey := mt.NumIn() >= 4 && mt.In(2) == str && mt.In(3) == str
			for j := 2; j < mt.NumIn(); j++ {
				p := mt.In(j)
				if p.Kind() == reflect.Pointer {
					p = p.Elem()
				}
				if p.Kind() == reflect.Struct {
					if _, ok := p.FieldByName("Key"); ok {
						takesKey = true
					}
				}
			}
			if _, listed := calls[m.Name]; takesKey && !listed {
				t.Errorf("Lending.%s names a key and is not in this test", m.Name)
			}
		}
	}
}
