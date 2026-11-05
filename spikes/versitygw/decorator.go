package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// decorator is the pass-through wrapper lendisk's decorator would grow
// from: it embeds the real backend and overrides a few methods to show
// what reaches it.
type decorator struct {
	backend.Backend
	logf func(string, ...any)
}

// accountFrom returns the authenticated account for the request ctx.
//
// versitygw passes ctx.RequestCtx() (a *fasthttp.RequestCtx) to every
// backend call. The auth middleware stores the account with fiber's
// Locals under the string key "account" (utils.ContextKeyAccount), and
// fiber's Locals is fasthttp's UserValue, which RequestCtx.Value returns
// for a string key. So no patch is needed.
func accountFrom(ctx context.Context) (auth.Account, bool) {
	a, ok := ctx.Value(string(utils.ContextKeyAccount)).(auth.Account)
	// An unsigned request reaches GetBucketPolicy and GetBucketAcl (the
	// public-bucket check) with a zero Account set, not an absent one.
	return a, ok && a.Access != ""
}

func (d *decorator) who(ctx context.Context) string {
	a, ok := accountFrom(ctx)
	if !ok {
		return "<no account>"
	}
	return fmt.Sprintf("%s(role=%s)", a.Access, a.Role)
}

// Errors the spike injects by key prefix, to test Q-I.5.
var (
	errQuota = s3err.APIError{
		Code:           "QuotaExceeded",
		Description:    "lendisk: lent quota exhausted (spike)",
		HTTPStatusCode: http.StatusInsufficientStorage, // 507
	}
	errDeniedQuota = s3err.APIError{
		Code:           "AccessDenied",
		Description:    "lendisk:quota-exceeded lent quota exhausted (spike)",
		HTTPStatusCode: http.StatusForbidden,
	}
)

func injected(key string) error {
	switch {
	case strings.HasPrefix(key, "spike-quota/"):
		return errQuota
	case strings.HasPrefix(key, "spike-precond/"):
		return s3err.GetAPIError(s3err.ErrPreconditionFailed)
	case strings.HasPrefix(key, "spike-denied/"):
		return errDeniedQuota
	case strings.HasPrefix(key, "spike-wrapped/"):
		// A wrapped APIError: ProcessController uses a type assertion,
		// not errors.As, so this becomes a 500 InternalError.
		return fmt.Errorf("decorator: %w", errQuota)
	}
	return nil
}

func (d *decorator) CreateBucket(ctx context.Context, in *s3.CreateBucketInput, acl []byte) error {
	d.logf("decorator CreateBucket bucket=%s account=%s", deref(in.Bucket), d.who(ctx))
	return d.Backend.CreateBucket(ctx, in, acl)
}

func (d *decorator) PutObject(ctx context.Context, in s3response.PutObjectInput) (s3response.PutObjectOutput, error) {
	key := deref(in.Key)
	d.logf("decorator PutObject bucket=%s key=%s account=%s", deref(in.Bucket), key, d.who(ctx))
	if err := injected(key); err != nil {
		return s3response.PutObjectOutput{}, err
	}
	return d.Backend.PutObject(ctx, in)
}

func (d *decorator) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	d.logf("decorator CompleteMultipartUpload bucket=%s key=%s account=%s", deref(in.Bucket), deref(in.Key), d.who(ctx))
	return d.Backend.CompleteMultipartUpload(ctx, in)
}

func (d *decorator) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	d.logf("decorator DeleteObject bucket=%s key=%s account=%s", deref(in.Bucket), deref(in.Key), d.who(ctx))
	return d.Backend.DeleteObject(ctx, in)
}

func (d *decorator) GetObject(ctx context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	d.logf("decorator GetObject bucket=%s key=%s account=%s", deref(in.Bucket), deref(in.Key), d.who(ctx))
	return d.Backend.GetObject(ctx, in)
}

func (d *decorator) HeadObject(ctx context.Context, in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	key := deref(in.Key)
	d.logf("decorator HeadObject bucket=%s key=%s account=%s", deref(in.Bucket), key, d.who(ctx))
	if err := injected(key); err != nil {
		return nil, err
	}
	return d.Backend.HeadObject(ctx, in)
}

func (d *decorator) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	d.logf("decorator ListObjectsV2 bucket=%s prefix=%s account=%s", deref(in.Bucket), deref(in.Prefix), d.who(ctx))
	return d.Backend.ListObjectsV2(ctx, in)
}

func (d *decorator) GetBucketAcl(ctx context.Context, in *s3.GetBucketAclInput) ([]byte, error) {
	d.logf("decorator GetBucketAcl bucket=%s account=%s", deref(in.Bucket), d.who(ctx))
	return d.Backend.GetBucketAcl(ctx, in)
}

func (d *decorator) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	d.logf("decorator GetBucketPolicy bucket=%s account=%s", bucket, d.who(ctx))
	return d.Backend.GetBucketPolicy(ctx, bucket)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
