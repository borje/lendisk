package gateway

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// D15, Q-H (#15 review): posix's PutObjectLockConfiguration turns object
// lock on for an existing bucket, so the gate refuses it, and the
// retention, legal-hold and lock headers that would then bind objects,
// for every account, own buckets included, as it refuses CreateBucket
// with lock. Afterwards the bucket still has no lock configuration.
func TestObjectLockRefusedOnOwnBuckets(t *testing.T) {
	h := newHarness(t, map[string]string{"laptop": "restic"})
	h.serve(false)
	c := h.client("laptop")
	ctx := context.Background()
	b, k := aws.String("laptop"), aws.String("data/ab/pack")
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: k, Body: bytes.NewReader([]byte("x"))}); err != nil {
		t.Fatal(err)
	}
	until := aws.Time(time.Now().Add(100 * 365 * 24 * time.Hour))
	for name, call := range map[string]func() error{
		"lock configuration": func() error {
			_, err := c.PutObjectLockConfiguration(ctx, &s3.PutObjectLockConfigurationInput{Bucket: b,
				ObjectLockConfiguration: &types.ObjectLockConfiguration{ObjectLockEnabled: types.ObjectLockEnabledEnabled,
					Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{Mode: types.ObjectLockRetentionModeCompliance, Years: aws.Int32(100)}}}})
			return err
		},
		"retention": func() error {
			_, err := c.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{Bucket: b, Key: k,
				Retention: &types.ObjectLockRetention{Mode: types.ObjectLockRetentionModeCompliance, RetainUntilDate: until}})
			return err
		},
		"legal hold": func() error {
			_, err := c.PutObjectLegalHold(ctx, &s3.PutObjectLegalHoldInput{Bucket: b, Key: k,
				LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn}})
			return err
		},
		"a put with lock headers": func() error {
			_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: aws.String("data/locked"), Body: bytes.NewReader([]byte("y")),
				ObjectLockMode: types.ObjectLockModeCompliance, ObjectLockRetainUntilDate: until})
			return err
		},
	} {
		err := call()
		var ae smithy.APIError
		if !errors.As(err, &ae) || ae.ErrorCode() != "AccessDenied" || !strings.Contains(ae.ErrorMessage(), "object lock") {
			t.Errorf("%s on an own bucket: %v; want the gate's refusal", name, err)
		}
	}
	if out, err := c.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: b}); err == nil &&
		out.ObjectLockConfiguration != nil && out.ObjectLockConfiguration.ObjectLockEnabled == types.ObjectLockEnabledEnabled {
		t.Fatal("the own bucket has object lock on")
	}
}
