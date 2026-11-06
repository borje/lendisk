package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
)

// Decorator wraps the store's backend (D4). It passes everything through
// for now; capture (#6), quota (#16) and tombstones (#17) are added as
// overriding methods. Overrides must return unwrapped s3err.APIError
// values for client-visible refusals (Q-I.5).
type Decorator struct {
	backend.Backend
}

// CreateBucket creates a bucket owned by the given access key, directly on
// the backend, as `bucket add` does without the daemon (EP-6). The owner is
// the only account versitygw lets use the bucket.
func CreateBucket(ctx context.Context, be backend.Backend, bucket, ownerAccess string) error {
	acl, err := json.Marshal(auth.ACL{Owner: ownerAccess})
	if err != nil {
		return err
	}
	owner := auth.Account{Access: ownerAccess, Role: auth.RoleUser}
	ctx = context.WithValue(ctx, "bucket-owner", owner) //nolint:staticcheck // posix reads this string key
	lock := false
	err = be.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     &bucket,
		ObjectOwnership:            types.ObjectOwnershipBucketOwnerEnforced,
		ObjectLockEnabledForBucket: &lock,
		CreateBucketConfiguration:  &types.CreateBucketConfiguration{},
	}, acl)
	if err != nil {
		return fmt.Errorf("creating bucket %s: %w", bucket, err)
	}
	return nil
}
