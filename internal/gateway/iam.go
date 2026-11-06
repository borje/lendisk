package gateway

import (
	"context"
	"errors"
	"log/slog"

	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"

	"github.com/borje/lendisk/internal/state"
)

// IAM is versitygw's IAMService backed by lendisk.db (EP-6, SEC-3). Each
// bucket's access key resolves to an account with the plain user role,
// which may use only buckets it owns; bucket add makes that key the
// bucket's owner. Nothing is ever given the admin role, and the
// user-management calls are refused (SEC-8).
type IAM struct {
	DB  *state.DB
	Log *slog.Logger
}

var errAdmin = s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)

// GetUserAccount resolves an access key, reading the database every time
// so a removed bucket's key stops working on the next request.
func (i *IAM) GetUserAccount(access string) (auth.Account, error) {
	b, err := i.DB.BucketByAccessKey(context.Background(), access)
	if errors.Is(err, state.ErrNotFound) {
		return auth.Account{}, auth.ErrNoSuchUser
	}
	if err != nil {
		i.Log.Error("IAM lookup failed", "err", err)
		return auth.Account{}, auth.ErrNoSuchUser
	}
	return auth.Account{Access: b.AccessKey, Secret: b.SecretKey, Role: auth.RoleUser}, nil
}

// ResolveAccounts returns the keys that do not exist.
func (i *IAM) ResolveAccounts(keys []string) ([]string, error) {
	missing := []string{}
	for _, k := range keys {
		if _, err := i.GetUserAccount(k); err != nil {
			missing = append(missing, k)
		}
	}
	return missing, nil
}

// CreateAccount is refused: accounts come from `bucket add` only.
func (i *IAM) CreateAccount(auth.Account) error { return errAdmin }

// UpdateUserAccount is refused.
func (i *IAM) UpdateUserAccount(string, auth.MutableProps) error { return errAdmin }

// DeleteUserAccount is refused.
func (i *IAM) DeleteUserAccount(string) error { return errAdmin }

// ListUserAccounts is refused, so no admin call can list secrets (SEC-8).
func (i *IAM) ListUserAccounts() ([]auth.Account, error) { return nil, errAdmin }

// Shutdown does nothing; the database belongs to the caller.
func (i *IAM) Shutdown() error { return nil }

var _ auth.IAMService = (*IAM)(nil)
