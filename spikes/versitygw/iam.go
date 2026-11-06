package main

import (
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
)

// spikeIAM is the external IAMService (Config.IAMService): one hardcoded
// account, read-only. lendisk's would look the key up in lendisk.db.
type spikeIAM struct{ acct auth.Account }

func newSpikeIAM() *spikeIAM {
	return &spikeIAM{acct: auth.Account{
		Access: spikeAccess,
		Secret: spikeSecret,
		// userplus may create buckets; user may not. Never admin.
		Role: auth.RoleUserPlus,
	}}
}

func (s *spikeIAM) GetUserAccount(access string) (auth.Account, error) {
	if access == s.acct.Access {
		return s.acct, nil
	}
	return auth.Account{}, auth.ErrNoSuchUser
}

func (s *spikeIAM) ResolveAccounts(keys []string) ([]string, error) {
	missing := []string{}
	for _, k := range keys {
		if k != s.acct.Access {
			missing = append(missing, k)
		}
	}
	return missing, nil
}

func (s *spikeIAM) CreateAccount(auth.Account) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}
func (s *spikeIAM) UpdateUserAccount(string, auth.MutableProps) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}
func (s *spikeIAM) DeleteUserAccount(string) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}
func (s *spikeIAM) ListUserAccounts() ([]auth.Account, error) {
	return []auth.Account{s.acct}, nil
}
func (s *spikeIAM) Shutdown() error { return nil }

var _ auth.IAMService = (*spikeIAM)(nil)
