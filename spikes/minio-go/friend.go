package main

import (
	"context"
	"fmt"
	"os"

	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/embedgw"
)

// The friend's one account (borrower1), as in spike #2.
const (
	spikeAccess = "borrower1"
	spikeSecret = "borrower1-secret-key-0123456789"
)

// runFriend serves embedded versitygw v1.10.0 on the posix backend over
// plain TCP at addr until ctx is cancelled. It stands in for a friend's
// node behind the tunnel: the tunnel is plain HTTP inside tailcat, so
// the client is not "secure" and minio-go signs payloads (see README).
// No decorator: Q-Y items 1, 2 and 4 are about the client.
func runFriend(ctx context.Context, storeDir, addr string) error {
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return err
	}
	ms := meta.XattrMeta{}
	if err := ms.Test(storeDir); err != nil {
		return fmt.Errorf("xattr check: %w", err)
	}
	be, err := posix.New(storeDir, ms, posix.PosixOpts{AbsolutePaths: true})
	if err != nil {
		return fmt.Errorf("posix backend: %w", err)
	}
	cfg := &embedgw.Config{
		RootUserAccess:    "root-not-used",
		RootUserSecret:    "root-not-used-secret-0123456789",
		Ports:             []string{addr},
		MaxConnections:    250000,
		MaxRequests:       100000,
		MultipartMaxParts: 10000,
		IAMService:        newSpikeIAM(),
		Quiet:             true,
	}
	return embedgw.RunVersityGW(ctx, be, cfg)
}
