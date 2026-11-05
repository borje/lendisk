// Package main is a throwaway spike (issue #2): embedded versitygw on the
// posix backend, behind a pass-through decorator and an external IAM
// service, serving S3 on one unix socket only.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/embedgw"
	"github.com/versity/versitygw/s3api"
)

// The one hardcoded account the IAM service knows.
const (
	spikeAccess = "borrower1"
	spikeSecret = "borrower1-secret-key-0123456789"
)

// runGateway builds the posix backend under storeDir, wraps it in the
// decorator and serves S3 on sock until ctx is cancelled.
func runGateway(ctx context.Context, storeDir, sock string, logf func(string, ...any)) error {
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		return err
	}
	_ = os.Remove(sock)

	ms := meta.XattrMeta{}
	if err := ms.Test(storeDir); err != nil {
		return fmt.Errorf("xattr check: %w", err)
	}
	// AbsolutePaths: without it posix.New chdir()s the whole process to
	// the store root.
	be, err := posix.New(storeDir, ms, posix.PosixOpts{AbsolutePaths: true})
	if err != nil {
		return fmt.Errorf("posix backend: %w", err)
	}

	cfg := &embedgw.Config{
		// Root is required by RunVersityGW. In lendisk it would be a
		// random pair held only in memory.
		RootUserAccess:    "root-not-used",
		RootUserSecret:    "root-not-used-secret-0123456789",
		Ports:             []string{sock},
		SocketPerm:        "0600",
		MaxConnections:    250000,
		MaxRequests:       100000,
		MultipartMaxParts: 10000,
		IAMService:        newSpikeIAM(),
		Quiet:             true,
		S3Options:         []s3api.Option{gate()},
		// Left empty on purpose: AdminPorts, WebuiPorts, WebuiS3Prefix,
		// WebsitePorts, StatsdServers, DogstatsServers, event sinks.
	}
	return embedgw.RunVersityGW(ctx, &decorator{Backend: be, logf: logf}, cfg)
}
