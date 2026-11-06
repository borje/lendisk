// Package gateway runs versitygw embedded in the daemon (D4, D15): the S3
// protocol on one internal unix socket, <data>/run/gw.sock, over the posix
// store, with lendisk's gate in front of its routing, lendisk's IAM service
// for credentials and lendisk's decorator around the backend.
//
// Rules a caller must keep:
//   - versitygw opens no TCP port (EP-5, SEC-8); every TCP listener is the
//     bridge's. Only the socket path goes in Config.Ports.
//   - Only one gateway runs per process (an embedgw limit).
//   - The root account's secret is random per start and never leaves this
//     process; the admin API embedgw mounts on the socket is refused by the
//     gate (SEC-8).
//   - Errors returned to versitygw from the decorator must be unwrapped
//     s3err.APIError values, or the client sees a retryable 500 (Q-I.5).
package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/embedgw"
	"github.com/versity/versitygw/s3api"

	"github.com/borje/lendisk/internal/state"
)

const versitygwModule = "github.com/versity/versitygw"

// Version is the linked versitygw module version, for `status` (EP-12).
func Version() string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == versitygwModule {
				if d.Replace != nil {
					return d.Replace.Version
				}
				return d.Version
			}
		}
	}
	return "unknown"
}

// maxSocketPath is the longest unix socket path Linux binds (sun_path is
// 108 bytes including the terminating NUL).
const maxSocketPath = 107

// SocketPath is the internal socket under the data directory. Run may
// move it if the path is too long; read Gateway.Socket after ready.
func SocketPath(dataDir string) string { return filepath.Join(dataDir, "run", "gw.sock") }

// Gateway is a configured, not yet running, embedded versitygw.
type Gateway struct {
	StoreDir string
	Socket   string
	DB       *state.DB
	Log      *slog.Logger
}

// OpenBackend opens the posix backend on dir. AbsolutePaths is required:
// without it posix.New changes the process's working directory (D15).
func OpenBackend(dir string) (backend.Backend, error) {
	be, err := posix.New(dir, meta.XattrMeta{}, posix.PosixOpts{AbsolutePaths: true})
	if err != nil {
		return nil, fmt.Errorf("opening store %s: %w", dir, err)
	}
	return be, nil
}

// Run serves S3 on the socket until ctx is cancelled. ready, if not nil,
// is closed once the socket accepts connections.
func (g *Gateway) Run(ctx context.Context, ready chan<- struct{}) error {
	if len(g.Socket) > maxSocketPath {
		// A unix socket path is limited to 108 bytes. A long data_dir
		// would make the bind fail, so use a private directory instead.
		dir, err := os.MkdirTemp("", "lendisk-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		g.Log.Warn("data_dir is too long for a unix socket path; using a private temporary directory",
			"wanted", g.Socket, "using", filepath.Join(dir, "gw.sock"))
		g.Socket = filepath.Join(dir, "gw.sock")
	}
	if err := os.MkdirAll(filepath.Dir(g.Socket), 0o700); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Dir(g.Socket), 0o700); err != nil {
		return err
	}
	if err := os.Remove(g.Socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	be, err := OpenBackend(g.StoreDir)
	if err != nil {
		return err
	}
	rootAccess, rootSecret := "lendisk-root-"+randomHex(8), randomHex(32)
	cfg := &embedgw.Config{
		RootUserAccess:    rootAccess,
		RootUserSecret:    rootSecret,
		Ports:             []string{g.Socket},
		SocketPerm:        "0600",
		MaxConnections:    250000,
		MaxRequests:       100000,
		MultipartMaxParts: 10000,
		IAMService:        &IAM{DB: g.DB, Log: g.Log},
		Quiet:             true,
		S3Options:         []s3api.Option{Gate()},
		// Deliberately empty: AdminPorts, WebuiPorts, WebsitePorts, metrics
		// and event sinks. The admin API is mounted on the socket anyway
		// and the gate refuses it (SEC-8).
	}

	errc := make(chan error, 1)
	go func() { errc <- embedgw.RunVersityGW(ctx, &Decorator{Backend: be}, cfg) }()
	if err := waitForSocket(ctx, g.Socket, errc); err != nil {
		var stopped *stoppedError
		if ctx.Err() != nil && !errors.As(err, &stopped) {
			// Stopped while starting: wait for versitygw to stop too, so it
			// never outlives Run (only one may run per process).
			<-errc
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	g.Log.Info("gateway serving", "socket", g.Socket, "store", g.StoreDir, "versitygw", Version())
	if ready != nil {
		close(ready)
	}
	err = <-errc
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func waitForSocket(ctx context.Context, sock string, errc <-chan error) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := net.DialTimeout("unix", sock, time.Second)
		if err == nil {
			c.Close()
			return nil
		}
		select {
		case err := <-errc:
			if err == nil {
				err = errors.New("gateway stopped before serving")
			}
			return &stoppedError{fmt.Errorf("gateway: %w", err)}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gateway socket %s not ready after 30s", sock)
		}
	}
}

// stoppedError means versitygw already returned, so errc is drained.
type stoppedError struct{ error }

func (e *stoppedError) Unwrap() error { return e.error }

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
