// Package gateway runs versitygw embedded in the daemon (D4, D15): the S3
// protocol on one internal unix socket, <data>/run/gw.sock, over the posix
// store, with lendisk's gate in front of its routing, lendisk's IAM service
// for credentials and lendisk's decorator around the backend. The
// decorator's outer layer (Lending) maps a borrower's sealed bucket names
// to its lent-<id16>-<sealed> buckets (PEER-8, SEC-16); its inner layer
// captures every write and delete of an own bucket into the replication
// queue (EP-2, EP-3, EP-7, REPL-3, REPL-10). A friend's node ID is an
// account whose secret is derived per pair (NET-7a).
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
//   - Queue rows a crash left pending are settled (Decorator.ResolvePending)
//     before anything is served; Run does this.
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

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/profile"
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
	// Self is the node's identity: friends' node IDs are accounts whose
	// secret is derived from it (NET-7a). Nil serves no friend.
	Self *identity.Identity
	// Profiles classify own buckets' keys (EP-7); nil means the
	// compiled-in ones.
	Profiles profile.Set
	Log      *slog.Logger

	// AfterStore is the decorator's crash-window hook. Tests only.
	AfterStore func()

	noCapture bool // tests: the decorator passes everything through
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
	dec, err := g.decorator(ctx, be)
	if err != nil {
		if ctx.Err() != nil {
			return nil // stopped while starting
		}
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
		IAMService:        &IAM{DB: g.DB, Self: g.Self, Log: g.Log},
		Quiet:             true,
		S3Options:         []s3api.Option{Gate()},
		// Deliberately empty: AdminPorts, WebuiPorts, WebsitePorts, metrics
		// and event sinks. The admin API is mounted on the socket anyway
		// and the gate refuses it (SEC-8).
	}

	errc := make(chan error, 1)
	lend := &Lending{Next: dec, DB: g.DB}
	go func() { errc <- embedgw.RunVersityGW(ctx, lend, cfg) }()
	// waitForSocket drains errc whenever it returns early, so versitygw
	// never outlives Run (only one may run per process).
	if err := waitForSocket(ctx, g.Socket, errc); err != nil {
		if ctx.Err() != nil {
			return nil // stopped while starting
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

// decorator wraps be and settles what a crash left pending before
// anything is served (EP-3).
func (g *Gateway) decorator(ctx context.Context, be backend.Backend) (*Decorator, error) {
	d := &Decorator{Backend: be, DB: g.DB, Profiles: g.Profiles, Log: g.Log, AfterStore: g.AfterStore}
	if d.Profiles == nil {
		set, err := profile.Load("")
		if err != nil {
			return nil, err
		}
		d.Profiles = set
	}
	if g.noCapture {
		d.DB = nil
	}
	done, dropped, err := d.ResolvePending(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture: settling the queue rows left by the last run: %w", err)
	}
	if done+dropped > 0 {
		g.Log.Info("capture: settled queue rows left by the last run", "done", done, "dropped", dropped)
	}
	return d, nil
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
			return fmt.Errorf("gateway: %w", err)
		case <-ctx.Done():
			// Stopped while starting: let versitygw finish, since only
			// one may run per process and the next start would fail.
			<-errc
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gateway socket %s not ready after 30s", sock)
		}
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
