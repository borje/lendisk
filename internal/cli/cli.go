// Package cli is lendisk's command line: the daemon (`serve`) and the
// commands that manage a node.
package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/logging"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/recovery"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/store"
	"github.com/borje/lendisk/internal/transport"
	"github.com/borje/lendisk/internal/version"
)

// app carries what every command needs; tests construct it directly.
type app struct {
	in     *bufio.Reader
	out    io.Writer
	errOut io.Writer
	flags  *config.Flags
	// pick chooses which phrase positions init asks the user to type back.
	pick func(n, k int) []int
	// newPhrase makes init's recovery phrase.
	newPhrase func() (string, error)
	// ctx ends `serve` when done; nil means on SIGINT or SIGTERM.
	ctx context.Context
	// storeCheck runs EP-12's filesystem checks; zero means the real ones.
	storeCheck store.Checker
	// serving, if set, is called once `serve` accepts S3 requests, with
	// the loopback endpoint's address.
	serving func(addr string)
	// regions offers the tunnel's relay regions; nil means the public map.
	regions transport.RegionSource
	// afterStore is the capture crash-window hook (gateway.AfterStore);
	// tests only.
	afterStore func()
	// beforeHash is the gateway's hook before a completed upload is
	// hashed (gateway.BeforeHash); tests only.
	beforeHash func()
	// tuneBridge, if set, adjusts the bridge before it serves. Tests only.
	tuneBridge func(*bridge.Bridge)
	// tuneReplication, if set, adjusts the replication worker before it
	// starts (timings, the OnRead hook); tests only.
	tuneReplication func(*replica.Worker)
	// tuneAlerts, if set, adjusts the alert service before it starts
	// (its interval); tests only.
	tuneAlerts func(*alert.Service)
	// tuneManifest, if set, adjusts the manifest pusher before it
	// starts (its poll); tests only.
	tuneManifest func(*manifest.Pusher)
	// clock, random and dialTimeout serve invites and peer add (peer.go);
	// zero values mean time.Now, crypto/rand and a minute.
	clock       func() time.Time
	random      io.Reader
	dialTimeout time.Duration
	// sleep pauses `status --wait` between polls; nil means a timer.
	// Tests advance their clock in it.
	sleep func(ctx context.Context, d time.Duration) error
	// redeemTransport, if set, wraps peer add's redeem transport, and
	// beforeFriendship, if set, runs after the redeem answer and before
	// the friendship row is written, its error ending peer add there:
	// the lost-answer and crash windows (#51); tests only.
	redeemTransport  func(http.RoundTripper) http.RoundTripper
	beforeFriendship func() error
	// listBudget is how long recover lists before leaving the rest to
	// the daemon; zero means defaultListBudget. Tests set it.
	listBudget time.Duration
	// afterFetch, if set, runs in recover once the manifests are
	// fetched and before the places are listed; tests only.
	afterFetch func()
	// tuneControl, if set, adjusts the control API server before it
	// serves (a slow list); tests only.
	tuneControl func(*control.Server)
	// tuneFetch, if set, adjusts the read-through's fetcher before the
	// gateway serves. Tests only.
	tuneFetch func(*replica.Fetcher)
	// tuneRecovery, if set, adjusts the daemon's recovery lister before
	// it starts (its timings); tests only.
	tuneRecovery func(*recovery.Lister)
	// tuneEnding, if set, adjusts the daemon's farewell (ending
	// friendships, #25) before it starts; tests only.
	tuneEnding func(*farewell)
}

func (a *app) checker() store.Checker {
	if a.storeCheck.Xattr == nil {
		return store.DefaultChecker
	}
	return a.storeCheck
}

// Main runs the command line and returns the process exit code.
func Main(args []string, in io.Reader, out, errOut io.Writer) int {
	a := &app{in: bufio.NewReader(in), out: out, errOut: errOut, pick: randomPositions, newPhrase: identity.NewPhrase}
	return a.run(args)
}

func (a *app) run(args []string) int {
	root := a.rootCmd()
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(a.errOut, "lendisk:", err)
		var ue usageError
		if errors.As(err, &ue) {
			return 2
		}
		var ce codeError
		if errors.As(err, &ce) {
			return ce.code
		}
		return 1
	}
	return 0
}

type usageError struct{ error }

// codeError is a failure with its own exit code, other than 1 and the
// usage errors' 2 (`status --wait`: 3 timed out, 4 cannot complete).
type codeError struct {
	error
	code int
}

func (e codeError) Unwrap() error { return e.error }

func (a *app) rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "lendisk",
		Short:         "Friends lend each other disk space for offsite backups",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetIn(a.in)
	root.SetOut(a.out)
	root.SetErr(a.errOut)
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return usageError{err} })
	a.flags = config.RegisterFlags(root.PersistentFlags())
	root.AddCommand(a.initCmd(), a.recoverCmd(), a.statusCmd(), a.serveCmd(), a.bucketCmd())
	root.AddCommand(a.endpointCmd())
	root.AddCommand(a.inviteCmd(), a.peerCmd())
	usageArgs(root)
	return root
}

// usageArgs makes cobra's argument errors usage errors, which exit 2: a
// missing or extra argument, and an unknown command or subcommand. A
// command group (one with subcommands and nothing to run) refuses any
// argument as an unknown command and prints its help when given none or
// `help` (`lendisk bucket help add` prints add's help).
func usageArgs(c *cobra.Command) {
	for _, sub := range c.Commands() {
		usageArgs(sub)
	}
	if c.HasSubCommands() && !c.Runnable() {
		c.Args = func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 || args[0] == "help" {
				return nil
			}
			msg := fmt.Sprintf("unknown command %q for %q", args[0], cmd.CommandPath())
			if s := cmd.SuggestionsFor(args[0]); len(s) > 0 {
				msg += "; did you mean " + strings.Join(s, " or ") + "?"
			}
			return usageError{errors.New(msg)}
		}
		c.RunE = func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				sub, rest, err := cmd.Find(args[1:])
				if err != nil || sub == cmd || len(rest) > 0 {
					return usageError{fmt.Errorf("unknown command %q for %q", strings.Join(args[1:], " "), cmd.CommandPath())}
				}
				return sub.Help()
			}
			return cmd.Help()
		}
		return
	}
	if check := c.Args; check != nil {
		c.Args = func(cmd *cobra.Command, args []string) error {
			if err := check(cmd, args); err != nil {
				return usageError{err}
			}
			return nil
		}
	}
}

// load resolves the configuration and a logger writing to w.
func (a *app) load(w io.Writer) (*config.Loaded, *slog.Logger, error) {
	cfg, err := a.flags.Load()
	if err != nil {
		return nil, nil, err
	}
	log := logging.New(w, cfg.Log.Level, cfg.Log.Format)
	if !cfg.FileExists {
		log.Debug("no config file; using defaults", "path", cfg.Path)
	}
	for _, k := range cfg.Unknown {
		log.Warn("config key not used by this version", "key", k, "file", cfg.Path)
	}
	for _, l := range cfg.Endpoint.Listen {
		if l.TLS == "off" { // EP-17
			log.Warn("endpoint.listen with tls = \"off\": request metadata and object contents cross the network unencrypted; only restic and Kopia buckets are protected by the backup tool's own encryption",
				"listen", l.String())
		}
	}
	return cfg, log, nil
}

// ensureDir makes dir a private directory for lendisk's secrets (#53).
// A missing one is created at 0700. An existing one that group or
// others can reach is tightened to 0700 only when it holds nothing but
// lendisk's own files (it is empty, or a bind mount holding
// lendisk.toml); otherwise it is used for other things, such as $HOME
// when the config is ./lendisk.toml, and lendisk refuses rather than
// change its permissions.
func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	perm := fi.Mode().Perm()
	if perm == 0o700 {
		return nil
	}
	if perm&0o077 != 0 {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if !ownEntry(e.Name()) {
				return fmt.Errorf("%s has mode %04o and holds files that are not lendisk's (%s): "+
					"lendisk keeps secrets there and will not change the permissions of a directory used for other things; "+
					"use a dedicated directory (--config, --data-dir), or run `chmod 700 %s` if it is meant to be private",
					dir, perm, e.Name(), dir)
			}
		}
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("%s has mode %04o and cannot be set to 0700: %w", dir, perm, err)
	}
	return nil
}

// ownEntry reports whether name is one lendisk keeps in its config or
// data directory.
func ownEntry(name string) bool {
	switch name {
	case "lendisk.toml", "identity.key", "endpoint.crt", "endpoint.key", "profiles", "store", "run", "locks":
		return true
	}
	return strings.HasPrefix(name, "lendisk.db")
}

func randomPositions(n, k int) []int {
	p := rand.Perm(n)[:k]
	// Ask in ascending order; it is easier to read off the paper.
	for i := 1; i < len(p); i++ {
		for j := i; j > 0 && p[j] < p[j-1]; j-- {
			p[j], p[j-1] = p[j-1], p[j]
		}
	}
	return p
}
