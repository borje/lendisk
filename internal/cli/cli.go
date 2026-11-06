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
	"os"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/logging"
	"github.com/borje/lendisk/internal/store"
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
		return 1
	}
	return 0
}

type usageError struct{ error }

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
	root.AddCommand(a.initCmd(), a.statusCmd(), a.serveCmd(), a.bucketCmd())
	return root
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
	return cfg, log, nil
}

// ensureDir creates dir at mode 0700, or tightens an existing one to 0700.
func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if fi.Mode().Perm() != 0o700 {
		if err := os.Chmod(dir, 0o700); err != nil {
			return fmt.Errorf("%s has mode %04o and cannot be set to 0700: %w", dir, fi.Mode().Perm(), err)
		}
	}
	return nil
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
