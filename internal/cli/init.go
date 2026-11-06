package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

const (
	confirmWords    = 3
	confirmAttempts = 3
)

func (a *app) initCmd() *cobra.Command {
	var restore bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create this node's identity and recovery phrase",
		Long: `init creates the node's identity from a new 24-word recovery phrase,
shows the phrase once and asks for some of its words back, so that you
have written it down. The phrase is never stored; it is the only way to
recover the node after losing this machine.

With --restore, init reads an existing phrase from standard input
instead and recreates the same identity on this machine.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.runInit(cmd.Context(), restore)
		},
	}
	cmd.Flags().BoolVar(&restore, "restore", false, "recreate the identity from an existing recovery phrase read from stdin")
	return cmd
}

func (a *app) runInit(ctx context.Context, restore bool) error {
	cfg, log, err := a.load(a.errOut)
	if err != nil {
		return err
	}
	confDir := cfg.ConfigDir()
	if _, err := identity.Load(confDir); err == nil || !errors.Is(err, identity.ErrNoIdentity) {
		if err == nil {
			return fmt.Errorf("this node already has an identity in %s; `lendisk status` shows it", confDir)
		}
		return err
	}
	if err := ensureDir(confDir); err != nil {
		return fmt.Errorf("config directory: %w", err)
	}
	if err := ensureDir(cfg.DataDir); err != nil {
		return fmt.Errorf("data directory: %w", err)
	}

	var phrase string
	if restore {
		phrase, err = a.readPhrase()
	} else {
		phrase, err = a.newPhraseConfirmed()
	}
	if err != nil {
		return err
	}
	id, err := identity.FromPhrase(phrase)
	phrase = ""
	if err != nil {
		return err
	}
	nodeID := id.NodeID().String()

	// Check the database before writing the key: a database that belongs to
	// another identity must not be adopted silently.
	db, err := state.Open(ctx, cfg.DataDir, log)
	if err != nil {
		return err
	}
	defer db.Close()
	if prev, ok, err := db.GetNode(ctx, "id"); err != nil {
		return err
	} else if ok && prev != nodeID {
		return fmt.Errorf("%s belongs to node %s, not %s; use another data_dir or move it aside", db.Path, prev, nodeID)
	}

	if err := id.Save(confDir); err != nil {
		return err
	}
	if err := db.SetNode(ctx, "id", nodeID); err != nil {
		return err
	}
	if err := db.SetNode(ctx, "initialised_at", time.Now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	a.chooseRegion(ctx, cfg, db, log)
	if !cfg.FileExists {
		if err := writeInitialConfig(cfg); err != nil {
			return err
		}
	}
	log.Info("identity created", "node_id", nodeID, "restored", restore)

	fmt.Fprintf(a.out, "\nNode ID: %s\n", nodeID)
	fmt.Fprintf(a.out, "Config:  %s\nData:    %s\n", cfg.Path, cfg.DataDir)
	return nil
}

// newPhraseConfirmed shows a fresh phrase and asks for some of its words
// back (ID-2).
func (a *app) newPhraseConfirmed() (string, error) {
	phrase, err := a.newPhrase()
	if err != nil {
		return "", err
	}
	words := strings.Fields(phrase)
	w := a.out
	fmt.Fprintln(w, "Your recovery phrase. Write it down on paper and keep it somewhere safe.")
	fmt.Fprintln(w, "It is shown once and never stored. With it and one friend's address you")
	fmt.Fprintln(w, "can rebuild this node; without it, losing this machine loses the node.")
	fmt.Fprintln(w)
	for i := 0; i < len(words); i += 4 {
		for j := i; j < i+4; j++ {
			fmt.Fprintf(w, "  %2d. %-10s", j+1, words[j])
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "To confirm you wrote it down, type the words asked for.")
	for _, pos := range a.pick(len(words), confirmWords) {
		ok := false
		for try := 0; try < confirmAttempts && !ok; try++ {
			fmt.Fprintf(w, "Word %d: ", pos+1)
			line, err := a.readLine()
			if err != nil {
				return "", fmt.Errorf("confirmation not completed; nothing was written: %w", err)
			}
			ok = identity.NormalizePhrase(line) == words[pos]
			if !ok {
				fmt.Fprintln(w, "That is not the word in that position.")
			}
		}
		if !ok {
			return "", errors.New("confirmation failed; nothing was written. Run `lendisk init` again for a new phrase")
		}
	}
	fmt.Fprintln(w, "Confirmed.")
	return phrase, nil
}

func (a *app) readPhrase() (string, error) {
	fmt.Fprint(a.out, "Recovery phrase (24 words): ")
	line, err := a.readLine()
	if err != nil {
		return "", fmt.Errorf("reading recovery phrase: %w", err)
	}
	return line, nil
}

func (a *app) readLine() (string, error) {
	line, err := a.in.ReadString('\n')
	if err != nil && !(errors.Is(err, io.EOF) && line != "") {
		if errors.Is(err, io.EOF) {
			return "", errors.New("unexpected end of input")
		}
		return "", err
	}
	return strings.TrimSpace(line), nil
}

// writeInitialConfig writes a minimal config file when none exists. It is
// the user's file from then on; the daemon never writes it (OPS-6).
func writeInitialConfig(cfg *config.Loaded) error {
	body := fmt.Sprintf(`# lendisk configuration. Written once by `+"`lendisk init`"+`; edit freely.
# Every setting also has a flag and a LENDISK_* environment variable.
# Friendships, buckets and overflow targets are not here: they live in
# %s.

role = %q
data_dir = %q

[log]
level = %q
format = %q
`, filepath.Join(cfg.DataDir, state.FileName), cfg.Role, cfg.DataDir, cfg.Log.Level, cfg.Log.Format)
	f, err := os.OpenFile(cfg.Path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("writing initial config: %w", err)
	}
	if _, err := f.WriteString(body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
