package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/capacity"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
)

const (
	confirmWords    = 3
	confirmAttempts = 3
)

func (a *app) initCmd() *cobra.Command {
	var restore, lenderOnly bool
	var plan initPlan
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Create this node's identity and recovery phrase",
		Long: `init creates the node's identity from a new 24-word recovery phrase,
shows the phrase once and asks for some of its words back, so that you
have written it down. The phrase is never stored; it is the only way to
recover the node after losing this machine.

With --restore, init reads an existing phrase from standard input
instead and recreates the same identity on this machine.

Then init asks how large the repository you back up will be and at how
many copies, and says how much to lend your friends in total, written
as lending.max_total into the config it creates (LEND-7).
--repository-size and --copies answer without asking; with nothing more
on standard input, init asks nothing.

With --lender-only the node lends and backs up nothing (PEER-9): init
writes role = "lender" and asks nothing about a repository. Setting
role = "full" later and restarting lets it add buckets; its friendships
stay as they are.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := plan.parse(); err != nil {
				return usageError{err}
			}
			if lenderOnly && (plan.sizeFlag != "" || plan.copies != 0) {
				return usageError{errors.New("--lender-only: a lender-only node backs up nothing, so --repository-size and --copies do not apply")}
			}
			return a.runInit(cmd.Context(), restore, lenderOnly, plan)
		},
	}
	cmd.Flags().BoolVar(&restore, "restore", false, "recreate the identity from an existing recovery phrase read from stdin")
	cmd.Flags().BoolVar(&lenderOnly, "lender-only", false, "make a node that only lends, with no buckets and nothing on loopback (PEER-9): writes role = \"lender\"")
	cmd.Flags().StringVar(&plan.sizeFlag, "repository-size", "", "the expected size of the repository you back up, such as 100G (LEND-7); asked if not given")
	cmd.Flags().IntVar(&plan.copies, "copies", 0, "how many friends hold each object (EP-6); asked if not given")
	return cmd
}

// initPlan is what init learns for capacity planning (LEND-7): the
// expected repository size and copies, from flags or asked.
type initPlan struct {
	sizeFlag string
	size     int64
	copies   int
}

func (p *initPlan) parse() error {
	if p.sizeFlag != "" {
		n, err := parseNumber(p.sizeFlag)
		if err != nil {
			return fmt.Errorf("--repository-size: %w", err)
		}
		p.size = n
	}
	if p.copies < 0 || p.copies > maxCopies {
		return fmt.Errorf("--copies %d: want 1 to %d", p.copies, maxCopies)
	}
	return nil
}

// maxCopies bounds the copies init accepts.
const maxCopies = 20

// askPlan asks for what the flags left out. An empty answer to the size,
// or the end of input, skips capacity planning: a script that pipes a
// phrase is asked nothing more. An empty answer to copies takes
// defaultCopies (replication.copies).
func (a *app) askPlan(p *initPlan, defaultCopies int) error {
	if p.size == 0 {
		for try := 0; ; try++ {
			fmt.Fprint(a.out, "\nExpected repository size, such as 100G (Enter to skip): ")
			line, err := a.readLine()
			if err != nil || line == "" {
				fmt.Fprintln(a.out)
				return nil
			}
			if p.size, err = parseNumber(line); err == nil {
				break
			}
			if try == confirmAttempts-1 {
				return fmt.Errorf("repository size: %w", err)
			}
			fmt.Fprintf(a.out, "%v\n", err)
		}
	}
	for try := 0; p.copies == 0; try++ {
		fmt.Fprintf(a.out, "Copies of each object at friends [%d]: ", defaultCopies)
		line, err := a.readLine()
		if err != nil || line == "" {
			p.copies = defaultCopies
			break
		}
		if c, err := strconv.Atoi(line); err == nil && c >= 1 && c <= maxCopies {
			p.copies = c
			break
		}
		if try == confirmAttempts-1 {
			return fmt.Errorf("copies %q: want a whole number from 1 to %d", line, maxCopies)
		}
		fmt.Fprintf(a.out, "Want a whole number from 1 to %d.\n", maxCopies)
	}
	return nil
}

func (a *app) runInit(ctx context.Context, restore, lenderOnly bool, plan initPlan) error {
	cfg, log, err := a.load(a.errOut)
	if err != nil {
		return err
	}
	if lenderOnly {
		// PEER-9: the role is the config file's, which init writes only
		// when there is none; one that says otherwise is the user's.
		if cfg.FileExists && cfg.Role != "lender" {
			return usageError{fmt.Errorf("--lender-only: %s sets role = %q; set role = \"lender\" there, or move the file aside", cfg.Path, cfg.Role)}
		}
		cfg.Role = "lender"
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
	// EP-12: refuse a store the filesystem cannot hold, before the user
	// writes a phrase down for a node that cannot run.
	floor, _ := config.ParseFloor(cfg.Store.FreeSpaceFloor)
	if err := a.checker().Prepare(cfg.StorePath(), floor); err != nil {
		return err
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
	// LEND-7; a lender-only node backs up nothing (PEER-9).
	defaultCopies := cfg.Replication.Copies.Int()
	if cfg.Role == "full" {
		if plan.size > 0 && plan.copies == 0 {
			plan.copies = defaultCopies
		}
		if err := a.askPlan(&plan, defaultCopies); err != nil {
			return err
		}
	}
	var lend int64
	if plan.size > 0 {
		lend = capacity.SuggestMaxTotal(plan.size, plan.copies)
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
	if err := db.SetNode(ctx, "initialised_at", a.now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	a.chooseRegion(ctx, cfg, db, log)
	if !cfg.FileExists {
		if err := writeInitialConfig(cfg, lend, plan.copies); err != nil {
			return err
		}
	}
	log.Info("identity created", "node_id", nodeID, "restored", restore)

	fmt.Fprintf(a.out, "\nNode ID: %s\n", nodeID)
	fmt.Fprintf(a.out, "Config:  %s\nData:    %s\n", cfg.Path, cfg.DataDir)
	if lend > 0 {
		a.planLines(cfg, plan, lend)
	}
	return nil
}

// planLines say what to lend and how much disk the node needs (LEND-7,
// DESIGN §4): c × S plus the tombstone margin, and S more for the local
// copy.
func (a *app) planLines(cfg *config.Loaded, plan initPlan, lend int64) {
	size := capacity.RoundUp(plan.size)
	disk := capacity.RoundUp(plan.size + lend)
	fmt.Fprintf(a.out, "\nFor a repository of %s at %d copies, lend about %s in total: %d × %s, plus %d%% for what friends keep for the retention window.\n",
		formatSize(size), plan.copies, formatSize(lend), plan.copies, formatSize(size), capacity.TombstoneMargin)
	if cfg.FileExists {
		fmt.Fprintf(a.out, "Set it as lending.max_total = %q in %s.\n", formatSize(lend), cfg.Path)
	} else {
		fmt.Fprintf(a.out, "It is lending.max_total in %s.\n", cfg.Path)
	}
	fmt.Fprintf(a.out, "This disk then holds about %s: the local copy and what you lend.\n", formatSize(disk))
	fmt.Fprintf(a.out, "You are fully protected while your repository is at most your friends' repositories combined divided by %d;\n", plan.copies)
	fmt.Fprintln(a.out, "`lendisk status` shows the protectable size once you have friends.")
	if sp, err := a.storeSpace(cfg); err == nil && sp < uint64(disk) {
		fmt.Fprintf(a.out, "! The store's filesystem holds %s, less than that.\n", humanSize(int64(min(sp, uint64(1<<62)))))
	}
}

// storeSpace is the size of the store's filesystem.
func (a *app) storeSpace(cfg *config.Loaded) (uint64, error) {
	statfs := a.lendStatfs
	if statfs == nil {
		statfs = a.checker().Statfs
	}
	sp, err := statfs(cfg.StorePath())
	return sp.Total, err
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
func writeInitialConfig(cfg *config.Loaded, maxTotal int64, copies int) error {
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
	if maxTotal > 0 {
		// LEND-7: what init suggested from the expected repository size.
		body += fmt.Sprintf("\n[lending]\n# c × S plus 20%% for what friends keep for the retention window (LEND-7).\nmax_total = %q\n", formatSize(maxTotal))
	}
	if copies > 0 && copies != cfg.Replication.Copies.Int() {
		body += fmt.Sprintf("\n[replication]\ncopies = %d\n", copies)
	}
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
