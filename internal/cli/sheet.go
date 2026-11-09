package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/sheet"
	"github.com/borje/lendisk/internal/state"
)

// recoverySheetCmd prints the recovery sheet (ID-7, S3-8, #33) and
// records what it said, so the daemon alerts when it goes out of date.
func (a *app) recoverySheetCmd() *cobra.Command {
	var withPhrase, withSecrets, asJSON bool
	cmd := &cobra.Command{
		Use:   "recovery-sheet",
		Short: "Print the recovery sheet: a page for paper to rebuild this node, or restore without lendisk",
		Long: `recovery-sheet prints a page meant for paper (ID-7): this node's ID,
every friend's address, the overflow targets, how to rebuild the node
with ` + "`lendisk recover`" + `, each bucket's name at its lenders, how many
replicas a restore without lendisk needs and how to combine them.

The recovery phrase is stored nowhere, so the page has a blank to write
it in by hand. With --with-phrase the phrase is read from standard input,
checked against this node, and printed on the page. Overflow targets'
access keys are printed only with --with-secrets.

The daemon alerts when the printed sheet goes out of date (a friend's
address changes, a friend, overflow target or bucket is added or
removed); print it again then.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, log, err := a.load(a.errOut)
			if err != nil {
				return err
			}
			id, err := identity.Load(cfg.ConfigDir())
			if err != nil {
				return err
			}
			phrase := ""
			if withPhrase {
				// The prompt goes to stderr: stdout is the sheet.
				fmt.Fprint(a.errOut, "Recovery phrase (24 words): ")
				line, err := a.readLine()
				fmt.Fprintln(a.errOut)
				if err != nil {
					return fmt.Errorf("reading recovery phrase: %w", err)
				}
				typed, err := identity.FromPhrase(line)
				if err != nil {
					return err
				}
				if typed.NodeID() != id.NodeID() {
					return fmt.Errorf("the phrase is not this node's: it derives node %s, and this node is %s; check the words",
						typed.NodeID(), id.NodeID())
				}
				phrase = identity.NormalizePhrase(line)
			}
			db, err := state.Open(cmd.Context(), cfg.DataDir, log)
			if err != nil {
				return err
			}
			defer db.Close()
			s, err := sheet.Build(cmd.Context(), db, id, withSecrets, a.getenv())
			if err != nil {
				return err
			}
			s.Printed, s.Phrase = a.now().UTC(), phrase
			if asJSON {
				enc := json.NewEncoder(a.out)
				enc.SetIndent("", "  ")
				err = enc.Encode(s)
			} else {
				err = s.WriteText(a.out)
			}
			if err != nil {
				return err
			}
			return sheet.Record(cmd.Context(), db, s, s.Printed)
		},
	}
	cmd.Flags().BoolVar(&withPhrase, "with-phrase", false, "read the recovery phrase from standard input and print it on the sheet")
	cmd.Flags().BoolVar(&withSecrets, "with-secrets", false, "print overflow targets' access and secret keys")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print the sheet as JSON")
	return cmd
}
