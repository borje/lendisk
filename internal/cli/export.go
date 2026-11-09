package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/export"
)

// exportCmd writes a friend's replica held here as plain files in the
// backup tool's layout (LEND-6, #35): the command the friend's recovery
// sheet prints (sheet.ExportCommand).
func (a *app) exportCmd() *cobra.Command {
	var lent string
	var kopia bool
	cmd := &cobra.Command{
		Use:   "export --lent <stored name> <directory> [--kopia-layout]",
		Short: "Write a friend's replica held here as plain files their backup tool opens without lendisk",
		Long: `export writes the replica of a friend's bucket that this node holds into
<directory>, as plain files in the backup tool's own layout, for the
friend to restore with no lendisk at all (LEND-6, P-5): one file per key
at its key path, which is restic's repository layout, or with
--kopia-layout the sharded layout of a Kopia filesystem repository.
lendisk's tombstones and the storage engine's housekeeping are left out.

The bucket is named as this node's store names it, lent-<id16>-<sealed>:
this node never learns the real name. The friend's recovery sheet gives
it, and ` + "`ls <store>`" + ` lists them. <directory> must be new or empty.

It reads the store directory directly and needs no daemon; it works with
lendisk stopped. With the daemon running, an object deleted meanwhile is
left out and one written after the export passed is not in it.

The friend copies the exports of enough friends side by side and combines
them as their recovery sheet says; see docs/restore-without-lendisk.md.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if lent == "" {
				return usageError{errors.New("--lent <stored name> is required: the lent-… name of the friend's bucket in this node's store")}
			}
			cfg, _, err := a.load(a.errOut)
			if err != nil {
				return err
			}
			r, err := export.Lent(cfg.StorePath(), lent, args[0], kopia)
			if errors.Is(err, export.ErrNotLent) {
				return usageError{err}
			} else if err != nil {
				return fmt.Errorf("exporting %s: %w; remove %s and run the export again", lent, err, args[0])
			}
			if r.Kopia && !kopia {
				fmt.Fprintf(a.errOut, "%s holds kopia.repository: it is a Kopia repository, which opens from a directory only in Kopia's layout; export it again with --kopia-layout.\n", lent)
			}
			if r.Gone > 0 {
				fmt.Fprintf(a.errOut, "%d objects were deleted while the export ran and are not in it.\n", r.Gone)
			}
			layout := "restic's layout (one file per key)"
			if kopia {
				layout = "Kopia's filesystem layout"
			}
			fmt.Fprintf(a.out, "Exported %d objects (%s) of %s to %s, in %s.\n", r.Objects, humanSize(r.Bytes), lent, args[0], layout)
			return nil
		},
	}
	cmd.Flags().StringVar(&lent, "lent", "", "the friend's bucket, by its name in this node's store (lent-<id16>-<sealed>)")
	cmd.Flags().BoolVar(&kopia, "kopia-layout", false, "write Kopia's sharded filesystem layout, for a Kopia bucket")
	return cmd
}
