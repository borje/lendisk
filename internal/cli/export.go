package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/export"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/seed"
	"github.com/borje/lendisk/internal/state"
)

// exportCmd writes plain files in the backup tool's layout, in one of
// two forms: a friend's replica held here (--lent, LEND-6, #35: the
// command the friend's recovery sheet prints, sheet.ExportCommand), or
// one of this node's buckets for a friend to seed from (--for, REPL-8,
// #36).
func (a *app) exportCmd() *cobra.Command {
	var lent, forPeer string
	var kopia bool
	cmd := &cobra.Command{
		Use:   "export (<bucket> --for <friend> | --lent <stored name> [--kopia-layout]) <directory>",
		Short: "Write a bucket as plain files: to seed a friend from a disk, or a friend's replica held here",
		Long: `export writes plain files in the backup tool's own layout, in one of two forms.

  export <bucket> --for <friend> <directory>

writes what of this node's bucket is placed at that friend (its share of
the data objects and all metadata, each at its latest captured version)
to <directory>, say on a removable disk, for the friend to load with
` + "`lendisk import <directory>`" + ` instead of over the network (REPL-8). The
layout follows the bucket's profile (Kopia's sharded layout for kopia).
.lendisk/seed.json says what the disk holds and for whom; only that
friend accepts it. From the export on, the daemon sends that friend
nothing more of the bucket: it lists the friend's copy, and what the
import stored with the hash this node captured is acknowledged and never
sent. The rest, and every write since, go by the network once all is
confirmed, an hour after the import confirmed its last object, or after
30 days if nothing was imported. ` + "`lendisk status`" + ` shows the seed.

  export --lent <stored name> <directory> [--kopia-layout]

writes the replica of a friend's bucket that this node holds into
<directory>, as plain files in the backup tool's own layout, for the
friend to restore with no lendisk at all (LEND-6, P-5): one file per key
at its key path, which is restic's repository layout, or with
--kopia-layout the sharded layout of a Kopia filesystem repository.
lendisk's tombstones and the storage engine's housekeeping are left out.
The bucket is named as this node's store names it, lent-<id16>-<sealed>:
this node never learns the real name. The friend's recovery sheet gives
it, and ` + "`ls <store>`" + ` lists them. It reads the store directory directly
and needs no daemon; it works with lendisk stopped. With the daemon
running, an object deleted meanwhile is left out and one written after
the export passed is not in it. The friend copies the exports of enough
friends side by side and combines them as their recovery sheet says;
see docs/restore-without-lendisk.md.

<directory> must be new or empty. Files are written 0600 and directories
0700.`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case forPeer != "" && lent != "":
				return usageError{errors.New("give --for <friend> to seed a friend, or --lent <stored name> to export a friend's replica, not both")}
			case forPeer != "":
				if kopia {
					return usageError{errors.New("--kopia-layout is for --lent: with --for the layout follows the bucket's profile")}
				}
				if len(args) != 2 {
					return usageError{errors.New("usage: lendisk export <bucket> --for <friend> <directory>")}
				}
				return a.exportFor(cmd.Context(), args[0], forPeer, args[1])
			case lent == "":
				return usageError{errors.New("give --for <friend> to seed a friend with a bucket, or --lent <stored name>: the lent-… name of a friend's bucket in this node's store")}
			case len(args) != 1:
				return usageError{errors.New("usage: lendisk export --lent <stored name> <directory> [--kopia-layout]")}
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
	cmd.Flags().StringVar(&forPeer, "for", "", "the friend to seed: write what of <bucket> is placed there (its node ID)")
	cmd.Flags().StringVar(&lent, "lent", "", "the friend's bucket, by its name in this node's store (lent-<id16>-<sealed>)")
	cmd.Flags().BoolVar(&kopia, "kopia-layout", false, "with --lent: write Kopia's sharded filesystem layout, for a Kopia bucket")
	return cmd
}

// exportFor writes what of bucket is placed at friend to dir (REPL-8).
func (a *app) exportFor(ctx context.Context, bucket, friend, dir string) error {
	peer, err := identity.ParseNodeID(friend)
	if err != nil {
		return usageError{fmt.Errorf("--for %q is not a node ID: %w", friend, err)}
	}
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	b, err := db.GetBucket(ctx, bucket)
	if errors.Is(err, state.ErrNotFound) {
		return fmt.Errorf("no bucket %q on this node: `lendisk status` lists them", bucket)
	} else if err != nil {
		return err
	}
	if _, err := db.GetFriendship(ctx, peer.String()); errors.Is(err, state.ErrNotFound) {
		return fmt.Errorf("node %s is not a friend of this node: `lendisk peer list` lists them", peer)
	} else if err != nil {
		return err
	}
	id, err := identity.Load(cfg.ConfigDir())
	if err != nil {
		return err
	}
	kopia := b.Profile == "kopia"
	e := &seed.Exporter{DB: db, Self: id, StoreDir: cfg.StorePath(), Now: a.now}
	r, err := e.Export(ctx, bucket, peer, kopia, dir)
	if errors.Is(err, seed.ErrNothingPlaced) {
		return fmt.Errorf("nothing of bucket %s is placed at %s: `lendisk status` shows what each friend holds", bucket, peer)
	} else if err != nil {
		return fmt.Errorf("exporting %s for %s: %w; nothing is held back from the network: remove %s and run the export again", bucket, peer, err, dir)
	}
	if r.Changed > 0 {
		fmt.Fprintf(a.errOut, "%d objects changed while the export ran and are not in it; the network sends them.\n", r.Changed)
	}
	layout := "restic's layout (one file per key)"
	if kopia {
		layout = "Kopia's filesystem layout"
	}
	fmt.Fprintf(a.out, "Exported %d objects (%s) of bucket %s for %s to %s, in %s.\n", r.Objects, humanSize(r.Bytes), bucket, peer, dir, layout)
	fmt.Fprintf(a.out, "Until the friend has imported them, this node sends %s nothing more of %s.\n", peer, bucket)
	fmt.Fprintf(a.out, "On the friend's node, with its daemon running: lendisk import %s\n", dir)
	return nil
}
