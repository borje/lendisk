package cli

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/seed"
	"github.com/borje/lendisk/internal/state"
)

// importCmd loads a seeding disk a friend exported for this node
// (REPL-8, #36) into the friend's bucket here.
func (a *app) importCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "import <directory>",
		Short: "Load a friend's bucket from a disk the friend exported for this node",
		Long: `import loads the objects a friend wrote with
` + "`lendisk export <bucket> --for <this node>`" + ` into that friend's bucket
here, instead of over the network (REPL-8). Every object goes through
this node's running daemon exactly as one the friend sends: the
friendship's quota and the store's free-space floor, the hash the friend
recorded for it, and a key already held with other bytes are checked as
for any put, so a damaged disk stores nothing wrong. The disk's manifest
(.lendisk/seed.json) must authenticate as the friend's and name this
node.

An object refused alone (damaged on the disk, or a key held here with
other bytes) is reported and the import goes on; the friend's node sends
it over the network later. A full quota or floor stops the import. Run it
again to finish one that stopped: objects already here are not stored
twice. The friend's node sees what arrived when it next lists its copy
here, and sends the rest.

The daemon must be running. Exit codes: 0 every object stored; 1 an
error, an import that stopped, or objects refused; 2 a usage error.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runImport(cmd.Context(), args[0])
		},
	}
}

func (a *app) runImport(ctx context.Context, dir string) error {
	cfg, db, err := a.openState(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	id, err := identity.Load(cfg.ConfigDir())
	if err != nil {
		return err
	}
	m, err := seed.ReadManifest(dir, id)
	if err != nil {
		return err
	}
	if role, err := db.PeerRole(ctx, m.Borrower, a.now()); err != nil {
		return err
	} else if role != state.RolePeer {
		return fmt.Errorf("node %s, which exported this disk, is not a friend of this node: `lendisk peer list` lists them", m.Borrower)
	}
	socket := gateway.SocketPath(cfg.DataDir)
	if !daemonRunning(cfg.DataDir) {
		return errors.New("the daemon is not running, and import stores through it so the friendship's quota and checks apply: start `lendisk serve`, then import again")
	}
	var total int64
	for _, o := range m.Objects {
		total += o.Size
	}
	fmt.Fprintf(a.errOut, "Importing %d objects (%s) of %s's bucket %s.\n", len(m.Objects), humanSize(total), m.Borrower, m.Bucket)
	shown := int64(-1)
	im := &seed.Importer{Self: id,
		Dial: func(ctx context.Context) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		Progress: func(done, all int, bytes int64) {
			pct := int64(done) * 100 / int64(all)
			if total > 0 {
				pct = bytes * 100 / total
			}
			if pct > shown || done == all {
				shown = pct
				fmt.Fprintf(a.errOut, "  %3d%%  %d/%d objects, %s\n", pct, done, all, humanSize(bytes))
			}
		}}
	r, err := im.Import(ctx, dir, m)
	for _, f := range r.Refused {
		fmt.Fprintf(a.errOut, "Refused %s: %s.\n", f.Key, f.Reason)
	}
	if err != nil {
		return fmt.Errorf("%w; %d of %d objects are stored: fix the cause and run the same import again", err, r.Objects, len(m.Objects))
	}
	fmt.Fprintf(a.out, "Imported %d objects (%s) of %s's bucket %s.\n", r.Objects, humanSize(r.Bytes), m.Borrower, m.Bucket)
	if len(r.Refused) > 0 {
		return fmt.Errorf("%d objects were refused; %s's node sends them over the network", len(r.Refused), m.Borrower)
	}
	return nil
}
