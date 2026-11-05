package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/version"
)

// Status is the `status --json` document. Its top-level shape is stable:
// later versions add fields and sections (buckets, peers, alerts…) but do
// not rename or remove these. StatusSchema changes only on a breaking
// change, which should not happen.
type Status struct {
	Schema  int         `json:"schema"`
	Version string      `json:"version"`
	Node    StatusNode  `json:"node"`
	Paths   StatusPaths `json:"paths"`
}

// StatusNode describes this node.
type StatusNode struct {
	ID   string `json:"id"`
	Role string `json:"role"`
}

// StatusPaths are the directories the node uses.
type StatusPaths struct {
	Config string `json:"config"`
	Data   string `json:"data"`
}

// StatusSchema is the current `status --json` schema number.
const StatusSchema = 1

func (a *app) statusCmd() *cobra.Command {
	var asJSON, quiet bool
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show this node's state",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, _, err := a.load(a.errOut)
			if err != nil {
				return err
			}
			id, err := identity.Load(cfg.ConfigDir())
			if err != nil {
				return err
			}
			if quiet {
				return nil
			}
			st := Status{
				Schema:  StatusSchema,
				Version: version.Version,
				Node:    StatusNode{ID: id.NodeID().String(), Role: cfg.Role},
				Paths:   StatusPaths{Config: cfg.ConfigDir(), Data: cfg.DataDir},
			}
			if asJSON {
				enc := json.NewEncoder(a.out)
				enc.SetIndent("", "  ")
				return enc.Encode(st)
			}
			fmt.Fprintf(a.out, "Node ID: %s\n", st.Node.ID)
			fmt.Fprintf(a.out, "Role:    %s\n", st.Node.Role)
			fmt.Fprintf(a.out, "Config:  %s\n", st.Paths.Config)
			fmt.Fprintf(a.out, "Data:    %s\n", st.Paths.Data)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print status as JSON")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "print nothing; exit 0 if the node is set up (for health checks)")
	return cmd
}
