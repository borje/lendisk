package cli

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/version"
)

// Status is the `status --json` document. Its top-level shape is stable:
// later versions add fields and sections (buckets, peers, alerts…) but do
// not rename or remove these. StatusSchema changes only on a breaking
// change, which should not happen.
type Status struct {
	Schema   int            `json:"schema"`
	Version  string         `json:"version"`
	Node     StatusNode     `json:"node"`
	Paths    StatusPaths    `json:"paths"`
	Endpoint StatusEndpoint `json:"endpoint"`
	Versions StatusVersions `json:"versions"`
	Buckets  []StatusBucket `json:"buckets"`
}

// StatusEndpoint is where backup tools reach the node (EP-1). S3 is empty
// in the lender-only profile, which has no endpoint (PEER-9).
type StatusEndpoint struct {
	S3 string `json:"s3"`
}

// StatusVersions are the linked components' versions (EP-12).
type StatusVersions struct {
	Versitygw string `json:"versitygw"`
}

// StatusBucket is one of the node's own buckets.
type StatusBucket struct {
	Name    string `json:"name"`
	Profile string `json:"profile"`
	Copies  int    `json:"copies"`
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
			cfg, log, err := a.load(a.errOut)
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
				Schema:   StatusSchema,
				Version:  version.Version,
				Node:     StatusNode{ID: id.NodeID().String(), Role: cfg.Role},
				Paths:    StatusPaths{Config: cfg.ConfigDir(), Data: cfg.DataDir},
				Versions: StatusVersions{Versitygw: gateway.Version()},
				Buckets:  []StatusBucket{},
			}
			if cfg.Role == "full" {
				st.Endpoint.S3 = "http://" + cfg.Endpoint.Loopback
			}
			db, err := state.Open(cmd.Context(), cfg.DataDir, log)
			if err != nil {
				return err
			}
			defer db.Close()
			bs, err := db.Buckets(cmd.Context())
			if err != nil {
				return err
			}
			for _, b := range bs {
				st.Buckets = append(st.Buckets, StatusBucket{Name: b.Name, Profile: b.Profile, Copies: b.Copies})
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
			if st.Endpoint.S3 != "" {
				fmt.Fprintf(a.out, "S3:      %s\n", st.Endpoint.S3)
			}
			fmt.Fprintf(a.out, "Buckets: %d\n", len(st.Buckets))
			for _, b := range st.Buckets {
				fmt.Fprintf(a.out, "  %s (%s, copies %d)\n", b.Name, b.Profile, b.Copies)
			}
			fmt.Fprintf(a.out, "versitygw %s\n", st.Versions.Versitygw)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print status as JSON")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "print nothing; exit 0 if the node is set up (for health checks)")
	return cmd
}
