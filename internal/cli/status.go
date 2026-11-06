package cli

import (
	"cmp"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
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
	// Listen is every non-loopback listener (EP-17), one per address.
	Listen []StatusListener `json:"listen"`
}

// StatusListener is one [[endpoint.listen]] address (EP-17).
type StatusListener struct {
	// URL is the endpoint for backup tools; empty for an interface that
	// has no address now.
	URL       string `json:"url"`
	Interface string `json:"interface,omitempty"`
	TLS       string `json:"tls"`       // generate, files or off
	Plaintext bool   `json:"plaintext"` // TLS is off
}

// lanListeners resolves the [[endpoint.listen]] entries for status.
func lanListeners(cfg *config.Loaded) []StatusListener {
	out := []StatusListener{}
	for _, e := range cfg.Endpoint.Listen {
		addrs := bridge.Resolve([]config.Listen{e}, nil)
		if len(addrs) == 0 {
			out = append(out, StatusListener{Interface: e.Interface, TLS: e.TLS, Plaintext: e.TLS == "off"})
		}
		for _, a := range addrs {
			out = append(out, StatusListener{URL: a.URL(), Interface: e.Interface, TLS: e.TLS, Plaintext: a.Plaintext()})
		}
	}
	return out
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
	// Address is the tunnel address friends dial (SEC-14), empty until
	// a relay region is saved.
	Address string `json:"address"`
}

// StatusPaths are the directories the node uses.
type StatusPaths struct {
	Config string `json:"config"`
	Data   string `json:"data"`
}

func listenerLine(l StatusListener) string {
	what := cmp.Or(l.URL, l.Interface+" (no address now)")
	switch {
	case l.Plaintext:
		return what + " (plaintext)"
	case l.TLS == "generate":
		return what + " (TLS, generated certificate)"
	default:
		return what + " (TLS, your certificate)"
	}
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
				Node:     StatusNode{ID: id.NodeID().String(), Role: cfg.Role, Address: nodeAddress(cmd.Context(), cfg, id, log)},
				Paths:    StatusPaths{Config: cfg.ConfigDir(), Data: cfg.DataDir},
				Versions: StatusVersions{Versitygw: gateway.Version()},
				Buckets:  []StatusBucket{},
			}
			if cfg.Role == "full" {
				st.Endpoint.S3 = "http://" + cfg.Endpoint.Loopback
			}
			st.Endpoint.Listen = lanListeners(cfg)
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
			fmt.Fprintf(a.out, "Address: %s\n", cmp.Or(st.Node.Address, "none yet; `lendisk serve` chooses a relay region"))
			fmt.Fprintf(a.out, "Config:  %s\n", st.Paths.Config)
			fmt.Fprintf(a.out, "Data:    %s\n", st.Paths.Data)
			if st.Endpoint.S3 != "" {
				fmt.Fprintf(a.out, "S3:      %s\n", st.Endpoint.S3)
			}
			for _, l := range st.Endpoint.Listen {
				fmt.Fprintf(a.out, "LAN:     %s\n", listenerLine(l))
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
