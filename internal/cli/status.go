package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/transport"
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
	// Places are where replicas go: each friend, for now (#18).
	Places []StatusPlace `json:"places"`
	// Alerts are the pending alerts, oldest first (OPS-1, OPS-2, #13):
	// what the daemon last recorded, also while it is stopped.
	Alerts []StatusAlert `json:"alerts"`
}

// StatusAlert is one pending alert: what the hook and the webhook
// received (alert.Message), less the node.
type StatusAlert struct {
	Kind     string `json:"kind"`              // alert.Kind…
	Subject  string `json:"subject,omitempty"` // node ID, bucket, or absent
	Message  string `json:"message"`
	RaisedAt string `json:"raised_at"` // RFC 3339, UTC
}

// StatusPlace is one place in the pool and its replication FIFO
// (REPL-1, REPL-4, REPL-11).
type StatusPlace struct {
	Place string `json:"place"` // the friend's node ID
	Kind  string `json:"kind"`  // friend
	// State is how the place answered last: ok, retrying, quota (paused
	// for quota, LEND-2), removed (PEER-5); store when it waits because
	// this node's store cannot be read, Detail saying why (#57); empty
	// before its first answer.
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
	// Queue is what waits to be sent there.
	Queue StatusQueue `json:"queue"`
	// LagSeconds is the age of the oldest object waiting (REPL-11).
	LagSeconds int64 `json:"lag_seconds"`
	// Refused counts objects the place refused for good (EP-11, LEND-4)
	// and not written again since (#57); objects the store could not
	// supply count against their bucket.
	Refused int64 `json:"refused"`
	// LastOK is when the place last acknowledged anything (RFC 3339).
	LastOK string `json:"last_ok,omitempty"`
}

// placeStates are StatusPlace.State in words.
var placeStates = map[string]string{
	state.ConditionOK:       "ok",
	state.ConditionRetrying: "retrying",
	state.ConditionQuota:    "paused for quota",
	state.ConditionRemoved:  "removed",
	state.ConditionStore:    "waiting for this node's store",
	"":                      "not contacted yet",
}

// places lists every friend and every other place with rows, friends
// first in friendship order.
func places(ctx context.Context, db *state.DB, now time.Time) ([]StatusPlace, error) {
	fs, err := db.Friendships(ctx)
	if err != nil {
		return nil, err
	}
	stats, err := db.PlaceStats(ctx, now)
	if err != nil {
		return nil, err
	}
	conds, err := db.PlaceConditions(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	seen := map[string]bool{}
	for _, f := range fs {
		names = append(names, f.Peer)
		seen[f.Peer] = true
	}
	var rest []string
	for p := range stats {
		if !seen[p] {
			rest = append(rest, p)
		}
	}
	slices.Sort(rest)
	out := []StatusPlace{}
	for _, p := range append(names, rest...) {
		st, c := stats[p], conds[p]
		sp := StatusPlace{Place: p, Kind: "friend", State: c.Condition, Detail: c.Detail,
			Queue:      StatusQueue{Objects: st.Queued, Bytes: st.QueuedBytes},
			LagSeconds: int64(st.Lag / time.Second), Refused: st.Refused}
		if !c.LastOK.IsZero() {
			sp.LastOK = c.LastOK.UTC().Format(time.RFC3339)
		}
		out = append(out, sp)
	}
	return out, nil
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
	Tailcat   string `json:"tailcat"`
}

// StatusBucket is one of the node's own buckets.
type StatusBucket struct {
	Name    string `json:"name"`
	Profile string `json:"profile"`
	Copies  int    `json:"copies"`
	// Queue counts what the bucket captured (REPL-3): Objects and Bytes
	// are every done queue row, puts and deletes, ever captured, and the
	// bytes of its puts; queue rows are never removed once done (§3.4),
	// so this is not the backlog, which is places[].queue. Pending is
	// writes still in flight to the store. The name stays: status
	// --json never renames.
	Queue StatusQueue `json:"queue"`
	// Refused counts objects not replicated because the store could not
	// supply them as captured: gone, changed or rotted since capture, a
	// directory, or too large for one PUT (#50). Each raised an alert.
	Refused int64 `json:"refused"`
	// BelowCopies counts the live objects that fewer than Copies places
	// hold or will hold, and their bytes (REPL-4): the pool lacked room,
	// a place refused them, or a friend gone past replace_after held them.
	BelowCopies StatusBelow `json:"below_copies"`
	// CopiesNudge is true for copies 1 the user did not choose with
	// --copies: one lost friend loses data (EP-6).
	CopiesNudge bool `json:"copies_nudge,omitempty"`
}

// StatusBelow counts objects and bytes below a bucket's copies.
type StatusBelow struct {
	Objects int64 `json:"objects"`
	Bytes   int64 `json:"bytes"`
}

// StatusQueue is a count of queue rows and their bytes. For a bucket it
// is every captured write and delete (EP-2) and the writes still in
// flight to the store; for a place, what waits to be sent there.
type StatusQueue struct {
	Objects int64 `json:"objects"`
	Bytes   int64 `json:"bytes"`
	Pending int64 `json:"pending"`
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
				Versions: StatusVersions{Versitygw: gateway.Version(), Tailcat: transport.Version()},
				Buckets:  []StatusBucket{},
				Places:   []StatusPlace{},
				Alerts:   []StatusAlert{},
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
			queues, err := db.QueueStats(cmd.Context())
			if err != nil {
				return err
			}
			refused, err := db.RefusedObjects(cmd.Context())
			if err != nil {
				return err
			}
			below, err := db.BelowCopies(cmd.Context())
			if err != nil {
				return err
			}
			for _, b := range bs {
				q, bc := queues[b.Name], below[b.Name]
				st.Buckets = append(st.Buckets, StatusBucket{Name: b.Name, Profile: b.Profile, Copies: b.Copies,
					Queue: StatusQueue{Objects: q.Entries, Bytes: q.Bytes, Pending: q.Pending}, Refused: refused[b.Name],
					BelowCopies: StatusBelow{Objects: bc.Objects, Bytes: bc.Bytes}, CopiesNudge: nudgeCopies(b)})
			}
			if st.Places, err = places(cmd.Context(), db, a.now()); err != nil {
				return err
			}
			as, err := db.ActiveAlerts(cmd.Context())
			if err != nil {
				return err
			}
			for _, al := range as {
				st.Alerts = append(st.Alerts, StatusAlert{Kind: al.Kind, Subject: al.Subject, Message: al.Message,
					RaisedAt: al.RaisedAt.UTC().Format(time.RFC3339)})
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
				fmt.Fprintf(a.out, "  %s (%s, copies %d), captured %d objects, %d bytes", b.Name, b.Profile, b.Copies,
					b.Queue.Objects, b.Queue.Bytes)
				if b.Queue.Pending > 0 {
					fmt.Fprintf(a.out, ", %d writes in flight", b.Queue.Pending)
				}
				if b.Refused > 0 {
					fmt.Fprintf(a.out, ", %d not replicated (the store's copy changed or cannot be sent; see the log)", b.Refused)
				}
				if b.BelowCopies.Objects > 0 {
					fmt.Fprintf(a.out, ", %d objects, %d bytes at fewer than %d places", b.BelowCopies.Objects,
						b.BelowCopies.Bytes, b.Copies)
				}
				if b.CopiesNudge {
					fmt.Fprintf(a.out, "; %s", copiesNudge)
				}
				fmt.Fprintln(a.out)
			}
			if len(st.Places) > 0 {
				fmt.Fprintf(a.out, "Places:  %d\n", len(st.Places))
			}
			for _, p := range st.Places {
				fmt.Fprintf(a.out, "  %s (%s): %s, queue %d objects, %d bytes, lag %s", p.Place, p.Kind, placeStates[p.State],
					p.Queue.Objects, p.Queue.Bytes, time.Duration(p.LagSeconds)*time.Second)
				if p.Refused > 0 {
					fmt.Fprintf(a.out, ", %d refused", p.Refused)
				}
				if p.Detail != "" {
					fmt.Fprintf(a.out, " (%s)", p.Detail)
				}
				fmt.Fprintln(a.out)
			}
			if len(st.Alerts) == 0 {
				fmt.Fprintln(a.out, "Alerts:  none")
			} else {
				fmt.Fprintf(a.out, "Alerts:  %d\n", len(st.Alerts))
			}
			for _, al := range st.Alerts {
				fmt.Fprintf(a.out, "  since %s: %s\n", al.RaisedAt, al.Message)
			}
			fmt.Fprintf(a.out, "versitygw %s, tailcat %s\n", st.Versions.Versitygw, st.Versions.Tailcat)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "print status as JSON")
	cmd.Flags().BoolVar(&quiet, "quiet", false, "print nothing; exit 0 if the node is set up (for health checks)")
	return cmd
}
