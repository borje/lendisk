package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/bridge"
	"github.com/borje/lendisk/internal/config"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/profile"
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
	// Pool is the friends that hold copies (#19).
	Pool StatusPool `json:"pool"`
	// Places are where replicas go: each friend, for now (#18).
	Places []StatusPlace `json:"places"`
	// Alerts are the pending alerts, oldest first (OPS-1, OPS-2, #13):
	// what the daemon last recorded, also while it is stopped.
	Alerts []StatusAlert `json:"alerts"`
	// Manifest is the node manifest's version and which friends do not
	// hold it yet (ID-4, #28).
	Manifest StatusManifest `json:"manifest"`
	// Recovery is what a recovered node still has to list (ID-5, #29).
	Recovery StatusRecovery `json:"recovery"`
	// FormerFriends are friendships that ended and whose replicas are
	// kept until the retention window ends (PEER-5, #25).
	FormerFriends []StatusFormer `json:"former_friends"`
	// Lending is the effective cap on the sum of the node's numbers
	// (LEND-1, #24); absent when it cannot be read.
	Lending *StatusLending `json:"lending,omitempty"`
}

// StatusFormer is one ended friendship in its retention window.
type StatusFormer struct {
	Peer      string `json:"peer"`
	RemovedAt string `json:"removed_at"` // RFC 3339, UTC
	// ByPeer is true when the friend ended it, false when this node did.
	ByPeer bool `json:"by_peer"`
	// NotifiedAt is when the friend was told (or told this node); empty
	// until then, NotifyError saying why the last try failed.
	NotifiedAt  string `json:"notified_at,omitempty"`
	NotifyError string `json:"notify_error,omitempty"`
	// PurgeAfter is when what it stored here is deleted, and it deletes
	// what it holds of this node's.
	PurgeAfter string `json:"purge_after"` // RFC 3339, UTC
}

// StatusRecovery is the lists of places a recovered node has not read
// yet: each place that was offline at `recover` until the daemon lists
// it. Empty on a node that was never recovered or has read them all.
type StatusRecovery struct {
	Pending []StatusRelist `json:"pending"`
	// Conflicts are recovered objects the friends disagree about, as
	// many for one version as for another (or a metadata object one
	// friend alone holds): none is used until `bucket resolve` picks.
	Conflicts []StatusConflict `json:"conflicts"`
	// Newest are recovered objects of rewritable keys (EP-7) the friends
	// disagreed about where one friend's rewrites had replaced the
	// others' version: that newest version is used, and `bucket
	// resolve` may pick another (#74).
	Newest []StatusNewest `json:"newest"`
	// Unverified counts, per bucket outside the encryption guarantee
	// (EP-8), the recovered objects one friend alone listed: nothing
	// checked them. Encrypted tools check their own objects.
	Unverified []StatusUnverified `json:"unverified"`
}

// StatusConflict is one recovered object in conflict.
type StatusConflict struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Reason string `json:"reason"`
	// Places maps each friend that lists it to the SHA-256 it holds.
	Places map[string]string `json:"places"`
}

// StatusNewest is one recovered object decided as the newest version.
type StatusNewest struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	SHA256 string `json:"sha256"` // the version used
	// DecidedBy is the friend whose replaced versions named the others'
	// version older.
	DecidedBy string `json:"decided_by"`
	Reason    string `json:"reason"`
	// Places maps each friend that lists it to the SHA-256 it holds.
	Places map[string]string `json:"places"`
}

// StatusUnverified counts a bucket's single-source objects.
type StatusUnverified struct {
	Bucket  string `json:"bucket"`
	Objects int64  `json:"objects"`
}

// StatusRelist is one list still to read.
type StatusRelist struct {
	Place  string `json:"place"`
	Bucket string `json:"bucket"`
	// Error is why the last attempt failed, if one did.
	Error string `json:"error,omitempty"`
}

// StatusManifest is the node manifest's delivery (ID-4).
type StatusManifest struct {
	// Version counts the manifest's content changes; 0 before the
	// daemon first built it.
	Version   int64  `json:"version"`
	UpdatedAt string `json:"updated_at,omitempty"` // RFC 3339, UTC
	// Friends is how many friends should hold it.
	Friends int `json:"friends"`
	// Pending are the friends not known to hold this version: never
	// acknowledged it, or the last push to them (the daily one too)
	// failed.
	Pending []StatusManifestPending `json:"pending"`
}

// StatusManifestPending is a friend still to receive the manifest.
type StatusManifestPending struct {
	Peer string `json:"peer"`
	// Held is the version it last acknowledged; 0 if none.
	Held int64 `json:"held"`
	// Error is why the last push failed, if it did.
	Error string `json:"error,omitempty"`
}

// manifestStatus reads the manifest's version and its delivery.
func manifestStatus(ctx context.Context, db *state.DB) (StatusManifest, error) {
	out := StatusManifest{Pending: []StatusManifestPending{}}
	v, err := db.ManifestVersion(ctx)
	if err != nil {
		return out, err
	}
	pushes, err := db.ManifestPushes(ctx)
	if err != nil {
		return out, err
	}
	fs, err := db.Friendships(ctx)
	if err != nil {
		return out, err
	}
	out.Version, out.Friends = v.Seq, len(fs)
	if v.Seq > 0 {
		out.UpdatedAt = v.UpdatedAt.UTC().Format(time.RFC3339)
	}
	for _, f := range fs {
		// A failed push, the daily re-push included, means the friend
		// may not hold it: it may have lost its store (ID-4).
		if p := pushes[f.Peer]; p.Seq != v.Seq || v.Seq == 0 || p.Error != "" {
			out.Pending = append(out.Pending, StatusManifestPending{Peer: f.Peer, Held: p.Seq, Error: p.Error})
		}
	}
	return out, nil
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
	// Removed is true for a former friend (PEER-5, #25): its copies no
	// longer count, and nothing more is sent there.
	Removed bool `json:"removed,omitempty"`
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
	// Dropping counts copies re-placement is deleting there (REPL-13,
	// #26): moved to other places, or surplus after copies was lowered.
	// They wait in its FIFO as deletes, not in Queue.
	Dropping int64 `json:"dropping"`
	// LastOK is when the place last acknowledged anything (RFC 3339).
	LastOK string `json:"last_ok,omitempty"`
}

// replacingLine says what re-placement is doing for bucket b, or "".
func replacingLine(b StatusBucket) string {
	r := b.Replacing
	var parts []string
	if r.Queued > 0 {
		parts = append(parts, fmt.Sprintf("%d copies (%d bytes) placed after the pool changed wait to be sent", r.Queued, r.QueuedBytes))
	}
	if r.Dropping > 0 {
		parts = append(parts, fmt.Sprintf("%d copies moved away wait to be deleted", r.Dropping))
	}
	if r.BelowSince != "" {
		parts = append(parts, fmt.Sprintf("objects below %d copies with nowhere to place them since %s", b.Copies, r.BelowSince))
	}
	if len(parts) == 0 {
		return ""
	}
	return "re-placement: " + strings.Join(parts, "; ")
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
		sp := StatusPlace{Place: p, Kind: "friend", Removed: !seen[p], State: c.Condition, Detail: c.Detail,
			Queue:      StatusQueue{Objects: st.Queued, Bytes: st.QueuedBytes},
			LagSeconds: int64(st.Lag / time.Second), Refused: st.Refused, Dropping: st.Dropping}
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
	// a place refused them, or a friend gone past replace_after or
	// removed (PEER-5) held them, until re-placement (#26).
	BelowCopies StatusBelow `json:"below_copies"`
	// Replacing is re-placement in progress (REPL-13, OPS-1, #26).
	Replacing StatusReplacing `json:"replacing"`
	// SingleCopy is true when copies is 1: each object is held by one
	// friend, so losing that friend's disk loses the offsite copy (EP-6).
	SingleCopy bool `json:"single_copy"`
	// OutsideTrust is true when the bucket's tool does not encrypt
	// (the generic profile): friends hold its objects in the clear, so
	// it is outside lendisk's trust guarantee (EP-8, P-3).
	OutsideTrust bool `json:"outside_trust"`
	// Offsite is how much of the bucket is offsite at Copies places:
	// the watermark, the latest snapshot and whether it is offsite
	// (REPL-4, REPL-11, #20).
	Offsite StatusOffsite `json:"offsite"`
	// Changed are keys written with other bytes than a friend
	// acknowledged, which the bucket's profile does not name rewritable:
	// the local write stands, but is not replicated (EP-11, #22).
	Changed []StatusChanged `json:"changed"`
	// KopiaFormat1 is set while the bucket holds a Kopia repository in
	// format version 1, which lendisk does not support (COMPAT-1, #79):
	// writes are accepted and replicated as before. Absent otherwise.
	KopiaFormat1 *StatusKopiaFormat1 `json:"kopia_format1,omitempty"`
}

// StatusKopiaFormat1 says a bucket holds a Kopia format-1 repository.
type StatusKopiaFormat1 struct {
	Since string `json:"since"` // RFC 3339, UTC: when Key was captured
	Key   string `json:"key"`   // the first format-1 blob captured
	Fix   string `json:"fix"`   // the command that upgrades it
}

// StatusChanged is one changed key (EP-11).
type StatusChanged struct {
	Key        string `json:"key"`
	Seq        int64  `json:"seq"`
	CapturedAt string `json:"captured_at"` // RFC 3339, UTC
	// Acknowledged is the SHA-256 a friend acknowledged, or said it
	// holds when it refused the new bytes; empty if it did not say.
	Acknowledged string `json:"acknowledged_sha256"`
	// SHA256 is the new bytes', held only in the local store.
	SHA256 string `json:"sha256"`
	// Places maps each friend holding another version to its SHA-256.
	Places map[string]string `json:"places"`
	// ReportedBy are the friends in Places that refused the new bytes
	// (412) saying they hold their hash: their word, never acknowledged
	// here, and a broken or hostile friend may say so of every key.
	// Empty when the change was held back at capture.
	ReportedBy []string `json:"reported_by"`
}

// changedShown is how many changed keys status prints per bucket;
// status --json lists them all.
const changedShown = 10

// StatusPool is the pool of friends that hold copies (D6).
type StatusPool struct {
	// Friends counts the friends that are places for copies: with an
	// address, not refusing this node, not gone past replace_after.
	Friends int `json:"friends"`
	// SingleFriend is true with exactly one: one lost disk loses every
	// offsite copy (EP-6).
	SingleFriend bool `json:"single_friend"`
}

// StatusReplacing is a bucket's re-placement in progress (REPL-13, #26):
// copies it placed after the pool changed that still wait to be sent,
// copies it is deleting at places that no longer hold them, and, while
// objects stay below copies with nowhere to put the missing copies,
// since when (OPS-2's below_copies alert fires once that is longer than
// replication.replace_after).
type StatusReplacing struct {
	Queued      int64  `json:"queued"`
	QueuedBytes int64  `json:"queued_bytes"`
	Dropping    int64  `json:"dropping"`
	BelowSince  string `json:"below_since,omitempty"` // RFC 3339, UTC
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

// changedLines are the lines status prints under bucket b for its
// changed keys (EP-11): each key with the acknowledged hash and the new
// one, and what to do.
func changedLines(b StatusBucket) []string {
	if len(b.Changed) == 0 {
		return nil
	}
	out := []string{fmt.Sprintf("! %s written with other bytes than a friend acknowledged or holds; the local write stands but is not replicated (EP-11):",
		plural(int64(len(b.Changed)), "key", "keys"))}
	for i, c := range b.Changed {
		if i == changedShown {
			out = append(out, fmt.Sprintf("  … and %d more (status --json lists them all)", len(b.Changed)-changedShown))
			break
		}
		if len(c.ReportedBy) > 0 {
			f := c.ReportedBy[0]
			out = append(out, fmt.Sprintf("  %s: friend %s reports holding other bytes, %s (as reported by %s), now %s",
				c.Key, f, cmp.Or(c.Places[f], "unknown"), f, c.SHA256))
			continue
		}
		out = append(out, fmt.Sprintf("  %s: acknowledged %s, now %s", c.Key, cmp.Or(c.Acknowledged, "unknown"), c.SHA256))
	}
	return append(out, "  write the acknowledged bytes back, or, to keep the new ones, delete the key and write it again;",
		"  a friend that reports other bytes for every key is at fault (see its refusals under Places)")
}

// kopiaFormat1 is what status says of a bucket holding a Kopia
// format-1 repository (COMPAT-1), nil for any other.
func kopiaFormat1(b state.Bucket) *StatusKopiaFormat1 {
	if !b.Format1() {
		return nil
	}
	return &StatusKopiaFormat1{Since: b.Format1At.UTC().Format(time.RFC3339), Key: b.Format1Key, Fix: alert.KopiaUpgrade}
}

// StatusSchema is the current `status --json` schema number.
const StatusSchema = 1

func (a *app) statusCmd() *cobra.Command {
	var asJSON, quiet bool
	var wait string
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show this node's state",
		Long: `Show this node's state.

With --wait <bucket>, wait instead until the bucket's latest snapshot
captured before the wait began (for a bucket with no snapshot keys, its
latest write) is offsite at the bucket's copies, so a backup script can
block on replication. Exit codes: 0 offsite; 1 an error, or the daemon
is not running; 2 a usage error; 3 timed out; 4 it cannot be offsite as
things stand (objects refused by a friend, dropped at a full or gone
friend, or with no room in the pool), which status shows.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if wait != "" && asJSON {
				return usageError{errors.New("--wait prints one line; leave out --json")}
			}
			if timeout < 0 {
				return usageError{fmt.Errorf("--timeout %v is negative; 0 waits without a limit", timeout)}
			}
			cfg, log, err := a.load(a.errOut)
			if err != nil {
				return err
			}
			id, err := identity.Load(cfg.ConfigDir())
			if err != nil {
				return err
			}
			if wait != "" {
				db, err := state.Open(cmd.Context(), cfg.DataDir, log)
				if err != nil {
					return err
				}
				defer db.Close()
				db.Clock, db.RerouteAfter, db.ReplaceAfter = a.clock, cfg.RerouteAfter(), cfg.ReplaceAfter()
				return a.waitOffsite(cmd.Context(), cfg, db, wait, timeout, quiet)
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

				FormerFriends: []StatusFormer{},
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
			// As the daemon judges reachability: which friends are gone (D6).
			db.Clock, db.RerouteAfter, db.ReplaceAfter = a.clock, cfg.RerouteAfter(), cfg.ReplaceAfter()
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
			replacing, err := db.Replacing(cmd.Context())
			if err != nil {
				return err
			}
			belowSince, err := db.BelowCopiesSince(cmd.Context())
			if err != nil {
				return err
			}
			profiles, err := profile.Load(filepath.Join(cfg.ConfigDir(), "profiles"))
			if err != nil {
				return err
			}
			cks, err := db.ChangedKeys(cmd.Context())
			if err != nil {
				return err
			}
			changed := map[string][]StatusChanged{}
			for _, c := range cks {
				changed[c.Bucket] = append(changed[c.Bucket], StatusChanged{Key: c.Key, Seq: c.Seq,
					CapturedAt: c.CapturedAt.UTC().Format(time.RFC3339), Acknowledged: c.Acknowledged, SHA256: c.SHA256,
					Places: c.Places, ReportedBy: append([]string{}, c.ReportedBy...)})
			}
			for _, b := range bs {
				q, bc := queues[b.Name], below[b.Name]
				off, err := offsiteStatus(cmd.Context(), db, b, profiles.Get(b.Profile))
				if err != nil {
					return err
				}
				if changed[b.Name] == nil {
					changed[b.Name] = []StatusChanged{}
				}
				r := replacing[b.Name]
				rs := StatusReplacing{Queued: r.Queued, QueuedBytes: r.QueuedBytes, Dropping: r.Dropping}
				if bs, ok := belowSince[b.Name]; ok {
					rs.BelowSince = bs.Since.UTC().Format(time.RFC3339)
				}
				st.Buckets = append(st.Buckets, StatusBucket{Name: b.Name, Profile: b.Profile, Copies: b.Copies, Replacing: rs,
					Queue: StatusQueue{Objects: q.Entries, Bytes: q.Bytes, Pending: q.Pending}, Refused: refused[b.Name],
					BelowCopies: StatusBelow{Objects: bc.Objects, Bytes: bc.Bytes}, SingleCopy: b.Copies == 1,
					OutsideTrust: !profiles.Get(b.Profile).Encrypted, Offsite: off,
					Changed: changed[b.Name], KopiaFormat1: kopiaFormat1(b)})
			}
			if st.Pool.Friends, err = db.PoolSize(cmd.Context()); err != nil {
				return err
			}
			st.Pool.SingleFriend = st.Pool.Friends == 1
			if l, err := a.lendingStatus(cmd.Context(), cfg, db); err == nil {
				st.Lending = &l
			} else {
				log.Warn("cannot read the lending cap", "err", err)
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
			if st.Manifest, err = manifestStatus(cmd.Context(), db); err != nil {
				return err
			}
			formers, err := db.FormerFriends(cmd.Context())
			if err != nil {
				return err
			}
			for _, f := range formers {
				sf := StatusFormer{Peer: f.Peer, RemovedAt: f.RemovedAt.UTC().Format(time.RFC3339), ByPeer: f.ByPeer,
					NotifyError: f.NotifyError, PurgeAfter: f.PurgeAfter.UTC().Format(time.RFC3339)}
				if !f.NotifiedAt.IsZero() {
					sf.NotifiedAt = f.NotifiedAt.UTC().Format(time.RFC3339)
				}
				st.FormerFriends = append(st.FormerFriends, sf)
			}
			rls, err := db.Relists(cmd.Context())
			if err != nil {
				return err
			}
			st.Recovery.Pending = []StatusRelist{}
			for _, r := range rls {
				st.Recovery.Pending = append(st.Recovery.Pending, StatusRelist{Place: r.Place, Bucket: r.Bucket, Error: r.Error})
			}
			cs, err := db.Conflicts(cmd.Context())
			if err != nil {
				return err
			}
			st.Recovery.Conflicts = []StatusConflict{}
			for _, c := range cs {
				st.Recovery.Conflicts = append(st.Recovery.Conflicts, StatusConflict{Bucket: c.Bucket, Key: c.Key,
					Reason: c.Reason, Places: c.Places})
			}
			ns, err := db.NewestKeys(cmd.Context())
			if err != nil {
				return err
			}
			st.Recovery.Newest = []StatusNewest{}
			for _, k := range ns {
				st.Recovery.Newest = append(st.Recovery.Newest, StatusNewest{Bucket: k.Bucket, Key: k.Key, SHA256: k.SHA256,
					DecidedBy: k.DecidedBy, Reason: k.Reason, Places: k.Places})
			}
			singles, err := db.SingleSources(cmd.Context())
			if err != nil {
				return err
			}
			st.Recovery.Unverified = []StatusUnverified{}
			if len(singles) > 0 {
				for _, b := range bs {
					if n := singles[b.Name]; n > 0 && !profiles.Get(b.Profile).Encrypted {
						st.Recovery.Unverified = append(st.Recovery.Unverified, StatusUnverified{Bucket: b.Name, Objects: n})
					}
				}
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
					if len(st.FormerFriends) > 0 {
						fmt.Fprint(a.out, " (copies at an ended friendship no longer count)")
					}
				}
				fmt.Fprintln(a.out)
				if b.OutsideTrust {
					fmt.Fprintf(a.out, "    %s\n", outsideTrustLine)
				}
				if b.SingleCopy {
					fmt.Fprintf(a.out, "    %s\n", singleCopyLine)
				}
				for _, l := range offsiteLines(b) {
					fmt.Fprintf(a.out, "    %s\n", l)
				}
				for _, l := range changedLines(b) {
					fmt.Fprintf(a.out, "    %s\n", l)
				}
				if f := b.KopiaFormat1; f != nil {
					fmt.Fprintf(a.out, "    ! Kopia repository in format version 1, which lendisk does not support (blob %s, %s);\n", f.Key, f.Since)
					fmt.Fprintf(a.out, "      writes are accepted and replicated as before; run `%s` (Kopia 0.9 or later)\n", f.Fix)
				}
				if l := replacingLine(b); l != "" {
					fmt.Fprintf(a.out, "    %s\n", l)
				}
			}
			if st.Pool.SingleFriend {
				fmt.Fprintln(a.out, singleFriendLine)
			}
			if st.Lending != nil {
				fmt.Fprintf(a.out, "Lending: at most %s (%s); numbers and open invites take %s\n",
					humanSize(st.Lending.MaxTotal), st.Lending.From, humanSize(st.Lending.Numbers))
			}
			if len(st.Places) > 0 {
				fmt.Fprintf(a.out, "Places:  %d\n", len(st.Places))
			}
			for _, p := range st.Places {
				if p.Removed {
					fmt.Fprintf(a.out, "  %s (former friend): its copies no longer count; nothing more is sent there\n", p.Place)
					continue
				}
				fmt.Fprintf(a.out, "  %s (%s): %s, queue %d objects, %d bytes, lag %s", p.Place, p.Kind, placeStates[p.State],
					p.Queue.Objects, p.Queue.Bytes, time.Duration(p.LagSeconds)*time.Second)
				if p.Refused > 0 {
					fmt.Fprintf(a.out, ", %d refused", p.Refused)
				}
				if p.Dropping > 0 {
					fmt.Fprintf(a.out, ", %d moved copies to delete", p.Dropping)
				}
				if p.Detail != "" {
					fmt.Fprintf(a.out, " (%s)", p.Detail)
				}
				fmt.Fprintln(a.out)
			}
			switch m := st.Manifest; {
			case m.Version == 0:
				fmt.Fprintln(a.out, "Manifest: not built yet; `lendisk serve` builds it")
			case m.Friends == 0:
				fmt.Fprintf(a.out, "Manifest: version %d, no friend to hold it\n", m.Version)
			case len(m.Pending) == 0:
				fmt.Fprintf(a.out, "Manifest: version %d, held by every friend\n", m.Version)
			default:
				fmt.Fprintf(a.out, "Manifest: version %d, not yet at %d of %d friends\n", m.Version, len(m.Pending), m.Friends)
				for _, p := range m.Pending {
					fmt.Fprintf(a.out, "  %s holds version %d", p.Peer, p.Held)
					if p.Error != "" {
						fmt.Fprintf(a.out, " (last push: %s)", p.Error)
					}
					fmt.Fprintln(a.out)
				}
			}
			if n := len(st.Recovery.Pending); n > 0 {
				fmt.Fprintf(a.out, "Recovery: %d lists of what a friend holds not read yet; read when the friend answers\n", n)
				for _, r := range st.Recovery.Pending {
					fmt.Fprintf(a.out, "  %s at %s", r.Bucket, r.Place)
					if r.Error != "" {
						fmt.Fprintf(a.out, " (last try: %s)", r.Error)
					}
					fmt.Fprintln(a.out)
				}
			}
			if n := len(st.Recovery.Conflicts); n > 0 {
				fmt.Fprintf(a.out, "Recovery: %d objects in conflict, used by nothing until you pick a friend's version:\n", n)
				for _, c := range st.Recovery.Conflicts {
					fmt.Fprintf(a.out, "  %s %s: %s\n", c.Bucket, c.Key, c.Reason)
					for _, p := range slices.Sorted(maps.Keys(c.Places)) {
						fmt.Fprintf(a.out, "    %s holds %s: `lendisk bucket resolve %s %s %s`\n", p, c.Places[p], c.Bucket, c.Key, p)
					}
				}
			}
			if n := len(st.Recovery.Newest); n > 0 {
				fmt.Fprintf(a.out, "Recovery: %d objects the friends disagreed about; the newest version is used:\n", n)
				for _, k := range st.Recovery.Newest {
					fmt.Fprintf(a.out, "  %s %s: %s\n", k.Bucket, k.Key, k.Reason)
					for _, p := range slices.Sorted(maps.Keys(k.Places)) {
						if k.Places[p] != k.SHA256 {
							fmt.Fprintf(a.out, "    to use %s's %s instead: `lendisk bucket resolve %s %s %s`\n", p, k.Places[p], k.Bucket, k.Key, p)
						}
					}
				}
			}
			for _, u := range st.Recovery.Unverified {
				fmt.Fprintf(a.out, "Recovery: %d objects of %s came from one friend alone, unverified\n", u.Objects, u.Bucket)
			}
			if n := len(st.FormerFriends); n > 0 {
				fmt.Fprintf(a.out, "Ended friendships: %d, replicas kept until the purge\n", n)
				tw := tabwriter.NewWriter(a.out, 0, 4, 1, ' ', 0)
				formerLines(tw, st.FormerFriends)
				tw.Flush()
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
	cmd.Flags().StringVar(&wait, "wait", "", "wait until `bucket`'s latest snapshot is offsite at its copies (exit 3 on timeout, 4 if it cannot be)")
	cmd.Flags().DurationVar(&timeout, "timeout", 24*time.Hour, "with --wait, how long to wait; 0 waits without a limit")
	return cmd
}
