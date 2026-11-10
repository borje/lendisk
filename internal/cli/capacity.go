package cli

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/borje/lendisk/internal/capacity"
	"github.com/borje/lendisk/internal/state"
)

// Capacity planning in status (LEND-7, LEND-8, OPS-1, PEER-9; #37).

// LowUseFor is how long one side of a friendship must have used less
// than half of the quota before status says so (LEND-7).
const LowUseFor = 30 * 24 * time.Hour

// StatusCapacity is capacity planning in `status --json` (LEND-7):
// fields are added, never renamed or removed. Sizes are bytes.
type StatusCapacity struct {
	// RepositorySize is the node's repository as friends count it (each
	// live object plus state.ObjectOverhead), Metadata its metadata part,
	// held at every friend; LocalCopy the live bytes in the local store.
	RepositorySize int64 `json:"repository_size"`
	Metadata       int64 `json:"metadata"`
	LocalCopy      int64 `json:"local_copy"`
	// Copies is c: the largest copies of a bucket holding data.
	Copies int `json:"copies"`
	// Protectable is the protectable size; FairProtectable the most fair
	// quotas could protect, each quota replaced by the friend's
	// repository size (DESIGN §4).
	Protectable     int64 `json:"protectable"`
	FairProtectable int64 `json:"fair_protectable"`
	// Alerting is true while the repository is above 90% of the
	// protectable size (OPS-2); Deficit is how far it is above it, 0
	// when not. Coverable is false when no quotas can cover it (fewer
	// friends than copies): a friend or an overflow target is needed.
	Alerting  bool  `json:"alerting"`
	Deficit   int64 `json:"deficit"`
	Coverable bool  `json:"coverable"`
	// OverflowSplit is true while friends cannot hold Copies copies of
	// the repository and an overflow target exists: each data object then
	// goes to Copies − 1 friends and one overflow target (REPL-12).
	OverflowSplit bool `json:"overflow_split"`
	// Friends is each friendship's quota and use both ways.
	Friends []StatusCapacityFriend `json:"friends"`
	// Overflow is each overflow target's room, use and cost (S3-7).
	Overflow []StatusCapacityOverflow `json:"overflow"`
	// Lent is the quota lent to all friends, LentUsed what they store
	// here; Received the quota they lend this node (the same, as every
	// friendship is symmetric), ReceivedUnused what of it this node does
	// not use (PEER-9: all of it on a lender-only node).
	Lent           int64 `json:"lent"`
	LentUsed       int64 `json:"lent_used"`
	Received       int64 `json:"received"`
	ReceivedUnused int64 `json:"received_unused"`
}

// StatusCapacityFriend is one friendship in capacity planning.
type StatusCapacityFriend struct {
	Peer  string `json:"peer"`
	Quota int64  `json:"quota"`
	// UseThere is what this node stores at the friend (its count,
	// reconciled from the friend's); UseHere what the friend stores here.
	UseThere int64 `json:"use_there"`
	UseHere  int64 `json:"use_here"`
	// RepositorySize is the friend's, as it reported it; absent before
	// its first report.
	RepositorySize *int64 `json:"repository_size,omitempty"`
	// Cover is the quota the two should agree on to cover the deficit
	// (LEND-8), while the repository is above 90% of the protectable
	// size; absent otherwise or when this friendship need not change.
	Cover int64 `json:"cover,omitempty"`
	// LowThereSince and LowHereSince are set when this node (there) or
	// the friend (here) has used less than half of the quota for
	// LowUseFor: since when (RFC 3339, UTC).
	LowThereSince string `json:"low_there_since,omitempty"`
	LowHereSince  string `json:"low_here_since,omitempty"`
}

// StatusCapacityOverflow is one overflow target's room (max_bytes) and
// what it holds (S3-7).
type StatusCapacityOverflow struct {
	Name     string `json:"name"`
	MaxBytes int64  `json:"max_bytes"`
	// Use is what it holds or will hold, metadata included, as the
	// provider counts bytes (objects at their size plus
	// state.ObjectOverhead).
	Use int64 `json:"use"`
	// Endpoint, Bucket and Prefix are where it is; keys go under
	// <prefix>/<sealed name>/ (S3-2). Never the credential (SEC-3).
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix"`
	// DataObjects and DataBytes are the live data objects for which it
	// is one of the copies.
	DataObjects int64 `json:"data_objects"`
	DataBytes   int64 `json:"data_bytes"`
	// Percent is Use of MaxBytes; at 100 no new copies go there (S3-7).
	Percent int64 `json:"percent"`
	// PricePerGB and MonthlyCost are the user's price per GB (10^9
	// bytes) and month and the estimate for Use; absent without a price.
	PricePerGB  float64 `json:"price_per_gb,omitempty"`
	MonthlyCost float64 `json:"monthly_cost,omitempty"`
	// Versioning, Lifecycle and VersionRemoval are what overflow add
	// found (S3-4, S3-5); Versioning is read again daily since #97.
	Versioning     string `json:"versioning"`
	Lifecycle      string `json:"lifecycle"`
	VersionRemoval string `json:"version_removal"`
	// VersioningChecked is when the daily read of versioning last got an
	// answer (RFC 3339, UTC); absent before the first (#97).
	VersioningChecked string `json:"versioning_checked,omitempty"`
	// VersioningChanged is when a read found Versioning changed last,
	// and VersioningBefore what it was until then; absent when it never
	// changed (#97).
	VersioningChanged string `json:"versioning_changed,omitempty"`
	VersioningBefore  string `json:"versioning_before,omitempty"`
	// EnvPrefix names the credential's variables: <env_prefix>ACCESS_KEY
	// and <env_prefix>SECRET_KEY.
	EnvPrefix string `json:"env_prefix"`
	// Retention is who keeps deleted copies for the window (S3-4):
	// "provider" (versioning and its lifecycle rule) or "client-side"
	// (no versioning: lendisk holds each delete until the window ends).
	Retention string `json:"retention"`
	// Tombstones and TombstoneBytes are the deletes held for a
	// client-side target and what they still hold there (in Use);
	// TombstoneNextDue when the first is sent (RFC 3339, UTC). Absent
	// when none.
	Tombstones       int64  `json:"tombstones,omitempty"`
	TombstoneBytes   int64  `json:"tombstone_bytes,omitempty"`
	TombstoneNextDue string `json:"tombstone_next_due,omitempty"`
	// DeleteDenied and DeleteDeniedBytes are the copies, drained back
	// to friends or deleted, the target refused to delete for want of
	// permission (S3-5, #40): they stay there, in Use, and the delete
	// is tried again daily. Absent when none.
	DeleteDenied      int64 `json:"delete_denied,omitempty"`
	DeleteDeniedBytes int64 `json:"delete_denied_bytes,omitempty"`
	// Removing is when `overflow remove` began removing the target
	// (RFC 3339, UTC), and KeepData whether its copies are left there;
	// absent for a target in use (#96). It is listed until its row
	// goes, once nothing is left there.
	Removing string `json:"removing,omitempty"`
	KeepData bool   `json:"keep_data,omitempty"`
}

// overflowStatus reads each overflow target's room, use and cost (S3-7).
func overflowStatus(ctx context.Context, db *state.DB) ([]StatusCapacityOverflow, error) {
	targets, err := db.Overflows(ctx)
	if err != nil {
		return nil, err
	}
	holds, err := db.OverflowHolds(ctx)
	if err != nil {
		return nil, err
	}
	tombs, err := db.OverflowTombHolds(ctx)
	if err != nil {
		return nil, err
	}
	changes, err := db.LastVersioningChanges(ctx)
	if err != nil {
		return nil, err
	}
	out := []StatusCapacityOverflow{}
	for _, o := range targets {
		h := holds[o.Name]
		so := StatusCapacityOverflow{Name: o.Name, MaxBytes: o.MaxBytes, Use: h.Use, Endpoint: o.Endpoint, Bucket: o.Bucket,
			Prefix: o.Prefix, DataObjects: h.DataObjects, DataBytes: h.DataBytes, PricePerGB: o.PricePerGB,
			Versioning: o.Versioning, Lifecycle: o.Lifecycle, VersionRemoval: o.VersionRemoval, EnvPrefix: o.EnvPrefix,
			Retention: o.RetentionMode(), VersioningChecked: rfc3339(o.VersioningCheckedAt), Removing: rfc3339(o.RemovingAt),
			KeepData: o.KeepData}
		if ch, ok := changes[o.Name]; ok {
			so.VersioningChanged, so.VersioningBefore = rfc3339(ch.At), ch.Previous
		}
		t := tombs[o.Name]
		if t.Tombs > 0 {
			so.Tombstones, so.TombstoneBytes, so.TombstoneNextDue = t.Tombs, t.Bytes, rfc3339(t.NextDue)
		}
		so.DeleteDenied, so.DeleteDeniedBytes = t.Denied, t.DeniedBytes
		if o.MaxBytes > 0 {
			so.Percent = h.Use * 100 / o.MaxBytes
		}
		if o.PricePerGB > 0 {
			so.MonthlyCost = math.Round(float64(h.Use)/1e9*o.PricePerGB*100) / 100
		}
		out = append(out, so)
	}
	return out, nil
}

// overflowLine is one overflow target in status's capacity lines.
func overflowLine(o StatusCapacityOverflow) string {
	h := capacity.Human
	l := fmt.Sprintf("    %s: holds %s of %s (%d%%), %s (%s) for which it is one of the copies", o.Name, h(o.Use), h(o.MaxBytes),
		o.Percent, countObjects(o.DataObjects), h(o.DataBytes))
	if o.PricePerGB > 0 {
		l += fmt.Sprintf("; about %.2f a month at %g per GB", o.MonthlyCost, o.PricePerGB)
	}
	l += fmt.Sprintf("; versioning %s", o.Versioning)
	if o.VersioningChanged != "" {
		l += fmt.Sprintf(" since %s (was %s)", o.VersioningChanged, o.VersioningBefore)
	}
	if o.Retention == state.RetentionClientSide {
		// S3-4: lendisk holds each delete for the window; this guards
		// against mistakes, not a compromised node.
		l += " (retention: client-side"
		if o.Tombstones > 0 {
			l += fmt.Sprintf("; %d deletes held, %s", o.Tombstones, h(o.TombstoneBytes))
		}
		l += ")"
	}
	if o.DeleteDenied > 0 {
		// S3-5 (#40): a credential without delete permission.
		l += fmt.Sprintf("; %d copies kept, %s: its credential may not delete them (tried again daily)",
			o.DeleteDenied, h(o.DeleteDeniedBytes))
	}
	if o.VersionRemoval == state.VersionRemovalAllowed {
		l += "; its credential can remove versions"
	}
	switch {
	case o.Removing != "" && o.KeepData:
		l += fmt.Sprintf("; being removed since %s: its copies move elsewhere and stay there too (--keep-data)", o.Removing)
	case o.Removing != "":
		l += fmt.Sprintf("; being removed since %s: its copies move elsewhere, then are deleted there", o.Removing)
	case o.Percent >= 100:
		l += "; full: no new copies go there"
	}
	return l
}

func countObjects(n int64) string {
	if n == 1 {
		return "1 data object"
	}
	return fmt.Sprintf("%d data objects", n)
}

// capacityStatus reads capacity planning at now.
func (a *app) capacityStatus(ctx context.Context, db *state.DB, defaultCopies int, now time.Time) (*StatusCapacity, error) {
	plan, err := capacity.Read(ctx, db, defaultCopies)
	if err != nil {
		return nil, err
	}
	local, err := db.RepositorySize(ctx)
	if err != nil {
		return nil, err
	}
	fs, err := db.Friendships(ctx)
	if err != nil {
		return nil, err
	}
	use, err := db.PlaceUse(ctx)
	if err != nil {
		return nil, err
	}
	c := &StatusCapacity{RepositorySize: plan.Size(), Metadata: plan.Metadata, LocalCopy: local, Copies: plan.Copies,
		Protectable: plan.Protectable(), FairProtectable: plan.FairProtectable(), Alerting: plan.Alerting(),
		Friends: []StatusCapacityFriend{}, Overflow: []StatusCapacityOverflow{}}
	c.Deficit = max(c.RepositorySize-c.Protectable, 0)
	if c.Overflow, err = overflowStatus(ctx, db); err != nil {
		return nil, err
	}
	cover, ok := plan.Cover()
	c.Coverable, c.OverflowSplit = ok, plan.Split()
	low := func(since time.Time) string {
		if since.IsZero() || now.Sub(since) < LowUseFor {
			return ""
		}
		return rfc3339(since)
	}
	for _, f := range fs {
		here, _, err := db.LentUsage(ctx, f.Peer)
		if err != nil {
			return nil, err
		}
		sf := StatusCapacityFriend{Peer: f.Peer, Quota: f.Quota(), UseThere: max(use[f.Peer], 0), UseHere: here,
			Cover: cover[f.Peer], LowThereSince: low(f.LowThereSince), LowHereSince: low(f.LowHereSince)}
		if !f.SeenAt.IsZero() {
			size := f.PeerRepositorySize
			sf.RepositorySize = &size
		}
		c.Friends = append(c.Friends, sf)
		c.Lent += sf.Quota
		c.LentUsed += sf.UseHere
		c.Received += sf.Quota
		c.ReceivedUnused += max(sf.Quota-sf.UseThere, 0)
	}
	return c, nil
}

// capacityLines are status's lines on capacity planning.
func capacityLines(c *StatusCapacity, role string) []string {
	if c == nil {
		return nil
	}
	h := capacity.Human
	var out []string
	if role == "lender" {
		out = append(out, fmt.Sprintf("Lending only: lends %s to %s, who use %s of it; received %s from them, unused until this node has a bucket",
			h(c.Lent), plural(int64(len(c.Friends)), "friend", "friends"), h(c.LentUsed), h(c.Received)))
	} else {
		out = append(out, fmt.Sprintf("Capacity: repository %s at %d copies as friends count it (local copy %s); protectable %s; with fair quotas (each friend's repository size) %s",
			h(c.RepositorySize), c.Copies, h(c.LocalCopy), h(c.Protectable), h(c.FairProtectable)))
		out = append(out, fmt.Sprintf("  lends %s to %s, who use %s of it; received %s, of which %s unused",
			h(c.Lent), plural(int64(len(c.Friends)), "friend", "friends"), h(c.LentUsed), h(c.Received), h(c.ReceivedUnused)))
		if c.Alerting {
			l := fmt.Sprintf("  ! the repository is above %d%% of the protectable size", capacity.AlertPercent)
			if c.Deficit > 0 {
				l += fmt.Sprintf(", by %s: copies beyond it are paused for quota", h(c.Deficit))
			}
			out = append(out, l)
			if !c.Coverable {
				out = append(out, fmt.Sprintf("    with %s at %d copies no quota covers it: add another friend or an overflow target",
					plural(int64(len(c.Friends)), "friend", "friends"), c.Copies))
			} else {
				out = append(out, "    to cover it, agree these quotas, each side running `lendisk peer quota <friend> <size>`:")
				for _, f := range c.Friends {
					if f.Cover > 0 {
						out = append(out, fmt.Sprintf("      %s: %s (now %s)", f.Peer, capacity.Format(f.Cover), capacity.Format(f.Quota)))
					}
				}
			}
		}
	}
	for _, f := range c.Friends {
		l := fmt.Sprintf("  %s: quota %s; you use %s there, it uses %s here", f.Peer, capacity.Format(f.Quota), h(f.UseThere), h(f.UseHere))
		if f.RepositorySize != nil {
			l += "; its repository " + h(*f.RepositorySize)
		}
		out = append(out, l)
		if f.LowThereSince != "" && role != "lender" {
			out = append(out, fmt.Sprintf("    this node has used under half of the quota there since %s; you could both lower the number", f.LowThereSince))
		}
		if f.LowHereSince != "" {
			out = append(out, fmt.Sprintf("    friend %s has used under half of the quota here since %s; you could both lower the number", f.Peer, f.LowHereSince))
		}
	}
	if role != "lender" {
		if len(c.Overflow) == 0 {
			out = append(out, "  Overflow targets: none")
		}
		if len(c.Overflow) > 0 {
			l := fmt.Sprintf("  Overflow targets: %d, for the copies no friend has room for", len(c.Overflow))
			if c.OverflowSplit {
				l = fmt.Sprintf("  Overflow targets: %d; friends cannot hold %d copies of the repository, so each object goes to %s and an overflow target",
					len(c.Overflow), c.Copies, plural(int64(c.Copies-1), "friend", "friends"))
			}
			out = append(out, l)
		}
		for _, o := range c.Overflow {
			out = append(out, overflowLine(o))
		}
	}
	return out
}
