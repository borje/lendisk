// Package alert keeps the node's pending alerts and delivers them
// (OPS-2, OPS-3, DESIGN §3.8).
//
// An alert is a condition read from lendisk.db: a friend unreachable
// past alerts.unreachable_after, this node's store unreadable, objects
// the store could not supply (VER-5), objects a place refused (LEND-4),
// keys changed after a friend acknowledged them (EP-11), a friendship
// the friend ended and the purge of an ended one (PEER-5), and a
// friendship's quota nearly or wholly used either way (PEER-6, #24),
// a bucket holding a Kopia format-1 repository (COMPAT-1, #79),
// objects below copies past replication.replace_after (REPL-13, #26),
// a friend's replica that failed verification or has not passed it
// for days (VER-1, SEC-7, #27), the store reconciler's held deletes
// and missing store marker (EP-14, #8), bit-rot the sampler found in
// own objects and in friends' (VER-5, #31), a printed recovery
// sheet gone out of date (ID-7, #33), and the repository above 90% of
// the protectable size (LEND-7, LEND-8, #37).
// The Service re-reads them every Interval, and at once after Check;
// state.SyncAlerts keeps one row per condition, raised once while it
// holds and cleared when it ends. Each raise is delivered at least once
// to the command hook (Message as JSON on stdin) and to the webhook (the
// same JSON POSTed), retried with backoff until delivered or cleared; a
// crash between a send and its record sends it again.
//
// Rules a caller must keep:
//   - A new alert is a condition the evaluator reads from state, with a
//     Kind of its own added to kinds; code that detects it records the
//     condition in lendisk.db and calls Check.
//   - Check never blocks, and nothing the Service does waits on the
//     caller: replication and the gateway only write state.
//   - The webhook URL may hold a secret: log it only through Redact.
package alert

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/borje/lendisk/internal/capacity"
	"github.com/borje/lendisk/internal/state"
	"github.com/borje/lendisk/internal/version"
)

// Alert kinds (OPS-2). The key of an alert is its kind, then ':' and its
// subject when it has one.
const (
	// KindFriendUnreachable: a friend has not answered for longer than
	// alerts.unreachable_after; the subject is its node ID.
	KindFriendUnreachable = "friend_unreachable"
	// KindStoreUnreadable: this node's store cannot be read, and
	// replication waits (#57); no subject.
	KindStoreUnreadable = "store_unreadable"
	// KindObjectsNotReplicated: objects of a bucket were refused because
	// the store could not supply them as captured: gone, changed or
	// rotted (VER-5), or too large for one PUT (#50); the subject is the
	// bucket.
	KindObjectsNotReplicated = "objects_not_replicated"
	// KindPlaceRefused: a place refused objects for good: it keeps
	// refusing a rewrite of a rewritable key (#74) or rejects bytes that
	// match their capture hash (#57); the subject is the place. Its 412
	// to a key that is not rewritable counts too, and raises
	// KindKeyChanged naming it (#22): a friend may refuse every key.
	KindPlaceRefused = "place_refused"
	// KindPoolTooSmall: a bucket's copies exceed the places in the pool
	// (OPS-2, #19), which re-placement cannot fix: the pool needs a new
	// friendship. The subject is the bucket.
	KindPoolTooSmall = "pool_too_small"
	// KindCopiesDropped: unsent copies of a bucket's objects were dropped
	// at a friend that is full or gone past replace_after, with no other
	// friend with room (state.DB.Reroute, #19): data held at fewer places
	// than copies until written again or re-placed (#26). The subject is
	// the bucket.
	KindCopiesDropped = "copies_dropped"
	// KindUploadUnmatchable: completed uploads of a bucket whose rows
	// cannot be matched to the object in the store (#62); each holds the
	// bucket's metadata, and so every friend's queue, until it settles
	// (#20). Subject: the bucket.
	KindUploadUnmatchable = "upload_unmatchable"
	// KindKeyChanged: keys of a bucket were written with other bytes than
	// a place acknowledged, and are not rewritable in its profile, so the
	// change is not replicated (EP-11, #22): held back at capture, or
	// refused by a lender (412). Subject: the bucket. It clears when each
	// such key is written back to the acknowledged bytes or deleted.
	KindKeyChanged = "key_changed"
	// KindFriendRemoved: a friend ended the friendship (its remove call,
	// PEER-5, #25): its copies of this node's data no longer count, and
	// the two replicas are purged at the end of the retention window.
	// Subject: the former friend. It clears at the purge, or when the
	// two become friends again.
	KindFriendRemoved = "friend_removed"
	// KindFriendRefuses: a friend keeps refusing this node as no friend
	// of it (its place `removed`, repeated over at least MinRefusing and
	// again within RefusedRecent): it has probably ended the friendship
	// without this node hearing (PEER-5, #83). Nothing ends by itself;
	// the message says to run `lendisk peer remove` if it did. Subject:
	// the friend. It clears when the friend answers otherwise, or the
	// refusals stop being repeated.
	KindFriendRefuses = "friend_refuses"
	// KindPurgeSoon: an ended friendship's replicas, the former friend's
	// here and this node's there, are purged within PurgeWarning (PEER-5,
	// #25). Subject: the former friend.
	KindPurgeSoon = "replica_purge_soon"
	// KindQuotaHigh: this node uses more than QuotaHighPercent of a
	// friendship's quota at the friend (OPS-2, #24). Subject: the friend.
	KindQuotaHigh = "quota_high"
	// KindQuotaExhausted: this node uses all of a friendship's quota at
	// the friend, or more after a lowering, or the friend's last answer
	// was a 507 (OPS-2, LEND-2, PEER-6, #24): no new copies go there and
	// nothing is deleted. Subject: the friend.
	KindQuotaExhausted = "quota_exhausted"
	// KindLentOverQuota: a friend stores more here than the friendship's
	// quota, after a lowering (PEER-6, #24): no new objects are accepted
	// from it and nothing is deleted. The friend gets quota_exhausted.
	// Subject: the friend.
	KindLentOverQuota = "lent_over_quota"
	// KindKopiaFormat1: a kopia bucket captured a blob only Kopia's
	// repository format version 1 writes (COMPAT-1, #79), which lendisk
	// does not support; writes are accepted and replicated as before.
	// Subject: the bucket. It clears when the bucket captures the blob
	// `kopia repository set-parameters --upgrade` writes, or is deleted.
	KindKopiaFormat1 = "kopia_format1"
	// KindBelowCopies: live data objects of a bucket have been below its
	// copies for longer than BelowAfter (replication.replace_after,
	// REPL-13's threshold), as the re-placement pass found them (OPS-2,
	// #26): re-placement had nowhere to put the missing copies, so the
	// pool needs a friend, more quota or overflow. Subject: the bucket.
	// It clears at the first pass that finds none below.
	KindBelowCopies = "below_copies"
	// KindReplicaUnverified: a friend failed a challenge of its replica
	// of a bucket (VER-1, OPS-2, #27): it does not hold bytes it
	// acknowledged. The replica is unverified and re-placement moves its
	// copies of the bucket to other friends. Subject: the friend's node
	// ID and the bucket, as <peer>/<bucket>. It clears when `lendisk
	// verify <bucket> --peer <id> --full` passes, or the friendship ends.
	KindReplicaUnverified = "replica_unverified"
	// KindReplicaUnchecked: a friend's replica of a bucket that holds
	// acknowledged objects has not passed a challenge for UncheckedAfter
	// (3 days): the friend answers every challenge with an error, or its
	// FIFO never empties (SEC-7, VER-1, #27 review). Nothing is drained;
	// it says the replica's state is unknown. Subject: <peer>/<bucket>.
	// It clears at the next pass.
	KindReplicaUnchecked = "replica_unchecked"
	// KindReconcileHeld: the store reconciler found more than its delete
	// limit's share of a bucket's objects missing from the store and
	// enqueued none of them as deletes (EP-14, #8): an emptied or
	// unmounted bucket directory would otherwise delete the backup at
	// every friend. Subject: the bucket. It clears at the first pass that
	// holds nothing back: the user allowed the deletes (`lendisk bucket
	// reconcile`) or restored the objects.
	KindReconcileHeld = "reconcile_held"
	// KindStoreMarkerMissing: the store marker the reconciler made is
	// gone, so the store may be unmounted or replaced, and nothing is
	// reconciled (EP-14, #8). No subject. It clears when a pass finds it
	// again.
	KindStoreMarkerMissing = "store_marker_missing"
	// KindBitrot: the bit-rot sampler found own objects of a bucket whose
	// bytes no longer hash to the version captured (VER-5, #31). Subject:
	// the bucket. It holds while one is not repaired (kept, as no friend
	// holds its version, or waiting for a friend that does to answer) and for
	// state.RotShown after the last repair, so a disk that is failing is
	// told even when every object came back.
	KindBitrot = "bitrot"
	// KindLentBitrot: the sampler found objects a friend stores here whose
	// bytes no longer hash to the hash they were stored with (VER-5,
	// LEND-4). Subject: the friend. This node cannot repair them; it holds
	// while it holds them.
	KindLentBitrot = "lent_bitrot"
	// KindRecoverySheetStale: the recovery sheet last printed no longer
	// matches the node's state (ID-7, #33): a friend's address changed,
	// or a friend, overflow target or bucket was added or removed. No
	// subject. It clears when `lendisk recovery-sheet` prints it again.
	KindRecoverySheetStale = "recovery_sheet_stale"
	// KindProtectable: the repository is above capacity.AlertPercent of
	// the protectable size (OPS-2, LEND-7, #37). No subject. The message
	// names, per friendship, the number that would cover it (LEND-8), or
	// says a friend or an overflow target is needed. A quota alert: it
	// clears QuotaHold after its condition ends.
	KindProtectable = "protectable_size"
)

// DefaultUncheckedAfter is Service.UncheckedAfter when zero.
const DefaultUncheckedAfter = 3 * 24 * time.Hour

// KopiaUpgrade is the command that upgrades a Kopia format-1 repository
// (COMPAT-1).
const KopiaUpgrade = "kopia repository set-parameters --upgrade"

// KopiaFormat1Message says what a bucket holding a Kopia format-1
// repository means, for the alert and status alike.
func KopiaFormat1Message(b state.Bucket) string {
	return fmt.Sprintf("bucket %s holds a Kopia repository in format version 1 (Kopia 0.8 or earlier; it wrote %s at %s), which lendisk does not support: its compaction and cleanup blobs reach only `copies` friends, not every friend. Writes are accepted and replicated as before. Run `%s` with Kopia 0.9 or later (every client of the repository must then be 0.9 or later)",
		b.Name, b.Format1Key, b.Format1At.UTC().Format(time.RFC3339), KopiaUpgrade)
}

// ReconcileHeldMessage says what deletes the store reconciler held back
// for a bucket (EP-14), for the alert and status alike.
func ReconcileHeldMessage(bucket string, r state.ReconcileState) string {
	return fmt.Sprintf("bucket %s: %d of the %d objects this node held are missing from the store (since %s); they are not deleted at any friend, since an emptied or unmounted directory looks the same. If you removed them, allow their deletes: lendisk bucket reconcile %s --allow-deletes %d; otherwise put them back, or read them through from friends",
		bucket, r.Held, r.HeldOf, r.HeldAt.UTC().Format(time.RFC3339), bucket, r.Held)
}

// BitrotMessage says what the sampler found rotten in an own bucket
// (VER-5), rs its records, for the alert and status alike.
func BitrotMessage(bucket string, rs []state.Rot) string {
	var fixed, waiting, kept int
	for _, r := range rs {
		switch {
		case !r.RepairedAt.IsZero():
			fixed++
		case r.Refetch:
			waiting++
		default:
			kept++
		}
	}
	first := rs[0]
	var parts []string
	if fixed > 0 {
		parts = append(parts, fmt.Sprintf("%d fetched again from friends (repaired)", fixed))
	}
	if waiting > 0 {
		parts = append(parts, fmt.Sprintf("%d waiting for a friend holding their version to answer (the rotten copy stays until one does)", waiting))
	}
	if kept > 0 {
		parts = append(parts, fmt.Sprintf("%d kept rotten, as no friend holds their version", kept))
	}
	return fmt.Sprintf("bucket %s: %s whose bytes no longer match the hash they were captured with (first %s: %s, now %s, found %s): %s. The store's disk is corrupting data: check it (smartctl -a, the kernel log)",
		bucket, countObjects(int64(len(rs))), first.Key, first.SHA256, first.Found, first.FoundAt.UTC().Format(time.RFC3339),
		strings.Join(parts, ", "))
}

// LentBitrotMessage says what the sampler found rotten of a friend's
// replicas here (VER-5, LEND-4), for the alert and status alike.
func LentBitrotMessage(friend string, rs []state.Rot) string {
	first := rs[0]
	return fmt.Sprintf("friend %s: %s it stores here no longer match the hash they were stored with (first %s in its bucket %s, found %s). This node cannot repair a friend's data and leaves it as it is; the friend's challenges find it and place those copies elsewhere. This node's disk is corrupting data: check it (smartctl -a, the kernel log)",
		friend, countObjects(int64(len(rs))), first.Key, first.Sealed, first.FoundAt.UTC().Format(time.RFC3339))
}

// QuotaHighPercent is the use of a quota above which KindQuotaHigh is
// raised (OPS-2).
const QuotaHighPercent = 90

// DefaultQuotaHold is how long a quota alert stays raised after its condition
// ends (#24 review): a friend can flip a quota alert's condition with
// every quota call, and each raise is a delivery, so a quota alert
// clears only once its condition has been false this long.
const DefaultQuotaHold = 10 * time.Minute

func (s *Service) quotaHold() time.Duration { return orDefault(s.QuotaHold, DefaultQuotaHold) }

// quotaKind reports whether kind is a quota alert (hysteresis, QuotaHold).
func quotaKind(kind string) bool {
	return kind == KindQuotaHigh || kind == KindQuotaExhausted || kind == KindLentOverQuota || kind == KindProtectable
}

// PurgeWarning is how long before an ended friendship's purge
// KindPurgeSoon is raised.
const PurgeWarning = 7 * 24 * time.Hour

// kinds are the alert kinds the evaluator owns.
var kinds = []string{KindFriendUnreachable, KindStoreUnreadable, KindObjectsNotReplicated, KindPlaceRefused,
	KindPoolTooSmall, KindCopiesDropped, KindUploadUnmatchable, KindKeyChanged, KindFriendRemoved, KindFriendRefuses, KindPurgeSoon,
	KindQuotaHigh, KindQuotaExhausted, KindLentOverQuota, KindKopiaFormat1, KindBelowCopies, KindReplicaUnverified,
	KindReplicaUnchecked, KindReconcileHeld, KindStoreMarkerMissing, KindBitrot, KindLentBitrot, KindRecoverySheetStale,
	KindProtectable}

// Message is the JSON object the hook receives on stdin and the webhook
// as its body (OPS-3). Fields are added, never renamed or removed;
// Schema changes only on a breaking change.
type Message struct {
	Schema   int    `json:"schema"`            // 1
	Event    string `json:"event"`             // raised
	Kind     string `json:"kind"`              // Kind…
	Subject  string `json:"subject,omitempty"` // node ID, bucket, or absent
	Message  string `json:"message"`           // for people
	RaisedAt string `json:"raised_at"`         // RFC 3339, UTC
	Node     string `json:"node"`              // this node's ID
}

// MessageSchema is Message's schema number.
const MessageSchema = 1

// Service evaluates and delivers alerts until Run's context ends.
type Service struct {
	DB  *state.DB
	Log *slog.Logger
	// Node is this node's ID, sent in each Message.
	Node string
	// Command is the hook (alerts.command): a program run without a
	// shell, the Message on stdin; exit status 0 is delivered. Empty
	// means none.
	Command string
	// Webhook is the URL each Message is POSTed to (alerts.webhook); a
	// 2xx answer is delivered. Empty means none.
	Webhook string
	// UnreachableAfter is how long a friend may go unanswered before
	// it is alerted (default 24h).
	UnreachableAfter time.Duration
	// MinRetrying is how long a place must have been retrying before it
	// counts as unreachable (default 1 hour). The reachability probe
	// (#65) keeps a running node's last answer from a friend within
	// replica.DefaultProbeEvery, but not across this node's own downtime:
	// a node off for longer than UnreachableAfter, meeting a one-minute
	// blip as it starts, would otherwise alert at its first pass. Kept
	// (#65 decision): an hour is four failed probes in a row.
	MinRetrying time.Duration
	// MinRefusing is how long a friend must have kept refusing this
	// node as no friend, from its first refusal to its last, before
	// KindFriendRefuses (default 1 hour): one stray refusal is not
	// enough. RefusedRecent is how recent the last refusal must be
	// (default 2 hours, twice the worker's hourly re-ask of a removed
	// place): a condition no longer being asked again does not count.
	MinRefusing   time.Duration
	RefusedRecent time.Duration
	// BelowAfter is how long a bucket's objects may stay below its
	// copies before KindBelowCopies is raised (default 7 days, REPL-13's
	// replication.replace_after).
	BelowAfter time.Duration
	// UncheckedAfter is how long a replica holding acknowledged objects
	// may go without passing a challenge before KindReplicaUnchecked
	// (default DefaultUncheckedAfter).
	UncheckedAfter time.Duration
	// Sheet, if set, compares the last printed recovery sheet with the
	// node's state (sheet.Stale, ID-7): when it was printed and what
	// changed since, nil when nothing did or none was printed.
	Sheet func(ctx context.Context) (printed time.Time, changes []string, err error)

	// Copies is replication.copies, the copies capacity planning counts
	// for a node with no bucket (default state.DefaultCopies).
	Copies int

	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// Interval is how often conditions are re-read (default 1 minute);
	// the first pass is one Interval after start, giving the replication
	// worker time to contact each friend again.
	Interval time.Duration
	// MinBackoff and MaxBackoff bound the wait after a failed delivery
	// (defaults 1 s and 5 minutes).
	MinBackoff, MaxBackoff time.Duration
	// Timeout bounds one delivery attempt (default 30 s).
	Timeout time.Duration
	// QuotaHold is how long a quota alert stays raised after its
	// condition ends (default DefaultQuotaHold).
	QuotaHold time.Duration
	// RunCommand runs the hook; nil means exec. Tests set it.
	RunCommand func(ctx context.Context, command string, stdin []byte) error
	// HTTP posts to the webhook; nil means a plain http.Client.
	HTTP *http.Client

	once           sync.Once
	wake, delivery chan struct{}
	// held is, per quota alert's key, when its condition last held
	// (QuotaHold); only evaluate touches it.
	held map[string]time.Time
}

func (s *Service) init() {
	s.once.Do(func() {
		s.wake = make(chan struct{}, 1)
		s.delivery = make(chan struct{}, 1)
	})
}

// Check asks for the conditions to be read again now, without waiting.
// A nil Service ignores it.
func (s *Service) Check() {
	if s == nil {
		return
	}
	s.init()
	poke(s.wake)
}

func poke(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// Run evaluates and delivers alerts until ctx ends.
func (s *Service) Run(ctx context.Context) {
	s.init()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.deliverLoop(ctx) }()
	t := time.NewTicker(orDefault(s.Interval, time.Minute))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-t.C:
		case <-s.wake:
		}
		if err := s.evaluate(ctx); err != nil && ctx.Err() == nil {
			s.Log.Error("alerts: reading conditions failed; trying again", "err", err)
		}
	}
}

// evaluate reads every condition and syncs the alert rows with them.
func (s *Service) evaluate(ctx context.Context) error {
	now := s.now()
	current, err := s.conditions(ctx, now)
	if err != nil {
		return err
	}
	if current, err = s.hold(ctx, current, now); err != nil {
		return err
	}
	raised, cleared, err := s.DB.SyncAlerts(ctx, kinds, current, now)
	if err != nil {
		return err
	}
	for _, a := range raised {
		s.Log.Warn("alert raised", "alert", true, "kind", a.Kind, "subject", a.Subject, "message", a.Message)
	}
	for _, a := range cleared {
		s.Log.Info("alert cleared", "kind", a.Kind, "subject", a.Subject)
	}
	if len(raised) > 0 && s.delivery != nil {
		poke(s.delivery)
	}
	return nil
}

// hold keeps each raised quota alert whose condition held within
// QuotaHold of now in current, as it stands, and notes when the quota
// alerts in current hold.
func (s *Service) hold(ctx context.Context, current []state.Alert, now time.Time) ([]state.Alert, error) {
	if s.held == nil {
		s.held = map[string]time.Time{}
	}
	in := map[string]bool{}
	for _, a := range current {
		in[a.Key] = true
		if quotaKind(a.Kind) {
			s.held[a.Key] = now
		}
	}
	active, err := s.DB.ActiveAlerts(ctx)
	if err != nil {
		return nil, err
	}
	// quota_high and quota_exhausted of one friend are one condition at
	// two levels: the current one replaces the other.
	other := map[string]string{KindQuotaHigh: KindQuotaExhausted, KindQuotaExhausted: KindQuotaHigh}
	for _, a := range active {
		t, ok := s.held[a.Key]
		if !ok || in[a.Key] || !quotaKind(a.Kind) || now.Sub(t) >= s.quotaHold() {
			continue
		}
		if o, paired := other[a.Kind]; paired && in[o+":"+a.Subject] {
			continue
		}
		current = append(current, a)
	}
	for k, t := range s.held {
		if now.Sub(t) >= s.quotaHold() {
			delete(s.held, k)
		}
	}
	return current, nil
}

func alertOf(kind, subject, message string) state.Alert {
	key := kind
	if subject != "" {
		key += ":" + subject
	}
	return state.Alert{Key: key, Kind: kind, Subject: subject, Message: message}
}

func objects(n int64) string {
	if n == 1 {
		return "1 object"
	}
	return fmt.Sprintf("%d objects", n)
}

// conditions are the alerts that hold at now.
func (s *Service) conditions(ctx context.Context, now time.Time) ([]state.Alert, error) {
	fs, err := s.DB.Friendships(ctx)
	if err != nil {
		return nil, err
	}
	conds, err := s.DB.PlaceConditions(ctx)
	if err != nil {
		return nil, err
	}
	buckets, err := s.DB.RefusedObjects(ctx)
	if err != nil {
		return nil, err
	}
	places, err := s.DB.RefusedAtPlaces(ctx)
	if err != nil {
		return nil, err
	}
	var out []state.Alert
	after := orDefault(s.UnreachableAfter, 24*time.Hour)
	minRetrying := orDefault(s.MinRetrying, time.Hour)
	for _, f := range fs {
		c := conds[f.Peer]
		// Retrying for at least MinRetrying, and silent for at least the
		// threshold counted from its last answer: a friend that went off
		// long before the next backup tried it has been gone since then.
		// One that never answered is counted from its first failure.
		if c.Condition != state.ConditionRetrying || now.Sub(c.Since) < minRetrying {
			continue
		}
		from := c.LastOK
		if from.IsZero() {
			from = c.Since
		}
		if now.Sub(from) < after {
			continue
		}
		msg := fmt.Sprintf("friend %s has not answered since %s (longer than %v); replication to it waits: %s",
			f.Peer, from.UTC().Format(time.RFC3339), after, c.Detail)
		out = append(out, alertOf(KindFriendUnreachable, f.Peer, msg))
	}
	minRefusing, recent := orDefault(s.MinRefusing, time.Hour), orDefault(s.RefusedRecent, 2*time.Hour)
	for _, f := range fs {
		c := conds[f.Peer]
		// Refused at more than one ask, over at least minRefusing, the
		// last one recent, all within this friendship (#83).
		if c.Condition != state.ConditionRemoved || c.RefusedAt.Sub(c.Since) < minRefusing ||
			now.Sub(c.RefusedAt) > recent || c.Since.Before(f.CreatedAt) {
			continue
		}
		msg := fmt.Sprintf("friend %s has refused this node as no friend of it since %s (last at %s): it has probably ended the friendship without this node hearing. Nothing new is placed there, and its copies of this node's data are being placed at other friends. If it ended the friendship, end it here too: lendisk peer remove %s; if not, ask its owner to check its lendisk",
			f.Peer, c.Since.UTC().Format(time.RFC3339), c.RefusedAt.UTC().Format(time.RFC3339), f.Peer)
		out = append(out, alertOf(KindFriendRefuses, f.Peer, msg))
	}
	var store []string
	for p, c := range conds {
		if c.Condition == state.ConditionStore {
			store = append(store, p)
		}
	}
	if len(store) > 0 {
		slices.Sort(store)
		msg := fmt.Sprintf("this node's store cannot be read; replication to %d places waits: %s",
			len(store), conds[store[0]].Detail)
		out = append(out, alertOf(KindStoreUnreadable, "", msg))
	}
	for _, b := range sortedKeys(buckets) {
		msg := fmt.Sprintf("bucket %s: %s not replicated: the store's copy is gone, changed or rotted since capture, or too large for one PUT (see the log); writing the object again heals it",
			b, objects(buckets[b]))
		out = append(out, alertOf(KindObjectsNotReplicated, b, msg))
	}
	for _, p := range sortedKeys(places) {
		msg := fmt.Sprintf("friend %s refused %s for good: it says it holds other bytes at the key (key_changed lists them), keeps refusing a rewrite of a rewritable key (a tomb of another version, or an older lendisk), or rejects bytes that match their capture hash (see the log)",
			p, objects(places[p]))
		out = append(out, alertOf(KindPlaceRefused, p, msg))
	}
	changed, err := s.DB.ChangedKeys(ctx)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(changed); {
		j := i
		for j < len(changed) && changed[j].Bucket == changed[i].Bucket {
			j++
		}
		c := changed[i]
		more := ""
		if n := j - i - 1; n > 0 {
			more = fmt.Sprintf(" and %s more", countKeys(int64(n)))
		}
		msg := fmt.Sprintf("bucket %s: key %s%s changed after a friend acknowledged it (acknowledged %s, now %s); the local write stands but is not replicated, so friends keep the old version (EP-11). Write the acknowledged bytes back, or, to keep the new ones, delete the key and write it again (friends keep the old version as a tombstone); `lendisk status` lists them",
			c.Bucket, c.Key, more, cmp.Or(c.Acknowledged, "unknown"), c.SHA256)
		if len(c.ReportedBy) > 0 {
			// The friend's word, never acknowledged here: it may be right
			// (a send whose answer was lost, another friend's version after
			// recover) or broken (#22 review).
			f := c.ReportedBy[0]
			msg = fmt.Sprintf("bucket %s: friend %s reports holding other bytes at key %s%s: %s (as reported by %s), where this node wrote %s; not replicated there (EP-11). If %s refuses every key, it is at fault (see place_refused for it); otherwise delete the key and write it again, which replaces its version (it keeps that as a tombstone); `lendisk status` lists them",
				c.Bucket, f, c.Key, more, cmp.Or(c.Places[f], "unknown"), f, c.SHA256, f)
		}
		out = append(out, alertOf(KindKeyChanged, c.Bucket, msg))
		i = j
	}
	bs, err := s.DB.Buckets(ctx)
	if err != nil {
		return nil, err
	}
	pool, err := s.DB.PoolSize(ctx)
	if err != nil {
		return nil, err
	}
	for _, b := range bs {
		if b.Format1() {
			out = append(out, alertOf(KindKopiaFormat1, b.Name, KopiaFormat1Message(b)))
		}
		if b.Copies <= pool {
			continue
		}
		msg := fmt.Sprintf("bucket %s keeps %d copies of each object, but the pool has only %s: objects are held at fewer places until another friend joins (lendisk invite)",
			b.Name, b.Copies, countPlaces(pool))
		out = append(out, alertOf(KindPoolTooSmall, b.Name, msg))
	}
	recs, err := s.DB.ReconcileStates(ctx)
	if err != nil {
		return nil, err
	}
	for _, b := range bs {
		if r := recs[b.Name]; r.Held > 0 {
			out = append(out, alertOf(KindReconcileHeld, b.Name, ReconcileHeldMessage(b.Name, r)))
		}
	}
	rots, err := s.DB.Rots(ctx)
	if err != nil {
		return nil, err
	}
	own, lent := map[string][]state.Rot{}, map[string][]state.Rot{}
	for _, r := range rots {
		switch {
		case r.Lent():
			lent[r.Borrower] = append(lent[r.Borrower], r)
		case r.RepairedAt.IsZero() || now.Sub(r.RepairedAt) <= state.RotShown:
			own[r.Bucket] = append(own[r.Bucket], r)
		}
	}
	for _, b := range slices.Sorted(maps.Keys(own)) {
		out = append(out, alertOf(KindBitrot, b, BitrotMessage(b, own[b])))
	}
	for _, f := range slices.Sorted(maps.Keys(lent)) {
		out = append(out, alertOf(KindLentBitrot, f, LentBitrotMessage(f, lent[f])))
	}
	if m, _, err := s.DB.GetNode(ctx, state.NodeMarkerMissing); err != nil {
		return nil, err
	} else if m != "" {
		msg := fmt.Sprintf("the store marker %s is missing, so the store may be unmounted or replaced; nothing written or removed outside the S3 endpoint is replicated until it is back (EP-14). Check that the store is mounted; if it is the right one, recreate the marker: touch %s",
			m, m)
		out = append(out, alertOf(KindStoreMarkerMissing, "", msg))
	}
	dropped, err := s.DB.DroppedBelowCopies(ctx)
	if err != nil {
		return nil, err
	}
	unmatched, err := s.DB.UnmatchableRows(ctx)
	if err != nil {
		return nil, err
	}
	for _, b := range slices.Sorted(maps.Keys(unmatched)) {
		u := unmatched[b]
		msg := fmt.Sprintf("bucket %s: %s completed by the backup tool cannot be matched to the store (%s: %s), so nothing after it replicates to any friend; restore the store's xattrs or rewrite the key",
			b, countObjects(u.Objects), u.Key, u.Detail)
		out = append(out, alertOf(KindUploadUnmatchable, b, msg))
	}
	belowSince, err := s.DB.BelowCopiesSince(ctx)
	if err != nil {
		return nil, err
	}
	belowAfter := orDefault(s.BelowAfter, 7*24*time.Hour)
	copiesOf := map[string]int{}
	for _, b := range bs {
		copiesOf[b.Name] = b.Copies
	}
	for _, b := range slices.Sorted(maps.Keys(belowSince)) {
		bc := belowSince[b]
		if now.Sub(bc.Since) < belowAfter {
			continue
		}
		msg := fmt.Sprintf("bucket %s has had objects at fewer than its %d copies since %s (longer than %v); now %s (%s): re-placement has nowhere to put the missing copies. Add a friend (lendisk invite), agree a higher quota (lendisk peer quota), or lower copies (lendisk bucket set %s --copies N)",
			b, copiesOf[b], bc.Since.UTC().Format(time.RFC3339), belowAfter, countObjects(bc.Objects), gb(bc.Bytes), b)
		out = append(out, alertOf(KindBelowCopies, b, msg))
	}
	for _, b := range slices.Sorted(maps.Keys(dropped)) {
		d := dropped[b]
		msg := fmt.Sprintf("bucket %s has %s (%d bytes) at fewer places than its copies: a friend that is full, no longer answers or was removed was to hold them, and no other friend has room; add room or a friend (lendisk invite)",
			b, countObjects(d.Objects), d.Bytes)
		out = append(out, alertOf(KindCopiesDropped, b, msg))
	}
	checks, err := s.DB.ReplicaChecks(ctx)
	if err != nil {
		return nil, err
	}
	unchecked := orDefault(s.UncheckedAfter, DefaultUncheckedAfter)
	for _, c := range checks {
		if from := c.LastPassOrHeld(); !c.Unverified() && !c.HeldSince.IsZero() && now.Sub(from) >= unchecked {
			why := "it has not been challenged since (its copies kept being sent, or it was unreachable)"
			if c.Error != "" {
				why = "the last challenge got no answer: " + c.Error
			}
			msg := fmt.Sprintf("friend %s's replica of bucket %s has not passed a challenge since %s (longer than %v): %s. Nothing is moved; whether it still holds the data is unknown. `lendisk verify %s --peer %s` checks it now",
				c.Place, c.Bucket, from.UTC().Format(time.RFC3339), unchecked, why, c.Bucket, c.Place)
			out = append(out, alertOf(KindReplicaUnchecked, c.Place+"/"+c.Bucket, msg))
		}
		if !c.Unverified() {
			continue
		}
		msg := fmt.Sprintf("friend %s failed the verification of its replica of bucket %s at %s: %s. The replica is unverified, and its copies of %s are being placed at other friends (REPL-13); `lendisk verify %s --peer %s --full` passing marks it verified again",
			c.Place, c.Bucket, c.UnverifiedAt.UTC().Format(time.RFC3339), c.Detail, c.Bucket, c.Bucket, c.Place)
		out = append(out, alertOf(KindReplicaUnverified, c.Place+"/"+c.Bucket, msg))
	}
	quota, err := s.quotaConditions(ctx, fs, conds)
	if err != nil {
		return nil, err
	}
	out = append(out, quota...)
	plan, err := capacity.Read(ctx, s.DB, cmp.Or(s.Copies, state.DefaultCopies))
	if err != nil {
		return nil, err
	}
	if plan.Alerting() {
		out = append(out, alertOf(KindProtectable, "", ProtectableMessage(plan)))
	}
	if s.Sheet != nil {
		printed, changes, err := s.Sheet(ctx)
		if err != nil {
			return nil, err
		}
		if len(changes) > 0 {
			msg := fmt.Sprintf("the recovery sheet printed at %s is out of date: %s. Print it again with `lendisk recovery-sheet` and replace the old copy",
				printed.UTC().Format(time.RFC3339), strings.Join(changes, ", "))
			out = append(out, alertOf(KindRecoverySheetStale, "", msg))
		}
	}
	formers, err := s.DB.FormerFriends(ctx)
	if err != nil {
		return nil, err
	}
	for _, f := range formers {
		purge := f.PurgeAfter.UTC().Format(time.RFC3339)
		if f.ByPeer {
			msg := fmt.Sprintf("friend %s ended the friendship on %s: its copies of this node's data no longer count (status shows what is below copies), and it no longer reaches this node. What it stored here is kept until %s, then deleted, and it deletes what it holds of this node's then",
				f.Peer, f.RemovedAt.UTC().Format(time.RFC3339), purge)
			out = append(out, alertOf(KindFriendRemoved, f.Peer, msg))
		}
		if !now.Before(f.PurgeAfter.Add(-PurgeWarning)) {
			msg := fmt.Sprintf("the ended friendship with %s is purged at %s: what it stored here is deleted, and it deletes what it holds of this node's. To keep both, become friends again before then (lendisk invite)",
				f.Peer, purge)
			out = append(out, alertOf(KindPurgeSoon, f.Peer, msg))
		}
	}
	return out, nil
}

// quotaConditions are the quota alerts of friendships fs (OPS-2,
// PEER-6, #24): this node's use at each friend (its count, reconciled
// from the friend's own in the quota call) against the quota, and what
// each friend stores here against it.
func (s *Service) quotaConditions(ctx context.Context, fs []state.Friendship, conds map[string]state.PlaceCondition) ([]state.Alert, error) {
	use, err := s.DB.PlaceUse(ctx)
	if err != nil {
		return nil, err
	}
	var out []state.Alert
	for _, f := range fs {
		q, u := f.Quota(), max(use[f.Peer], 0)
		numbers := fmt.Sprintf("the smaller of this node's number %s and theirs %s", gb(f.OwnNumber), gb(f.PeerNumber))
		switch {
		case conds[f.Peer].Condition == state.ConditionQuota || q > 0 && u >= q:
			msg := fmt.Sprintf("this node stores %s at friend %s, which has no room left for it: the quota is %s, %s. No new copies go there and nothing is deleted; copies go to other friends where there is room. Agree a higher number with them (lendisk peer quota on both sides)",
				gb(u), f.Peer, gb(q), numbers)
			out = append(out, alertOf(KindQuotaExhausted, f.Peer, msg))
		case q > 0 && u*100 > q*QuotaHighPercent:
			msg := fmt.Sprintf("this node uses %d%% of its quota at friend %s: %s of %s, %s. At the quota no new copies go there; agree a higher number with them (lendisk peer quota on both sides)",
				u*100/q, f.Peer, gb(u), gb(q), numbers)
			out = append(out, alertOf(KindQuotaHigh, f.Peer, msg))
		}
		lent, _, err := s.DB.LentUsage(ctx, f.Peer)
		if err != nil {
			return nil, err
		}
		if lent > q {
			msg := fmt.Sprintf("friend %s stores %s here, more than the friendship's quota of %s (%s): no new objects are accepted from it and nothing is deleted; its node places them elsewhere",
				f.Peer, gb(lent), gb(q), numbers)
			out = append(out, alertOf(KindLentOverQuota, f.Peer, msg))
		}
	}
	return out, nil
}

// ProtectableMessage says that the repository of plan is above
// capacity.AlertPercent of its protectable size and names, per
// friendship, the number that would cover it (OPS-2, LEND-8, #37).
func ProtectableMessage(plan capacity.Plan) string {
	what := fmt.Sprintf("the repository (%s as friends count it) is above %d%% of the protectable size (%s at %d copies)",
		capacity.Human(plan.Size()), capacity.AlertPercent, capacity.Human(plan.Protectable()), plan.Copies)
	if plan.Protectable() < plan.Size() {
		what += ": copies beyond it are paused for quota and some objects are below copies"
	}
	cover, ok := plan.Cover()
	if !ok {
		return what + fmt.Sprintf(". With %s at %d copies no quota covers it: add another friend or an overflow target",
			plural(int64(len(plan.Friends)), "friend", "friends"), plan.Copies)
	}
	return what + ". To cover it, agree these quotas, each side running lendisk peer quota <friend> <size>: " + CoverList(plan, cover)
}

// CoverList lists cover's numbers in plan's friendship order, as
// "friend B: 68M (now 50M)".
func CoverList(plan capacity.Plan, cover map[string]int64) string {
	var parts []string
	for _, f := range plan.Friends {
		if n, ok := cover[f.Peer]; ok {
			parts = append(parts, fmt.Sprintf("friend %s: %s (now %s)", f.Peer, capacity.Format(n), capacity.Format(f.Quota)))
		}
	}
	return strings.Join(parts, "; ")
}

// gb writes bytes for a person: GiB with one decimal, or bytes below one
// MiB.
func gb(n int64) string {
	if n < 1<<20 {
		return fmt.Sprintf("%d bytes", n)
	}
	return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
}

func plural(n int64, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

func countObjects(n int64) string {
	if n == 1 {
		return "1 object"
	}
	return fmt.Sprintf("%d objects", n)
}

func countKeys(n int64) string {
	if n == 1 {
		return "1 key"
	}
	return fmt.Sprintf("%d keys", n)
}

func countPlaces(n int) string {
	if n == 1 {
		return "1 place"
	}
	return fmt.Sprintf("%d places", n)
}

func sortedKeys(m map[string]int64) []string {
	var out []string
	for k, n := range m {
		if n > 0 {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// deliverLoop delivers every undelivered raise, backing off while any
// delivery fails; a new raise wakes it.
func (s *Service) deliverLoop(ctx context.Context) {
	minB, maxB := orDefault(s.MinBackoff, time.Second), orDefault(s.MaxBackoff, 5*time.Minute)
	backoff := minB
	for {
		var retry <-chan time.Time
		var timer *time.Timer
		if s.deliverPending(ctx, backoff) {
			backoff = minB
		} else {
			timer = time.NewTimer(backoff)
			retry = timer.C
			backoff = min(2*backoff, maxB)
		}
		select {
		case <-ctx.Done():
		case <-s.delivery:
		case <-retry:
		}
		if timer != nil {
			timer.Stop()
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// deliverPending tries each undelivered channel of each active alert
// once; false means something failed and is retried after backoff.
func (s *Service) deliverPending(ctx context.Context, backoff time.Duration) bool {
	if s.Command == "" && s.Webhook == "" {
		return true
	}
	as, err := s.DB.ActiveAlerts(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.Log.Error("alerts: reading pending alerts failed; retrying", "err", err, "retry_in", backoff)
		}
		return false
	}
	ok := true
	for _, a := range as {
		body, err := json.Marshal(Message{Schema: MessageSchema, Event: "raised", Kind: a.Kind, Subject: a.Subject,
			Message: a.Message, RaisedAt: a.RaisedAt.UTC().Format(time.RFC3339), Node: s.Node})
		if err != nil {
			s.Log.Error("alerts: encoding failed", "kind", a.Kind, "err", err)
			continue
		}
		for _, ch := range []struct {
			name, target string
			done         time.Time
			send         func(context.Context, []byte) error
		}{
			{state.ChannelHook, s.Command, a.HookAt, s.runHook},
			{state.ChannelWebhook, s.Webhook, a.WebhookAt, s.postWebhook},
		} {
			if ch.target == "" || !ch.done.IsZero() {
				continue
			}
			actx, cancel := context.WithTimeout(ctx, orDefault(s.Timeout, 30*time.Second))
			err := ch.send(actx, body)
			cancel()
			if ctx.Err() != nil {
				return false
			}
			if err != nil {
				ok = false
				s.Log.Warn("alert delivery failed; retrying", "channel", ch.name, "to", s.target(ch.name),
					"kind", a.Kind, "subject", a.Subject, "err", err, "retry_in", backoff)
				continue
			}
			if err := s.DB.MarkAlertDelivered(ctx, a, ch.name, s.now()); err != nil {
				s.Log.Error("alerts: recording a delivery failed", "kind", a.Kind, "err", err)
				ok = false
				continue
			}
			s.Log.Info("alert delivered", "channel", ch.name, "to", s.target(ch.name), "kind", a.Kind, "subject", a.Subject)
		}
	}
	return ok
}

// target names a channel's destination for the log, the webhook
// without its secret.
func (s *Service) target(channel string) string {
	if channel == state.ChannelHook {
		return s.Command
	}
	return Redact(s.Webhook)
}

func (s *Service) runHook(ctx context.Context, body []byte) error {
	run := s.RunCommand
	if run == nil {
		run = RunCommand
	}
	err := run(ctx, s.Command, body)
	if errors.Is(err, ErrLeftChildren) {
		s.Log.Warn("alert hook left processes running after it exited 0; delivered, and they were killed",
			"command", s.Command)
		return nil
	}
	return err
}

// CheckCommand checks alerts.command: the absolute path of one
// executable regular file. It is run with no arguments and no shell, so
// a command line such as "ntfy.sh --topic x" is refused rather than
// failing at the first alert.
//
// A path holding spaces is accepted when the whole string names such a
// file. Otherwise, when its first word does, the rest are arguments,
// which a small script must carry.
func CheckCommand(command string) error {
	const rule = "alerts.command must be the path to one program, with no arguments; put arguments in a small script"
	if !filepath.IsAbs(command) {
		return fmt.Errorf("%s (alerts.command = %q is not an absolute path)", rule, command)
	}
	fi, err := os.Stat(command)
	if err != nil {
		if first := strings.Fields(command)[0]; first != command && executable(first) {
			return fmt.Errorf("%s (alerts.command = %q holds arguments)", rule, command)
		}
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("alerts.command: %s does not exist: create it, or clear alerts.command", command)
		}
		return fmt.Errorf("%s: %w", rule, err)
	}
	switch {
	case !fi.Mode().IsRegular():
		return fmt.Errorf("%s (%s is not a regular file)", rule, command)
	case fi.Mode().Perm()&0o111 == 0:
		return fmt.Errorf("%s (%s is not executable: chmod +x it)", rule, command)
	}
	return nil
}

func executable(path string) bool {
	fi, err := os.Stat(path)
	// Access checks execute permission for the daemon's own user, not
	// just that some execute bit is set.
	return err == nil && fi.Mode().IsRegular() && syscall.Access(path, 0x1 /* X_OK */) == nil
}

// waitDelay is how long RunCommand waits for the hook's output pipes
// to close once it exited; tests shorten it.
var waitDelay = 5 * time.Second

// ErrLeftChildren: the hook exited 0 but left a process holding its
// output past waitDelay. The alert was delivered; the process group was
// killed.
var ErrLeftChildren = errors.New("the hook exited 0 but left processes holding its output; they were killed")

// RunCommand runs command without a shell, stdin on its standard input,
// in a process group of its own. When ctx ends, and again once it
// returns, the whole group is killed, so nothing the hook started
// outlives it (except a child that left the group with setsid). A
// failure carries the start of what it printed; ErrLeftChildren is a
// success.
func RunCommand(ctx context.Context, command string, stdin []byte) error {
	cmd := exec.CommandContext(ctx, command)
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = waitDelay // a child that keeps the pipes open
	out, err := cmd.CombinedOutput()
	if cmd.Process != nil {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) // ESRCH when nothing is left
	}
	if errors.Is(err, exec.ErrWaitDelay) {
		// Wait reports it only after an exit status of 0.
		return ErrLeftChildren
	}
	if err != nil {
		if o := strings.TrimSpace(string(out)); o != "" {
			if len(o) > 512 {
				o = o[:512] + "…"
			}
			return fmt.Errorf("%w: %s", err, o)
		}
		return err
	}
	return nil
}

func (s *Service) postWebhook(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Webhook, bytes.NewReader(body))
	if err != nil {
		return errors.New("alerts.webhook is not a valid URL")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "lendisk/"+version.Version)
	// No redirects: a 3xx is a failure, and a followed redirect would put
	// the Location, maybe a secret too, in the client's errors.
	client := *cmp.Or(s.HTTP, http.DefaultClient)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		// url.Error repeats the whole URL, secret and all.
		var ue *url.Error
		if errors.As(err, &ue) {
			return fmt.Errorf("POST %s: %w", Redact(s.Webhook), ue.Err)
		}
		return fmt.Errorf("POST %s failed", Redact(s.Webhook))
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("POST %s: %s", Redact(s.Webhook), resp.Status)
	}
	return nil
}

// Redact is a URL for the log: its scheme and host only, since its path
// or query may be a secret (a healthchecks.io UUID, an ntfy topic).
func Redact(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "(the configured webhook)"
	}
	r := u.Scheme + "://" + u.Host
	if u.Path != "" && u.Path != "/" || u.RawQuery != "" {
		r += "/…"
	}
	return r
}
