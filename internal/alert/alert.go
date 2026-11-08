// Package alert keeps the node's pending alerts and delivers them
// (OPS-2, OPS-3, DESIGN §3.8).
//
// An alert is a condition read from lendisk.db: a friend unreachable
// past alerts.unreachable_after, this node's store unreadable, objects
// the store could not supply (VER-5), objects a place refused (LEND-4),
// keys changed after a friend acknowledged them (EP-11), a friendship
// the friend ended and the purge of an ended one (PEER-5).
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
	// KindPurgeSoon: an ended friendship's replicas, the former friend's
	// here and this node's there, are purged within PurgeWarning (PEER-5,
	// #25). Subject: the former friend.
	KindPurgeSoon = "replica_purge_soon"
)

// PurgeWarning is how long before an ended friendship's purge
// KindPurgeSoon is raised.
const PurgeWarning = 7 * 24 * time.Hour

// kinds are the alert kinds the evaluator owns.
var kinds = []string{KindFriendUnreachable, KindStoreUnreadable, KindObjectsNotReplicated, KindPlaceRefused,
	KindPoolTooSmall, KindCopiesDropped, KindUploadUnmatchable, KindKeyChanged, KindFriendRemoved, KindPurgeSoon}

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
	// counts as unreachable (default 1 hour): reachability is learned
	// only from replication, so without it a nightly backup meeting a
	// one-minute blip 24h after the last answer would alert at once.
	// The reachability probe (#65) may let this be revisited.
	MinRetrying time.Duration

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
	// RunCommand runs the hook; nil means exec. Tests set it.
	RunCommand func(ctx context.Context, command string, stdin []byte) error
	// HTTP posts to the webhook; nil means a plain http.Client.
	HTTP *http.Client

	once           sync.Once
	wake, delivery chan struct{}
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
		if b.Copies <= pool {
			continue
		}
		msg := fmt.Sprintf("bucket %s keeps %d copies of each object, but the pool has only %s: objects are held at fewer places until another friend joins (lendisk invite)",
			b.Name, b.Copies, countPlaces(pool))
		out = append(out, alertOf(KindPoolTooSmall, b.Name, msg))
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
	for _, b := range slices.Sorted(maps.Keys(dropped)) {
		d := dropped[b]
		msg := fmt.Sprintf("bucket %s has %s (%d bytes) at fewer places than its copies: a friend that is full, no longer answers or was removed was to hold them, and no other friend has room; add room or a friend (lendisk invite)",
			b, countObjects(d.Objects), d.Bytes)
		out = append(out, alertOf(KindCopiesDropped, b, msg))
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
