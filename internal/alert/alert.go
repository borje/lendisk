// Package alert keeps the node's pending alerts and delivers them
// (OPS-2, OPS-3, DESIGN §3.8).
//
// An alert is a condition read from lendisk.db: a friend unreachable
// past alerts.unreachable_after, this node's store unreadable, objects
// the store could not supply (VER-5), objects a place refused (EP-11).
// The Service re-reads them every Interval, and at once after Check;
// state.SyncAlerts keeps one row per condition, raised once while it
// holds and cleared when it ends. Each raise is delivered once to the
// command hook (Message as JSON on stdin) and once to the webhook (the
// same JSON POSTed), retried with backoff until delivered or cleared.
//
// Rules a caller must keep:
//   - A new alert is a condition the evaluator reads from state, with a
//     Kind of its own added to kinds; code that detects it records the
//     condition in lendisk.db and calls Check.
//   - Check never blocks, and nothing the Service does waits on the
//     caller: replication and the gateway only write state.
//   - The webhook URL may hold a secret: log it only through redact.
package alert

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	// KindPlaceRefused: a place refused objects for good: it holds the
	// key with another hash (EP-11) or rejects bytes that match their
	// capture hash (#57); the subject is the place.
	KindPlaceRefused = "place_refused"
)

// kinds are the alert kinds the evaluator owns.
var kinds = []string{KindFriendUnreachable, KindStoreUnreadable, KindObjectsNotReplicated, KindPlaceRefused}

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
	for _, f := range fs {
		c := conds[f.Peer]
		if c.Condition != state.ConditionRetrying {
			continue
		}
		// Counted from its last answer: a friend that went off long
		// before the next backup tried it has been gone since then. One
		// that never answered is counted from its first failure.
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
		msg := fmt.Sprintf("friend %s refused %s for good: it holds the key with another hash, or rejects bytes that match their capture hash (see the log)",
			p, objects(places[p]))
		out = append(out, alertOf(KindPlaceRefused, p, msg))
	}
	return out, nil
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
	if s.RunCommand != nil {
		return s.RunCommand(ctx, s.Command, body)
	}
	return RunCommand(ctx, s.Command, body)
}

// CheckCommand checks alerts.command: the absolute path of one
// executable regular file. It is run with no arguments and no shell, so
// a command line such as "ntfy.sh --topic x" is refused rather than
// failing at the first alert.
func CheckCommand(command string) error {
	const rule = "alerts.command must be the path to one program, with no arguments; put arguments in a small script"
	if strings.ContainsAny(command, " \t\n") {
		return fmt.Errorf("%s (alerts.command = %q holds spaces)", rule, command)
	}
	if !filepath.IsAbs(command) {
		return fmt.Errorf("%s (alerts.command = %q is not an absolute path)", rule, command)
	}
	fi, err := os.Stat(command)
	switch {
	case err != nil:
		return fmt.Errorf("%s: %w", rule, err)
	case !fi.Mode().IsRegular():
		return fmt.Errorf("%s (%s is not a regular file)", rule, command)
	case fi.Mode().Perm()&0o111 == 0:
		return fmt.Errorf("%s (%s is not executable: chmod +x it)", rule, command)
	}
	return nil
}

// RunCommand runs command without a shell, stdin on its standard input,
// in a process group of its own: when ctx ends, the whole group is
// killed, so nothing the hook started outlives it. A failure carries the
// start of what it printed.
func RunCommand(ctx context.Context, command string, stdin []byte) error {
	cmd := exec.CommandContext(ctx, command)
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second // a child that keeps the pipes open
	out, err := cmd.CombinedOutput()
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
