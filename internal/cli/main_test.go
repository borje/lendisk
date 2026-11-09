package cli

import (
	"bufio"
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/alert"
	"github.com/borje/lendisk/internal/control"
	"github.com/borje/lendisk/internal/gateway"
	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/quota"
	"github.com/borje/lendisk/internal/recovery"
	"github.com/borje/lendisk/internal/replica"
	"github.com/borje/lendisk/internal/seed"
	"github.com/borje/lendisk/internal/verify"
)

const (
	// envExec makes the test binary run the lendisk command line instead
	// of the tests, so a test can run the daemon as a real process it can
	// kill.
	envExec = "LENDISK_TEST_EXEC"
	// envCrashAfterStore is the probability that such a daemon SIGKILLs
	// itself after the store accepted a captured request and before its
	// queue row is completed: the window startup settles (EP-3).
	envCrashAfterStore = "LENDISK_TEST_CRASH_AFTER_STORE"
	// envReplFast=1 makes the replication worker poll and retry in
	// milliseconds and give up on an absent friend in seconds (the
	// read-through's fetches too), and the
	// manifest pusher and the recovery lister make a round every 200 ms
	// and back off for at most a second (the daily re-push stays daily).
	envReplFast = "LENDISK_TEST_REPL_FAST"
	// envManifestPoll is the manifest pusher's time between rounds (a Go
	// duration), overriding envReplFast's.
	envManifestPoll = "LENDISK_TEST_MANIFEST_POLL"
	// envSendDelay is a pause after each read of an object being sent to
	// a friend (a Go duration), so a test can act mid-transfer.
	envSendDelay = "LENDISK_TEST_SEND_DELAY"
	// envCrashMidSend is the probability, per read of an object being
	// sent, that the daemon SIGKILLs itself mid-transfer.
	envCrashMidSend = "LENDISK_TEST_CRASH_MID_SEND"
	// envListDelay is a pause before the daemon answers each page of a
	// friend's list (a Go duration): a friend slow to list.
	envListDelay = "LENDISK_TEST_LIST_DELAY"
	// envClock names a file holding a Go duration the daemon's clock runs
	// ahead of real time by (offsetClock): a test moves the daemon days
	// ahead by rewriting it.
	envClock = "LENDISK_TEST_CLOCK"
	// envAlertInterval is the alert service's time between passes (a Go
	// duration).
	envAlertInterval = "LENDISK_TEST_ALERT_INTERVAL"
	// envRerouteEvery is the replication worker's time between re-routing
	// passes, on its clock (a Go duration).
	envRerouteEvery = "LENDISK_TEST_REROUTE_EVERY"
	// envHashDelay is a pause before a completed multipart upload, which
	// the client has been answered for, is hashed (a Go duration): its
	// queue row stays pending meanwhile.
	envHashDelay = "LENDISK_TEST_HASH_DELAY"
	// envPauseRetry is the replication worker's PauseRetry (a Go
	// duration), overriding envReplFast's: how long a place paused for
	// quota waits unless the quota rises.
	envPauseRetry = "LENDISK_TEST_PAUSE_RETRY"
	// envQuotaHold is the alert service's QuotaHold (a Go duration),
	// read with envAlertInterval.
	envQuotaHold = "LENDISK_TEST_QUOTA_HOLD"
	// envFarewellInterval is the farewell's time between passes (a Go
	// duration).
	envFarewellInterval = "LENDISK_TEST_FAREWELL_INTERVAL"
	// envCrashAtByte makes the daemon SIGKILL itself once a body it sends
	// a friend has reached this byte of its object (#23).
	envCrashAtByte = "LENDISK_TEST_CRASH_AT_BYTE"
	// envReloadEvery is how often the daemon looks at its config file (a
	// Go duration, #23).
	envReloadEvery = "LENDISK_TEST_RELOAD_EVERY"
	// envBitrotEvery is the bit-rot sampler's time between steps (a Go
	// duration, #31).
	envBitrotEvery = "LENDISK_TEST_BITROT_EVERY"
)

// offsetClock is real time plus the duration in the file at path, read
// at every call (0 while the file is missing or empty): an injected
// clock a test can move for a daemon in another process.
func offsetClock(path string) func() time.Time {
	return func() time.Time {
		b, _ := os.ReadFile(path)
		d, _ := time.ParseDuration(strings.TrimSpace(string(b)))
		return time.Now().Add(d)
	}
}

// slowList is a lender's index that answers each page after a delay.
type slowList struct {
	control.LentLister
	delay time.Duration
}

func (s slowList) List(ctx context.Context, borrower identity.NodeID, sealed, after string, limit int) ([]gateway.LentObject, string, error) {
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-time.After(s.delay):
	}
	return s.LentLister.List(ctx, borrower, sealed, after, limit)
}

func TestMain(m *testing.M) {
	if os.Getenv(envExec) == "1" {
		a := &app{in: bufio.NewReader(os.Stdin), out: os.Stdout, errOut: os.Stderr, pick: randomPositions, newPhrase: identity.NewPhrase}
		if p, err := strconv.ParseFloat(os.Getenv(envCrashAfterStore), 64); err == nil && p > 0 {
			a.afterStore = func() {
				if rand.Float64() < p {
					syscall.Kill(os.Getpid(), syscall.SIGKILL)
					select {} // never complete the row
				}
			}
		}
		a.tuneReplication = tuneFromEnv
		if d, err := time.ParseDuration(os.Getenv(envHashDelay)); err == nil {
			a.beforeHash = func() { time.Sleep(d) }
		}
		if f := os.Getenv(envClock); f != "" {
			a.clock = offsetClock(f)
		}
		if d, err := time.ParseDuration(os.Getenv(envAlertInterval)); err == nil {
			a.tuneAlerts = func(s *alert.Service) {
				s.Interval = d
				if h, err := time.ParseDuration(os.Getenv(envQuotaHold)); err == nil {
					s.QuotaHold = h
				}
			}
		}
		if d, err := time.ParseDuration(os.Getenv(envReloadEvery)); err == nil {
			a.reloadEvery = d
		}
		if d, err := time.ParseDuration(os.Getenv(envBitrotEvery)); err == nil {
			a.tuneSample = func(s *gateway.SampleSettings) { s.Every = d }
		}
		if d, err := time.ParseDuration(os.Getenv(envFarewellInterval)); err == nil {
			a.tuneEnding = func(f *farewell) { f.Interval = d }
		}
		a.tuneManifest = func(p *manifest.Pusher) {
			if os.Getenv(envReplFast) == "1" {
				p.Poll, p.Timeout = 200*time.Millisecond, 10*time.Second
				p.MinBackoff, p.MaxBackoff = 200*time.Millisecond, time.Second
			}
			if d, err := time.ParseDuration(os.Getenv(envManifestPoll)); err == nil {
				p.Poll = d
			}
		}
		a.tuneControl = func(s *control.Server) {
			if d, err := time.ParseDuration(os.Getenv(envListDelay)); err == nil {
				s.Lent = slowList{LentLister: s.Lent, delay: d}
			}
			if os.Getenv(envReplFast) == "1" {
				s.QuotaEvery = 200 * time.Millisecond
			}
		}
		a.tuneFetch = func(f *replica.Fetcher) {
			if os.Getenv(envReplFast) == "1" {
				f.Client = replica.Options{DialTimeout: 5 * time.Second, StallTimeout: 10 * time.Second}
			}
		}
		a.tuneQuota = func(x *quota.Exchanger) {
			if os.Getenv(envReplFast) == "1" {
				x.Poll, x.Timeout = 200*time.Millisecond, 10*time.Second
				x.MinBackoff, x.MaxBackoff = 200*time.Millisecond, time.Second
			}
		}
		a.tuneVerify = func(v *verify.Verifier) {
			if os.Getenv(envReplFast) == "1" {
				v.Poll = 200 * time.Millisecond
			}
		}
		a.tuneSeed = func(c *seed.Confirmer) {
			if os.Getenv(envReplFast) == "1" {
				c.Poll, c.Timeout, c.Quiet = 200*time.Millisecond, 10*time.Second, 2*time.Second
			}
		}
		a.tuneRecovery = func(l *recovery.Lister) {
			if os.Getenv(envReplFast) == "1" {
				l.Poll, l.Timeout = 200*time.Millisecond, 10*time.Second
				l.MinBackoff, l.MaxBackoff = 200*time.Millisecond, time.Second
			}
		}
		os.Exit(a.run(os.Args[1:]))
	}
	raiseParallel()
	os.Exit(m.Run())
}

// fastReplication are the replication timings for tests.
func fastReplication(w *replica.Worker) {
	w.Poll, w.MinBackoff, w.MaxBackoff, w.PauseRetry = 50*time.Millisecond, 100*time.Millisecond, time.Second, 2*time.Second
	w.Client = replica.Options{DialTimeout: 5 * time.Second, StallTimeout: 10 * time.Second}
}

// tuneFromEnv applies envReplFast, envSendDelay and envCrashMidSend.
func tuneFromEnv(w *replica.Worker) {
	if os.Getenv(envReplFast) == "1" {
		fastReplication(w)
	}
	if d, err := time.ParseDuration(os.Getenv(envRerouteEvery)); err == nil {
		w.RerouteEvery = d
	}
	if d, err := time.ParseDuration(os.Getenv(envPauseRetry)); err == nil {
		w.PauseRetry = d
	}
	if at, err := strconv.ParseInt(os.Getenv(envCrashAtByte), 10, 64); err == nil {
		w.OnRead = func(_ string, _, read int64) {
			if read >= at {
				syscall.Kill(os.Getpid(), syscall.SIGKILL)
				select {} // never record the part
			}
		}
		return
	}
	delay, _ := time.ParseDuration(os.Getenv(envSendDelay))
	p, _ := strconv.ParseFloat(os.Getenv(envCrashMidSend), 64)
	if delay <= 0 && p <= 0 {
		return
	}
	w.OnRead = func(_ string, _, read int64) {
		time.Sleep(delay)
		if p > 0 && read > 0 && rand.Float64() < p {
			syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {} // never acknowledge
		}
	}
}
