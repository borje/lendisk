package cli

import (
	"bufio"
	"math/rand/v2"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/borje/lendisk/internal/identity"
	"github.com/borje/lendisk/internal/manifest"
	"github.com/borje/lendisk/internal/replica"
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
	// milliseconds and give up on an absent friend in seconds, and the
	// manifest pusher make a round every 200 ms and back off for at most
	// a second (its daily re-push stays daily).
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
)

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
		a.tuneManifest = func(p *manifest.Pusher) {
			if os.Getenv(envReplFast) == "1" {
				p.Poll, p.Timeout = 200*time.Millisecond, 10*time.Second
				p.MinBackoff, p.MaxBackoff = 200*time.Millisecond, time.Second
			}
			if d, err := time.ParseDuration(os.Getenv(envManifestPoll)); err == nil {
				p.Poll = d
			}
		}
		os.Exit(a.run(os.Args[1:]))
	}
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
