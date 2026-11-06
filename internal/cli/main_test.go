package cli

import (
	"bufio"
	"math/rand/v2"
	"os"
	"strconv"
	"syscall"
	"testing"

	"github.com/borje/lendisk/internal/identity"
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
		os.Exit(a.run(os.Args[1:]))
	}
	os.Exit(m.Run())
}
