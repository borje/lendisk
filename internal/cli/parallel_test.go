package cli

import (
	"flag"
	"sync"
	"testing"
)

// How the tests share one test process (#82). Every test runs in
// parallel with the others, holding what it needs for its whole run:
//
//   - serving: versitygw runs once per process, so a test that serves
//     in this process (serve, serveLogged, or `serve` through run)
//     holds the gateway, and those tests run one at a time;
//   - daemons: a race-built daemon process is heavy on a small machine,
//     so a test holds a slot for each daemon process it runs at once.
//     LENDISK_TEST_DAEMONS sets the number of slots (default 8).
//
// A test waiting for the gateway or a slot holds one of go test's
// -parallel places, so TestMain raises -parallel's default
// (parallelDefault) and these two bound the work instead. A test that
// needs a quiet process stays serial, saying why: go test runs the
// serial tests before the parallel ones. Two read the process's own
// sockets; TestEndingAFriendshipTellsTheFriendAndLeavesTheOthers loses
// the remove call's answer under load.

var (
	gatewayHeld sync.Mutex
	daemonSlots = newSlots(envInt("LENDISK_TEST_DAEMONS", 8))
)

// parallelDefault is -parallel unless it is given: more than the tests
// that can wait for the gateway at once.
const parallelDefault = "64"

// raiseParallel sets -parallel to parallelDefault unless the command
// line set it. TestMain calls it before m.Run.
func raiseParallel() {
	flag.Parse()
	set := false
	flag.Visit(func(f *flag.Flag) { set = set || f.Name == "test.parallel" })
	if !set {
		flag.Set("test.parallel", parallelDefault)
	}
}

// parallel marks t parallel and holds daemons process slots until it
// ends.
func parallel(t *testing.T, daemons int) {
	t.Helper()
	t.Parallel()
	daemonSlots.take(t, daemons)
}

// serving marks t parallel and holds the gateway, then daemons process
// slots, until it ends.
func serving(t *testing.T, daemons int) {
	t.Helper()
	t.Parallel()
	gatewayHeld.Lock()
	t.Cleanup(gatewayHeld.Unlock)
	daemonSlots.take(t, daemons)
}

type slots struct {
	mu   sync.Mutex
	cond *sync.Cond
	max  int
	free int
}

func newSlots(n int) *slots {
	s := &slots{max: n, free: n}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// take holds n slots until t ends; more than there are means all.
func (s *slots) take(t *testing.T, n int) {
	n = min(n, s.max)
	if n <= 0 {
		return
	}
	s.mu.Lock()
	for s.free < n {
		s.cond.Wait()
	}
	s.free -= n
	s.mu.Unlock()
	t.Cleanup(func() {
		s.mu.Lock()
		s.free += n
		s.mu.Unlock()
		s.cond.Broadcast()
	})
}
