//go:build race

package state

import "time"

// passLimit bounds one re-routing pass in tests: the race detector
// slows SQLite several times over.
const passLimit = 5 * time.Second

// settleKeys and settleWait size the large-bucket settling test: the
// race detector makes SQLite about twenty times slower, so it settles a
// tenth of the keys; a batch still ends after SettleBudget, so the wait
// allowed is the same.
const (
	settleKeys = 20_000
	settleWait = time.Second
)

// offsiteObjects, offsiteLimit and offsiteFactor size and bound the
// large-bucket watermark test: a fifth of the objects, as the race
// detector makes its query about forty times slower, and no limit in
// time, as a loaded CI runner is several times slower again (9.6 s
// there for 1.6 s here); the yardstick bounds it instead.
const (
	offsiteObjects = 4_000
	offsiteLimit   = 0
	offsiteFactor  = 8 // it measures under 3 with or without -race
)
