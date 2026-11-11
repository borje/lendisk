//go:build race

package state

import "time"

// passLimit and passGrowth bound one re-routing pass in tests: no limit
// in time, as the race detector slows SQLite several times over and a
// loaded CI runner several times again (5.5 s there for 3000 rows);
// the pass must instead grow no worse than linearly with the rows.
const (
	passLimit  = 0
	passGrowth = 8 // linear work grows 4 times for 4 times the rows
)

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

// foldHold and foldWait bound the full-fold test: a loaded CI runner
// under the race detector stretched one step past the budget to a
// 530 ms batch (7974d73), so the bound is the settle test's second.
const (
	foldHold = time.Second
	foldWait = 1500 * time.Millisecond
)
