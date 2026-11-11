//go:build !race

package state

import "time"

// passLimit bounds one re-routing pass in tests; passGrowth, its
// growth with the rows, is checked only under the race detector.
const (
	passLimit  = 2 * time.Second
	passGrowth = 0
)

// settleKeys and settleWait size the large-bucket settling test: a
// 2,000-key batch takes about 0.06 s.
const (
	settleKeys = 200_000
	settleWait = time.Second
)

// offsiteObjects, offsiteLimit and offsiteFactor size and bound the
// large-bucket watermark test.
const (
	offsiteObjects = 20_000
	offsiteLimit   = 2 * time.Second
	offsiteFactor  = 8 // it measures under 3 with or without -race
)

// foldHold and foldWait bound the full-fold test: how long one batch
// may hold the write lock (a batch ends after the step that passes
// SettleBudget), and how long a write beside it may wait.
const (
	foldHold = 500 * time.Millisecond
	foldWait = 750 * time.Millisecond
)
