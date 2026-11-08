//go:build !race

package state

import "time"

// passLimit bounds one re-routing pass in tests.
const passLimit = 2 * time.Second

// settleKeys and settleWait size the large-bucket settling test: a
// 2,000-key batch takes about 0.06 s.
const (
	settleKeys = 200_000
	settleWait = time.Second
)

// offsiteObjects sizes the large-bucket watermark test.
const offsiteObjects = 20_000
