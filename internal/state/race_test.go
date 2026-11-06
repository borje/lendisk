//go:build race

package state

import "time"

// passLimit bounds one re-routing pass in tests: the race detector
// slows SQLite several times over.
const passLimit = 5 * time.Second
