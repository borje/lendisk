//go:build !race

package state

import "time"

// passLimit bounds one re-routing pass in tests.
const passLimit = 2 * time.Second
