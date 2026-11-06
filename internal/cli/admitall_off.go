//go:build !admitall

package cli

// admitAll replaces tunnel admission with admit-everyone in the
// `admitall` test build only (DESIGN §3.9, SEC-11). It is false in every
// real build.
const admitAll = false
