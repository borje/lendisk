//go:build portmapper

package main

// tailcat does not link Tailscale's port mapper (Q-R). Building with
// -tags portmapper links it the way tsnet does, which sets
// portmappertype.HookNewPortMapper so magicsock creates a UPnP/NAT-PMP/PCP
// client for every tailcat engine in the process.
import _ "tailscale.com/feature/portmapper"
