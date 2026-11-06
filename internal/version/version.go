// Package version holds the build's version, set at link time with
// -ldflags "-X github.com/borje/lendisk/internal/version.Version=v0.1.0",
// and the versions of the modules linked into it (EP-12).
package version

import "runtime/debug"

// Version is the release, or "dev" for a local build.
var Version = "dev"

// Module is the version of module path linked into this binary, as
// go.sum pins it (its replacement's, if replaced), or "unknown".
func Module(path string) string {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == path {
				if d.Replace != nil {
					return d.Replace.Version
				}
				return d.Version
			}
		}
	}
	return "unknown"
}
