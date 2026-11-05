// Package version holds the build's version, set at link time with
// -ldflags "-X github.com/borje/lendisk/internal/version.Version=v0.1.0".
package version

// Version is the release, or "dev" for a local build.
var Version = "dev"
