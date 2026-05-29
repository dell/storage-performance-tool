// Package buildinfo exposes build-time metadata injected via ldflags.
package buildinfo //nolint:revive // 'buildinfo' does not shadow a top-level stdlib package

import (
	"regexp"
	"strings"
)

// Version, Commit, and BuildDate are injected at build time via ldflags.
// Defaults keep local builds informative without extra wiring.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// releaseVersionRE matches a clean semver core (major.minor.patch), optionally
// followed by a standard pre-release suffix (e.g. "5.10.0-rc.1"). It is used by
// IsRelease together with an explicit dev-marker check.
var releaseVersionRE = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+`)

// IsRelease reports whether this binary is an official release build (a clean
// semver version injected at release time) rather than a local/dev build.
//
// Local builds are dev-marked by the Makefile (e.g. "5.10.3-dev+abc1234") so they
// are never mistaken for a release; the bare default is "dev". Anything carrying
// the "-dev" marker, the "dev" default, or a non-semver string is treated as a dev
// build. Pre-release tags such as "5.10.0-rc.1" are treated as releases (the
// publish workflow pushes a matching v-prefixed image tag for them).
func IsRelease() bool {
	if Version == "" || Version == "dev" {
		return false
	}
	if strings.Contains(Version, "-dev") {
		return false
	}
	return releaseVersionRE.MatchString(Version)
}

// Summary returns a human-readable sentence summarizing build metadata.
func Summary() string {
	return "SPT " + Version + " (commit " + Commit + ", built " + BuildDate + ")"
}
