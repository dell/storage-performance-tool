// Package buildinfo exposes build-time metadata injected via ldflags.
package buildinfo //nolint:revive // 'buildinfo' does not shadow a top-level stdlib package

// Version, Commit, and BuildDate are injected at build time via ldflags.
// Defaults keep local builds informative without extra wiring.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// Summary returns a human-readable sentence summarizing build metadata.
func Summary() string {
	return "SPT " + Version + " (commit " + Commit + ", built " + BuildDate + ")"
}
