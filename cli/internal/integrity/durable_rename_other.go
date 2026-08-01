//go:build !linux && !windows

package integrity

import (
	"fmt"
	"runtime"
)

// Verification commands reject non-Linux platforms before creating run evidence because the CLI
// cannot currently establish the complete crash-durable publication contract there.
func renameNoReplace(_, _ string) error {
	return fmt.Errorf("crash-durable integrity publication is unsupported on %s", runtime.GOOS)
}
