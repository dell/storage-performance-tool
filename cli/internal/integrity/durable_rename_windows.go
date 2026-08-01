//go:build windows

package integrity

import "fmt"

// Verification commands reject Windows before creating run evidence because the CLI cannot
// currently establish the required crash-durable parent-directory barrier there.
func renameNoReplace(_, _ string) error {
	return fmt.Errorf(
		"crash-durable integrity publication is unsupported on Windows",
	)
}
