package constants

import "strings"

const (
	// DeleteIdentityModeSingle identifies one target per logical DELETE request.
	DeleteIdentityModeSingle = "single"
	// DeleteIdentityModeBatch identifies multiple targets per logical DELETE request.
	DeleteIdentityModeBatch = "batch"
	// DeleteSelectionOrderCanonical names the deterministic global DELETE selection order.
	DeleteSelectionOrderCanonical = "canonical"
	// DeleteFailurePolicyModeFixed selects a fixed failed-object threshold.
	DeleteFailurePolicyModeFixed = "fixed"
	// DeleteFailurePolicyModePercentage selects a cumulative object-outcome percentage.
	DeleteFailurePolicyModePercentage = "percentage"
	// StepOpCleanup is the operation word that ends the step identity of a seeded DELETE's cleanup.
	StepOpCleanup = "cleanup"
)

// IsSeededDeleteCleanupStepID reports whether stepID identifies the cleanup phase of a seeded
// DELETE scenario.
func IsSeededDeleteCleanupStepID(stepID string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(stepID)), "-"+StepOpCleanup)
}
