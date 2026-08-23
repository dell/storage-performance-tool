package constants

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
)
