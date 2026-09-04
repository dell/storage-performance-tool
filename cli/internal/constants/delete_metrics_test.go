package constants

import "testing"

func TestDeleteMetricIdentityAndPolicyConstantsMatchSchemaV4(t *testing.T) {
	tests := map[string]string{
		"identity single":   DeleteIdentityModeSingle,
		"identity batch":    DeleteIdentityModeBatch,
		"selection order":   DeleteSelectionOrderCanonical,
		"fixed policy":      DeleteFailurePolicyModeFixed,
		"percentage policy": DeleteFailurePolicyModePercentage,
	}
	want := map[string]string{
		"identity single":   "single",
		"identity batch":    "batch",
		"selection order":   "canonical",
		"fixed policy":      "fixed",
		"percentage policy": "percentage",
	}
	for name, value := range tests {
		if value != want[name] {
			t.Fatalf("%s = %q, want %q", name, value, want[name])
		}
	}
}
