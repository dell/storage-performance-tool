package cmd

import (
	"testing"

	"github.com/spf13/cobra"
)

// newResultsFlagsTestCmd creates a minimal Cobra command with only the
// results-related flags needed for Phase 1 tests.
func newResultsFlagsTestCmd() *cobra.Command {
	c := &cobra.Command{Use: "run"}
	c.Flags().Bool("auto-results", true, "")
	c.Flags().String("results-dir", "./results", "")
	c.Flags().String("label", "", "")
	return c
}

func TestBuildResultsOptions_Defaults(t *testing.T) {
	cmd := newResultsFlagsTestCmd()
	// Do not set any flags; expect defaults

	got := buildResultsOptions(cmd)

	if !got.AutoResults {
		t.Fatalf("AutoResults default = %v, want true", got.AutoResults)
	}
	if got.ResultsDir != "./results" {
		t.Fatalf("ResultsDir default = %q, want %q", got.ResultsDir, "./results")
	}
	if got.Label != "mt" {
		t.Fatalf("Label default (sanitized) = %q, want %q", got.Label, "mt")
	}
}

func TestBuildResultsOptions_Overrides(t *testing.T) {
	cmd := newResultsFlagsTestCmd()
	cmd.Flags().Set("auto-results", "false")
	cmd.Flags().Set("results-dir", "./out")
	cmd.Flags().Set("label", "run42")

	got := buildResultsOptions(cmd)

	if got.AutoResults {
		t.Fatalf("AutoResults override = %v, want false", got.AutoResults)
	}
	if got.ResultsDir != "./out" {
		t.Fatalf("ResultsDir override = %q, want %q", got.ResultsDir, "./out")
	}
	if got.Label != "run42" {
		t.Fatalf("Label override (sanitized) = %q, want %q", got.Label, "run42")
	}
}

func TestSanitizeLabel_InvalidCharsAndLength(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"run 42!*", "run_42__"},
		{"with/slash\\and:colon", "with_slash_and_colon"},
		{"Upper.Case-OK_123", "Upper.Case-OK_123"},
		{"", "mt"},
	}

	for _, tc := range cases {
		got := sanitizeLabel(tc.in)
		if got != tc.want {
			t.Errorf("sanitizeLabel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	// Long label should be truncated to 64 chars
	long := "a_______________________________________________________________extra" // >64
	got := sanitizeLabel(long)
	if len(got) != 64 {
		t.Fatalf("sanitizeLabel length = %d, want 64 (got %q)", len(got), got)
	}
}
