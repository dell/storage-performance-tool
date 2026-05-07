package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
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

func TestPrepareTraceOptions_ExplicitPathPrecedence(t *testing.T) {
	tmpDir := t.TempDir()
	explicit := filepath.Join(tmpDir, "explicit.log")
	got, err := prepareTraceOptions(explicit, true, true, filepath.Join(tmpDir, "results-root"), "20260506.204108.064", nil)
	if err != nil {
		t.Fatalf("prepareTraceOptions returned error: %v", err)
	}
	if got.Path != explicit {
		t.Fatalf("Path = %q, want %q", got.Path, explicit)
	}
	if !got.Append {
		t.Fatalf("Append = false, want true")
	}
	if got.Auto {
		t.Fatalf("Auto = true, want false")
	}
}

func TestPrepareTraceOptions_AutoDefaultsFreshFile(t *testing.T) {
	tmpDir := t.TempDir()
	resultsRoot := filepath.Join(tmpDir, "results", "mt-20260506.204108.064")
	runToken := "20260506.204108.064"
	got, err := prepareTraceOptions("", true, true, resultsRoot, runToken, nil)
	if err != nil {
		t.Fatalf("prepareTraceOptions returned error: %v", err)
	}
	wantPath := filepath.Join(resultsRoot, "spt-"+runToken+".trace.log")
	if got.Path != wantPath {
		t.Fatalf("Path = %q, want %q", got.Path, wantPath)
	}
	if got.Append {
		t.Fatalf("Append = true, want false for auto trace")
	}
	if !got.Auto {
		t.Fatalf("Auto = false, want true")
	}
}

func TestPrepareTraceOptions_AutoFallbackToCWD(t *testing.T) {
	tmpDir := t.TempDir()
	resultsRoot := filepath.Join(tmpDir, "as-file")
	if err := os.WriteFile(resultsRoot, []byte("file-blocking-dir-create"), 0o600); err != nil {
		t.Fatalf("write sentinel file: %v", err)
	}
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir tempdir: %v", err)
	}
	defer func() { _ = os.Chdir(oldWD) }()

	var sb strings.Builder
	runToken := "20260506.204108.064"
	got, err := prepareTraceOptions("", false, true, resultsRoot, runToken, &sb)
	if err != nil {
		t.Fatalf("prepareTraceOptions returned error: %v", err)
	}
	wantFallback := filepath.Join(".", "spt-"+runToken+".trace.log")
	if got.Path != wantFallback {
		t.Fatalf("Path = %q, want %q", got.Path, wantFallback)
	}
	if got.Append {
		t.Fatalf("Append = true, want false for fallback auto trace")
	}
	if !got.Auto {
		t.Fatalf("Auto = false, want true")
	}
	if !strings.Contains(sb.String(), "falling back") {
		t.Fatalf("expected fallback warning, got %q", sb.String())
	}
}

func TestAppendTraceToResultsManifest_AddsRunFileAndCopies(t *testing.T) {
	tmpDir := t.TempDir()
	resultsRoot := filepath.Join(tmpDir, "results", "mt-20260506.204108.064")
	if err := os.MkdirAll(resultsRoot, 0o755); err != nil {
		t.Fatalf("mkdir results root: %v", err)
	}

	manifest := &results.Manifest{
		BaseURL:   "http://example",
		OutputDir: resultsRoot,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(resultsRoot, constants.ResultsManifestFileName), data, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	traceSrc := filepath.Join(tmpDir, "trace.log")
	traceContent := "hello trace\n"
	if err := os.WriteFile(traceSrc, []byte(traceContent), 0o600); err != nil {
		t.Fatalf("write trace source: %v", err)
	}

	if err := appendTraceToResultsManifest(resultsRoot, traceSrc); err != nil {
		t.Fatalf("appendTraceToResultsManifest error: %v", err)
	}

	traceDest := filepath.Join(resultsRoot, filepath.Base(traceSrc))
	gotTrace, err := os.ReadFile(traceDest)
	if err != nil {
		t.Fatalf("read copied trace: %v", err)
	}
	if string(gotTrace) != traceContent {
		t.Fatalf("copied trace content = %q, want %q", string(gotTrace), traceContent)
	}

	updatedBytes, err := os.ReadFile(filepath.Join(resultsRoot, constants.ResultsManifestFileName))
	if err != nil {
		t.Fatalf("read updated manifest: %v", err)
	}
	var updated results.Manifest
	if err := json.Unmarshal(updatedBytes, &updated); err != nil {
		t.Fatalf("unmarshal updated manifest: %v", err)
	}
	if len(updated.RunFiles) != 1 {
		t.Fatalf("RunFiles length = %d, want 1", len(updated.RunFiles))
	}
	if updated.RunFiles[0].Name != filepath.Base(traceSrc) {
		t.Fatalf("RunFiles[0].Name = %q, want %q", updated.RunFiles[0].Name, filepath.Base(traceSrc))
	}
	if updated.RunFiles[0].Status != "ok" {
		t.Fatalf("RunFiles[0].Status = %q, want ok", updated.RunFiles[0].Status)
	}
}

func TestAppendTraceToResultsManifest_NoManifestNoError(t *testing.T) {
	tmpDir := t.TempDir()
	traceSrc := filepath.Join(tmpDir, "trace.log")
	if err := os.WriteFile(traceSrc, []byte("x"), 0o600); err != nil {
		t.Fatalf("write trace source: %v", err)
	}
	if err := appendTraceToResultsManifest(filepath.Join(tmpDir, "missing-root"), traceSrc); err != nil {
		t.Fatalf("appendTraceToResultsManifest should ignore missing manifest, got: %v", err)
	}
}
