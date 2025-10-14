package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
)

// fakeAbortResolve simulates a detected port conflict where the user chooses Abort.
func fakeAbortResolve(ctx context.Context, port string, forceMode bool) (*portcheck.ResolutionResult, error) {
	return &portcheck.ResolutionResult{
		Strategy:   portcheck.StrategyAbort,
		Success:    false,
		Message:    "Operation cancelled by user",
		RetryAfter: 0,
	}, nil
}

func TestAbortDoesNotPrintUsage(t *testing.T) {
	// Ensure we don't accidentally start any interactive UI in tests
	prevTesting := isTesting
	isTesting = true
	defer func() { isTesting = prevTesting }()

	// Swap the resolver with our fake abort and restore afterward
	prevResolver := resolvePortConflictFunc
	resolvePortConflictFunc = fakeAbortResolve
	defer func() { resolvePortConflictFunc = prevResolver }()

	// Capture output
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs([]string{"run", "mock", "--headless"})

	// Execute and expect an error due to abort
	_, err := rootCmd.ExecuteC()
	if err == nil {
		t.Fatalf("expected error from abort resolution, got nil")
	}

	out := buf.String()

	// Ensure helpful abort messaging is present
	if !strings.Contains(out, "Operation cancelled") && !strings.Contains(out, "cancelled") {
		t.Errorf("expected abort message in output; got: %s", out)
	}

	// Critically: ensure Cobra usage/help is NOT printed
	if strings.Contains(out, "Usage:") {
		t.Errorf("unexpected usage/help output after abort: %s", out)
	}

	// Minor timeout to allow any deferred cleanup to complete
	time.Sleep(50 * time.Millisecond)
}
