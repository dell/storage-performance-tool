package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/tui"
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
	// Do not let a developer's cli/.env select real remote hosts. Keep a hostile
	// HOSTS value in the process environment to prove that the explicit local
	// flag wins and that this test cannot cross the remote launch boundary.
	t.Chdir(t.TempDir())
	clearEnvDefaultsTestEnv(t)
	t.Setenv("HOSTS", "test-not-connect.invalid")
	setGlobalRunFlagForTest(t, "test-hosts", "127.0.0.1")

	// Ensure we don't accidentally start any interactive UI in tests.
	prevTesting := isTesting
	isTesting = true
	t.Cleanup(func() { isTesting = prevTesting })

	// Swap the resolver with our fake abort and restore afterward.
	prevResolver := resolvePortConflictFunc
	resolverCalls := 0
	resolvePortConflictFunc = func(ctx context.Context, port string, forceMode bool) (*portcheck.ResolutionResult, error) {
		resolverCalls++
		return fakeAbortResolve(ctx, port, forceMode)
	}
	t.Cleanup(func() { resolvePortConflictFunc = prevResolver })

	prevConnect := connectMultiHostOrchestratorFunc
	remoteOrchestratorCalls := 0
	errRemoteOrchestratorReached := errors.New("remote orchestrator must not be reached")
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
		remoteOrchestratorCalls++
		return errRemoteOrchestratorReached
	}
	t.Cleanup(func() { connectMultiHostOrchestratorFunc = prevConnect })

	// Capture output.
	buf := new(bytes.Buffer)
	rootCmd.SetOut(buf)
	rootCmd.SetErr(buf)
	rootCmd.SetArgs([]string{"run", "mock", "--headless"})
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	})

	_, err := rootCmd.ExecuteC()
	if err == nil || err.Error() != "operation cancelled by user" {
		t.Fatalf("ExecuteC() error = %v, want operation cancelled by user", err)
	}
	if errors.Is(err, errRemoteOrchestratorReached) {
		t.Fatalf("ExecuteC() reached remote orchestrator: %v", err)
	}
	if resolverCalls != 1 {
		t.Fatalf("port conflict resolver calls = %d, want 1", resolverCalls)
	}
	if remoteOrchestratorCalls != 0 {
		t.Fatalf("remote orchestrator calls = %d, want 0", remoteOrchestratorCalls)
	}

	out := buf.String()
	if !strings.Contains(out, "Operation cancelled") && !strings.Contains(out, "cancelled") {
		t.Errorf("expected abort message in output; got: %s", out)
	}
	if strings.Contains(out, "Usage:") {
		t.Errorf("unexpected usage/help output after abort: %s", out)
	}
}
