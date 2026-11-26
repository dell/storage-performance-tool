package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/spf13/cobra"
)

// fakeAbortResolve simulates a detected port conflict where the user chooses Abort.
func fakeAbortResolve(_ context.Context, _ string, _ portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
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
	isolateLoggingGlobals(t)
	debug = false
	logLevel = "info"
	logAppend = false
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
	resolvePortConflictFunc = func(ctx context.Context, port string, opts portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
		resolverCalls++
		return fakeAbortResolve(ctx, port, opts)
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

// An abort nobody chose reports what holds the port and what to do, not a
// cancellation by the user.
func TestCheckPortConflictsReportsUnattendedAbort(t *testing.T) {
	const message = "port 9999 is held by an SPT engine (run 41, state RUNNING); it was left running."
	prevResolver := resolvePortConflictFunc
	resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{
			Strategy: portcheck.StrategyAbort,
			Message:  message,
			Error:    portcheck.ErrPortConflictUnattended,
		}, nil
	}
	t.Cleanup(func() { resolvePortConflictFunc = prevResolver })

	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Bool("force", false, "")
	cmd.Flags().Int(flagAPIPort, 9999, "")
	err := checkPortConflicts(cmd, true)
	if err == nil || err.Error() != message {
		t.Fatalf("checkPortConflicts() error = %v, want %q", err, message)
	}
}

// The resolver budgets each phase: an overall deadline here would count the time
// an operator spends at the prompt against the engine's shutdown.
func TestCheckPortConflictsSetsNoOverallDeadline(t *testing.T) {
	prevResolver := resolvePortConflictFunc
	resolvePortConflictFunc = func(ctx context.Context, _ string, _ portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
		if deadline, ok := ctx.Deadline(); ok {
			t.Errorf("resolver context has deadline %v", deadline)
		}
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	t.Cleanup(func() { resolvePortConflictFunc = prevResolver })

	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Bool("force", false, "")
	cmd.Flags().Int(flagAPIPort, 9999, "")
	if err := checkPortConflicts(cmd, true); err != nil {
		t.Fatalf("checkPortConflicts() error = %v", err)
	}
}

// An interrupted recheck at the conflict prompt says it was interrupted, not that
// the port is still in use: the probe that was cut short showed nothing.
func TestCheckPortConflictsReportsAnInterruptedRetry(t *testing.T) {
	const message = "Interrupted while rechecking port 23999: context canceled"
	prevResolver := resolvePortConflictFunc
	resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{
			Strategy: portcheck.StrategyRetry,
			Message:  message,
			Error:    context.Canceled,
		}, nil
	}
	t.Cleanup(func() { resolvePortConflictFunc = prevResolver })

	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Bool("force", false, "")
	cmd.Flags().Int(flagAPIPort, 23999, "")
	err := checkPortConflicts(cmd, true)
	if err == nil || err.Error() != message {
		t.Fatalf("checkPortConflicts() error = %v, want %q", err, message)
	}
}

// Sol re-review 5 Low 8: Ctrl-C at the conflict prompt, or during detection,
// returns the cancellation with the resolver's interruption, which is what the
// command reports, not a generic failure to resolve the conflict.
func TestCheckPortConflictsReportsAnInterruptionAtThePrompt(t *testing.T) {
	const message = "Interrupted while waiting for an answer about port 23999: context canceled"
	prevResolver := resolvePortConflictFunc
	resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{
			Strategy: portcheck.StrategyRetry,
			Message:  message,
			Error:    context.Canceled,
		}, context.Canceled
	}
	t.Cleanup(func() { resolvePortConflictFunc = prevResolver })

	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().Bool("force", false, "")
	cmd.Flags().Int(flagAPIPort, 23999, "")
	err := checkPortConflicts(cmd, true)
	if err == nil || err.Error() != message {
		t.Fatalf("checkPortConflicts() error = %v, want %q", err, message)
	}
}
