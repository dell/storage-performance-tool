package cmd

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestAutoResultsSetupFailureIsArtifactOutcome(t *testing.T) {
	originalTracker := newRunTrackerFunc
	originalDiscover := discoverStepIDsFunc
	originalFleetDiscover := discoverFleetStepIDsFunc
	originalFetcher := newResultsFetcherFunc
	originalSummary := generateRunSummaryFunc
	originalMkdir := makeResultsDirFunc
	originalArchive := archivePreparedRunInputsFunc
	t.Cleanup(func() {
		newRunTrackerFunc = originalTracker
		discoverStepIDsFunc = originalDiscover
		discoverFleetStepIDsFunc = originalFleetDiscover
		newResultsFetcherFunc = originalFetcher
		generateRunSummaryFunc = originalSummary
		makeResultsDirFunc = originalMkdir
		archivePreparedRunInputsFunc = originalArchive
	})

	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &fakeRunTracker{result: &portcheck.RunResult{FinalState: constants.StateCompleted}}
	}
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return []string{"mt-001-create"}, nil
	}
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return nil, nil
	}
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{output: output, manifest: &results.Manifest{}}
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer) error { return nil }

	for _, test := range []struct {
		name  string
		setup func(error)
	}{
		{
			name: "result root",
			setup: func(failure error) {
				makeResultsDirFunc = func(string, os.FileMode) error { return failure }
				archivePreparedRunInputsFunc = originalArchive
			},
		},
		{
			name: "prepared archive",
			setup: func(failure error) {
				makeResultsDirFunc = originalMkdir
				archivePreparedRunInputsFunc = func(*runMetadata, string, string) error {
					return failure
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			failure := errors.New(test.name + " failed")
			test.setup(failure)
			resultsRoot := t.TempDir()
			metadata := &runMetadata{ResultsRoot: resultsRoot}
			monitor := startAutoResultsMonitor(
				context.Background(), "http://example", "mt", resultsRoot,
				[]string{"mt-001-create"}, 42, false, nil, "9999", false, 0,
				"", metadata, io.Discard, io.Discard, "", nil)
			monitor.Arm()
			outcome := <-monitor.done
			if !errors.Is(outcome.ArtifactErr, failure) {
				t.Fatalf("ArtifactErr = %v, want %v", outcome.ArtifactErr, failure)
			}
			if !errors.Is(outcome.Lifecycle.Artifacts.Err, failure) {
				t.Fatalf("artifact lifecycle = %+v, want %v", outcome.Lifecycle.Artifacts, failure)
			}
			if metadata.Lifecycle == nil ||
				!strings.Contains(metadata.Lifecycle.Artifacts.Error, failure.Error()) {
				t.Fatalf("metadata artifact lifecycle = %+v, want %v", metadata.Lifecycle, failure)
			}
			storedBytes, readErr := os.ReadFile(
				filepath.Join(resultsRoot, constants.ResultsMetadataFileName))
			if readErr != nil {
				t.Fatalf("read durable lifecycle metadata: %v", readErr)
			}
			if !strings.Contains(string(storedBytes), failure.Error()) {
				t.Fatalf("durable lifecycle metadata omitted artifact failure: %s", storedBytes)
			}
			runErr := resolveVerificationRunError(
				nil, outcome, true,
				scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify, RunID: 42})
			if runErr == nil || !strings.Contains(runErr.Error(), failure.Error()) {
				t.Fatalf("verification result = %v, want setup failure", runErr)
			}
		})
	}
}

func TestResolveRunCompletionErrorFailsDeleteOnMissingTerminalMetrics(t *testing.T) {
	failure := errors.New("authoritative DELETE detail is missing")
	outcome := autoResultsOutcome{DeleteMetricsErr: failure}
	err := resolveRunCompletionError(
		nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeDelete})
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "terminal DELETE metrics") {
		t.Fatalf("DELETE completion error = %v, want terminal metrics failure", err)
	}

	nonDelete := resolveRunCompletionError(
		nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeRead})
	if nonDelete != nil {
		t.Fatalf("non-DELETE result inherited DELETE metrics failure: %v", nonDelete)
	}
}

func TestResolveRunCompletionErrorFailsDeleteOnRejectedTerminalOutcome(t *testing.T) {
	failure := terminalDeleteOutcomeError(map[string]*deletemetrics.Metrics{
		"mt-002-delete": {
			FailurePolicy: deletemetrics.FailurePolicy{Outcome: deletemetrics.OutcomeFailed},
		},
	})
	outcome := autoResultsOutcome{
		Tracker:           &portcheck.RunResult{FinalState: constants.StateCompleted},
		DeleteTerminalErr: failure,
	}
	err := resolveRunCompletionError(
		nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeDelete})
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "mt-002-delete") {
		t.Fatalf("DELETE rejected outcome error = %v, want terminal policy failure", err)
	}
}

func TestResolveRunCompletionErrorReportsVerificationFailureAsTerminalVerdict(t *testing.T) {
	metrics := &deletemetrics.Metrics{
		FailurePolicy: deletemetrics.FailurePolicy{Outcome: deletemetrics.OutcomeFailed},
		Verification: deletemetrics.Verification{
			AcceptedPresent: 1, CorrectnessFailures: 1,
		},
	}
	failure := terminalDeleteOutcomeError(map[string]*deletemetrics.Metrics{
		"mt-002-delete": metrics,
	})
	if failure == nil || !strings.Contains(strings.ToLower(failure.Error()), "verification") ||
		strings.Contains(strings.ToLower(failure.Error()), "incomplete") {
		t.Fatalf("DELETE verification terminal error = %v, want classified verification verdict", failure)
	}
}

func TestResolveRunCompletionErrorPreservesPresenterFailureWithoutAutoResults(t *testing.T) {
	presenterFailure := errors.New("owned DELETE presentation failed")
	got := resolveRunCompletionError(
		presenterFailure, autoResultsOutcome{}, false,
		scenario.Params{WorkloadType: scenario.WorkloadTypeDelete})
	if got != presenterFailure {
		t.Fatalf("DELETE presenter error = %v, want original %v", got, presenterFailure)
	}
}

func TestResolveRunCompletionErrorKeepsSeededCleanupFailureOutOfDeleteVerdict(t *testing.T) {
	presenterFailure := &runcontrol.OwnedEngineTerminalFailure{Detail: "cleanup partially failed"}
	cleanupFailure := autoResultsOutcome{Tracker: &portcheck.RunResult{
		FinalState:      constants.StateFailed,
		FailureStepID:   "mt-003-20260824.140000.000-cleanup",
		FailureCategory: "execution",
		FailureMessage:  "cleanup partially failed",
	}}
	params := scenario.Params{WorkloadType: scenario.WorkloadTypeDelete, Cleanup: true}
	if got := resolveRunCompletionError(nil, cleanupFailure, true, params); got != nil {
		t.Fatalf("cleanup-only failure changed measured DELETE verdict: %v", got)
	}
	if got := resolveRunCompletionError(presenterFailure, cleanupFailure, true, params); got != nil {
		t.Fatalf("cleanup-only presenter failure changed measured DELETE exit status: %v", got)
	}
	independentFailure := errors.New("orchestrator cleanup failed")
	if got := resolveRunCompletionError(independentFailure, cleanupFailure, true, params); !errors.Is(got, independentFailure) {
		t.Fatalf("independent run failure was hidden by cleanup neutrality: %v", got)
	}
	if got := resolveRunCompletionError(
		errors.Join(presenterFailure, independentFailure), cleanupFailure, true, params,
	); !errors.Is(got, independentFailure) {
		t.Fatalf("joined independent run failure was hidden by cleanup neutrality: %v", got)
	}

	measuredFailure := cleanupFailure
	measuredFailure.Tracker = &portcheck.RunResult{
		FinalState:      constants.StateFailed,
		FailureStepID:   "mt-002-20260824.140000.000-delete",
		FailureCategory: "execution",
		FailureMessage:  "failure budget exceeded",
	}
	if got := resolveRunCompletionError(nil, measuredFailure, true, params); got == nil {
		t.Fatal("measured DELETE failure was hidden by cleanup mode")
	}

	params.Cleanup = false
	if got := resolveRunCompletionError(nil, cleanupFailure, true, params); got == nil {
		t.Fatal("ordinary DELETE failure with cleanup-shaped step ID was ignored")
	}
}

func TestJoinFallbackPreparedCleanupReportsCleanupFailureWithoutPrimary(t *testing.T) {
	cleanupFailure := errors.New("prepared cleanup failed")
	prepared := runcontrol.NewPreparedRun(
		scenario.Params{}, nil, nil, scenario.StepPlan{}, "",
		func(context.Context) error { return cleanupFailure })

	got := joinFallbackPreparedCleanup(context.Background(), nil, prepared)
	if !errors.Is(got, cleanupFailure) {
		t.Fatalf("fallback result = %v, want cleanup failure", got)
	}
}

func TestJoinFallbackPreparedCleanupPreservesPrimaryAndUsesFreshBudget(t *testing.T) {
	primary := errors.New("command failed")
	cleanupFailure := errors.New("prepared cleanup failed")
	calls := 0
	prepared := runcontrol.NewPreparedRun(
		scenario.Params{}, nil, nil, scenario.StepPlan{}, "",
		func(ctx context.Context) error {
			calls++
			if err := ctx.Err(); err != nil {
				t.Fatalf("fallback cleanup inherited cancellation: %v", err)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("fallback cleanup did not receive a deadline")
			}
			return cleanupFailure
		})
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	got := joinFallbackPreparedCleanup(parent, primary, prepared)
	if !errors.Is(got, primary) || !errors.Is(got, cleanupFailure) {
		t.Fatalf("joined error = %v, want primary and cleanup failure", got)
	}
	_ = joinFallbackPreparedCleanup(parent, primary, prepared)
	if calls != 1 {
		t.Fatalf("cleanup calls = %d, want exactly one", calls)
	}
}

func TestPrepareRunBundlePreCanceledCleanupSkipsRemovalStages(t *testing.T) {
	var externalCalls, scenarioCalls int
	path := filepath.Join(t.TempDir(), "prepared.js")
	deps := runPreparationDependencies{
		PrepareExternal: func(params scenario.Params) (scenario.Params, error) { return params, nil },
		CleanupExternal: func(context.Context, scenario.Params) error {
			externalCalls++
			return nil
		},
		GenerateScenario: func(scenario.Params) (string, error) { return "scenario", nil },
		GenerateDefaults: func(scenario.Params) ([]byte, error) { return []byte("defaults"), nil },
		BuildStepPlan:    func(string) (scenario.StepPlan, error) { return scenario.StepPlan{}, nil },
		WriteScenario:    os.WriteFile,
		RemoveScenario: func(string) error {
			scenarioCalls++
			return nil
		},
	}
	prepared, err := prepareRunBundleWithDependencies(
		scenario.Params{WorkloadType: WorkloadTypeMock}, path, false, deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := prepared.Cleanup(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup error = %v, want context cancellation", err)
	}
	if externalCalls != 0 || scenarioCalls != 0 {
		t.Fatalf("canceled cleanup calls external/scenario = %d/%d, want 0/0",
			externalCalls, scenarioCalls)
	}
}

func TestPrepareRunBundleCancellationBetweenCleanupStagesSkipsScenarioRemoval(t *testing.T) {
	var externalCalls, scenarioCalls int
	ctx, cancel := context.WithCancel(context.Background())
	path := filepath.Join(t.TempDir(), "prepared.js")
	deps := runPreparationDependencies{
		PrepareExternal: func(params scenario.Params) (scenario.Params, error) { return params, nil },
		CleanupExternal: func(context.Context, scenario.Params) error {
			externalCalls++
			cancel()
			return nil
		},
		GenerateScenario: func(scenario.Params) (string, error) { return "scenario", nil },
		GenerateDefaults: func(scenario.Params) ([]byte, error) { return []byte("defaults"), nil },
		BuildStepPlan:    func(string) (scenario.StepPlan, error) { return scenario.StepPlan{}, nil },
		WriteScenario:    os.WriteFile,
		RemoveScenario: func(string) error {
			scenarioCalls++
			return nil
		},
	}
	prepared, err := prepareRunBundleWithDependencies(
		scenario.Params{WorkloadType: WorkloadTypeMock}, path, false, deps)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Cleanup(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup error = %v, want context cancellation", err)
	}
	if externalCalls != 1 || scenarioCalls != 0 {
		t.Fatalf("cleanup calls external/scenario = %d/%d, want 1/0",
			externalCalls, scenarioCalls)
	}
}

func TestCleanupPreparedItemFilesHonorsPreCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := scenario.CleanupPreparedItemFiles(ctx, scenario.Params{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup error = %v, want context cancellation", err)
	}
}
