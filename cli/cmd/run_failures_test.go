package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/results/summary"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// writeAllFailedRunDir writes the artifacts of one write phase whose requests all failed.
func writeAllFailedRunDir(t *testing.T) string {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "mt-20250926.172619.540")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}
	stepID := "mt-001-20250926.172621.307-create"
	metricsName := fmt.Sprintf("%s.%s", stepID, constants.ResultsArtifactSuffixMetricsTotal)
	metricsCSV := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s],BWLast[MiB/s],DurationAvg[us],DurationMin[us],DurationQ_0.5[us],DurationAvg[us],DurationQ_0.99[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.5[us],LatencyAvg[us],LatencyQ_0.99[us],LatencyMax[us]\n" +
		"\"2025-09-26T17:27:00Z\",CREATE,8,4,8,8,0,2499,0,7.554,244.988254,0,0,0,0,79900,28000,59000,79876,108000,623000,79900,28000,59000,79876,108000,623000\n"
	if err := os.WriteFile(filepath.Join(runDir, metricsName), []byte(metricsCSV), 0o644); err != nil {
		t.Fatalf("write metrics csv: %v", err)
	}
	manifest := &results.Manifest{
		BaseURL:     "http://engine.example:9999",
		OutputDir:   runDir,
		GeneratedAt: time.Now().UTC(),
		Steps: []results.StepManifest{{
			StepID:      stepID,
			CompletedAt: time.Now().UTC(),
			Files:       []results.FileStatus{{Name: metricsName, Size: int64(len(metricsCSV)), Status: "ok"}},
		}},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsManifestFileName), data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	meta := &runMetadata{
		GeneratedAt:     time.Date(2025, 9, 26, 17, 27, 28, 0, time.UTC),
		WorkloadType:    "write",
		ResultsRoot:     filepath.Base(runDir),
		ScenarioParams:  scenario.Params{ObjectSize: "1MB", ObjectCount: 2500, Threads: 8},
		ExpectedStepIDs: []string{stepID},
		ActualStepIDs:   []string{stepID},
	}
	if err := writeRunMetadata(meta, runDir); err != nil {
		t.Fatalf("write run metadata: %v", err)
	}
	return runDir
}

func allFailedOutcome(state string) autoResultsOutcome {
	return autoResultsOutcome{
		Tracker:         &portcheck.RunResult{FinalState: state},
		AllFailedPhases: []summary.AllFailedPhase{{Label: "Create", FailureCount: 5301}},
	}
}

func TestACleanRunWithAPhaseThatAllFailedExits1(t *testing.T) {
	for _, workload := range []string{
		scenario.WorkloadTypeRead, "write", scenario.WorkloadTypeMixed, scenario.WorkloadTypeList, scenario.WorkloadTypeTables,
	} {
		err := resolveRunCompletionError(nil, allFailedOutcome(constants.StateCompleted), true, scenario.Params{WorkloadType: workload})
		var exit *ExitCodeError
		if !errors.As(err, &exit) || exit.Code != constants.ExitCodeWorkloadFailure {
			t.Fatalf("%s: err = %v, want exit %d", workload, err, constants.ExitCodeWorkloadFailure)
		}
		if !strings.Contains(exit.Msg, "phase Create failed (5301 failed, 0 succeeded)") {
			t.Fatalf("%s: message = %q", workload, exit.Msg)
		}
	}
}

func TestAllPhasesThatAllFailedAreNamed(t *testing.T) {
	outcome := allFailedOutcome(constants.StateCompleted)
	outcome.AllFailedPhases = append(outcome.AllFailedPhases, summary.AllFailedPhase{Label: "Read", FailureCount: 7})
	err := resolveOrdinaryRunError(nil, outcome)
	if err == nil || !strings.Contains(err.Error(), "Create") || !strings.Contains(err.Error(), "phase Read failed (7 failed") {
		t.Fatalf("err = %v", err)
	}
}

func TestARunWithNoPhaseThatAllFailedKeepsItsResult(t *testing.T) {
	outcome := autoResultsOutcome{Tracker: &portcheck.RunResult{FinalState: constants.StateCompleted}}
	if err := resolveOrdinaryRunError(nil, outcome); err != nil {
		t.Fatalf("err = %v", err)
	}
	// With no tracker or summary, as without auto-results, there are no counts to judge.
	if err := resolveOrdinaryRunError(nil, autoResultsOutcome{}); err != nil {
		t.Fatalf("no evidence: err = %v", err)
	}
}

func TestOnlyACompletedRunIsJudged(t *testing.T) {
	for _, state := range []string{constants.StateStopped, constants.StateRunning, ""} {
		if err := resolveOrdinaryRunError(nil, allFailedOutcome(state)); err != nil {
			t.Fatalf("state %q: err = %v", state, err)
		}
	}
	outcome := allFailedOutcome(constants.StateCompleted)
	outcome.Tracker = nil
	if err := resolveOrdinaryRunError(nil, outcome); err != nil {
		t.Fatalf("no tracker: err = %v", err)
	}
}

func TestASavedSummaryOfAPhaseThatAllFailedReachesTheExitCode(t *testing.T) {
	runDir := writeAllFailedRunDir(t)
	runSummary, err := generateRunSummary(context.Background(), runDir, io.Discard)
	if err != nil {
		t.Fatalf("generate summary: %v", err)
	}
	outcome := autoResultsOutcome{
		Tracker:         &portcheck.RunResult{FinalState: constants.StateCompleted},
		AllFailedPhases: runSummary.AllFailedPhases(),
	}
	got := resolveOrdinaryRunError(nil, outcome)
	var exit *ExitCodeError
	if !errors.As(got, &exit) || !strings.Contains(exit.Msg, "every request of phase Create failed (2499 failed, 0 succeeded)") {
		t.Fatalf("err = %v", got)
	}
}

func TestAnErrorAlreadyEndingTheRunIsNotReplaced(t *testing.T) {
	stopped := errors.New("interrupted by signal")
	if err := resolveOrdinaryRunError(stopped, allFailedOutcome(constants.StateCompleted)); !errors.Is(err, stopped) {
		t.Fatalf("err = %v", err)
	}
}

func TestAnEngineFailureStaysTheCause(t *testing.T) {
	outcome := allFailedOutcome(constants.StateFailed)
	outcome.Tracker.FailureStepID = "s1"
	outcome.Tracker.FailureCategory = "input"
	outcome.Tracker.FailureMessage = "items file line 7 is blank"
	err := resolveOrdinaryRunError(nil, outcome)
	if err == nil || !strings.Contains(err.Error(), "items file line 7 is blank") || strings.Contains(err.Error(), "phase Create") {
		t.Fatalf("err = %v", err)
	}
}

func TestVerificationAndDeleteKeepTheirOwnContracts(t *testing.T) {
	outcome := allFailedOutcome(constants.StateCompleted)
	if err := resolveRunCompletionError(nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeDelete}); err != nil {
		t.Fatalf("delete: err = %v", err)
	}
	for _, workload := range []string{scenario.WorkloadTypeWriteVerify, scenario.WorkloadTypeReadVerify} {
		err := resolveRunCompletionError(nil, outcome, true, scenario.Params{WorkloadType: workload})
		if err != nil && strings.Contains(err.Error(), "phase Create") {
			t.Fatalf("%s took the ordinary rule: %v", workload, err)
		}
	}
}
