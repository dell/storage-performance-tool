package cmd

import (
	"context"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestResolveVerificationRunErrorCorruptionPrecedesOtherFailures(t *testing.T) {
	outcome := autoResultsOutcome{
		Tracker:         &portcheck.RunResult{FinalState: constants.StateFailed, FailureStepID: "verify", FailureCategory: "execution", FailureMessage: "failed"},
		Finalization:    &integrity.FinalizeOutcome{CorruptCount: 2},
		FinalizationErr: errors.New("later publication failure"),
	}
	err := resolveVerificationRunError(errors.New("orchestrator failure"), outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify})
	var exitErr *ExitCodeError
	if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeIntegrityCorruption {
		t.Fatalf("error = %#v, want exit %d", err, constants.ExitCodeIntegrityCorruption)
	}
}

func TestResolveVerificationRunErrorObservedCorruptionSurvivesArtifactFailure(t *testing.T) {
	corrupt := int64(3)
	for _, outcome := range []autoResultsOutcome{
		{
			Tracker:              &portcheck.RunResult{FinalState: constants.StateCompleted},
			ObservedCorruptCount: &corrupt,
			ArtifactErr:          errors.New("artifact fetch failed"),
		},
		{
			Tracker:              &portcheck.RunResult{FinalState: constants.StateCompleted},
			ObservedCorruptCount: &corrupt,
			FinalizationErr:      errors.New("completion validation failed"),
		},
	} {
		err := resolveVerificationRunError(
			nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeReadVerify})
		var exitErr *ExitCodeError
		if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeIntegrityCorruption {
			t.Fatalf("error = %#v, want exit %d", err, constants.ExitCodeIntegrityCorruption)
		}
	}
}

func TestResolveVerificationRunErrorCleanEmptyReadRequiresExplicitAllowance(t *testing.T) {
	params := scenario.Params{WorkloadType: scenario.WorkloadTypeReadVerify}
	outcome := autoResultsOutcome{
		Tracker:      &portcheck.RunResult{FinalState: constants.StateCompleted},
		Finalization: &integrity.FinalizeOutcome{Complete: true, EmptySelection: true},
	}
	err := resolveVerificationRunError(nil, outcome, true, params)
	var exitErr *ExitCodeError
	if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
		t.Fatalf("error = %#v, want exit %d", err, constants.ExitCodeWorkloadFailure)
	}
	outcome.Finalization.EmptyAllowed = true
	if err = resolveVerificationRunError(nil, outcome, true, params); err != nil {
		t.Fatalf("explicit clean empty selection should pass: %v", err)
	}
}

func TestVerifierMayBeMissingOnlyWhenNotStartedCompletedOrAnEmptySelectionIsAllowed(t *testing.T) {
	if !verifierMayBeMissing(integrityplan.Plan{}, portcheck.StepLifecycleNotStarted) {
		t.Fatal("a READ step that never started has no live evidence")
	}
	if !verifierMayBeMissing(integrityplan.Plan{AllowEmpty: true}, portcheck.StepLifecycleStarted) {
		t.Fatal("--allow-empty-selection says a READ step may publish nothing")
	}
	if !verifierMayBeMissing(integrityplan.Plan{}, portcheck.StepLifecycleCompleted) {
		t.Fatal("a completed READ step can end between two polls of the live metrics")
	}
	for _, lifecycle := range []portcheck.StepLifecycle{
		portcheck.StepLifecycleStarted, portcheck.StepLifecyclePlanned, portcheck.StepLifecycleFailed, "",
	} {
		if verifierMayBeMissing(integrityplan.Plan{}, lifecycle) {
			t.Fatalf("without the flag a READ step in lifecycle %q must appear", lifecycle)
		}
	}
}

func TestStepsToFetchAddsOnlyUnseenCompletedPlannedSteps(t *testing.T) {
	list, read := "mt-001-x-list", "mt-002-x-verify"
	plan := func(allowEmpty bool) integrityplan.Plan {
		return integrityplan.Plan{
			AllowEmpty: allowEmpty,
			Producer:   &integrityplan.PlannedStep{ID: list, Role: integrityplan.StepRoleList},
			Verifier:   integrityplan.PlannedStep{ID: read},
		}
	}
	tracker := func(listLifecycle, readLifecycle portcheck.StepLifecycle) *portcheck.RunResult {
		return &portcheck.RunResult{Steps: map[string]portcheck.StepCompletion{
			list: {Lifecycle: listLifecycle}, read: {Lifecycle: readLifecycle},
		}}
	}
	completed, started := portcheck.StepLifecycleCompleted, portcheck.StepLifecycleStarted
	tests := []struct {
		name     string
		plan     integrityplan.Plan
		seen     []string
		observed integrity.StepRoles
		tracker  *portcheck.RunResult
		want     []string
	}{
		{"allowed and READ unseen", plan(true), []string{list}, integrity.StepRoles{List: list},
			tracker(completed, completed), []string{list, read}},
		{"READ unseen", plan(false), []string{list}, integrity.StepRoles{List: list},
			tracker(completed, completed), []string{list, read}},
		{"LIST unseen", plan(false), []string{read}, integrity.StepRoles{Read: read},
			tracker(completed, completed), []string{read, list}},
		{"both seen", plan(false), []string{list, read}, integrity.StepRoles{List: list, Read: read},
			tracker(completed, completed), []string{list, read}},
		{"READ never started", plan(false), []string{list}, integrity.StepRoles{List: list},
			tracker(completed, portcheck.StepLifecycleNotStarted), []string{list}},
		{"READ only planned", plan(false), []string{list}, integrity.StepRoles{List: list},
			tracker(completed, portcheck.StepLifecyclePlanned), []string{list}},
		{"READ still running", plan(false), []string{list}, integrity.StepRoles{List: list},
			tracker(completed, started), []string{list}},
		{"READ failed", plan(false), []string{list}, integrity.StepRoles{List: list},
			tracker(completed, portcheck.StepLifecycleFailed), []string{list}},
		{"LIST still running", plan(false), []string{read}, integrity.StepRoles{Read: read},
			tracker(started, completed), []string{read}},
		{"no tracker", plan(false), []string{list}, integrity.StepRoles{List: list}, nil, []string{list}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seen := slices.Clone(test.seen)
			got := stepsToFetch(seen, test.plan, test.observed, test.tracker)
			if !slices.Equal(got, test.want) {
				t.Fatalf("stepsToFetch() = %v, want %v", got, test.want)
			}
			if !slices.Equal(seen, test.seen) {
				t.Fatalf("stepsToFetch() changed its input to %v", seen)
			}
		})
	}
}

// A READ step with nothing to read can end between two polls, and the run must still fetch its files,
// with or without --allow-empty-selection.
func TestAutoResultsFetchesAnUnseenCompletedReadStep(t *testing.T) {
	const runID = int64(42)
	list, read := "mt-001-20260823.100000.000-list", "mt-002-20260823.100000.000-verify"
	for _, allowEmpty := range []bool{true, false} {
		t.Run(strconv.FormatBool(allowEmpty), func(t *testing.T) {
			installAutoResultsFakes(t, []string{list})
			newRunTrackerFunc = func(string) autoResultsRunTracker {
				return &fakeRunTracker{result: &portcheck.RunResult{
					FinalState: constants.StateCompleted,
					RunID:      runID,
					Steps: map[string]portcheck.StepCompletion{
						list: {StepID: list, Lifecycle: portcheck.StepLifecycleCompleted, Planned: true, Started: true},
						read: {StepID: read, Lifecycle: portcheck.StepLifecycleCompleted, Planned: true},
					},
				}}
			}
			fetcher := &fakeFetcher{manifest: &results.Manifest{}}
			newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
				fetcher.output = output
				return fetcher
			}
			plan := integrityplan.Plan{
				RunID: runID, Workload: scenario.WorkloadTypeReadVerify, Kind: integrityplan.PlanKindReadDiscovered,
				Producer:   &integrityplan.PlannedStep{ID: list, Number: 1, Role: integrityplan.StepRoleList},
				Verifier:   integrityplan.PlannedStep{ID: read, Number: 2, Role: integrityplan.StepRoleVerify},
				Input:      integrityplan.InputDiscovered,
				AllowEmpty: allowEmpty,
			}
			if !plan.Valid() {
				t.Fatal("invalid plan")
			}
			root := t.TempDir()
			monitor := startAutoResultsMonitor(
				context.Background(), "http://127.0.0.1:1", "mt", root, []string{list, read}, runID,
				false, nil, "9999", false, 0, "", &runMetadata{ResultsRoot: root}, io.Discard, io.Discard, "", nil,
				&integrity.FinalizeOptions{Workload: scenario.WorkloadTypeReadVerify, RunID: runID, Plan: plan.Clone()})
			monitor.Arm()
			<-monitor.done

			want := []string{list, read}
			fetcher.mu.Lock()
			defer fetcher.mu.Unlock()
			if !slices.Equal(fetcher.stepIDs, want) {
				t.Fatalf("fetched %v, want %v", fetcher.stepIDs, want)
			}
		})
	}
}

func TestEmptySelectionForgivesOnlyAMissingLiveCountWhenNothingWasRead(t *testing.T) {
	liveErr := errors.New("verification READ step could not be identified")
	tests := []struct {
		name         string
		finalization *integrity.FinalizeOutcome
		finalizeErr  error
		wantDropped  bool
	}{
		{"empty and nothing read", &integrity.FinalizeOutcome{Complete: true, EmptySelection: true}, nil, true},
		{"objects selected", &integrity.FinalizeOutcome{Complete: true, SelectionCount: 1}, nil, false},
		{"empty but something read", &integrity.FinalizeOutcome{EmptySelection: true, VerificationAttemptedCount: 1}, nil, false},
		{"finalization failed", &integrity.FinalizeOutcome{EmptySelection: true}, errors.New("broken"), false},
		{"no finalization", nil, nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := autoResultsOutcome{
				Finalization: test.finalization, FinalizationErr: test.finalizeErr, CorruptionMetricsErr: liveErr,
			}
			dropLiveCorruptionErrorOfEmptySelection(&outcome)
			if dropped := outcome.CorruptionMetricsErr == nil; dropped != test.wantDropped {
				t.Fatalf("dropped = %t, want %t", dropped, test.wantDropped)
			}
		})
	}
}

func TestResolveVerificationRunErrorEmptySelectionDiagnosticNamesWorkload(t *testing.T) {
	base := autoResultsOutcome{
		Tracker: &portcheck.RunResult{FinalState: constants.StateCompleted},
		Finalization: &integrity.FinalizeOutcome{
			Complete:       true,
			EmptySelection: true,
		},
	}
	for _, test := range []struct {
		workload string
		want     string
	}{
		{workload: scenario.WorkloadTypeReadVerify, want: "read-verify requires --allow-empty-selection"},
		{workload: scenario.WorkloadTypeWriteVerify, want: "write-verify produced no objects eligible for verification"},
	} {
		err := resolveVerificationRunError(
			nil, base, true, scenario.Params{WorkloadType: test.workload})
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("%s diagnostic = %v, want containing %q", test.workload, err, test.want)
		}
	}
}

func TestResolveVerificationRunErrorRejectsIncompleteFinalizationOutcome(t *testing.T) {
	outcome := autoResultsOutcome{
		Tracker:      &portcheck.RunResult{FinalState: constants.StateCompleted},
		Finalization: &integrity.FinalizeOutcome{},
	}
	err := resolveVerificationRunError(
		nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify})
	var exitErr *ExitCodeError
	if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure ||
		!strings.Contains(exitErr.Error(), "finalization is incomplete") {
		t.Fatalf("error = %#v, want explicit incomplete-finalization exit 1", err)
	}
}

func TestResolveVerificationRunErrorFinalizationFailureIsExitOne(t *testing.T) {
	outcome := autoResultsOutcome{
		Tracker:         &portcheck.RunResult{FinalState: constants.StateCompleted},
		Finalization:    &integrity.FinalizeOutcome{},
		FinalizationErr: errors.New("missing verified completion"),
	}
	err := resolveVerificationRunError(nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify})
	var exitErr *ExitCodeError
	if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
		t.Fatalf("error = %#v, want exit %d", err, constants.ExitCodeWorkloadFailure)
	}
}

func TestResolveVerificationRunErrorPreservesResourceCleanupFailure(t *testing.T) {
	cleanupErr := errors.New("container removal failed")
	outcome := autoResultsOutcome{
		Tracker:      &portcheck.RunResult{FinalState: constants.StateCompleted},
		Finalization: &integrity.FinalizeOutcome{Complete: true},
		Lifecycle: runcontrol.Outcome{
			Diagnostics: runcontrol.CompletedPhase(nil),
			Removal:     runcontrol.CompletedPhase(cleanupErr),
			Resources:   runcontrol.ResourceDispositionRetained,
		},
	}
	err := resolveVerificationRunError(
		nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify})
	var exitErr *ExitCodeError
	if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
		t.Fatalf("error = %#v, want exit %d", err, constants.ExitCodeWorkloadFailure)
	}
	if !strings.Contains(err.Error(), cleanupErr.Error()) ||
		!strings.Contains(err.Error(), "resources remain managed") {
		t.Fatalf("cleanup outcome was not preserved: %v", err)
	}
}

func TestResolveVerificationRunErrorDeferredWritePolicy(t *testing.T) {
	params := scenario.Params{WorkloadType: scenario.WorkloadTypeWriteVerify, DeferVerification: true}
	outcome := autoResultsOutcome{
		Tracker:      &portcheck.RunResult{FinalState: constants.StateCompleted},
		Finalization: &integrity.FinalizeOutcome{Complete: true, VerificationDeferred: true},
	}
	if err := resolveVerificationRunError(nil, outcome, true, params); err != nil {
		t.Fatalf("nonempty deferred seed should pass: %v", err)
	}
	outcome.Finalization.EmptySelection = true
	err := resolveVerificationRunError(nil, outcome, true, params)
	var exitErr *ExitCodeError
	if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
		t.Fatalf("empty deferred seed error = %#v, want exit %d", err, constants.ExitCodeWorkloadFailure)
	}
	outcome.Finalization.EmptySelection = false
	outcome.Finalization.VerificationDeferred = false
	err = resolveVerificationRunError(nil, outcome, true, params)
	if !errors.As(err, &exitErr) || !strings.Contains(err.Error(), "finalization mode does not match") {
		t.Fatalf("deferred mode mismatch error = %#v", err)
	}
}
