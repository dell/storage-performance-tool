package cmd

import (
	"errors"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results/summary"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

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
