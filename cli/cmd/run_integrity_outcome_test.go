package cmd

import (
	"errors"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
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
		Finalization: &integrity.FinalizeOutcome{EmptySelection: true},
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
