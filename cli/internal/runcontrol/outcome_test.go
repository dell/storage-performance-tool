/*
Copyright © 2026 Dell Technologies
*/

package runcontrol

import (
	"errors"
	"testing"
)

func TestOutcomePreservesIndependentPhaseErrors(t *testing.T) {
	artifactErr := errors.New("artifacts timed out")
	cleanupErr := errors.New("removal failed")
	outcome := Outcome{
		Artifacts: CompletedPhase(artifactErr),
		Removal:   CompletedPhase(cleanupErr),
		Resources: ResourceDispositionRetained,
	}
	if !errors.Is(outcome.Error(), artifactErr) || !errors.Is(outcome.Error(), cleanupErr) {
		t.Fatalf("aggregate error lost a phase: %v", outcome.Error())
	}
	if outcome.Resources != ResourceDispositionRetained {
		t.Fatalf("resources = %q, want retained", outcome.Resources)
	}
}

func TestFinalizationOutcomePreservesDiagnosticsAndRemoval(t *testing.T) {
	diagnosticsErr := errors.New("diagnostics timed out")
	removalErr := errors.New("remove failed")
	outcome := FinalizationOutcome{
		Diagnostics: CompletedPhase(diagnosticsErr),
		Removal:     CompletedPhase(removalErr),
		Resources:   ResourceDispositionRetained,
	}
	if !errors.Is(outcome.Error(), diagnosticsErr) || !errors.Is(outcome.Error(), removalErr) {
		t.Fatalf("finalization aggregate = %v", outcome.Error())
	}
}
