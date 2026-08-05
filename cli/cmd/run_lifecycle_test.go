/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

func TestUpdateRunLifecycleMetadataPreservesIndependentOutcomes(t *testing.T) {
	artifactErr := errors.New("artifact failed")
	diagnosticsErr := errors.New("diagnostics timed out")
	removalErr := errors.New("remove failed")
	preparedErr := errors.New("prepared input removal failed")
	meta := &runMetadata{}
	outcome := &autoResultsOutcome{
		Tracker: &portcheck.RunResult{FinalState: constants.StateFailed, FailureStepID: "read", FailureCategory: "integrity", FailureMessage: "corrupt"},
		Lifecycle: runcontrol.Outcome{
			Workload:       runcontrol.CompletedPhase(nil),
			Artifacts:      runcontrol.CompletedPhase(artifactErr),
			Shutdown:       runcontrol.CompletedPhase(nil),
			Diagnostics:    runcontrol.CompletedPhase(diagnosticsErr),
			Removal:        runcontrol.CompletedPhase(removalErr),
			PreparedInputs: runcontrol.CompletedPhase(preparedErr),
			Summary:        runcontrol.CompletedPhase(nil),
			Resources:      runcontrol.ResourceDispositionRetained,
		},
	}
	updateRunLifecycleMetadata(meta, outcome)
	if meta.Lifecycle == nil {
		t.Fatal("lifecycle metadata was not created")
	}
	if meta.Lifecycle.Workload.State != constants.StateFailed ||
		meta.Lifecycle.Workload.FailureStepID != "read" {
		t.Fatalf("workload = %+v", meta.Lifecycle.Workload)
	}
	if meta.Lifecycle.Artifacts.Error != artifactErr.Error() ||
		meta.Lifecycle.Diagnostics.Error != diagnosticsErr.Error() ||
		meta.Lifecycle.Removal.Error != removalErr.Error() ||
		meta.Lifecycle.PreparedInputs.Error != preparedErr.Error() {
		t.Fatalf("phase outcomes = %+v", meta.Lifecycle)
	}
	if meta.Lifecycle.ResourceDisposition != runcontrol.ResourceDispositionRetained {
		t.Fatalf("resource disposition = %q", meta.Lifecycle.ResourceDisposition)
	}
}

func TestUpdateRunLifecycleMetadataOmitsFailureAttributionForCompletedRun(t *testing.T) {
	meta := &runMetadata{}
	outcome := &autoResultsOutcome{
		Tracker: &portcheck.RunResult{
			FinalState:      constants.StateCompleted,
			FailureStepID:   "none-20260804.201852.427",
			FailureCategory: "stale",
			FailureMessage:  "must not escape",
		},
		Lifecycle: runcontrol.Outcome{Workload: runcontrol.CompletedPhase(nil)},
	}

	updateRunLifecycleMetadata(meta, outcome)
	got := meta.Lifecycle.Workload
	if got.State != constants.StateCompleted {
		t.Fatalf("state = %q, want %q", got.State, constants.StateCompleted)
	}
	if got.FailureStepID != "" || got.FailureCategory != "" || got.FailureMessage != "" {
		t.Fatalf("completed workload retained failure attribution: %+v", got)
	}
}

func TestLifecyclePhaseMetadataIdentifiesJoinedDeadline(t *testing.T) {
	phase := lifecyclePhaseFromOutcome(runcontrol.CompletedPhase(
		errors.Join(errors.New("artifact request failed"), context.DeadlineExceeded)))
	if !phase.Started || !phase.Completed || !phase.TimedOut {
		t.Fatalf("deadline phase = %+v, want completed timeout", phase)
	}
	if phase.Error == "" {
		t.Fatal("deadline phase omitted failure detail")
	}
}
