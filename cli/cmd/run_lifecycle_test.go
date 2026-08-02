/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
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
	meta := &runMetadata{}
	outcome := &autoResultsOutcome{
		Tracker:           &portcheck.RunResult{FinalState: constants.StateFailed, FailureStepID: "read", FailureCategory: "integrity", FailureMessage: "corrupt"},
		ArtifactErr:       artifactErr,
		ArtifactStarted:   true,
		ArtifactCompleted: true,
		ShutdownStarted:   true,
		SummaryStarted:    true,
		SummaryCompleted:  true,
		ResourceFinalization: &runcontrol.FinalizationOutcome{
			Diagnostics: runcontrol.CompletedPhase(diagnosticsErr),
			Removal:     runcontrol.CompletedPhase(removalErr),
			Resources:   runcontrol.ResourceDispositionRetained,
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
		meta.Lifecycle.Removal.Error != removalErr.Error() {
		t.Fatalf("phase outcomes = %+v", meta.Lifecycle)
	}
	if meta.Lifecycle.ResourceDisposition != runcontrol.ResourceDispositionRetained {
		t.Fatalf("resource disposition = %q", meta.Lifecycle.ResourceDisposition)
	}
}
