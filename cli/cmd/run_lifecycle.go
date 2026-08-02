/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"errors"

	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

func updateRunLifecycleMetadata(meta *runMetadata, outcome *autoResultsOutcome) {
	if meta == nil || outcome == nil {
		return
	}
	lifecycle := &runLifecycleMetadata{
		Artifacts: lifecyclePhaseMetadata{
			Started: outcome.ArtifactStarted, Completed: outcome.ArtifactCompleted,
			Error: errorText(errors.Join(
				outcome.ArtifactErr, outcome.CorruptionMetricsErr, outcome.FinalizationErr)),
		},
		Shutdown: lifecyclePhaseMetadata{
			Started: outcome.ShutdownStarted, Completed: outcome.ShutdownStarted,
			Error: errorText(outcome.ShutdownErr),
		},
		Summary: lifecyclePhaseMetadata{
			Started: outcome.SummaryStarted, Completed: outcome.SummaryCompleted,
			Error: errorText(outcome.SummaryErr),
		},
		ResourceDisposition: runcontrol.ResourceDispositionUnknown,
	}
	lifecycle.Workload.Started = outcome.Tracker != nil || outcome.TrackerErr != nil
	lifecycle.Workload.Completed = outcome.Tracker != nil
	lifecycle.Workload.Error = errorText(outcome.TrackerErr)
	if outcome.Tracker != nil {
		lifecycle.Workload.State = outcome.Tracker.FinalState
		lifecycle.Workload.FailureStepID = outcome.Tracker.FailureStepID
		lifecycle.Workload.FailureCategory = outcome.Tracker.FailureCategory
		lifecycle.Workload.FailureMessage = outcome.Tracker.FailureMessage
	}
	if outcome.ResourceFinalization != nil {
		lifecycle.Diagnostics = lifecyclePhaseFromOutcome(
			outcome.ResourceFinalization.Diagnostics)
		lifecycle.Removal = lifecyclePhaseFromOutcome(outcome.ResourceFinalization.Removal)
		if outcome.ResourceFinalization.WaitErr != nil {
			lifecycle.Removal.Error = errorText(errors.Join(
				outcome.ResourceFinalization.Removal.Err,
				outcome.ResourceFinalization.WaitErr))
		}
		lifecycle.ResourceDisposition = outcome.ResourceFinalization.Resources
	}
	meta.Lifecycle = lifecycle
}

func lifecyclePhaseFromOutcome(outcome runcontrol.PhaseOutcome) lifecyclePhaseMetadata {
	return lifecyclePhaseMetadata{
		Started: outcome.Started, Completed: outcome.Completed, Error: errorText(outcome.Err),
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
