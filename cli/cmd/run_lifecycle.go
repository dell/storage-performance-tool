/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"

	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

func updateRunLifecycleMetadata(meta *runMetadata, outcome *autoResultsOutcome) {
	if meta == nil || outcome == nil {
		return
	}
	lifecycle := &runLifecycleMetadata{
		Workload:            lifecyclePhaseFromOutcome(outcome.Lifecycle.Workload),
		Artifacts:           lifecyclePhaseFromOutcome(outcome.Lifecycle.Artifacts),
		Shutdown:            lifecyclePhaseFromOutcome(outcome.Lifecycle.Shutdown),
		Diagnostics:         lifecyclePhaseFromOutcome(outcome.Lifecycle.Diagnostics),
		Removal:             lifecyclePhaseFromOutcome(outcome.Lifecycle.Removal),
		PreparedInputs:      lifecyclePhaseFromOutcome(outcome.Lifecycle.PreparedInputs),
		Summary:             lifecyclePhaseFromOutcome(outcome.Lifecycle.Summary),
		ResourceDisposition: outcome.Lifecycle.Resources,
	}
	if outcome.Tracker != nil {
		lifecycle.Workload.State = outcome.Tracker.FinalState
		lifecycle.Workload.FailureStepID = outcome.Tracker.FailureStepID
		lifecycle.Workload.FailureCategory = outcome.Tracker.FailureCategory
		lifecycle.Workload.FailureMessage = outcome.Tracker.FailureMessage
	}
	meta.Lifecycle = lifecycle
}

func lifecyclePhaseFromOutcome(outcome runcontrol.PhaseOutcome) lifecyclePhaseMetadata {
	return lifecyclePhaseMetadata{
		Started:   outcome.Started,
		Completed: outcome.Completed,
		TimedOut:  errors.Is(outcome.Err, context.DeadlineExceeded),
		Error:     errorText(outcome.Err),
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
