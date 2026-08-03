/*
Copyright © 2026 Dell Technologies
*/

// Package runcontrol defines presentation-neutral lifecycle outcomes and the
// session coordinator shared by headless and TUI execution paths.
package runcontrol

import "errors"

// ResourceDisposition records who owns process resources after finalization.
type ResourceDisposition string

const (
	// ResourceDispositionUnknown means final ownership could not be established.
	ResourceDispositionUnknown ResourceDisposition = "unknown"
	// ResourceDispositionNotOwned means the launcher never acquired resources.
	ResourceDispositionNotOwned ResourceDisposition = "not-owned"
	// ResourceDispositionRemoved means all launcher-owned resources were released.
	ResourceDispositionRemoved ResourceDisposition = "removed"
	// ResourceDispositionRetained means cleanup ownership remains available for retry.
	ResourceDispositionRetained ResourceDisposition = "retained-for-retry"
)

// PhaseOutcome preserves one lifecycle phase independently of every other
// phase. Completed means the phase returned; Err may still report failure.
type PhaseOutcome struct {
	Started   bool
	Completed bool
	Err       error
}

// CompletedPhase constructs the common synchronous phase result.
func CompletedPhase(err error) PhaseOutcome {
	return PhaseOutcome{Started: true, Completed: true, Err: err}
}

// FinalizationOutcome separates optional diagnostics from mandatory resource
// removal and records whether cleanup ownership was actually released.
type FinalizationOutcome struct {
	Diagnostics PhaseOutcome
	Removal     PhaseOutcome
	Resources   ResourceDisposition
	WaitErr     error
}

// Error returns the aggregate error without erasing phase attribution.
func (o FinalizationOutcome) Error() error {
	return errors.Join(o.WaitErr, o.Diagnostics.Err, o.Removal.Err)
}

// Outcome accumulates the whole run without treating one phase error as proof
// that later phases did not run.
type Outcome struct {
	LaunchErr      error
	Presentation   PhaseOutcome
	Workload       PhaseOutcome
	Artifacts      PhaseOutcome
	Diagnostics    PhaseOutcome
	Shutdown       PhaseOutcome
	Removal        PhaseOutcome
	PreparedInputs PhaseOutcome
	Summary        PhaseOutcome
	Resources      ResourceDisposition
}

// Error returns all retained lifecycle failures in execution order.
func (o Outcome) Error() error {
	return errors.Join(
		o.LaunchErr,
		o.Presentation.Err,
		o.Workload.Err,
		o.Artifacts.Err,
		o.Diagnostics.Err,
		o.Shutdown.Err,
		o.Removal.Err,
		o.PreparedInputs.Err,
		o.Summary.Err,
	)
}

// MergeFinalization copies a resource finalizer result into the run outcome.
func (o *Outcome) MergeFinalization(final FinalizationOutcome) {
	if o == nil {
		return
	}
	o.Diagnostics = final.Diagnostics
	o.Removal = final.Removal
	o.Removal.Err = errors.Join(final.Removal.Err, final.WaitErr)
	o.Resources = final.Resources
}
