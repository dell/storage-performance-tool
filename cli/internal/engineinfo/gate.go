package engineinfo

import (
	"context"
	"fmt"
)

// FleetCollector is the collection boundary used by managed-run gates.
type FleetCollector interface {
	Collect(context.Context, []ParticipantDescriptor) (FleetResult, error)
}

// GateOutcome carries the complete safe evidence even when submission is denied.
type GateOutcome struct {
	Fleet    FleetResult
	Decision GateDecision
	Proceed  bool
}

// GateDecision identifies why submission proceeded or stopped.
type GateDecision string

// Supported managed-run gate decisions.
const (
	GateProceed           GateDecision = "proceed"
	GateRejectedMismatch  GateDecision = "rejected_mismatch"
	GateCollectionFailure GateDecision = "collection_failed"
)

// EvaluateGate applies the managed-run mismatch and force policy to one fleet.
// Force is deliberately narrow: it permits only a successfully collected,
// known mismatch and never overrides a collection or contract failure.
func EvaluateGate(
	ctx context.Context,
	collector FleetCollector,
	descriptors []ParticipantDescriptor,
	force bool,
) (GateOutcome, error) {
	if collector == nil {
		return GateOutcome{}, fmt.Errorf("engine identity collector is required")
	}
	fleet, err := collector.Collect(ctx, descriptors)
	outcome := GateOutcome{Fleet: fleet}
	if err != nil {
		outcome.Decision = GateCollectionFailure
		return outcome, fmt.Errorf("engine identity collection gate failed: %w", err)
	}

	switch fleet.Consistency.Status {
	case ConsistencyConsistent, ConsistencyIndeterminate:
		outcome.Decision = GateProceed
		outcome.Proceed = true
		return outcome, nil
	case ConsistencyMismatch:
		if !force {
			outcome.Decision = GateRejectedMismatch
			return outcome, fmt.Errorf("engine build identity mismatch rejected before scenario submission")
		}
		outcome.Decision = GateProceed
		outcome.Proceed = true
		outcome.Fleet.Consistency.Forced = true
		outcome.Fleet.Consistency.Reason = "engine build identity mismatch overridden by --force"
		return outcome, nil
	default:
		return outcome, fmt.Errorf("engine identity collection produced an unsupported consistency status")
	}
}
