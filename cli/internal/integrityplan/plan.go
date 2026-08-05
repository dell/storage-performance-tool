/*
Copyright © 2026 Dell Technologies
*/

// Package integrityplan defines the immutable planned roles shared by
// scenario preparation, result observation, and integrity finalization.
package integrityplan

import (
	"regexp"
	"strconv"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

// StepRole identifies one verification scenario responsibility.
type StepRole string

const (
	// StepRoleCreate publishes the written-object selection.
	StepRoleCreate StepRole = "create"
	// StepRoleList discovers and publishes a read-verification selection.
	StepRoleList StepRole = "list"
	// StepRoleVerify reads and verifies the selected objects.
	StepRoleVerify StepRole = "verify"
	// StepRoleCleanup deletes verified objects when cleanup is requested.
	StepRoleCleanup StepRole = "cleanup"
)

// InputProvenance identifies how the verifier receives its selection.
type InputProvenance string

const (
	// InputWritten uses the successful CREATE output from this scenario.
	InputWritten InputProvenance = "written"
	// InputDiscovered uses the LIST output from this scenario.
	InputDiscovered InputProvenance = "discovered"
	// InputExternal uses a CLI-staged caller manifest.
	InputExternal InputProvenance = "external"
)

// PlanKind identifies one complete, immutable integrity scenario shape.
type PlanKind string

const (
	// PlanKindWriteRead writes and then verifies the successful CREATE set.
	PlanKindWriteRead PlanKind = "write_read"
	// PlanKindWriteSeed writes a durable selection without a verification READ.
	PlanKindWriteSeed PlanKind = "write_seed"
	// PlanKindReadDiscovered verifies a selection produced by LIST.
	PlanKindReadDiscovered PlanKind = "read_discovered"
	// PlanKindReadExternal verifies a CLI-staged caller manifest.
	PlanKindReadExternal PlanKind = "read_external"
)

// PlannedStep binds one exact generated step ID to its semantic role.
type PlannedStep struct {
	ID     string
	Number int
	Role   StepRole
}

var runtimeStepID = regexp.MustCompile(`^mt-([0-9]{3})-[0-9]{8}\.[0-9]{6}\.[0-9]{3}-[a-z0-9_-]+\z`)

// Plan is the typed verification contract generated once before launch.
type Plan struct {
	RunID      int64
	Workload   string
	Kind       PlanKind
	Producer   *PlannedStep
	Verifier   PlannedStep
	Cleanup    *PlannedStep
	Input      InputProvenance
	Multipart  bool
	AllowEmpty bool
}

// Valid reports whether the complete immutable verification shape is internally consistent.
func (p Plan) Valid() bool {
	if p.RunID <= 0 {
		return false
	}
	hasVerifier := p.Verifier != (PlannedStep{})
	if p.Kind == PlanKindWriteSeed {
		if hasVerifier {
			return false
		}
	} else if !hasVerifier || p.Verifier.ID == "" ||
		p.Verifier.Number <= 0 || p.Verifier.Role != StepRoleVerify {
		return false
	}

	seen := make(map[string]struct{}, 3)
	numbers := make(map[int]struct{}, 3)
	if hasVerifier {
		seen[p.Verifier.ID] = struct{}{}
		numbers[p.Verifier.Number] = struct{}{}
	}
	for _, step := range []*PlannedStep{p.Producer, p.Cleanup} {
		if step == nil {
			continue
		}
		if step.ID == "" || step.Number <= 0 {
			return false
		}
		if _, duplicate := seen[step.ID]; duplicate {
			return false
		}
		if _, duplicate := numbers[step.Number]; duplicate {
			return false
		}
		seen[step.ID] = struct{}{}
		numbers[step.Number] = struct{}{}
	}

	producerIs := func(role StepRole) bool {
		return p.Producer != nil && p.Producer.Role == role
	}
	switch p.Kind {
	case PlanKindWriteRead:
		return p.Workload == workload.WriteVerify && p.Input == InputWritten &&
			producerIs(StepRoleCreate) && !p.AllowEmpty &&
			(p.Cleanup == nil || p.Cleanup.Role == StepRoleCleanup)
	case PlanKindWriteSeed:
		return p.Workload == workload.WriteVerify && p.Input == InputWritten &&
			producerIs(StepRoleCreate) && p.Cleanup == nil && !p.AllowEmpty
	case PlanKindReadDiscovered:
		return p.Workload == workload.ReadVerify && p.Input == InputDiscovered &&
			producerIs(StepRoleList) && p.Cleanup == nil
	case PlanKindReadExternal:
		return p.Workload == workload.ReadVerify && p.Input == InputExternal &&
			p.Producer == nil && p.Cleanup == nil
	default:
		return false
	}
}

// VerificationDeferred reports whether the plan intentionally ends after CREATE evidence.
func (p Plan) VerificationDeferred() bool {
	return p.Kind == PlanKindWriteSeed
}

// Clone returns a detached plan, including optional step pointers.
func (p Plan) Clone() Plan {
	clone := p
	if p.Producer != nil {
		producer := *p.Producer
		clone.Producer = &producer
	}
	if p.Cleanup != nil {
		cleanup := *p.Cleanup
		clone.Cleanup = &cleanup
	}
	return clone
}

// RuntimeStepNumber extracts the stable scenario ordinal from an engine step
// ID. Runtime timestamp reassignment does not change this ordinal.
func RuntimeStepNumber(stepID string) (int, bool) {
	match := runtimeStepID.FindStringSubmatch(stepID)
	if len(match) != 2 {
		return 0, false
	}
	number, err := strconv.Atoi(match[1])
	return number, err == nil && number > 0
}

// StepIDs returns the exact planned IDs in execution-role order.
func (p Plan) StepIDs() []string {
	ids := make([]string, 0, 3)
	if p.Producer != nil {
		ids = append(ids, p.Producer.ID)
	}
	if p.Verifier.ID != "" {
		ids = append(ids, p.Verifier.ID)
	}
	if p.Cleanup != nil {
		ids = append(ids, p.Cleanup.ID)
	}
	return ids
}
