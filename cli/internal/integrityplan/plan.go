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
	Producer   *PlannedStep
	Verifier   PlannedStep
	Cleanup    *PlannedStep
	Input      InputProvenance
	Multipart  bool
	AllowEmpty bool
}

// Valid reports whether the minimum immutable verification identity exists.
func (p Plan) Valid() bool {
	if p.RunID <= 0 || p.Verifier.ID == "" || p.Verifier.Role != StepRoleVerify {
		return false
	}
	if p.Verifier.Number <= 0 ||
		(p.Cleanup != nil && (p.Cleanup.ID == "" || p.Cleanup.Number <= 0 || p.Cleanup.Role != StepRoleCleanup)) {
		return false
	}
	seen := map[string]struct{}{p.Verifier.ID: {}}
	numbers := map[int]struct{}{p.Verifier.Number: {}}
	for _, step := range []*PlannedStep{p.Producer, p.Cleanup} {
		if step == nil {
			continue
		}
		if step.Number <= 0 {
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
	switch p.Workload {
	case workload.WriteVerify:
		return p.Input == InputWritten && p.Producer != nil &&
			p.Producer.ID != "" && p.Producer.Role == StepRoleCreate
	case workload.ReadVerify:
		switch p.Input {
		case InputDiscovered:
			return p.Producer != nil && p.Producer.ID != "" && p.Producer.Role == StepRoleList
		case InputExternal:
			return p.Producer == nil
		default:
			return false
		}
	default:
		return false
	}
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
	ids = append(ids, p.Verifier.ID)
	if p.Cleanup != nil {
		ids = append(ids, p.Cleanup.ID)
	}
	return ids
}
