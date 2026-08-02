/*
Copyright © 2026 Dell Technologies
*/

package runcontrol

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestPreparedRunKeepsTypedVerificationPlanImmutable(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 42, Workload: scenario.WorkloadTypeWriteVerify,
		Input: integrityplan.InputWritten,
		Producer: &integrityplan.PlannedStep{
			ID: "create-original", Number: 1, Role: integrityplan.StepRoleCreate,
		},
		Verifier: integrityplan.PlannedStep{
			ID: "verify-original", Number: 2, Role: integrityplan.StepRoleVerify,
		},
	}
	prepared := NewPreparedRun(
		scenario.Params{RunID: 42}, nil, nil, scenario.StepPlan{}, "run.js", nil, plan)
	plan.Producer.ID = "mutated-after-construction"

	got, ok := prepared.VerificationPlan()
	if !ok || got.Producer == nil || got.Producer.ID != "create-original" {
		t.Fatalf("typed plan = %+v, ok=%t", got, ok)
	}
	got.Producer.ID = "mutated-by-caller"
	again, ok := prepared.VerificationPlan()
	if !ok || again.Producer == nil || again.Producer.ID != "create-original" {
		t.Fatalf("typed plan accessor leaked mutation: %+v, ok=%t", again, ok)
	}
}
