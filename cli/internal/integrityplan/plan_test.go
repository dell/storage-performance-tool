/*
Copyright © 2026 Dell Technologies
*/

package integrityplan

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestPlanValidRequiresConsistentWorkloadProvenanceAndRoles(t *testing.T) {
	create := &PlannedStep{ID: "create", Number: 1, Role: StepRoleCreate}
	list := &PlannedStep{ID: "list", Number: 1, Role: StepRoleList}
	verify := PlannedStep{ID: "verify", Number: 2, Role: StepRoleVerify}
	tests := []struct {
		name string
		plan Plan
		want bool
	}{
		{
			name: "write",
			plan: Plan{RunID: 1, Workload: workload.WriteVerify, Kind: PlanKindWriteRead, Producer: create,
				Verifier: verify, Input: InputWritten},
			want: true,
		},
		{
			name: "deferred write",
			plan: Plan{RunID: 6, Workload: workload.WriteVerify, Kind: PlanKindWriteSeed, Producer: create,
				Input: InputWritten},
			want: true,
		},
		{
			name: "discovery read",
			plan: Plan{RunID: 2, Workload: workload.ReadVerify, Kind: PlanKindReadDiscovered, Producer: list,
				Verifier: verify, Input: InputDiscovered},
			want: true,
		},
		{
			name: "external read",
			plan: Plan{RunID: 3, Workload: workload.ReadVerify, Kind: PlanKindReadExternal,
				Verifier: verify, Input: InputExternal},
			want: true,
		},
		{
			name: "write with list producer",
			plan: Plan{RunID: 4, Workload: workload.WriteVerify, Kind: PlanKindWriteRead, Producer: list,
				Verifier: verify, Input: InputWritten},
		},
		{
			name: "external read with producer",
			plan: Plan{RunID: 5, Workload: workload.ReadVerify, Kind: PlanKindReadExternal, Producer: list,
				Verifier: verify, Input: InputExternal},
		},
		{
			name: "deferred write with verifier",
			plan: Plan{RunID: 7, Workload: workload.WriteVerify, Kind: PlanKindWriteSeed, Producer: create,
				Verifier: verify, Input: InputWritten},
		},
		{
			name: "deferred write with cleanup",
			plan: Plan{RunID: 8, Workload: workload.WriteVerify, Kind: PlanKindWriteSeed, Producer: create,
				Cleanup: &PlannedStep{ID: "cleanup", Number: 2, Role: StepRoleCleanup}, Input: InputWritten},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.plan.Valid(); got != test.want {
				t.Fatalf("Valid() = %t, want %t for %+v", got, test.want, test.plan)
			}
		})
	}
}

func TestDeferredPlanExposesOnlyProducerStep(t *testing.T) {
	plan := Plan{
		RunID: 9, Workload: workload.WriteVerify, Kind: PlanKindWriteSeed,
		Producer: &PlannedStep{ID: "create", Number: 1, Role: StepRoleCreate}, Input: InputWritten,
	}
	if !plan.VerificationDeferred() {
		t.Fatal("VerificationDeferred() = false, want true")
	}
	if ids := plan.StepIDs(); len(ids) != 1 || ids[0] != "create" {
		t.Fatalf("StepIDs() = %v, want [create]", ids)
	}

}
func TestRuntimeStepNumber(t *testing.T) {
	tests := map[string]int{
		"mt-001-20260802.190000.000-list":   1,
		"mt-023-20260802.190000.000-verify": 23,
		"mt-1-20260802.190000.000-list":     0,
		"mt-001-runtime-list":               0,
	}
	for stepID, want := range tests {
		got, ok := RuntimeStepNumber(stepID)
		if got != want || ok != (want > 0) {
			t.Fatalf("RuntimeStepNumber(%q) = %d, %t; want %d, %t", stepID, got, ok, want, want > 0)
		}
	}
}
