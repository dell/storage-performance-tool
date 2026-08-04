/*
Copyright © 2026 Dell Technologies
*/

package scenario

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
)

func TestBuildVerificationPlanGeneratedRoutes(t *testing.T) {
	tests := []struct {
		name         string
		params       Params
		wantKind     integrityplan.PlanKind
		wantInput    integrityplan.InputProvenance
		wantRole     integrityplan.StepRole
		wantVerifier bool
		wantCleanup  bool
	}{
		{
			name: "write verify multipart cleanup",
			params: Params{WorkloadType: WorkloadTypeWriteVerify, RunID: 101,
				Bucket: "b", ObjectSize: "10MiB", ObjectCount: 1, Threads: 1,
				PartSize: "5MiB", Cleanup: true, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindWriteRead, wantInput: integrityplan.InputWritten, wantRole: integrityplan.StepRoleCreate, wantVerifier: true,
			wantCleanup: true,
		},
		{
			name: "read verify discovery",
			params: Params{WorkloadType: WorkloadTypeReadVerify, RunID: 102,
				Bucket: "b", ObjectCount: 1, Threads: 1, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindReadDiscovered, wantInput: integrityplan.InputDiscovered, wantRole: integrityplan.StepRoleList, wantVerifier: true,
		},
		{
			name: "read verify external",
			params: Params{WorkloadType: WorkloadTypeReadVerify, RunID: 103,
				Bucket: "b", Threads: 1, ItemsFile: "/spt-input/items.csv",
				AllowEmptySelection: true, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindReadExternal, wantInput: integrityplan.InputExternal, wantVerifier: true,
		},
		{
			name: "deferred write verify",
			params: Params{WorkloadType: WorkloadTypeWriteVerify, RunID: 104,
				Bucket: "b", ObjectSize: "1MiB", ObjectCount: 1, Threads: 1,
				DeferVerification: true, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindWriteSeed, wantInput: integrityplan.InputWritten,
			wantRole: integrityplan.StepRoleCreate,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rendered, err := GenerateScenario(test.params)
			if err != nil {
				t.Fatal(err)
			}
			steps, err := BuildStepPlanFromScenario(rendered)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := BuildVerificationPlan(test.params, steps)
			if err != nil {
				t.Fatal(err)
			}
			if !plan.Valid() || plan.RunID != test.params.RunID || plan.Kind != test.wantKind || plan.Input != test.wantInput ||
				(plan.Verifier.Role == integrityplan.StepRoleVerify) != test.wantVerifier ||
				(plan.Cleanup != nil) != test.wantCleanup {
				t.Fatalf("typed plan = %+v", plan)
			}
			if test.wantRole == "" {
				if plan.Producer != nil {
					t.Fatalf("external plan producer = %+v, want nil", plan.Producer)
				}
			} else if plan.Producer == nil || plan.Producer.Role != test.wantRole {
				t.Fatalf("producer = %+v, want role %s", plan.Producer, test.wantRole)
			}
		})
	}
}

func TestBuildVerificationPlanFailsClosedOnAmbiguousRoles(t *testing.T) {
	params := Params{WorkloadType: WorkloadTypeWriteVerify, RunID: 104}
	_, err := BuildVerificationPlan(params, StepPlan{Steps: []StepInfo{
		{ID: "create-one", Op: "create"},
		{ID: "create-two", Op: "create"},
		{ID: "verify", Op: "verify"},
	}})
	if err == nil {
		t.Fatal("duplicate CREATE roles unexpectedly produced a typed plan")
	}
}
