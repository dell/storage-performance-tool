/*
Copyright © 2026 Dell Technologies
*/

package scenario

import (
	"strings"
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
		wantVersions integrityplan.DiscoveryVersions
		wantVerifier bool
		wantCleanup  bool
	}{
		{
			name: "write verify multipart cleanup",
			params: Params{WorkloadType: WorkloadTypeWriteVerify, RunID: 101,
				Bucket: "b", ObjectSize: "10MiB", ObjectCount: 1, Threads: 1,
				PartSize: "5MiB", Cleanup: true, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindWriteRead, wantInput: integrityplan.InputWritten, wantRole: integrityplan.StepRoleCreate,
			wantVersions: integrityplan.DiscoveryVersionsCurrent, wantVerifier: true,
			wantCleanup: true,
		},
		{
			name: "read verify discovery",
			params: Params{WorkloadType: WorkloadTypeReadVerify, RunID: 102,
				Bucket: "b", ObjectCount: 1, Threads: 1, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindReadDiscovered, wantInput: integrityplan.InputDiscovered, wantRole: integrityplan.StepRoleList,
			wantVersions: integrityplan.DiscoveryVersionsCurrent, wantVerifier: true,
		},
		{
			name: "read verify discovery all versions",
			params: Params{WorkloadType: WorkloadTypeReadVerify, RunID: 105,
				Bucket: "b", ObjectCount: 1, Threads: 1, Versions: VersionsAll, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindReadDiscovered, wantInput: integrityplan.InputDiscovered, wantRole: integrityplan.StepRoleList,
			wantVersions: integrityplan.DiscoveryVersionsAll, wantVerifier: true,
		},
		{
			name: "read verify external",
			params: Params{WorkloadType: WorkloadTypeReadVerify, RunID: 103,
				Bucket: "b", Threads: 1, ItemsFile: "/spt-input/items.csv",
				AllowEmptySelection: true, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindReadExternal, wantInput: integrityplan.InputExternal,
			wantVersions: integrityplan.DiscoveryVersionsCurrent, wantVerifier: true,
		},
		{
			name: "deferred write verify",
			params: Params{WorkloadType: WorkloadTypeWriteVerify, RunID: 104,
				Bucket: "b", ObjectSize: "1MiB", ObjectCount: 1, Threads: 1,
				DeferVerification: true, BaseTimestamp: "20260802.120000.000"},
			wantKind: integrityplan.PlanKindWriteSeed, wantInput: integrityplan.InputWritten,
			wantRole: integrityplan.StepRoleCreate, wantVersions: integrityplan.DiscoveryVersionsCurrent,
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
			if plan.DiscoveryVersions != test.wantVersions {
				t.Fatalf("typed plan versions = %q, want %q", plan.DiscoveryVersions, test.wantVersions)
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

func TestBuildVerificationPlanRejectsRenderedVersionDrift(t *testing.T) {
	params := Params{WorkloadType: WorkloadTypeReadVerify, RunID: 106,
		Bucket: "b", ObjectCount: 1, Threads: 1, Versions: VersionsAll,
		BaseTimestamp: "20260802.120000.000"}
	rendered, err := GenerateScenario(params)
	if err != nil {
		t.Fatal(err)
	}
	drifted := strings.Replace(rendered, `"include_versions": true`, `"include_versions": false`, 1)
	if drifted == rendered {
		t.Fatal("generated scenario is missing the all-version LIST setting")
	}
	steps, err := BuildStepPlanFromScenario(drifted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildVerificationPlan(params, steps); err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Fatalf("rendered version drift error = %v, want mismatch", err)
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
