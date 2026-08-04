/*
Copyright © 2026 Dell Technologies
*/

package integrity

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestPlannedStepRolesUsesTypedRolesWithoutIDSuffixes(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.WriteVerify, Kind: integrityplan.PlanKindWriteRead, Input: integrityplan.InputWritten,
		Producer: &integrityplan.PlannedStep{
			ID: "producer-with-custom-name", Number: 1, Role: integrityplan.StepRoleCreate,
		},
		Verifier: integrityplan.PlannedStep{
			ID: "reader-with-custom-name", Number: 2, Role: integrityplan.StepRoleVerify,
		},
	}
	got := PlannedStepRoles(plan)
	if got.Create != "producer-with-custom-name" || got.Read != "reader-with-custom-name" || got.List != "" {
		t.Fatalf("typed roles = %+v", got)
	}
}

func TestObservedStepRolesMatchesOnlyExactTypedIDs(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.WriteVerify, Kind: integrityplan.PlanKindWriteRead, Input: integrityplan.InputWritten,
		Producer: &integrityplan.PlannedStep{
			ID: "producer-with-custom-name", Number: 1, Role: integrityplan.StepRoleCreate,
		},
		Verifier: integrityplan.PlannedStep{
			ID: "reader-with-custom-name", Number: 2, Role: integrityplan.StepRoleVerify,
		},
	}
	observed := []string{"mt-001-20260802.190000.000-create", "mt-002-20260802.190000.000-verify"}
	binding, err := BindObservedStepRoles(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Roles != (StepRoles{Create: observed[0], Read: observed[1]}) {
		t.Fatalf("observed typed roles = %+v", binding.Roles)
	}
}

func TestObservedStepRolesBindsDeferredProducerOnly(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 56, Workload: workload.WriteVerify, Kind: integrityplan.PlanKindWriteSeed,
		Input: integrityplan.InputWritten,
		Producer: &integrityplan.PlannedStep{
			ID: "producer-with-custom-name", Number: 1, Role: integrityplan.StepRoleCreate,
		},
	}
	observed := []string{"mt-001-20260802.190000.000-create"}
	binding, err := BindObservedStepRoles(plan, observed)
	if err != nil {
		t.Fatal(err)
	}
	if !binding.Evidence || binding.Roles != (StepRoles{Create: observed[0]}) {
		t.Fatalf("observed deferred roles = %+v", binding)
	}
	if got := PlannedStepRoles(plan); got != (StepRoles{Create: "producer-with-custom-name"}) {
		t.Fatalf("planned deferred roles = %+v", got)
	}
}

func TestObservedStepRolesRejectsAmbiguousRuntimeOrdinal(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.ReadVerify, Kind: integrityplan.PlanKindReadExternal, Input: integrityplan.InputExternal,
		Verifier: integrityplan.PlannedStep{
			ID: "planned-reader", Number: 1, Role: integrityplan.StepRoleVerify,
		},
	}
	observed := []string{
		"mt-001-20260802.190000.000-verify",
		"mt-001-20260802.190001.000-other",
	}
	if _, err := BindObservedStepRoles(plan, observed); err == nil {
		t.Fatal("ambiguous ordinal unexpectedly bound a typed role")
	}
}

func TestObservedStepRolesRejectsExactIdentityWithOrdinalConflict(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.ReadVerify, Kind: integrityplan.PlanKindReadExternal, Input: integrityplan.InputExternal,
		Verifier: integrityplan.PlannedStep{
			ID: "planned-reader", Number: 1, Role: integrityplan.StepRoleVerify,
		},
	}
	observed := []string{"planned-reader", "mt-001-20260802.190001.000-other"}
	if _, err := BindObservedStepRoles(plan, observed); err == nil {
		t.Fatal("exact identity overrode conflicting ordinal evidence")
	}
}

func TestObservedStepRolesRejectsDuplicateIdentityEvidence(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.ReadVerify, Kind: integrityplan.PlanKindReadExternal, Input: integrityplan.InputExternal,
		Verifier: integrityplan.PlannedStep{
			ID: "planned-reader", Number: 1, Role: integrityplan.StepRoleVerify,
		},
	}
	if _, err := BindObservedStepRoles(plan, []string{"planned-reader", "planned-reader"}); err == nil {
		t.Fatal("duplicate runtime identity unexpectedly accepted")
	}
}

func TestObservedStepRolesDistinguishesMissingCompatibilityEvidence(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.ReadVerify, Kind: integrityplan.PlanKindReadExternal, Input: integrityplan.InputExternal,
		Verifier: integrityplan.PlannedStep{
			ID: "planned-reader", Number: 1, Role: integrityplan.StepRoleVerify,
		},
	}
	binding, err := BindObservedStepRoles(plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Evidence || binding.Roles != (StepRoles{}) {
		t.Fatalf("missing evidence binding = %+v", binding)
	}
}

func TestFinalizeResultsRejectsAmbiguousRuntimeIdentityBeforeArtifacts(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.ReadVerify, Kind: integrityplan.PlanKindReadExternal, Input: integrityplan.InputExternal,
		Verifier: integrityplan.PlannedStep{
			ID: "planned-reader", Number: 1, Role: integrityplan.StepRoleVerify,
		},
	}
	_, err := FinalizeResults(FinalizeOptions{
		Plan: plan, ResultsRoot: t.TempDir(),
		ObservedStepIDs: []string{"planned-reader", "mt-001-20260802.190001.000-other"},
	})
	if err == nil {
		t.Fatal("finalizer accepted ambiguous runtime identity evidence")
	}
}

func TestFinalizeResultsRejectsIdentityThatConflictsWithTypedPlan(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.WriteVerify, Kind: integrityplan.PlanKindWriteRead, Input: integrityplan.InputWritten,
		Producer: &integrityplan.PlannedStep{
			ID: "create", Number: 1, Role: integrityplan.StepRoleCreate,
		},
		Verifier: integrityplan.PlannedStep{
			ID: "verify", Number: 2, Role: integrityplan.StepRoleVerify,
		},
	}
	if _, err := FinalizeResults(FinalizeOptions{
		Plan: plan, Workload: workload.ReadVerify, RunID: plan.RunID,
	}); err == nil {
		t.Fatal("typed plan accepted a conflicting finalizer workload")
	}
}
