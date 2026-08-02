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
		RunID: 55, Workload: workload.WriteVerify, Input: integrityplan.InputWritten,
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
		RunID: 55, Workload: workload.WriteVerify, Input: integrityplan.InputWritten,
		Producer: &integrityplan.PlannedStep{
			ID: "producer-with-custom-name", Number: 1, Role: integrityplan.StepRoleCreate,
		},
		Verifier: integrityplan.PlannedStep{
			ID: "reader-with-custom-name", Number: 2, Role: integrityplan.StepRoleVerify,
		},
	}
	observed := []string{"mt-001-20260802.190000.000-create", "mt-002-20260802.190000.000-verify"}
	got := ObservedStepRoles(plan, observed)
	if got != (StepRoles{Create: observed[0], Read: observed[1]}) {
		t.Fatalf("observed typed roles = %+v", got)
	}
}

func TestObservedStepRolesDoesNotBindAmbiguousRuntimeOrdinal(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.ReadVerify, Input: integrityplan.InputExternal,
		Verifier: integrityplan.PlannedStep{
			ID: "planned-reader", Number: 1, Role: integrityplan.StepRoleVerify,
		},
	}
	observed := []string{
		"mt-001-20260802.190000.000-verify",
		"mt-001-20260802.190001.000-other",
	}
	if got := ObservedStepRoles(plan, observed); got != (StepRoles{}) {
		t.Fatalf("ambiguous ordinal bound typed role: %+v", got)
	}
}

func TestFinalizeResultsRejectsIdentityThatConflictsWithTypedPlan(t *testing.T) {
	plan := integrityplan.Plan{
		RunID: 55, Workload: workload.WriteVerify, Input: integrityplan.InputWritten,
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
