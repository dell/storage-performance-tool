package scenario

import "testing"

func TestBuildStepPlanFromScenario_WriteOnly(t *testing.T) {
	s, err := GenerateWriteScenario(Params{
		WorkloadType: "write",
		Endpoint:     "http://minio:9000",
		AccessKey:    "k",
		SecretKey:    "s",
		Bucket:       "b",
		Threads:      1,
		ObjectSize:   "1MB",
		ObjectCount:  5,
		Cleanup:      false,
	})
	if err != nil {
		t.Fatalf("GenerateWriteScenario error: %v", err)
	}
	plan, err := BuildStepPlanFromScenario(s)
	if err != nil {
		t.Fatalf("BuildStepPlanFromScenario error: %v\nScenario:\n%s", err, s)
	}
	if len(plan.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(plan.Steps))
	}
	if plan.Steps[0].Number != 1 || plan.Steps[0].Op != "create" {
		t.Fatalf("unexpected step: %#v", plan.Steps[0])
	}
}

func TestBuildStepPlanFromScenario_MockCleanup(t *testing.T) {
	s, err := GenerateMockScenario(Params{
		WorkloadType: "mock",
		Threads:      2,
		ObjectSize:   "1MB",
		ObjectCount:  3,
		Cleanup:      true,
	})
	if err != nil {
		t.Fatalf("GenerateMockScenario error: %v", err)
	}
	plan, err := BuildStepPlanFromScenario(s)
	if err != nil {
		t.Fatalf("BuildStepPlanFromScenario error: %v\nScenario:\n%s", err, s)
	}
	if len(plan.Steps) < 2 {
		t.Fatalf("expected at least 2 steps, got %d", len(plan.Steps))
	}
	if plan.Steps[0].Number != 1 || plan.Steps[0].Op != "create" {
		t.Fatalf("unexpected first step: %#v", plan.Steps[0])
	}
	if plan.Steps[1].Number != 2 || plan.Steps[1].Op != "delete" {
		t.Fatalf("unexpected second step: %#v", plan.Steps[1])
	}
}

func TestBuildStepPlanFromScenarioRejectsDuplicateStepIDs(t *testing.T) {
	scenario := `
		var first = {"id":"mt-001-20260802.120000.000-create"};
		var duplicate = {"id":"mt-001-20260802.120000.000-create"};
	`
	if _, err := BuildStepPlanFromScenario(scenario); err == nil {
		t.Fatal("duplicate rendered step IDs unexpectedly produced a plan")
	}
}
