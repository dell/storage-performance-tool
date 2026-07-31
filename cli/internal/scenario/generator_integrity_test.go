package scenario

import (
	"strings"
	"testing"
)

func TestGenerateWriteVerifyScenarioContract(t *testing.T) {
	got, err := GenerateWriteVerifyScenario(Params{
		WorkloadType: WorkloadTypeWriteVerify,
		RunID:        42, Bucket: "bucket", ObjectSize: "1MiB", ObjectCount: 3,
		Threads: 2, Cleanup: true, S3Driver: S3DriverAws,
		BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	required := []string{
		"CreateLoad.config", "ReadLoad.config", "DeleteLoad.config",
		"written.csv", "verified.csv", `"mode": "metadata"`,
		`"provenance": "engine_step"`, `"count": 0`, `"type": "s3-aws"`,
	}
	for _, want := range required {
		if !strings.Contains(got, want) {
			t.Errorf("scenario missing %q\n%s", want, got)
		}
	}
	plan, err := BuildStepPlanFromScenario(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %+v", plan.Steps)
	}
}

func TestGenerateReadVerifyScenarioDiscoveryAndStagedInput(t *testing.T) {
	discovery, err := GenerateReadVerifyScenario(Params{
		WorkloadType: WorkloadTypeReadVerify, RunID: 9, Bucket: "b", Prefix: "p/",
		ObjectCount: 7, Threads: 4, BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Load.config", "ReadLoad.config", "verify-input.csv", `"maxCount": 7`, `"fetch_metadata": true`} {
		if !strings.Contains(discovery, want) {
			t.Errorf("discovery scenario missing %q", want)
		}
	}

	staged, err := GenerateReadVerifyScenario(Params{
		WorkloadType: WorkloadTypeReadVerify, RunID: 10, Bucket: "b", Threads: 1,
		ItemsFile: "/spt-input/items/verify-input.csv", BaseTimestamp: "20260730.120000.000",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(staged, "\nLoad.config") || !strings.Contains(staged, `"provenance": "cli_stager"`) ||
		!strings.Contains(staged, "spt-cli-items-stager-v1") {
		t.Fatalf("unexpected staged scenario:\n%s", staged)
	}
}

func TestGenerateDefaultsIncludesVerificationRunID(t *testing.T) {
	got, err := GenerateDefaults(Params{
		WorkloadType: WorkloadTypeReadVerify, RunID: 123,
		Endpoint: "http://localhost:9000", AccessKey: "a", SecretKey: "s", Threads: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "run:\n    id: 123") && !strings.Contains(string(got), "run:\n  id: 123") {
		t.Fatalf("defaults missing run id:\n%s", got)
	}
}
