package scenario

import (
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestGenerateDeleteScenarioWiresFixedObjectFailureBudgetByDefault(t *testing.T) {
	generated, err := GenerateDeleteScenario(Params{
		WorkloadType: workload.Delete, RunID: 1101, Bucket: "owned", Threads: 1,
		DeleteBatchSize: DefaultDeleteBatchSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	configs := parseGeneratedScenarioConfigs(t, generated)
	budget := generatedConfigValue(t, configs[len(configs)-1], "load", "op", "failureBudget")
	want := map[string]any{
		"mode": "fixed", "maxFailedObjects": float64(DefaultMaxFailedObjects),
		"maxFailurePercent": float64(0), "graceSeconds": float64(30),
	}
	if got, ok := budget.(map[string]any); !ok || !sameFailureBudget(got, want) {
		t.Fatalf("default failure budget = %#v, want %#v", budget, want)
	}
}

func TestGenerateDeleteScenarioWiresPercentageFailureBudget(t *testing.T) {
	generated, err := GenerateDeleteScenario(Params{
		WorkloadType: workload.Delete, RunID: 1102, ItemsFile: "/input/items.csv", Threads: 1,
		DeleteBatchSize: 1, FailureBudgetMode: FailureBudgetModePercentage,
		MaxFailurePercent: 12.5, FailureBudgetGrace: 45 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	config := parseGeneratedScenarioConfigs(t, generated)[0]
	if got := generatedConfigValue(t, config, "load", "op", "failureBudget", "mode"); got != "percentage" {
		t.Fatalf("failure budget mode = %#v", got)
	}
	if got := generatedConfigValue(t, config, "load", "op", "failureBudget", "maxFailurePercent"); got != 12.5 {
		t.Fatalf("failure percentage = %#v", got)
	}
	if got := generatedConfigValue(t, config, "load", "op", "failureBudget", "graceSeconds"); got != float64(45) {
		t.Fatalf("failure grace = %#v", got)
	}
}

func TestGenerateDeleteScenarioRejectsUnrepresentableFailureBudgetGrace(t *testing.T) {
	for _, grace := range []time.Duration{-time.Second, 1500 * time.Millisecond} {
		t.Run(grace.String(), func(t *testing.T) {
			_, err := GenerateDeleteScenario(Params{
				WorkloadType: workload.Delete, RunID: 1103, ItemsFile: "/input/items.csv", Threads: 1,
				DeleteBatchSize: 1, FailureBudgetMode: FailureBudgetModePercentage,
				MaxFailurePercent: 12.5, FailureBudgetGrace: grace,
			})
			if err == nil {
				t.Fatalf("GenerateDeleteScenario() accepted failure grace %s", grace)
			}
		})
	}
}

func sameFailureBudget(got, want map[string]any) bool {
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return len(got) == len(want)
}
