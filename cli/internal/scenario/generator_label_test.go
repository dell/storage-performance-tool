package scenario

import (
	"fmt"
	"strings"
	"testing"
)

func TestLabelsAcrossGeneratedWorkloads(t *testing.T) {
	for _, workload := range []string{"write", "read", "mock", "list", "mixed", "write-verify", "read-verify", "delete", "tables"} {
		for _, cleanup := range []bool{false, true} {
			for _, label := range []string{"", "qs-write", "run 42/\"", strings.Repeat("a", 80)} {
				t.Run(fmt.Sprintf("%s/cleanup=%t/label=%q", workload, cleanup, label), func(t *testing.T) {
					p := Params{WorkloadType: workload, Bucket: "bucket", Threads: 1, ObjectSize: "1KiB", ObjectCount: 3, RunID: 42,
						Label: label, Cleanup: cleanup, BaseTimestamp: "20261001.120000.000", SaveItems: true,
						GetDistrib: 45, PutDistrib: 15, DeleteDistrib: 10, StatDistrib: 30, DeleteBatchSize: DefaultDeleteBatchSize}
					if workload == "mixed" {
						p.Duration = "1s"
						p.ObjectCount = 0
					}
					text, err := GenerateScenario(p)
					if err != nil {
						t.Fatal(err)
					}
					plan, err := BuildStepPlanFromScenario(text)
					if err != nil {
						t.Fatal(err)
					}
					for _, step := range plan.Steps {
						if !strings.HasPrefix(step.ID, SanitizeLabel(label)+"-") {
							t.Fatalf("unlabeled step: %s", step.ID)
						}
					}
					if label != "" && strings.Contains(text, "mt-") {
						t.Fatal("default prefix leaked into labeled scenario")
					}
					// Repeated rendering must preserve identities used by results collection.
					again, err := GenerateScenario(p)
					if err != nil {
						t.Fatal(err)
					}
					againPlan, err := BuildStepPlanFromScenario(again)
					if err != nil {
						t.Fatal(err)
					}
					if fmt.Sprint(plan) != fmt.Sprint(againPlan) {
						t.Fatal("step identities drifted on regeneration")
					}
				})
			}
		}
	}
}
