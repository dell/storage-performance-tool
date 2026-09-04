package scenario

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestGenerateSeededDeleteCleanupConsumesFrozenResidualAfterMeasuredPhase(t *testing.T) {
	for _, test := range []struct {
		name   string
		verify bool
	}{
		{name: "directly after timed delete"},
		{name: "after post verification", verify: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			generated, err := GenerateDeleteScenario(Params{
				WorkloadType:         workload.Delete,
				RunID:                889,
				Bucket:               "owned",
				ObjectCount:          3,
				Threads:              2,
				DeleteBatchSize:      2,
				Cleanup:              true,
				VerifyDelete:         test.verify,
				VerifyDeleteExplicit: test.verify,
				BaseTimestamp:        "20260822.120000.000",
			})
			if err != nil {
				t.Fatal(err)
			}

			plan, err := BuildStepPlanFromScenario(generated)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Steps) != 3 || plan.Steps[0].Op != stepOpSeed ||
				plan.Steps[1].Op != stepOpDelete || plan.Steps[2].Op != stepOpCleanup {
				t.Fatalf("seeded cleanup plan = %+v, want seed, delete, cleanup", plan.Steps)
			}

			configs := parseGeneratedScenarioConfigs(t, generated)
			if len(configs) != 3 {
				t.Fatalf("seeded cleanup configs = %d, want 3", len(configs))
			}
			measured, cleanup := configs[1], configs[2]
			if got, ok := configPath(measured, "load", "op", "delete", "postVerification"); ok != test.verify || (ok && got != true) {
				t.Fatalf("post-verification = %#v, present=%t, want enabled=%t", got, ok, test.verify)
			}
			if got := generatedConfigValue(t, cleanup, "item", "input", "file"); got != "residualFile" {
				t.Fatalf("cleanup input = %#v, want immutable measured residual", got)
			}
			if got := generatedConfigValue(t, cleanup, "load", "op", "type"); got != stepOpDelete {
				t.Fatalf("cleanup operation = %#v, want delete", got)
			}
			if got := generatedConfigValue(t, cleanup, "load", "op", "retry"); got != false {
				t.Fatalf("cleanup retry = %#v, want one idempotent attempt per residual identity", got)
			}
			if got := generatedConfigValue(t, cleanup, "load", "op", "limit", "fail", "count"); got != float64(0) {
				t.Fatalf("cleanup failure count limit = %#v, want unlimited best effort", got)
			}
			if got := generatedConfigValue(t, cleanup, "load", "op", "wait", "finish"); got != true {
				t.Fatalf("cleanup wait-finish = %#v, want terminal cleanup metrics", got)
			}
			if got := generatedConfigValue(t, cleanup, "load", "step", "id"); got != plan.Steps[2].ID {
				t.Fatalf("cleanup step id = %#v, want %q", got, plan.Steps[2].ID)
			}
			if _, ok := configPath(cleanup, "load", "op", "delete", "standalone"); ok {
				t.Fatal("best-effort cleanup entered the measured standalone DELETE spine")
			}
			if _, ok := configPath(cleanup, "storage", "integrity"); ok {
				t.Fatal("cleanup must accept the immutable residual without requiring a producer sidecar")
			}
			if _, ok := configPath(cleanup, "item", "output"); ok {
				t.Fatal("cleanup must not rewrite the measured residual inventory")
			}

			residualDeclaration := "var residualFile = homeDir + \"/log/\" + deleteStep + \"/items.csv\";"
			cleanupStart := strings.LastIndex(generated, "DeleteLoad.config")
			measuredStart := strings.Index(generated, "DeleteLoad.config")
			measuredGuard := "SeededDeleteCleanupFinalizer.rethrowIfInterrupted(failure)"
			if !strings.Contains(generated, residualDeclaration) ||
				measuredStart < 0 || cleanupStart <= measuredStart ||
				!strings.Contains(generated[measuredStart:cleanupStart], measuredGuard) ||
				!strings.Contains(generated[measuredStart:cleanupStart], "benchmarkFailure = failure") {
				t.Fatalf("cleanup is not guarded behind measured residual finalization:\n%s", generated)
			}
			if !strings.Contains(generated[cleanupStart:], "SeededDeleteCleanupFinalizer.finish") ||
				!strings.Contains(generated[cleanupStart:], "cleanupFailure, cleanupLoad, residualFile") {
				t.Fatalf("cleanup did not preserve the measured verdict through the engine finalizer:\n%s", generated)
			}
		})
	}
}
