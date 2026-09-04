package summary

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
)

func TestStoredSummaryModelRendersDetailedDeleteContractWithoutChangingGenericTotals(t *testing.T) {
	latency := &deletemetrics.TimingStat{Count: 2, MeanUs: 20, P50Us: 15, P90Us: 25, P99Us: 35, P999Us: 45}
	duration := &deletemetrics.TimingStat{Count: 2, MeanUs: 60, P50Us: 55, P90Us: 65, P99Us: 75, P999Us: 85}
	seed := 1.25
	scheduled := 1.5
	drain := 0.5
	total := 3.25
	stored := &deletemetrics.Metrics{
		Units: deletemetrics.Units{
			Requests: deletemetrics.RequestUnit,
			Objects:  deletemetrics.ObjectUnit,
			Batches:  deletemetrics.RequestUnit,
		},
		Requests:   deletemetrics.Requests{Attempted: 2, FullSuccess: 1, Partial: 1, PerSecond: 4.5},
		Objects:    deletemetrics.Objects{Selected: 175, Attempted: 175, Accepted: 174, Failed: 1, PerSecond: 393.75},
		Batches:    deletemetrics.Batches{ConfiguredSize: 100, ActualRequestCount: 2, ActualObjectCount: 175, MeanObjectsPerRequest: 87.5, FullBatchCount: 1, PartialBatchCount: 1, FullBatchPercent: 50},
		Completion: deletemetrics.Completion{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
		Versions:   deletemetrics.Versions{CurrentKey: 174, ExactVersion: 1},
		Buckets:    []deletemetrics.Bucket{{Bucket: "bucket-a", Selected: 175, Attempted: 175, Accepted: 174, Failed: 1}},
		Phases: deletemetrics.Phases{
			SeedSeconds: &seed, ScheduledDeleteSeconds: &scheduled, DrainSeconds: &drain, TotalWallSeconds: &total,
		},
		Identity:      deletemetrics.Identity{Mode: "batch", ConfiguredBatchSize: 100, SelectionOrder: "canonical"},
		FailurePolicy: deletemetrics.FailurePolicy{Mode: "percentage", Outcome: deletemetrics.OutcomeCompletedWithinFailureBudget, MaxFailurePercent: 2.5, GraceSeconds: 30, OperationalFailedObjects: 1, ObservedFailurePercent: 0.5714285714},
		Timing: deletemetrics.Timing{
			LatencyDefinition: deletemetrics.LatencyDefinition, DurationDefinition: deletemetrics.DurationDefinition,
			Latency: latency, Duration: duration,
		},
		Performance: deletemetrics.Performance{
			ObjectSize: deletemetrics.NotApplicable, DataMoved: deletemetrics.NotApplicable,
			Bandwidth: deletemetrics.NotApplicable, TTFB: deletemetrics.NotApplicable,
		},
		OutcomeTerminology: deletemetrics.OutcomeAccepted,
		Verification:       deletemetrics.Verification{Notice: deletemetrics.VerificationNotice},
		TerminalReconciled: true,
	}
	report := NewRenderer(RenderOptions{}).FullReport(&RunSummary{
		RunID: "run",
		Steps: []StepSummary{{PhaseLabel: "Delete", Operation: "DELETE", Delete: stored}},
	})
	for _, want := range []string{
		"DELETE Results", "attempted 2", "accepted 174", "p99.9 45 us",
		"Outcome            completed within failure budget",
		"object size, data moved, bandwidth, TTFB, object latency", "not confirmed object removal",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("stored DELETE model omitted %q:\n%s", want, report)
		}
	}
}

func TestBuildStepSummariesCarriesRuntimeDeleteModelIntoStoredSummary(t *testing.T) {
	stored := &deletemetrics.Metrics{
		Requests:           deletemetrics.Requests{Attempted: 1, FullSuccess: 1},
		Objects:            deletemetrics.Objects{Selected: 1, Attempted: 1, Accepted: 1},
		OutcomeTerminology: deletemetrics.OutcomeAccepted,
		TerminalReconciled: true,
	}
	data := &RunData{
		StepOrder: []string{"delete-step"},
		Steps: map[string]*StepData{
			"delete-step": {StepID: "delete-step", Status: StepStatusComplete, Delete: stored},
		},
	}
	steps, _, _ := buildStepSummaries(data, WorkloadSummary{Type: "delete"}, nil)
	if len(steps) != 1 || steps[0].Delete != stored {
		t.Fatalf("production summary path lost runtime DELETE model: %+v", steps)
	}
}

func TestAggregateAppendsSeededCleanupTimingWithoutMergingCleanupOutcome(t *testing.T) {
	measuredWall := 2.5
	stored := &deletemetrics.Metrics{
		Phases:             deletemetrics.Phases{TotalWallSeconds: &measuredWall},
		OutcomeTerminology: deletemetrics.OutcomeAccepted,
		TerminalReconciled: true,
	}
	deleteStep := "mt-002-20260824.140000.000-delete"
	cleanupStep := "mt-003-20260824.140000.000-cleanup"
	data := &RunData{
		Params: &RunParams{
			WorkloadType: "delete",
			ScenarioParams: ScenarioParams{
				WorkloadType: "delete",
				Cleanup:      true,
			},
		},
		StepOrder: []string{deleteStep, cleanupStep},
		Steps: map[string]*StepData{
			deleteStep: {StepID: deleteStep, Status: StepStatusComplete, Delete: stored},
			cleanupStep: {
				StepID: cleanupStep,
				Status: StepStatusPartial,
				Metrics: &MetricsTotals{Rows: []MetricsTotalsRow{{
					Operation: "DELETE", SuccessCount: 2, FailureCount: 1, StepDurationSeconds: 0.75,
				}}},
			},
		},
	}

	summary, err := Aggregate(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Steps) != 2 || summary.Steps[1].PhaseLabel != "Cleanup" ||
		summary.Steps[1].Status != StepStatusPartial || summary.Steps[1].Metrics == nil ||
		summary.Steps[1].Metrics.FailureCount != 1 {
		t.Fatalf("cleanup outcome was not retained as its own stored step: %+v", summary.Steps)
	}
	measured := summary.Steps[0].Delete
	if measured == nil || measured.Phases.CleanupSeconds == nil ||
		*measured.Phases.CleanupSeconds != 0.75 || measured.Phases.TotalWallSeconds == nil ||
		*measured.Phases.TotalWallSeconds != 3.25 {
		t.Fatalf("measured DELETE lifecycle did not append cleanup timing: %+v", measured)
	}
	if stored.Phases.CleanupSeconds != nil || *stored.Phases.TotalWallSeconds != measuredWall {
		t.Fatalf("aggregation mutated the immutable measured DELETE model: %+v", stored.Phases)
	}
}
