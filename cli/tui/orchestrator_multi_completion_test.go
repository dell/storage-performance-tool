/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
)

// TestShouldSignalCompletion proves and guards the fix for premature headless
// completion: the engine reports test_state=Completed whenever ops have started
// but instantaneous concurrency is 0. The async Netty driver hits that
// transiently between operations at low thread counts, so the headless
// coordinator used to abort a duration-bounded run within seconds of starting.
func TestShouldSignalCompletion(t *testing.T) {
	tests := []struct {
		name string
		agg  *PerformanceMetric
		want bool
	}{
		{
			name: "nil aggregate never completes",
			agg:  nil,
			want: false,
		},
		{
			name: "running state does not complete",
			agg:  &PerformanceMetric{TestState: constants.TestStateRunning},
			want: false,
		},
		{
			// THE FAULT: transient Completed only 5s into a 120s run.
			name: "transient completed during time-bounded run does not complete",
			agg: &PerformanceMetric{
				TestState:    constants.TestStateCompleted,
				HasLimit:     true,
				LimitType:    constants.LimitTypeTime,
				LimitTimeSec: 120,
				StepTime:     5,
			},
			want: false,
		},
		{
			name: "completed at time limit completes",
			agg: &PerformanceMetric{
				TestState:    constants.TestStateCompleted,
				HasLimit:     true,
				LimitType:    constants.LimitTypeTime,
				LimitTimeSec: 120,
				StepTime:     120,
			},
			want: true,
		},
		{
			name: "completed past time limit completes",
			agg: &PerformanceMetric{
				TestState:    constants.TestStateCompleted,
				HasLimit:     true,
				LimitType:    constants.LimitTypeTime,
				LimitTimeSec: 120,
				StepTime:     130,
			},
			want: true,
		},
		{
			name: "completed op-count below 100 percent does not complete",
			agg: &PerformanceMetric{
				TestState:         constants.TestStateCompleted,
				HasLimit:          true,
				LimitType:         constants.LimitTypeOpCount,
				CompletionPercent: 99,
			},
			want: false,
		},
		{
			name: "completed op-count at 100 percent completes",
			agg: &PerformanceMetric{
				TestState:         constants.TestStateCompleted,
				HasLimit:          true,
				LimitType:         constants.LimitTypeOpCount,
				CompletionPercent: 100,
			},
			want: true,
		},
		{
			name: "completed unbounded run completes",
			agg: &PerformanceMetric{
				TestState: constants.TestStateCompleted,
				HasLimit:  false,
			},
			want: true,
		},
		{
			name: "partial fleet never completes",
			agg: &PerformanceMetric{
				TestState: constants.TestStateCompleted,
				Partial:   true,
			},
			want: false,
		},
		{
			name: "unreconciled detailed delete never completes",
			agg: &PerformanceMetric{
				TestState: constants.TestStateCompleted,
				OpType:    "DELETE",
				Delete: &DeleteMetrics{
					Completion:         DeleteCompletionMetrics{RequestPercent: 100, ObjectPercent: 100},
					TerminalReconciled: false,
				},
			},
			want: false,
		},
		{
			name: "reconciled detailed delete completes",
			agg: &PerformanceMetric{
				TestState: constants.TestStateCompleted,
				OpType:    "DELETE",
				Delete: &DeleteMetrics{
					Completion: DeleteCompletionMetrics{
						RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true,
					},
					TerminalReconciled: true,
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldSignalCompletion(tt.agg); got != tt.want {
				t.Errorf("shouldSignalCompletion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldSignalCompletionForExpectedSteps(t *testing.T) {
	expectedSteps := []string{
		"mt-001-20260531.002657.899-seed",
		"mt-002-20260531.002657.899-read",
	}

	seedComplete := &PerformanceMetric{
		StepID:            expectedSteps[0],
		TestState:         constants.TestStateCompleted,
		HasLimit:          true,
		LimitType:         constants.LimitTypeOpCount,
		CompletionPercent: 100,
	}
	if shouldSignalCompletionForExpectedSteps(seedComplete, expectedSteps) {
		t.Fatal("seed step completion must not signal whole-scenario completion")
	}

	readComplete := &PerformanceMetric{
		StepID:       expectedSteps[1],
		TestState:    constants.TestStateCompleted,
		HasLimit:     true,
		LimitType:    constants.LimitTypeTime,
		LimitTimeSec: 120,
		StepTime:     120,
	}
	if !shouldSignalCompletionForExpectedSteps(readComplete, expectedSteps) {
		t.Fatal("final read step completion should signal whole-scenario completion")
	}
}

func TestShouldSignalCompletionForExpectedStepsFallback(t *testing.T) {
	complete := &PerformanceMetric{
		TestState: constants.TestStateCompleted,
		HasLimit:  false,
	}
	if !shouldSignalCompletionForExpectedSteps(complete, nil) {
		t.Fatal("missing expected step IDs should preserve existing completion behavior")
	}
}

func TestDistributedCompletionRetainsRejectedDeleteOutcome(t *testing.T) {
	metric := deleteNodeMetric("single", 1, 1, 1)
	metric.TestState = constants.TestStateCompleted
	metric.Delete.Timing.Latency = testTimingStat(1, 7)
	metric.Delete.Timing.Duration = testTimingStat(1, 11)
	metric.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeFailed
	update := &MultiNodeMetricsUpdate{
		Aggregated: metric,
		PerOpType:  map[string]*PerformanceMetric{"DELETE": metric},
	}

	orchestrator := &MultiHostTestOrchestrator{completionCh: make(chan struct{})}
	failure := terminalDeletePolicyFailure(update)
	if failure == nil {
		t.Fatal("failed DELETE policy outcome was not recognized")
	}
	orchestrator.recordCompletionFailure(failure)
	orchestrator.signalCompletion()
	select {
	case <-orchestrator.CompletionCh():
	default:
		t.Fatal("distributed terminal failure did not finish presentation")
	}
	if err := orchestrator.CompletionError(); err == nil ||
		!strings.Contains(err.Error(), "DELETE failure policy") {
		t.Fatalf("distributed completion error = %v, want rejected DELETE outcome", err)
	}
}
