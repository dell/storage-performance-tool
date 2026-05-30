/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldSignalCompletion(tt.agg); got != tt.want {
				t.Errorf("shouldSignalCompletion() = %v, want %v", got, tt.want)
			}
		})
	}
}
