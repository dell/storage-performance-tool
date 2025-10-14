package tui

import (
	"encoding/json"
	"testing"
	"time"
)

const testSampleTimestamp = "2025-09-23T16:18:56.000000000Z"

func newTestStep() JSONMetricsStep {
	return JSONMetricsStep{
		MetricsSchema:      2,
		Scope:              "node",
		Role:               "entry",
		ClusterID:          "cluster-test",
		NodeID:             "node-1",
		RunID:              "run-1",
		SampleTimestampRaw: testSampleTimestamp,
		StepID:             "step1",
		OpType:             "create",
		Timestamp:          time.Now().UnixMilli(),
		ElapsedTimeSeconds: 30.5,
		TestState:          1,
		CompletionPercent:  38,
		OverallCompletion:  40,
		Unbounded:          false,
		OverallUnbounded:   false,
		Operations: JSONMetricsOperations{
			SuccessCount:    1000,
			FailedCount:     5,
			SuccessRateLast: 150.5,
			FailedRateLast:  0.2,
		},
		Bandwidth: JSONMetricsBandwidth{
			BytesTotal:    1_048_576_000,
			BytesRateLast: 52_428_800.0,
		},
		Timing: JSONMetricsTiming{
			LatencyMeanUs:  2500.0,
			DurationMeanUs: 3000.0,
		},
		Concurrency: JSONMetricsConcurrency{
			Current: 8,
			Mean:    7.5,
		},
		Limit: &JSONMetricsLimit{
			Type:    "time",
			TimeSec: 300,
		},
	}
}

func marshalSteps(t *testing.T, steps []JSONMetricsStep) string {
	t.Helper()
	data, err := json.Marshal(steps)
	if err != nil {
		t.Fatalf("failed to marshal steps: %v", err)
	}
	return string(data)
}

func marshalWrappedSteps(t *testing.T, steps []JSONMetricsStep) string {
	t.Helper()
	payload := map[string]any{"steps": steps}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal wrapped steps: %v", err)
	}
	return string(data)
}
