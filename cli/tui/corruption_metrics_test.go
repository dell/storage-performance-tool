/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"encoding/json"
	"testing"
	"time"
)

func TestStepToMetricPreservesCorruptCountPresence(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    int64
		has     bool
	}{
		{name: "new engine", payload: `{"operations":{"success_count":2,"failed_count":3,"corrupt_count":1}}`, want: 1, has: true},
		{name: "ordinary legacy engine", payload: `{"operations":{"success_count":2,"failed_count":3}}`, want: 0, has: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var step JSONMetricsStep
			if err := json.Unmarshal([]byte(tt.payload), &step); err != nil {
				t.Fatal(err)
			}
			metric := stepToMetric(&step, "node", time.Unix(0, 0))
			if metric.CorruptCount != tt.want || metric.HasCorruptCount != tt.has {
				t.Fatalf("corrupt count/presence = %d/%t, want %d/%t", metric.CorruptCount, metric.HasCorruptCount, tt.want, tt.has)
			}
		})
	}
}

func TestMetricsAggregatorRequiresCorruptCountFromEverySource(t *testing.T) {
	aggregator := NewMetricsAggregator()
	complete := aggregator.Aggregate(map[string]*PerformanceMetric{
		"a": {Scope: "node", MetricsSchema: 3, CorruptCount: 1, HasCorruptCount: true},
		"b": {Scope: "node", MetricsSchema: 3, CorruptCount: 2, HasCorruptCount: true},
	})
	if complete == nil || !complete.HasCorruptCount || complete.CorruptCount != 3 {
		t.Fatalf("complete aggregate = %#v", complete)
	}

	incomplete := aggregator.Aggregate(map[string]*PerformanceMetric{
		"a": {Scope: "node", MetricsSchema: 3, CorruptCount: 1, HasCorruptCount: true},
		"b": {Scope: "node", MetricsSchema: 3},
	})
	if incomplete == nil || incomplete.HasCorruptCount {
		t.Fatalf("aggregate must retain missing-field evidence: %#v", incomplete)
	}
}
