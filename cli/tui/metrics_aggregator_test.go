/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"testing"
	"time"
)

func newNodeMetric(ops, mb, success, failed int64, latency, duration int64, current int, mean float64) *PerformanceMetric {
	ts := time.Now()
	return &PerformanceMetric{
		MetricsSchema:      2,
		Scope:              "node",
		Role:               "worker",
		StepID:             "step",
		OpType:             "CREATE",
		TestState:          1,
		SuccessCount:       success,
		FailedCount:        failed,
		OpsPerSec:          ops,
		MBPerSec:           mb,
		MeanLatency:        latency,
		MeanDuration:       duration,
		ConcurrencyCurrent: int64(current),
		ConcurrencyMean:    mean,
		SampleTimestamp:    ts,
		SampleTimestampRaw: ts.Format(time.RFC3339Nano),
		Timestamp:          ts,
	}
}

func TestMetricsAggregatorIgnoresNonNodeMetrics(t *testing.T) {
	aggregator := NewMetricsAggregator()

	fleetMetric := &PerformanceMetric{MetricsSchema: 2, Scope: "aggregate"}
	result := aggregator.Aggregate(map[string]*PerformanceMetric{"fleet": fleetMetric})
	if result != nil {
		t.Fatalf("expected nil aggregate when only fleet metrics provided, got %+v", result)
	}
}

func TestMetricsAggregatorAggregatesSingleNode(t *testing.T) {
	aggregator := NewMetricsAggregator()
	node := newNodeMetric(1000, 100, 500, 10, 40, 60, 8, 7.5)

	result := aggregator.Aggregate(map[string]*PerformanceMetric{"node0": node})
	if result == nil {
		t.Fatal("expected aggregate metric for single node")
	}
	if result.Scope != "aggregate" {
		t.Errorf("expected aggregate scope, got %q", result.Scope)
	}
	if result.OpsPerSec != node.OpsPerSec || result.MBPerSec != node.MBPerSec {
		t.Errorf("expected totals to match node values, got %+v", result)
	}
	if result.NodesCount != 1 {
		t.Errorf("expected NodesCount=1, got %d", result.NodesCount)
	}
}

func TestMetricsAggregatorAggregatesMultipleNodes(t *testing.T) {
	aggregator := NewMetricsAggregator()
	node1 := newNodeMetric(1000, 100, 500, 5, 50, 70, 10, 8)
	node2 := newNodeMetric(1500, 150, 900, 2, 40, 60, 12, 9)

	aggregated := aggregator.Aggregate(map[string]*PerformanceMetric{"node1": node1, "node2": node2})
	if aggregated == nil {
		t.Fatal("expected aggregated metric")
	}

	if aggregated.OpsPerSec != 2500 {
		t.Errorf("expected ops/sec=2500, got %d", aggregated.OpsPerSec)
	}
	if aggregated.MBPerSec != 250 {
		t.Errorf("expected MB/s=250, got %d", aggregated.MBPerSec)
	}
	if aggregated.SuccessCount != 1400 || aggregated.FailedCount != 7 {
		t.Errorf("unexpected totals success=%d failed=%d", aggregated.SuccessCount, aggregated.FailedCount)
	}
	// Weighted latency: (50*1000 + 40*1500)/2500 = 44
	if aggregated.MeanLatency != 44 {
		t.Errorf("expected weighted latency 44, got %d", aggregated.MeanLatency)
	}
	if aggregated.ConcurrencyCurrent != int64(22) {
		t.Errorf("expected concurrency current sum 22, got %d", aggregated.ConcurrencyCurrent)
	}
	if aggregated.NodesCount != 2 || len(aggregated.NodesPresent) != 2 {
		t.Errorf("expected two nodes recorded, got count=%d list=%v", aggregated.NodesCount, aggregated.NodesPresent)
	}
}

func TestMetricsAggregatorAggregatesTTFB(t *testing.T) {
	aggregator := NewMetricsAggregator()
	node1 := newNodeMetric(1000, 100, 500, 5, 50, 70, 10, 8)
	node1.MeanTTFB = 20
	node2 := newNodeMetric(1500, 150, 900, 2, 40, 60, 12, 9)
	node2.MeanTTFB = 30

	aggregated := aggregator.Aggregate(map[string]*PerformanceMetric{"node1": node1, "node2": node2})
	if aggregated == nil {
		t.Fatal("expected aggregated metric")
	}
	if aggregated.MeanTTFB != 26 {
		t.Errorf("expected weighted TTFB 26, got %d", aggregated.MeanTTFB)
	}
}

func TestMetricsAggregatorOpCountCompletion(t *testing.T) {
	aggregator := NewMetricsAggregator()
	node1 := newNodeMetric(100, 0, 1000, 0, 0, 0, 0, 0)
	node1.HasLimit = true
	node1.LimitType = "op_count"
	node1.LimitOpCount = 2000

	node2 := newNodeMetric(200, 0, 1500, 0, 0, 0, 0, 0)
	node2.HasLimit = true
	node2.LimitType = "op_count"
	node2.LimitOpCount = 3000

	aggregated := aggregator.Aggregate(map[string]*PerformanceMetric{"a": node1, "b": node2})
	if aggregated == nil {
		t.Fatal("expected aggregated metric")
	}
	if !aggregated.HasLimit || aggregated.LimitType != "op_count" {
		t.Fatalf("expected op_count limit, got HasLimit=%v type=%q", aggregated.HasLimit, aggregated.LimitType)
	}
	if aggregated.LimitOpCount != 5000 {
		t.Errorf("expected combined op_count limit 5000, got %d", aggregated.LimitOpCount)
	}
	// total work 2500 / 5000 = 0.5 -> 50%
	if aggregated.CompletionPercent < 49.9 || aggregated.CompletionPercent > 50.1 {
		t.Errorf("expected completion ~50%%, got %.2f", aggregated.CompletionPercent)
	}
}

func TestMetricsAggregatorLatestTimestamp(t *testing.T) {
	aggregator := NewMetricsAggregator()
	now := time.Now()
	earlier := newNodeMetric(10, 0, 10, 0, 0, 0, 0, 0)
	earlier.SampleTimestamp = now.Add(-10 * time.Second)
	earlier.SampleTimestampRaw = earlier.SampleTimestamp.Format(time.RFC3339Nano)
	earlier.Timestamp = earlier.SampleTimestamp

	later := newNodeMetric(10, 0, 10, 0, 0, 0, 0, 0)
	later.SampleTimestamp = now
	later.SampleTimestampRaw = later.SampleTimestamp.Format(time.RFC3339Nano)
	later.Timestamp = later.SampleTimestamp

	aggregated := aggregator.Aggregate(map[string]*PerformanceMetric{"early": earlier, "late": later})
	if aggregated == nil {
		t.Fatal("expected aggregated metric")
	}
	if !aggregated.SampleTimestamp.Equal(later.SampleTimestamp) {
		t.Errorf("expected latest sample timestamp, got %v", aggregated.SampleTimestamp)
	}
	if aggregated.Timestamp.Before(later.Timestamp) {
		t.Errorf("expected aggregated timestamp to reflect latest sample, got %v", aggregated.Timestamp)
	}
}

func TestMetricsAggregatorIgnoresAggregateScopedEntrySamples(t *testing.T) {
	aggregator := NewMetricsAggregatorWithEntry("entry")

	entry := newNodeMetric(600, 60, 6000, 0, 50, 70, 8, 7.5)
	entry.Role = "entry"
	entry.Scope = "aggregate" // aggregated payloads should be ignored by node-only reducer
	entry.NodesCount = 6
	entry.NodesPresent = []string{"node-0", "node-1", "node-2"}

	workerA := newNodeMetric(300, 30, 3000, 0, 40, 60, 8, 7.5)
	workerB := newNodeMetric(200, 20, 2000, 0, 35, 55, 6, 6.5)

	res := aggregator.Aggregate(map[string]*PerformanceMetric{
		"entry":    entry,
		"worker-a": workerA,
		"worker-b": workerB,
	})

	if res == nil {
		t.Fatal("expected aggregated metric")
	}

	expectedSuccess := workerA.SuccessCount + workerB.SuccessCount
	if res.SuccessCount != expectedSuccess {
		t.Fatalf("expected success count %d, got %d", expectedSuccess, res.SuccessCount)
	}
	if res.OpsPerSec != workerA.OpsPerSec+workerB.OpsPerSec {
		t.Fatalf("expected ops/sec %d, got %d", workerA.OpsPerSec+workerB.OpsPerSec, res.OpsPerSec)
	}
	if res.NodesCount != 2 {
		t.Fatalf("expected node count 2 (workers only), got %d", res.NodesCount)
	}
}

func TestMetricsAggregatorIncludesEntryWhenNodeScoped(t *testing.T) {
	aggregator := NewMetricsAggregatorWithEntry("entry")

	entry := newNodeMetric(400, 40, 4000, 0, 45, 65, 10, 8.5)
	entry.Role = "entry"

	worker := newNodeMetric(200, 20, 2000, 0, 35, 55, 6, 6.5)

	res := aggregator.Aggregate(map[string]*PerformanceMetric{
		"entry":  entry,
		"worker": worker,
	})

	if res == nil {
		t.Fatal("expected aggregated metric")
	}

	expectedSuccess := entry.SuccessCount + worker.SuccessCount
	if res.SuccessCount != expectedSuccess {
		t.Fatalf("expected success count %d, got %d", expectedSuccess, res.SuccessCount)
	}
	if res.NodesCount != 2 {
		t.Fatalf("expected node count 2, got %d", res.NodesCount)
	}
	if res.OpsPerSec != entry.OpsPerSec+worker.OpsPerSec {
		t.Fatalf("expected ops/sec %d, got %d", entry.OpsPerSec+worker.OpsPerSec, res.OpsPerSec)
	}
	if res.ConcurrencyCurrent != entry.ConcurrencyCurrent+worker.ConcurrencyCurrent {
		t.Fatalf("expected concurrency current %d, got %d",
			entry.ConcurrencyCurrent+worker.ConcurrencyCurrent, res.ConcurrencyCurrent)
	}
}
