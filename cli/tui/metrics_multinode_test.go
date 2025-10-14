/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"testing"
	"time"
)

func TestMetricsCollector_AddMultiNodeSample(t *testing.T) {
	mc := NewMetricsCollector()

	now := time.Now()

	// Create a multi-node update
	update := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{
			OpsPerSec:    3000,
			SuccessCount: 1500,
			Timestamp:    now,
		},
		PerNode: map[string]*PerformanceMetric{
			"node1": {
				OpsPerSec:    1000,
				SuccessCount: 500,
				Timestamp:    now,
			},
			"node2": {
				OpsPerSec:    2000,
				SuccessCount: 1000,
				Timestamp:    now,
			},
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"node1": {
				LastSeen:    now,
				IsConnected: true,
				IsActive:    true,
			},
			"node2": {
				LastSeen:    now,
				IsConnected: true,
				IsActive:    true,
			},
		},
	}

	mc.AddMultiNodeSample(&update)

	// Check aggregated samples
	samples := mc.GetSamples()
	if len(samples) != 1 {
		t.Errorf("Expected 1 aggregated sample, got %d", len(samples))
	}

	if samples[0].OpsPerSec != 3000 {
		t.Errorf("Expected aggregated OpsPerSec 3000, got %d", samples[0].OpsPerSec)
	}

	// Check per-node samples
	node1Samples := mc.GetNodeSamples("node1")
	if len(node1Samples) != 1 {
		t.Errorf("Expected 1 node1 sample, got %d", len(node1Samples))
	}

	if node1Samples[0].OpsPerSec != 1000 {
		t.Errorf("Expected node1 OpsPerSec 1000, got %d", node1Samples[0].OpsPerSec)
	}

	node2Samples := mc.GetNodeSamples("node2")
	if len(node2Samples) != 1 {
		t.Errorf("Expected 1 node2 sample, got %d", len(node2Samples))
	}

	if node2Samples[0].OpsPerSec != 2000 {
		t.Errorf("Expected node2 OpsPerSec 2000, got %d", node2Samples[0].OpsPerSec)
	}

	// Check node status
	nodeStatus := mc.GetNodeStatus()
	if len(nodeStatus) != 2 {
		t.Errorf("Expected 2 node statuses, got %d", len(nodeStatus))
	}

	if !nodeStatus["node1"].IsConnected {
		t.Error("Expected node1 to be connected")
	}

	if !nodeStatus["node2"].IsActive {
		t.Error("Expected node2 to be active")
	}
}

func TestMetricsCollector_GetNodeSamples_NonExistentNode(t *testing.T) {
	mc := NewMetricsCollector()

	samples := mc.GetNodeSamples("nonexistent")

	// Should return empty slice, not nil
	if len(samples) != 0 {
		t.Errorf("Expected empty slice for nonexistent node, got %d samples", len(samples))
	}
}

func TestMetricsCollector_GetActiveNodeCount(t *testing.T) {
	mc := NewMetricsCollector()

	// Initially no nodes
	if count := mc.GetActiveNodeCount(); count != 0 {
		t.Errorf("Expected 0 active nodes initially, got %d", count)
	}

	update := MultiNodeMetricsUpdate{
		NodeStatus: map[string]NodeConnectionStatus{
			"node1": {IsActive: true, IsConnected: true},
			"node2": {IsActive: false, IsConnected: true}, // Connected but not active
			"node3": {IsActive: true, IsConnected: true},
			"node4": {IsActive: true, IsConnected: false}, // Active but not connected
		},
	}

	mc.AddMultiNodeSample(&update)

	// Should count only nodes that are both active AND connected
	expectedCount := 2 // node1 and node3
	if count := mc.GetActiveNodeCount(); count != expectedCount {
		t.Errorf("Expected %d active nodes, got %d", expectedCount, count)
	}
}

func TestMetricsCollector_GetLatestSample_WithMultiNode(t *testing.T) {
	mc := NewMetricsCollector()

	// Initially no samples
	if latest := mc.GetLatestSample(); latest != nil {
		t.Error("Expected nil latest sample when no samples exist")
	}

	now := time.Now()

	update := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{
			OpsPerSec: 5000,
			Timestamp: now,
		},
	}

	mc.AddMultiNodeSample(&update)

	latest := mc.GetLatestSample()
	if latest == nil {
		t.Fatal("Expected non-nil latest sample")
	}

	if latest.OpsPerSec != 5000 {
		t.Errorf("Expected latest OpsPerSec 5000, got %d", latest.OpsPerSec)
	}
}

func TestMetricsCollector_GetLatestNodeSample(t *testing.T) {
	mc := NewMetricsCollector()

	// Test with nonexistent node
	if latest := mc.GetLatestNodeSample("nonexistent"); latest != nil {
		t.Error("Expected nil latest sample for nonexistent node")
	}

	now := time.Now()

	update := MultiNodeMetricsUpdate{
		PerNode: map[string]*PerformanceMetric{
			"node1": {
				OpsPerSec: 1500,
				Timestamp: now,
			},
		},
	}

	mc.AddMultiNodeSample(&update)

	latest := mc.GetLatestNodeSample("node1")
	if latest == nil {
		t.Fatal("Expected non-nil latest sample for node1")
	}

	if latest.OpsPerSec != 1500 {
		t.Errorf("Expected node1 latest OpsPerSec 1500, got %d", latest.OpsPerSec)
	}
}

func TestMetricsCollector_MultipleUpdatesWithRingBuffer(t *testing.T) {
	mc := NewMetricsCollector()

	// Add multiple updates to test ring buffer behavior
	for i := 0; i < 5; i++ {
		update := MultiNodeMetricsUpdate{
			Aggregated: &PerformanceMetric{
				OpsPerSec: int64(1000 * (i + 1)),
				Timestamp: time.Now().Add(time.Duration(i) * time.Second),
			},
			PerNode: map[string]*PerformanceMetric{
				"node1": {
					OpsPerSec: int64(500 * (i + 1)),
					Timestamp: time.Now().Add(time.Duration(i) * time.Second),
				},
			},
		}

		mc.AddMultiNodeSample(&update)
	}

	// Check that we have all 5 samples
	aggregatedSamples := mc.GetSamples()
	if len(aggregatedSamples) != 5 {
		t.Errorf("Expected 5 aggregated samples, got %d", len(aggregatedSamples))
	}

	node1Samples := mc.GetNodeSamples("node1")
	if len(node1Samples) != 5 {
		t.Errorf("Expected 5 node1 samples, got %d", len(node1Samples))
	}

	// Check that latest values are correct
	if aggregatedSamples[4].OpsPerSec != 5000 {
		t.Errorf("Expected latest aggregated OpsPerSec 5000, got %d", aggregatedSamples[4].OpsPerSec)
	}

	if node1Samples[4].OpsPerSec != 2500 {
		t.Errorf("Expected latest node1 OpsPerSec 2500, got %d", node1Samples[4].OpsPerSec)
	}
}

func TestMetricsCollector_AddMultiNodeSample_OnlyAggregated(t *testing.T) {
	mc := NewMetricsCollector()

	// Test with only aggregated data (no per-node data)
	update := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{
			OpsPerSec: 2000,
		},
		// No PerNode or Status data
	}

	mc.AddMultiNodeSample(&update)

	samples := mc.GetSamples()
	if len(samples) != 1 {
		t.Errorf("Expected 1 sample, got %d", len(samples))
	}

	if samples[0].OpsPerSec != 2000 {
		t.Errorf("Expected OpsPerSec 2000, got %d", samples[0].OpsPerSec)
	}

	// Should have no per-node data
	nodeStatus := mc.GetNodeStatus()
	if len(nodeStatus) != 0 {
		t.Errorf("Expected no node status, got %d nodes", len(nodeStatus))
	}
}

func TestMetricsCollector_AddMultiNodeSample_OnlyPerNode(t *testing.T) {
	mc := NewMetricsCollector()

	// Test with only per-node data (no aggregated data)
	update := MultiNodeMetricsUpdate{
		// No Aggregated data
		PerNode: map[string]*PerformanceMetric{
			"node1": {OpsPerSec: 1000},
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"node1": {IsActive: true, IsConnected: true},
		},
	}

	mc.AddMultiNodeSample(&update)

	// Should have no aggregated samples
	samples := mc.GetSamples()
	if len(samples) != 0 {
		t.Errorf("Expected no aggregated samples, got %d", len(samples))
	}

	// Should have per-node data
	node1Samples := mc.GetNodeSamples("node1")
	if len(node1Samples) != 1 {
		t.Errorf("Expected 1 node1 sample, got %d", len(node1Samples))
	}

	if node1Samples[0].OpsPerSec != 1000 {
		t.Errorf("Expected node1 OpsPerSec 1000, got %d", node1Samples[0].OpsPerSec)
	}
}

func TestMetricsCollector_ThreadSafety_MultiNodeSample(t *testing.T) {
	mc := NewMetricsCollector()

	// Simple concurrent access test
	// Not a comprehensive race condition test, but checks basic thread safety
	done := make(chan bool, 2)

	// Writer goroutine
	go func() {
		for i := 0; i < 10; i++ {
			update := MultiNodeMetricsUpdate{
				Aggregated: &PerformanceMetric{OpsPerSec: int64(i * 100)},
				PerNode: map[string]*PerformanceMetric{
					"node1": {OpsPerSec: int64(i * 50)},
				},
			}
			mc.AddMultiNodeSample(&update)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 10; i++ {
			_ = mc.GetSamples()
			_ = mc.GetNodeSamples("node1")
			_ = mc.GetActiveNodeCount()
		}
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	// Verify final state is consistent
	samples := mc.GetSamples()
	node1Samples := mc.GetNodeSamples("node1")

	if len(samples) != len(node1Samples) {
		t.Errorf("Expected same number of samples, got %d aggregated and %d node1", len(samples), len(node1Samples))
	}
}
