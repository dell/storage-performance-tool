/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"testing"
	"time"
)

func TestModel_Update_MultiNodeMetricsMsg(t *testing.T) {
	model := InitialModel()
	model.metricsCollector = NewMetricsCollector()

	now := time.Now()

	// Create a multi-node metrics message
	update := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{
			OpsPerSec:    5000,
			MiBPerSec:    500,
			MeanLatency:  30,
			SuccessCount: 2500,
			FailedCount:  10,
			Timestamp:    now,
			OpType:       "write",
		},
		PerNode: map[string]*PerformanceMetric{
			"node1": {
				OpsPerSec:    2000,
				MiBPerSec:    200,
				SuccessCount: 1000,
				FailedCount:  4,
				Timestamp:    now,
			},
			"node2": {
				OpsPerSec:    3000,
				MiBPerSec:    300,
				SuccessCount: 1500,
				FailedCount:  6,
				Timestamp:    now,
			},
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"node1": {
				LastSeen:    now,
				IsConnected: true,
				IsActive:    true,
				Error:       nil,
			},
			"node2": {
				LastSeen:    now,
				IsConnected: true,
				IsActive:    true,
				Error:       nil,
			},
		},
	}

	msg := multiNodeMetricsMsg(update)

	// Process the message
	updatedModel, cmd := model.Update(msg)

	// Should not return a command
	if cmd != nil {
		t.Errorf("Expected nil command, got %v", cmd)
	}

	// Type assert to get concrete model
	concreteModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatal("Expected concrete Model type")
	}

	// Check that metrics were added to collector
	samples := concreteModel.metricsCollector.GetSamples()
	if len(samples) != 1 {
		t.Errorf("Expected 1 aggregated sample, got %d", len(samples))
	}

	if samples[0].OpsPerSec != 5000 {
		t.Errorf("Expected aggregated OpsPerSec 5000, got %d", samples[0].OpsPerSec)
	}

	// Check per-node samples
	node1Samples := concreteModel.metricsCollector.GetNodeSamples("node1")
	if len(node1Samples) != 1 {
		t.Errorf("Expected 1 node1 sample, got %d", len(node1Samples))
	}

	if node1Samples[0].OpsPerSec != 2000 {
		t.Errorf("Expected node1 OpsPerSec 2000, got %d", node1Samples[0].OpsPerSec)
	}

	node2Samples := concreteModel.metricsCollector.GetNodeSamples("node2")
	if len(node2Samples) != 1 {
		t.Errorf("Expected 1 node2 sample, got %d", len(node2Samples))
	}

	if node2Samples[0].OpsPerSec != 3000 {
		t.Errorf("Expected node2 OpsPerSec 3000, got %d", node2Samples[0].OpsPerSec)
	}

	// Check node status
	nodeStatus := concreteModel.metricsCollector.GetNodeStatus()
	if len(nodeStatus) != 2 {
		t.Errorf("Expected 2 node statuses, got %d", len(nodeStatus))
	}

	if !nodeStatus["node1"].IsActive {
		t.Error("Expected node1 to be active")
	}

	if !nodeStatus["node2"].IsConnected {
		t.Error("Expected node2 to be connected")
	}
}

func TestModel_Update_MultiNodeMetricsMsg_HistoricalIndexAdjustment(t *testing.T) {
	model := InitialModel()
	model.metricsCollector = NewMetricsCollector()
	model.historicalIndex = -5 // Viewing historical data

	update := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{
			OpsPerSec: 1000,
			Timestamp: time.Now(),
		},
	}

	msg := multiNodeMetricsMsg(update)

	// Process the message
	updatedModel, _ := model.Update(msg)

	// Type assert to get concrete model
	concreteModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatal("Expected concrete Model type")
	}

	// Historical index should be decremented to maintain view on same data
	expectedHistoricalIndex := -6
	if concreteModel.historicalIndex != expectedHistoricalIndex {
		t.Errorf("Expected historicalIndex %d, got %d", expectedHistoricalIndex, concreteModel.historicalIndex)
	}
}

func TestModel_Update_MultiNodeMetricsMsg_LiveMode(t *testing.T) {
	model := InitialModel()
	model.metricsCollector = NewMetricsCollector()
	model.historicalIndex = 0 // Live mode

	update := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{
			OpsPerSec: 1000,
			Timestamp: time.Now(),
		},
	}

	msg := multiNodeMetricsMsg(update)

	// Process the message
	updatedModel, _ := model.Update(msg)

	// Type assert to get concrete model
	concreteModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatal("Expected concrete Model type")
	}

	// Historical index should remain 0 in live mode
	if concreteModel.historicalIndex != 0 {
		t.Errorf("Expected historicalIndex to remain 0, got %d", concreteModel.historicalIndex)
	}
}

func TestModel_Update_MultiNodeMetricsMsg_NilAggregated(t *testing.T) {
	model := InitialModel()
	model.metricsCollector = NewMetricsCollector()

	// Create update with nil aggregated data
	update := MultiNodeMetricsUpdate{
		Aggregated: nil,
		PerNode: map[string]*PerformanceMetric{
			"node1": {OpsPerSec: 1000, Timestamp: time.Now()},
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"node1": {IsActive: true, IsConnected: true},
		},
	}

	msg := multiNodeMetricsMsg(update)

	// Should handle gracefully without crashing
	updatedModel, cmd := model.Update(msg)

	if cmd != nil {
		t.Errorf("Expected nil command, got %v", cmd)
	}

	// Type assert to get concrete model
	concreteModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatal("Expected concrete Model type")
	}

	// Should have no aggregated samples
	samples := concreteModel.metricsCollector.GetSamples()
	if len(samples) != 0 {
		t.Errorf("Expected 0 aggregated samples, got %d", len(samples))
	}

	// Should have per-node data
	node1Samples := concreteModel.metricsCollector.GetNodeSamples("node1")
	if len(node1Samples) != 1 {
		t.Errorf("Expected 1 node1 sample, got %d", len(node1Samples))
	}
}

func TestModel_Update_LegacyApiMetricsMsg_StillWorks(t *testing.T) {
	model := InitialModel()
	model.metricsCollector = NewMetricsCollector()

	// Test that legacy apiMetricsMsg still works for backward compatibility
	metric := PerformanceMetric{
		OpsPerSec:    1500,
		MiBPerSec:    150,
		SuccessCount: 750,
		Timestamp:    time.Now(),
		OpType:       "read",
	}

	msg := apiMetricsMsg(metric)

	// Process the message
	updatedModel, cmd := model.Update(msg)

	if cmd != nil {
		t.Errorf("Expected nil command, got %v", cmd)
	}

	// Type assert to get concrete model
	concreteModel, ok := updatedModel.(Model)
	if !ok {
		t.Fatal("Expected concrete Model type")
	}

	// Should add to aggregated samples using legacy path
	samples := concreteModel.metricsCollector.GetSamples()
	if len(samples) != 1 {
		t.Errorf("Expected 1 sample, got %d", len(samples))
	}

	if samples[0].OpsPerSec != 1500 {
		t.Errorf("Expected OpsPerSec 1500, got %d", samples[0].OpsPerSec)
	}

	// Should not have any per-node data
	nodeStatus := concreteModel.metricsCollector.GetNodeStatus()
	if len(nodeStatus) != 0 {
		t.Errorf("Expected no node status from legacy message, got %d", len(nodeStatus))
	}
}

func TestModel_Update_MultipleMultiNodeMessages(t *testing.T) {
	model := InitialModel()
	model.metricsCollector = NewMetricsCollector()

	// Send multiple multi-node messages
	for i := 0; i < 3; i++ {
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
			NodeStatus: map[string]NodeConnectionStatus{
				"node1": {IsActive: true, IsConnected: true},
			},
		}

		msg := multiNodeMetricsMsg(update)
		updatedModel, _ := model.Update(msg)
		model = updatedModel.(Model)
	}

	// Should have 3 aggregated samples
	samples := model.metricsCollector.GetSamples()
	if len(samples) != 3 {
		t.Errorf("Expected 3 aggregated samples, got %d", len(samples))
	}

	// Latest should have 3000 OpsPerSec
	if samples[2].OpsPerSec != 3000 {
		t.Errorf("Expected latest OpsPerSec 3000, got %d", samples[2].OpsPerSec)
	}

	// Should have 3 node1 samples
	node1Samples := model.metricsCollector.GetNodeSamples("node1")
	if len(node1Samples) != 3 {
		t.Errorf("Expected 3 node1 samples, got %d", len(node1Samples))
	}

	// Latest node1 should have 1500 OpsPerSec
	if node1Samples[2].OpsPerSec != 1500 {
		t.Errorf("Expected latest node1 OpsPerSec 1500, got %d", node1Samples[2].OpsPerSec)
	}
}

func TestModel_Update_MultiNodeMetricsMsg_MessageTypeConversion(t *testing.T) {
	// Test that the message type conversion works correctly
	originalUpdate := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{
			OpsPerSec: 2500,
			Timestamp: time.Now(),
		},
		PerNode: map[string]*PerformanceMetric{
			"test-node": {OpsPerSec: 2500, Timestamp: time.Now()},
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"test-node": {IsActive: true, IsConnected: true},
		},
	}

	// Convert to message type and back
	msg := multiNodeMetricsMsg(originalUpdate)
	convertedUpdate := MultiNodeMetricsUpdate(msg)

	// Verify conversion preserves data
	if convertedUpdate.Aggregated.OpsPerSec != 2500 {
		t.Errorf("Expected aggregated OpsPerSec 2500, got %d", convertedUpdate.Aggregated.OpsPerSec)
	}

	if convertedUpdate.PerNode["test-node"].OpsPerSec != 2500 {
		t.Errorf("Expected per-node OpsPerSec 2500, got %d", convertedUpdate.PerNode["test-node"].OpsPerSec)
	}

	if !convertedUpdate.NodeStatus["test-node"].IsActive {
		t.Error("Expected test-node to be active")
	}
}

// Test that ensures the message handling doesn't interfere with other message types
func TestModel_Update_MultiNodeMetricsMsg_DoesNotAffectOtherMessages(t *testing.T) {
	model := InitialModel()
	model.metricsCollector = NewMetricsCollector()

	// Process a different message type first
	statusMsg := apiStatusMsg(TestStatus{
		State:   "RUNNING",
		Message: "Test running",
	})

	updatedModel1, _ := model.Update(statusMsg)
	model = updatedModel1.(Model)

	// Then process multi-node metrics
	update := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{OpsPerSec: 1000, Timestamp: time.Now()},
	}

	msg := multiNodeMetricsMsg(update)
	updatedModel2, _ := model.Update(msg)
	model = updatedModel2.(Model)

	// Both should work independently
	samples := model.metricsCollector.GetSamples()
	if len(samples) != 1 {
		t.Errorf("Expected 1 sample after processing both messages, got %d", len(samples))
	}

	if samples[0].OpsPerSec != 1000 {
		t.Errorf("Expected OpsPerSec 1000, got %d", samples[0].OpsPerSec)
	}
}
