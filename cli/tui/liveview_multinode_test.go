/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestLiveViewRenderer_RenderLiveView_MultiNode(t *testing.T) {
	params := &scenario.ScenarioParams{
		ObjectSize: "1MB",
	}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	// Create metrics collector with multi-node data
	mc := NewMetricsCollector()

	now := time.Now()

	// Add aggregated sample first
	aggregatedMetric := PerformanceMetric{
		OpsPerSec:          3000,
		MBPerSec:           300,
		MeanLatency:        45,
		SuccessCount:       1500,
		FailedCount:        10,
		Timestamp:          now,
		OpType:             "write",
		ConcurrencyCurrent: 25,
	}
	mc.AddSample(aggregatedMetric)

	// Add multi-node update
	update := MultiNodeMetricsUpdate{
		Aggregated: &aggregatedMetric,
		PerNode: map[string]*PerformanceMetric{
			"node1": {
				OpsPerSec:    1000,
				MBPerSec:     100,
				SuccessCount: 500,
				FailedCount:  3,
				Timestamp:    now,
			},
			"node2": {
				OpsPerSec:    2000,
				MBPerSec:     200,
				SuccessCount: 1000,
				FailedCount:  7,
				Timestamp:    now,
			},
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"node1": {IsActive: true, IsConnected: true, Phase: NodePhaseMetricsFlowing},
			"node2": {IsActive: true, IsConnected: true, Phase: NodePhaseMetricsFlowing},
		},
	}
	mc.AddMultiNodeSample(&update)

	// Render with width 80
	result := renderer.RenderLiveView(mc, 80)

	if result == "" {
		t.Fatal("Expected non-empty result")
	}

	lines := strings.Split(result, "\n")

	// Should have header + table header + Total row + 2 node rows = 5 lines minimum
	if len(lines) < 5 {
		t.Errorf("Expected at least 5 lines, got %d", len(lines))
	}

	// Check header contains phase info
	if !strings.Contains(lines[0], "Phase: WRITE") {
		t.Errorf("Expected header to contain 'Phase: WRITE', got: %s", lines[0])
	}

	// Check for table header (reverse video)
	foundTableHeader := false
	for _, line := range lines {
		if strings.Contains(line, "\033[7m") && strings.Contains(line, "Rank") {
			foundTableHeader = true
			break
		}
	}
	if !foundTableHeader {
		t.Error("Expected to find table header with reverse video formatting")
	}

	// Check for Total row
	foundTotalRow := false
	for _, line := range lines {
		if strings.Contains(line, "Total") && strings.Contains(line, "3000") { // 3000 IOPs
			foundTotalRow = true
			break
		}
	}
	if !foundTotalRow {
		t.Error("Expected to find Total row with aggregated data")
	}

	// Check for individual node rows (should have numbered nodes)
	foundNode0 := false
	foundNode1 := false
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "0") && strings.Contains(line, "1000") { // First node with 1000 IOPs
			foundNode0 = true
		}
		if strings.HasPrefix(strings.TrimSpace(line), "1") && strings.Contains(line, "2000") { // Second node with 2000 IOPs
			foundNode1 = true
		}
	}
	if !foundNode0 {
		t.Error("Expected to find node 0 row with 1000 IOPs")
	}
	if !foundNode1 {
		t.Error("Expected to find node 1 row with 2000 IOPs")
	}
}

func TestLiveViewRenderer_MultiNodePercentDisplay(t *testing.T) {
	renderer := NewLiveViewRenderer(nil)
	defer renderer.Stop()

	mc := NewMetricsCollector()

	// Two nodes with completion percent 40 and 60
	n1 := PerformanceMetric{Timestamp: time.Now(), OpType: "CREATE", CompletionPercent: 40}
	n2 := PerformanceMetric{Timestamp: time.Now(), OpType: "CREATE", CompletionPercent: 60}
	mc.AddMultiNodeSample(&MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{CompletionPercent: 50},
		PerNode:    map[string]*PerformanceMetric{"nodeA": &n1, "nodeB": &n2},
		NodeStatus: map[string]NodeConnectionStatus{
			"nodeA": {IsConnected: true, IsActive: true, Phase: NodePhaseMetricsFlowing},
			"nodeB": {IsConnected: true, IsActive: true, Phase: NodePhaseMetricsFlowing},
		},
	})

	out := renderer.RenderLiveView(mc, 100)
	if !strings.Contains(out, " 40 ") || !strings.Contains(out, " 60 ") {
		t.Errorf("Expected to find per-node percents 40 and 60. Output:\n%s", out)
	}
	if !strings.Contains(out, " 50 ") {
		t.Errorf("Expected to find aggregated percent 50. Output:\n%s", out)
	}
}

func TestLiveViewRenderer_MultiNodeInfinityDisplay(t *testing.T) {
	renderer := NewLiveViewRenderer(nil)
	defer renderer.Stop()

	mc := NewMetricsCollector()

	// Both nodes unbounded => total should show ∞
	n1 := PerformanceMetric{Timestamp: time.Now(), OpType: "CREATE", Unbounded: true}
	n2 := PerformanceMetric{Timestamp: time.Now(), OpType: "CREATE", Unbounded: true}
	mc.AddMultiNodeSample(&MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{Unbounded: true},
		PerNode:    map[string]*PerformanceMetric{"nodeA": &n1, "nodeB": &n2},
		NodeStatus: map[string]NodeConnectionStatus{"nodeA": {IsConnected: true, IsActive: true}, "nodeB": {IsConnected: true, IsActive: true}},
	})

	out := renderer.RenderLiveView(mc, 120)
	if !strings.Contains(out, " ∞ ") {
		t.Errorf("Expected infinity symbol in output. Output:\n%s", out)
	}
}

func TestLiveViewRenderer_RenderLiveView_SingleNode(t *testing.T) {
	params := &scenario.ScenarioParams{
		ObjectSize: "512KB",
	}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	// Create metrics collector with single node data
	mc := NewMetricsCollector()

	now := time.Now()

	// Add aggregated sample
	metric := PerformanceMetric{
		OpsPerSec:          1500,
		MBPerSec:           150,
		SuccessCount:       750,
		FailedCount:        5,
		Timestamp:          now,
		OpType:             "read",
		ConcurrencyCurrent: 10,
	}
	mc.AddSample(metric)

	// Add single-node update (simulating what the single-host orchestrator would send)
	update := MultiNodeMetricsUpdate{
		Aggregated: &metric,
		PerNode: map[string]*PerformanceMetric{
			"node-0": &metric,
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"node-0": {IsActive: true, IsConnected: true},
		},
	}
	mc.AddMultiNodeSample(&update)

	result := renderer.RenderLiveView(mc, 100)

	if result == "" {
		t.Fatal("Expected non-empty result")
	}

	lines := strings.Split(result, "\n")

	// Check header shows READ phase
	if !strings.Contains(lines[0], "Phase: READ") {
		t.Errorf("Expected header to contain 'Phase: READ', got: %s", lines[0])
	}

	// Should show Total row and single node "0" row (backward compatibility)
	foundTotalRow := false
	foundSingleNodeRow := false
	for _, line := range lines {
		if strings.Contains(line, "Total") && strings.Contains(line, "1500") {
			foundTotalRow = true
		}
		if strings.HasPrefix(strings.TrimSpace(line), "0") && strings.Contains(line, "1500") {
			foundSingleNodeRow = true
		}
	}

	if !foundTotalRow {
		t.Error("Expected to find Total row with 1500 IOPs")
	}
	if !foundSingleNodeRow {
		t.Error("Expected to find single node row labeled '0' with 1500 IOPs")
	}
}

func TestLiveViewRenderer_FormatMultiNodeTable_NoActiveNodes(t *testing.T) {
	params := &scenario.ScenarioParams{ObjectSize: "1MB"}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	mc := NewMetricsCollector()

	// Add aggregated metric but no active nodes
	aggregated := &PerformanceMetric{
		OpsPerSec:    0,
		MBPerSec:     0,
		SuccessCount: 0,
		FailedCount:  0,
	}

	result := renderer.formatMultiNodeTable(mc, aggregated, 80)

	lines := strings.Split(result, "\n")

	// Should have header + Total + empty node row
	if len(lines) < 3 {
		t.Errorf("Expected at least 3 lines, got %d", len(lines))
	}

	// Should have Total row with zeros
	foundTotalRow := false
	for _, line := range lines {
		if strings.Contains(line, "Total") && strings.Contains(line, "0") {
			foundTotalRow = true
			break
		}
	}
	if !foundTotalRow {
		t.Error("Expected to find Total row with zero values")
	}

	// Should have an empty node row (node "0" with zeros)
	foundEmptyNodeRow := false
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "0") {
			foundEmptyNodeRow = true
			break
		}
	}
	if !foundEmptyNodeRow {
		t.Error("Expected to find empty node row labeled '0'")
	}
}

func TestLiveViewRenderer_FormatMultiNodeTable_NodeSorting(t *testing.T) {
	params := &scenario.ScenarioParams{ObjectSize: "1MB"}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	mc := NewMetricsCollector()

	now := time.Now()

	// Add nodes in non-alphabetical order to test sorting
	update := MultiNodeMetricsUpdate{
		Aggregated: &PerformanceMetric{OpsPerSec: 4000},
		PerNode: map[string]*PerformanceMetric{
			"zebra":  {OpsPerSec: 1000, Timestamp: now},
			"apple":  {OpsPerSec: 2000, Timestamp: now},
			"banana": {OpsPerSec: 1000, Timestamp: now},
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"zebra":  {IsActive: true, IsConnected: true},
			"apple":  {IsActive: true, IsConnected: true},
			"banana": {IsActive: true, IsConnected: true},
		},
	}
	mc.AddMultiNodeSample(&update)

	aggregated := &PerformanceMetric{OpsPerSec: 4000}

	result := renderer.formatMultiNodeTable(mc, aggregated, 80)

	lines := strings.Split(result, "\n")

	// Find node rows (skip header and Total)
	nodeRows := []string{}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "0") || strings.HasPrefix(trimmed, "1") || strings.HasPrefix(trimmed, "2") {
			nodeRows = append(nodeRows, line)
		}
	}

	if len(nodeRows) != 3 {
		t.Errorf("Expected 3 node rows, got %d", len(nodeRows))
	}

	// Nodes should be sorted alphabetically: apple (0), banana (1), zebra (2)
	// Check that apple (2000 IOPs) comes first
	if !strings.Contains(nodeRows[0], "2000") {
		t.Errorf("Expected first node row to have 2000 IOPs (apple), got: %s", nodeRows[0])
	}

	// Check that zebra (1000 IOPs) comes last
	if !strings.Contains(nodeRows[2], "1000") {
		t.Errorf("Expected last node row to have 1000 IOPs (zebra), got: %s", nodeRows[2])
	}
}

func TestLiveViewRenderer_FormatMultiNodeTable_SomeNodesWithoutSamples(t *testing.T) {
	params := &scenario.ScenarioParams{ObjectSize: "1MB"}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	mc := NewMetricsCollector()

	now := time.Now()

	// Add some nodes with samples and some without
	update1 := MultiNodeMetricsUpdate{
		PerNode: map[string]*PerformanceMetric{
			"node1": {OpsPerSec: 1000, Timestamp: now},
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"node1": {IsActive: true, IsConnected: true},
			"node2": {IsActive: true, IsConnected: true}, // Connected but no samples yet
		},
	}
	mc.AddMultiNodeSample(&update1)

	aggregated := &PerformanceMetric{OpsPerSec: 1000}

	result := renderer.formatMultiNodeTable(mc, aggregated, 80)

	lines := strings.Split(result, "\n")

	// Should have both nodes, one with data and one with placeholders ('-')
	nodeRowsWithData := 0
	nodeRowsWithPlaceholders := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "0") || strings.HasPrefix(trimmed, "1") {
			if strings.Contains(line, "1000") {
				nodeRowsWithData++
			} else if strings.Contains(line, " - ") || strings.Contains(line, " -\n") {
				nodeRowsWithPlaceholders++
			}
		}
	}

	if nodeRowsWithData != 1 {
		t.Errorf("Expected 1 node row with data, got %d", nodeRowsWithData)
	}
	if nodeRowsWithPlaceholders != 1 {
		t.Errorf("Expected 1 node row with placeholders, got %d", nodeRowsWithPlaceholders)
	}
}

func TestLiveViewRenderer_DoneMiBCalculation_MultiNode(t *testing.T) {
	params := &scenario.ScenarioParams{
		ObjectSize: "2MB", // 2 MB per object
	}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	mc := NewMetricsCollector()

	// Add metrics with success count that should calculate DoneMiB
	aggregatedMetric := PerformanceMetric{
		SuccessCount: 500, // 500 * 2MB = 1000MB = ~976 MiB
	}

	result := renderer.formatMultiNodeTable(mc, &aggregatedMetric, 80)

	// Should show approximately 976 MiB (1000MB / 1.024^2)
	// expectedMiB calculation: 500 * 2 * 1000 * 1000 / (1024*1024) = 953 MiB

	if !strings.Contains(result, "Total") {
		t.Error("Expected Total row in result")
	}

	// The exact MiB calculation: 500 * 2MB = 1000MB = ~954 MiB
	// Expected: 500 * 2097152 / (1024*1024) = 1000 MiB
	lines := strings.Split(result, "\n")
	foundCorrectDoneMiB := false
	for _, line := range lines {
		if strings.Contains(line, "Total") {
			// Total row should show 1000 MiB (not 0)
			if strings.Contains(line, "     1000") { // Look for 1000 in DoneMiB column
				foundCorrectDoneMiB = true
				break
			}
			t.Logf("Total row found but incorrect: %s", line)
		}
	}
	if !foundCorrectDoneMiB {
		t.Error("Expected Total row to show 1000 MiB for DoneMiB value")
		t.Logf("Full result:\n%s", result)
	}
}
