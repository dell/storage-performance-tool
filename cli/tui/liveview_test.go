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

func TestNewLiveViewRenderer(t *testing.T) {
	params := &scenario.ScenarioParams{
		WorkloadType: "write",
		ObjectSize:   "1MB",
		Threads:      4,
		Duration:     "5m",
	}

	renderer := NewLiveViewRenderer(params)

	if renderer == nil {
		t.Fatal("NewLiveViewRenderer returned nil")
	}

	if renderer.scenarioParams != params {
		t.Error("ScenarioParams not stored correctly")
	}

	if renderer.cpuMonitor == nil {
		t.Error("CPU monitor not initialized")
	}

	if renderer.startTime.IsZero() {
		t.Error("Start time not set")
	}

	// Clean up
	renderer.Stop()
}

func TestLiveViewRenderer_Stop(t *testing.T) {
	params := &scenario.ScenarioParams{
		WorkloadType: "write",
		ObjectSize:   "1MB",
	}

	renderer := NewLiveViewRenderer(params)

	// Should not panic
	renderer.Stop()

	// Should be safe to call multiple times
	renderer.Stop()
}

func TestLiveViewRendererDeleteTransferColumnsAreNotApplicable(t *testing.T) {
	renderer := NewLiveViewRenderer(&scenario.ScenarioParams{
		WorkloadType: scenario.WorkloadTypeDelete,
		ObjectSize:   "1MiB",
	})
	defer renderer.Stop()

	metric := &PerformanceMetric{
		OpType:       "DELETE",
		SuccessCount: 100,
		OpsPerSec:    50,
		MiBPerSec:    100,
		Delete: &DeleteMetrics{Performance: DeletePerformanceApplicability{
			ObjectSize: "not_applicable",
			DataMoved:  "not_applicable",
			Bandwidth:  "not_applicable",
			TTFB:       "not_applicable",
		}},
	}
	collector := NewMetricsCollector()
	collector.AddMultiNodeSample(&MultiNodeMetricsUpdate{
		Aggregated: metric,
		PerNode:    map[string]*PerformanceMetric{"node-a": metric},
		NodeStatus: map[string]NodeConnectionStatus{"node-a": {IsConnected: true}},
	})

	table := renderer.formatMultiNodeTable(collector, metric, 120)
	if strings.Count(table, "N/A") < 4 {
		t.Fatalf("DELETE table fabricated transfer values: %q", table)
	}
}

func TestLiveViewRenderer_RenderEmptyView(t *testing.T) {
	params := &scenario.ScenarioParams{
		WorkloadType: "write",
		ObjectSize:   "1MB",
		Threads:      4,
	}

	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	width := 80

	// Test with nil metrics
	result := renderer.RenderLiveView(nil, width)
	if result == "" {
		t.Error("Expected non-empty result for nil metrics")
	}

	// Should contain basic structure
	if !strings.Contains(result, "Phase: INIT") {
		t.Error("Expected 'Phase: INIT' in empty view")
	}

	if !strings.Contains(result, "CPU:") {
		t.Error("Expected 'CPU:' in empty view")
	}

	// Test with empty metrics collector
	emptyMetrics := NewMetricsCollector()
	result = renderer.RenderLiveView(emptyMetrics, width)
	if result == "" {
		t.Error("Expected non-empty result for empty metrics")
	}
}

func TestLiveViewRenderer_RenderWithMetrics(t *testing.T) {
	params := &scenario.ScenarioParams{
		WorkloadType: "write",
		ObjectSize:   "1MB",
		Threads:      4,
		Duration:     "5m",
	}

	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	// Create metrics with sample data
	metrics := NewMetricsCollector()
	sampleMetric := PerformanceMetric{
		Timestamp:          time.Now(),
		OpType:             "CREATE",
		TestState:          1,
		ConcurrencyCurrent: 4,
		SuccessCount:       100,
		FailedCount:        5,
		OpsPerSec:          50,
		MiBPerSec:          50,
		StepTime:           10.5,
	}
	metrics.AddSample(sampleMetric)

	width := 80
	result := renderer.RenderLiveView(metrics, width)

	if result == "" {
		t.Error("Expected non-empty result with metrics")
	}

	// Check for expected content
	if !strings.Contains(result, "Phase: CREATE") {
		t.Error("Expected 'Phase: CREATE' in result")
	}

	if !strings.Contains(result, "Active: 4") {
		t.Error("Expected 'Active: 4' in result")
	}

	// Should contain table content (grid-free format)
	if !strings.Contains(result, "Rank") {
		t.Error("Expected column headers in result")
	}

	if !strings.Contains(result, "Total") {
		t.Error("Expected 'Total' row in result")
	}

	// After JSON-only changes, percent placeholder is never "??"; presence of node 0 row is sufficient
	if !strings.Contains(result, "\n    0 ") && !strings.Contains(result, "\n0 ") {
		t.Error("Expected node 0 row in result")
	}
}

func TestLiveViewRenderer_formatHeader(t *testing.T) {
	params := &scenario.ScenarioParams{
		WorkloadType: "write",
		ObjectSize:   "1MB",
		Threads:      4,
	}

	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	tests := []struct {
		name     string
		metric   PerformanceMetric
		contains []string
	}{
		{
			name: "CREATE operation",
			metric: PerformanceMetric{
				OpType:             "CREATE",
				TestState:          1,
				ConcurrencyCurrent: 4,
				StepTime:           10.5,
			},
			contains: []string{"Phase: CREATE", "Active: 4", "CPU:", "Elapsed:"},
		},
		{
			name: "READ operation",
			metric: PerformanceMetric{
				OpType:             "READ",
				TestState:          1,
				ConcurrencyCurrent: 8,
				StepTime:           25.0,
			},
			contains: []string{"Phase: READ", "Active: 8"},
		},
		{
			name: "empty operation",
			metric: PerformanceMetric{
				OpType:             "",
				TestState:          1,
				ConcurrencyCurrent: 2,
			},
			contains: []string{"Phase: UNKNOWN", "Active: 2"},
		},
		{
			name: "idle sample",
			metric: PerformanceMetric{
				OpType:             "none",
				TestState:          0,
				ConcurrencyCurrent: 0,
			},
			contains: []string{"Phase: Idle—waiting for run", "Active: 0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := renderer.formatHeader(&tt.metric)

			for _, expected := range tt.contains {
				if !strings.Contains(result, expected) {
					t.Errorf("Expected '%s' in header, got: %s", expected, result)
				}
			}
		})
	}
}

func TestLiveViewRenderer_formatTable(t *testing.T) {
	params := &scenario.ScenarioParams{
		WorkloadType: "write",
		ObjectSize:   "1MB",
		Threads:      4,
	}

	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	metric := PerformanceMetric{
		SuccessCount: 1000,
		FailedCount:  10,
		OpsPerSec:    100,
		MiBPerSec:    100,
		TestState:    1,
	}

	width := 80
	result := renderer.formatTable(&metric, width)

	// Check table structure (grid-free format)
	lines := strings.Split(result, "\n")
	if len(lines) != 3 {
		t.Errorf("Expected exactly 3 lines in grid-free table, got %d", len(lines))
	}

	// Check for expected column headers (no grid lines)
	if !strings.Contains(lines[0], "Rank") || !strings.Contains(lines[0], "DoneMiB") {
		t.Error("Expected column headers in first line")
	}

	// Check for data rows (no grid lines)
	totalFound := false
	nodeFound := false
	for _, line := range lines {
		if strings.Contains(line, "Total") {
			totalFound = true
		}
		if strings.Contains(line, "0") && !strings.Contains(line, "Total") {
			nodeFound = true
		}
	}

	if !totalFound {
		t.Error("Expected Total row in table")
	}

	if !nodeFound {
		t.Error("Expected node 0 row in table")
	}
}

func TestLiveViewRenderer_calculateDoneMiB(t *testing.T) {
	tests := []struct {
		name         string
		objectSize   string
		successCount int64
		workloadType string
		expected     int64
	}{
		{
			name:         "1MB objects",
			objectSize:   "1MB",
			successCount: 100,
			expected:     100, // 100 * 1MB = 100 MiB
		},
		{
			name:         "1KB objects",
			objectSize:   "1KB",
			successCount: 2048,
			expected:     2, // 2048 * 1KB = 2MB = 2 MiB
		},
		{
			name:         "512KB objects",
			objectSize:   "512KB",
			successCount: 4,
			expected:     2, // 4 * 512KB = 2MB = 2 MiB
		},
		{
			name:         "zero success count",
			objectSize:   "1MB",
			successCount: 0,
			expected:     0,
		},
		{
			name:         "invalid object size",
			objectSize:   "invalid",
			successCount: 100,
			expected:     0,
		},
		{
			name:         "empty object size",
			objectSize:   "",
			successCount: 100,
			expected:     0,
		},
		{
			name:         "list workload counts objects",
			objectSize:   "",
			successCount: 42,
			workloadType: scenario.WorkloadTypeList,
			expected:     42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := &scenario.ScenarioParams{
				ObjectSize:   tt.objectSize,
				WorkloadType: tt.workloadType,
			}

			renderer := NewLiveViewRenderer(params)
			defer renderer.Stop()

			result := renderer.calculateDoneMiB(tt.successCount)
			if result != tt.expected {
				t.Errorf("Expected %d MiB, got %d", tt.expected, result)
			}
		})
	}
}

func TestLiveViewRenderer_ListTableUsesListLabels(t *testing.T) {
	params := &scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeList}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	collector := NewMetricsCollector()
	agg := &PerformanceMetric{
		SuccessCount:       250,
		OpsPerSec:          125,
		MiBPerSec:          0,
		ConcurrencyCurrent: 1,
	}
	nodeMetric := &PerformanceMetric{
		SuccessCount:       250,
		OpsPerSec:          125,
		MiBPerSec:          0,
		ConcurrencyCurrent: 1,
	}
	collector.AddMultiNodeSample(&MultiNodeMetricsUpdate{
		Timestamp:  time.Now(),
		Aggregated: agg,
		PerNode: map[string]*PerformanceMetric{
			"node-1": nodeMetric,
		},
		NodeStatus: map[string]NodeConnectionStatus{
			"node-1": {IsConnected: true, Phase: NodePhaseMetricsFlowing},
		},
	})

	samples := collector.GetSamples()
	if len(samples) == 0 {
		t.Fatal("expected aggregated sample in collector")
	}

	table := renderer.formatMultiNodeTable(collector, &samples[len(samples)-1], 120)
	lines := strings.Split(table, "\n")
	if len(lines) < 2 {
		t.Fatalf("expected header and total rows, got %d line(s)", len(lines))
	}
	header := lines[0]
	if !strings.Contains(header, "Objects") {
		t.Errorf("header %q missing Objects column", header)
	}
	if !strings.Contains(header, "Ops/s") {
		t.Errorf("header %q missing Ops/s column", header)
	}
	if !strings.Contains(header, "Threads") {
		t.Errorf("header %q missing Threads column", header)
	}

	totalFields := strings.Fields(strings.TrimSpace(lines[1]))
	if len(totalFields) < 7 {
		t.Fatalf("expected at least 7 fields in total row, got %v", totalFields)
	}
	if totalFields[2] != "250" {
		t.Errorf("expected Objects column to show 250, got %s", totalFields[2])
	}
	if totalFields[5] != "1" {
		t.Errorf("expected Threads column to show 1, got %s", totalFields[5])
	}
}

func TestLiveViewRenderer_parseObjectSize(t *testing.T) {
	params := &scenario.ScenarioParams{}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{
			name:     "bytes",
			input:    "1024",
			expected: 1024,
		},
		{
			name:     "KB",
			input:    "1KB",
			expected: 1024,
		},
		{
			name:     "MB",
			input:    "1MB",
			expected: 1024 * 1024,
		},
		{
			name:     "GB",
			input:    "1GB",
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "lowercase kb",
			input:    "512kb",
			expected: 512 * 1024,
		},
		{
			name:     "with spaces",
			input:    " 2 MB ",
			expected: 2 * 1024 * 1024,
		},
		{
			name:     "empty string",
			input:    "",
			expected: 0,
		},
		{
			name:     "invalid format",
			input:    "invalid",
			expected: 0,
		},
		{
			name:     "negative number",
			input:    "-1MB",
			expected: -1 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := renderer.parseObjectSize(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %d bytes, got %d for input '%s'", tt.expected, result, tt.input)
			}
		})
	}
}

func TestLiveViewRenderer_calculateElapsed(t *testing.T) {
	params := &scenario.ScenarioParams{}
	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	// Test with step time
	metric := &PerformanceMetric{
		StepTime: 10.5,
	}

	elapsed := renderer.calculateElapsed(metric)
	expected := time.Duration(10.5 * float64(time.Second))
	if elapsed != expected {
		t.Errorf("Expected %v, got %v when using step time", expected, elapsed)
	}

	// Test without step time (should use wall clock)
	metric.StepTime = 0
	elapsed = renderer.calculateElapsed(metric)

	// Should be close to time since start (within a reasonable margin)
	wallClock := time.Since(renderer.startTime)
	diff := elapsed - wallClock
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Second {
		t.Errorf("Wall clock time calculation too far off: expected ~%v, got %v", wallClock, elapsed)
	}
}

func TestLiveViewRenderer_Integration(t *testing.T) {
	params := &scenario.ScenarioParams{
		WorkloadType: "write",
		ObjectSize:   "512KB",
		Threads:      4,
		Duration:     "5m",
	}

	renderer := NewLiveViewRenderer(params)
	defer renderer.Stop()

	// Create full metrics pipeline
	metrics := NewMetricsCollector()

	// Add a series of samples to simulate real workload
	for i := 0; i < 10; i++ {
		sample := PerformanceMetric{
			Timestamp:          time.Now().Add(time.Duration(i) * time.Second),
			OpType:             "CREATE",
			TestState:          1,
			ConcurrencyCurrent: 4,
			SuccessCount:       int64(50 * (i + 1)),
			FailedCount:        int64(i),
			OpsPerSec:          int64(45 + i),
			MiBPerSec:          int64(22 + i/2),
			StepTime:           float64(i + 1),
		}
		metrics.AddSample(sample)
	}

	width := 120
	result := renderer.RenderLiveView(metrics, width)

	if result == "" {
		t.Fatal("Expected non-empty integration result")
	}

	// Verify complete structure (grid-free format)
	expectedElements := []string{
		"Phase: CREATE",
		"CPU:",
		"Active: 4",
		"Elapsed:",
		"Rank",
		"Total",
		"0",
	}

	for _, element := range expectedElements {
		if !strings.Contains(result, element) {
			t.Errorf("Missing expected element '%s' in integration result", element)
		}
	}

	// Should reflect latest values
	if !strings.Contains(result, "500") { // Latest success count
		t.Error("Should show latest success count in table")
	}
}
