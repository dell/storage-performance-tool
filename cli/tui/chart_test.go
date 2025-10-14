/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"strings"
	"testing"
	"time"
)

func TestNewGenericChartRenderer(t *testing.T) {
	cr := NewGenericChartRenderer("Test Chart", "units", ExtractOpsPerSec, FormatOpsValue)

	if cr == nil {
		t.Fatal("NewGenericChartRenderer should not return nil")
	}

	if cr.currentScale != 100 {
		t.Errorf("Expected default scale 100, got %d", cr.currentScale)
	}

	if cr.title != "Test Chart" {
		t.Errorf("Expected title 'Test Chart', got %s", cr.title)
	}
}

func TestGenericChartRenderer_RenderEmptyChart(t *testing.T) {
	cr := NewGenericChartRenderer("Test Chart", "units", ExtractOpsPerSec, FormatOpsValue)
	mc := NewMetricsCollector()

	result := cr.RenderChart(mc, 50)

	// Should render empty chart with Y-axis labels
	lines := strings.Split(result, "\n")
	if len(lines) < 7 { // Header + 6 bar lines + timestamp line
		t.Errorf("Expected at least 7 lines, got %d", len(lines))
	}

	// Check that Y-axis labels are present
	found := false
	for _, line := range lines {
		if strings.Contains(line, "|") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected Y-axis labels with '|' character")
	}
}

func TestGenericChartRenderer_RenderWithData(t *testing.T) {
	cr := NewGenericChartRenderer("Test Chart", "ops/s", ExtractOpsPerSec, FormatOpsValue)
	mc := NewMetricsCollector()

	// Add some test data
	testMetrics := []PerformanceMetric{
		{Timestamp: time.Now(), OpsPerSec: 1000, OpType: "CREATE"},
		{Timestamp: time.Now(), OpsPerSec: 2000, OpType: "CREATE"},
		{Timestamp: time.Now(), OpsPerSec: 1500, OpType: "CREATE"},
	}

	for _, metric := range testMetrics {
		mc.AddSample(metric)
	}

	result := cr.RenderChart(mc, 50)

	// Should render chart with data
	lines := strings.Split(result, "\n")
	if len(lines) < 7 {
		t.Errorf("Expected at least 7 lines, got %d", len(lines))
	}

	// Check for header with latest value
	headerLine := lines[0]
	if !strings.Contains(headerLine, "ops/s") {
		t.Error("Expected header to contain latest ops/s value")
	}

	// Check that bars are rendered (should contain block characters or spaces)
	foundBars := false
	for i := 1; i < len(lines)-1; i++ { // Skip header and timestamp lines
		if strings.Contains(lines[i], "█") {
			foundBars = true
			break
		}
	}
	if !foundBars {
		t.Error("Expected to find bar characters (█) in chart")
	}
}

func TestGenericChartRenderer_ScaleUpdate(t *testing.T) {
	cr := NewGenericChartRenderer("Test Chart", "units", ExtractOpsPerSec, FormatOpsValue)
	mc := NewMetricsCollector()

	// Add a high value
	mc.AddSample(PerformanceMetric{Timestamp: time.Now(), OpsPerSec: 5000})

	// Force scale update by setting last update to past
	cr.lastScaleUpdate = time.Now().Add(-10 * time.Second)

	cr.updateScale(mc)

	// Scale should be updated to accommodate the high value
	if cr.currentScale < 5000 {
		t.Errorf("Expected scale to be >= 5000, got %d", cr.currentScale)
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1.0K"},
		{1500, "1.5K"},
		{1000000, "1.0M"},
		{2500000, "2.5M"},
	}

	for _, tt := range tests {
		result := formatValue(tt.input)
		if result != tt.expected {
			t.Errorf("formatValue(%d) = %s, expected %s", tt.input, result, tt.expected)
		}
	}
}

func TestGenericChartRenderer_CalculateBarHeight(t *testing.T) {
	cr := NewGenericChartRenderer("Test Chart", "units", ExtractOpsPerSec, FormatOpsValue)
	cr.currentScale = 1000

	tests := []struct {
		value    int64
		expected int
	}{
		{0, 0},
		{100, 0},  // 100/1000 * 6 = 0.6 -> 0
		{200, 1},  // 200/1000 * 6 = 1.2 -> 1
		{500, 3},  // 500/1000 * 6 = 3.0 -> 3
		{1000, 6}, // 1000/1000 * 6 = 6.0 -> 6
		{1500, 6}, // Should cap at chartHeight
	}

	for _, tt := range tests {
		result := cr.calculateBarHeight(tt.value)
		if result != tt.expected {
			t.Errorf("calculateBarHeight(%d) = %d, expected %d", tt.value, result, tt.expected)
		}
	}
}

func TestGenericChartRenderer_GetDisplaySamples(t *testing.T) {
	cr := NewGenericChartRenderer("Test Chart", "units", ExtractOpsPerSec, FormatOpsValue)

	// Create more samples than we want to display
	samples := make([]PerformanceMetric, 100)
	for i := 0; i < 100; i++ {
		samples[i] = PerformanceMetric{OpsPerSec: int64(i)}
	}

	// Request last 10 samples
	result := cr.getDisplaySamples(samples, 10)

	if len(result) != 10 {
		t.Errorf("Expected 10 samples, got %d", len(result))
	}

	// Should be the last 10 samples (90-99)
	if result[0].OpsPerSec != 90 {
		t.Errorf("Expected first displayed sample to be 90, got %d", result[0].OpsPerSec)
	}

	if result[9].OpsPerSec != 99 {
		t.Errorf("Expected last displayed sample to be 99, got %d", result[9].OpsPerSec)
	}
}

func TestGenericChartRenderer_NarrowWidth(t *testing.T) {
	cr := NewGenericChartRenderer("Test Chart", "units", ExtractOpsPerSec, FormatOpsValue)
	mc := NewMetricsCollector()

	// Test with very narrow width
	result := cr.RenderChart(mc, 5)

	if !strings.Contains(result, "too narrow") {
		t.Error("Expected 'too narrow' message for narrow width")
	}
}

func TestLatencyChartRenderer(t *testing.T) {
	cr := NewGenericChartRenderer("Mean Latency", "", ExtractMeanLatency, FormatLatencyValue)
	mc := NewMetricsCollector()

	// Add some test data with latency values
	testMetrics := []PerformanceMetric{
		{Timestamp: time.Now(), MeanLatency: 500, OpType: "CREATE"},     // 500 μs
		{Timestamp: time.Now(), MeanLatency: 1500, OpType: "CREATE"},    // 1.5 ms
		{Timestamp: time.Now(), MeanLatency: 5000, OpType: "CREATE"},    // 5 ms
		{Timestamp: time.Now(), MeanLatency: 1000000, OpType: "CREATE"}, // 1 s
	}

	for _, metric := range testMetrics {
		mc.AddSample(metric)
	}

	result := cr.RenderChart(mc, 50)

	// Check for header with latest value
	lines := strings.Split(result, "\n")
	headerLine := lines[0]
	if !strings.Contains(headerLine, "1.0s") {
		t.Errorf("Expected header to contain '1.0s', got: %s", headerLine)
	}
	if strings.Contains(headerLine, "μs μs") {
		t.Error("Header should not contain duplicate units")
	}
}

func TestFormatLatencyValue(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0μs"},
		{999, "999μs"},
		{1000, "1.0ms"},
		{1500, "1.5ms"},
		{1000000, "1.0s"},
		{2500000, "2.5s"},
	}

	for _, tt := range tests {
		result := FormatLatencyValue(tt.input)
		if result != tt.expected {
			t.Errorf("FormatLatencyValue(%d) = %s, expected %s", tt.input, result, tt.expected)
		}
	}
}
