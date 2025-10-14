package tui

import (
	"testing"
	"time"
)

// JSON-only pipeline: containerOutputMsg should NOT produce metrics; it only updates display buffer.
func TestContainerOutputAddsToDisplayOnly(t *testing.T) {
	model := InitialModel()

	lines := []string{
		"Starting...",
		"190334.125|250811190334|CREATE| 0|0.0| 0|0|0.004|0|0|0|0",
		"More logs",
	}

	for i, line := range lines {
		msg := containerOutputMsg(line)
		updatedModel, _ := model.Update(msg)
		model = updatedModel.(Model)

		if len(model.processOutput) != i+1 {
			t.Errorf("expected %d lines, got %d", i+1, len(model.processOutput))
		}
		if len(model.metricsCollector.GetSamples()) != 0 {
			t.Errorf("expected 0 metrics from console output, got %d", len(model.metricsCollector.GetSamples()))
		}
	}
}

// TestMetricsPipelineEndToEnd tests the complete flow from Docker output to chart data
func TestMetricsPipelineEndToEnd_JSONOnly(t *testing.T) {
	// Create model with mock Docker
	model := InitialModel()
	mockDocker := NewMockDockerManager()

	// Configure mock to output stats lines
	mockDocker.SetOutputLines([]string{
		"Starting Spt benchmark...",
		"190334.125|250811190334|CREATE|         0|0.0       |           0|     0|0.004  |0.0     |0.0    |         0|          0",
		"Some other log message",
		"190334.125|250811190344|CREATE|         0|0.0       |    15454937|     0|10.004 |1807670 |1807669|        18|         20",
	})

	// Simulate Docker streaming (display only)
	mockDocker.StreamOutput("test-container",
		func(line string) {
			msg := containerOutputMsg(line)
			updatedModel, _ := model.Update(msg)
			model = updatedModel.(Model)
		},
		func(line string) { t.Errorf("Unexpected stderr: %s", line) })

	time.Sleep(50 * time.Millisecond)
	if len(model.metricsCollector.GetSamples()) != 0 {
		t.Errorf("expected 0 metrics from console output stream, got %d", len(model.metricsCollector.GetSamples()))
	}

	// Verify chart renderers can use the data
	_ = model.opsChartRenderer.RenderChart(model.metricsCollector, 80) // no assertion; metrics come from API in production
}

// TestChartUpdateOnMetrics verifies that charts update when new metrics arrive
func TestChartUpdateOnMetricsViaAPI(t *testing.T) {
	model := InitialModel()

	// Initially, charts should show "No data"
	chartOutput := model.opsChartRenderer.RenderChart(model.metricsCollector, 80)
	if chartOutput == "" || chartOutput == "Chart too narrow" {
		t.Skip("Chart width too narrow for test")
	}

	// Inject a metrics update as the orchestrator would
	metric := PerformanceMetric{OpsPerSec: 1234, SuccessCount: 10}
	updatedModel, _ := model.Update(apiMetricsMsg(metric))
	model = updatedModel.(Model)

	// Chart should now have data
	chartWithData := model.opsChartRenderer.RenderChart(model.metricsCollector, 80)
	if chartWithData == chartOutput {
		t.Error("Chart output didn't change after receiving metrics")
	}

	// Verify the chart shows the correct value
	samples := model.metricsCollector.GetSamples()
	if len(samples) == 0 || samples[0].OpsPerSec != 1234 {
		t.Fatalf("expected API-injected metric present, got %v", samples)
	}
}

// TestMetricsParsingRobustness tests that metrics parsing handles mixed output correctly
func TestConsoleOutputRobustness_DisplayOnly(t *testing.T) {
	model := InitialModel()

	// Mix of valid stats, invalid lines, and regular output
	mixedOutput := []string{
		"Starting test...",
		"190334.125|250811190334|CREATE|         0|0.0       |           0|     0|0.004  |0.0     |0.0    |         0|          0", // Valid
		"Some random log message",
		"Partial|line|missing|fields", // Invalid - not enough fields
		"190334.125|250811190344|CREATE|         0|0.0       |    15454937|     0|10.004 |1807670 |1807669|        18|         20", // Valid
		"", // Empty line
		"Another message",
		"190334.125|250811190354|CREATE|         0|0.0       |    33626222|     0|20.007 |invalid |1804135|        19|         22", // Invalid - bad number
	}

	expectedMetricsCount := 0 // Console no longer produces metrics

	for _, line := range mixedOutput {
		msg := containerOutputMsg(line)
		updatedModel, _ := model.Update(msg)
		model = updatedModel.(Model)
	}

	// Should have collected no metrics from console
	samples := model.metricsCollector.GetSamples()
	if len(samples) != expectedMetricsCount {
		t.Errorf("Expected %d valid metrics from mixed output, got %d", expectedMetricsCount, len(samples))
		t.Log("The parser should be resilient to invalid lines")
	}

	// All output lines should still be in the display buffer
	if len(model.processOutput) != len(mixedOutput) {
		t.Errorf("Expected all %d lines in output buffer, got %d", len(mixedOutput), len(model.processOutput))
	}
}

// BenchmarkMetricsParsing measures the performance impact of parsing metrics
// Benchmark removed: console metrics parsing no longer performed

// TestRegressionPreventionChecklist documents what we're testing to prevent future regressions
func TestRegressionPreventionChecklist_JSONOnly(t *testing.T) {
	t.Log("Regression Prevention Checklist:")
	t.Log("✓ Metrics are sourced from JSON API only")
	t.Log("✓ Parsed API metrics must be added to metricsCollector")
	t.Log("✓ Charts must have access to collected metrics")
	t.Log("✓ Mixed output (stats + regular logs) must be handled correctly")
	t.Log("✓ Invalid stats lines must not break the pipeline")
	t.Log("✓ All output must still be displayed regardless of parsing success")

	// This test serves as documentation and will fail if someone removes it
	// without understanding the importance of these checks
	if testing.Short() {
		t.Skip("Skipping checklist in short mode")
	}

	// Run a quick smoke test
	model := InitialModel()
	updatedModel, _ := model.Update(apiMetricsMsg(PerformanceMetric{OpsPerSec: 42}))
	model = updatedModel.(Model)

	if len(model.metricsCollector.GetSamples()) != 1 {
		t.Fatal("REGRESSION DETECTED: API metrics not integrated!")
	}

	t.Log("All regression prevention checks passed")
}
