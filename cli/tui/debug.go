/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// DebugLayoutRenderer provides debugging utilities for TUI layout
type DebugLayoutRenderer struct {
	width  int
	height int
}

// NewDebugLayoutRenderer creates a new debug layout renderer
func NewDebugLayoutRenderer(width, height int) *DebugLayoutRenderer {
	return &DebugLayoutRenderer{
		width:  width,
		height: height,
	}
}

// RenderLayoutToString renders the complete TUI layout to a string for debugging
func (d *DebugLayoutRenderer) RenderLayoutToString(withSampleData bool) string {
	var result strings.Builder

	// Create a sample model with test data
	model := d.createSampleModel(withSampleData)

	result.WriteString("=== TUI LAYOUT DEBUG ===\n")
	result.WriteString(fmt.Sprintf("Terminal Size: %dx%d\n", d.width, d.height))
	result.WriteString("=========================\n\n")

	// Show viewport calculations
	result.WriteString("--- VIEWPORT CALCULATIONS ---\n")
	result.WriteString(fmt.Sprintf("Live View Height: %d\n", model.getLiveViewHeight()))
	result.WriteString(fmt.Sprintf("Process Output Height: %d\n", model.getProcessViewportHeight()))
	result.WriteString(fmt.Sprintf("Charts Height: %d (fixed)\n", 8))
	result.WriteString(fmt.Sprintf("Status Height: %d (fixed)\n", 1))

	totalUsed := model.getLiveViewHeight() + model.getProcessViewportHeight() + 8 + 1 + 6 // +6 for borders
	result.WriteString(fmt.Sprintf("Total Used: %d\n", totalUsed))
	result.WriteString(fmt.Sprintf("Available: %d\n", d.height))
	result.WriteString(fmt.Sprintf("Overflow: %d\n", totalUsed-d.height))
	result.WriteString("------------------------------\n\n")

	// Render the actual TUI
	result.WriteString("--- RENDERED TUI ---\n")
	tuiOutput := model.View()
	result.WriteString(tuiOutput)
	result.WriteString("\n--- END TUI ---\n\n")

	// Show line count analysis
	lines := strings.Split(tuiOutput, "\n")
	result.WriteString(fmt.Sprintf("Rendered Lines: %d\n", len(lines)))
	result.WriteString("Line breakdown:\n")
	for i, line := range lines {
		if i < 5 || i >= len(lines)-5 {
			result.WriteString(fmt.Sprintf("  %3d: %s\n", i+1, truncateString(line, 60)))
		} else if i == 5 {
			result.WriteString("  ... (middle lines omitted) ...\n")
		}
	}

	return result.String()
}

// createSampleModel creates a model with sample data for testing
func (d *DebugLayoutRenderer) createSampleModel(withData bool) Model {
	model := InitialModel()
	model.width = d.width
	model.height = d.height

	// Add sample scenario params
	params := &scenario.ScenarioParams{
		WorkloadType: "write",
		ObjectSize:   "1MB",
		Threads:      4,
		Duration:     "5m",
	}
	model.scenarioParams = params
	model.liveViewRenderer = NewLiveViewRenderer(params)

	if withData {
		// Add sample metrics
		metric := PerformanceMetric{
			Timestamp:          time.Now(),
			OpType:             "CREATE",
			ConcurrencyCurrent: 4,
			SuccessCount:       1000,
			FailedCount:        5,
			OpsPerSec:          200,
			MBPerSec:           200,
			StepTime:           30.5,
		}
		model.metricsCollector.AddSample(metric)

		// Add sample process output (combined spt output + spt messages)
		model.processOutput = []string{
			"Node Count: 1",
			"Concurrency: 4",
			"Operations: 1000",
			"Duration: 30.5s",
			"Successful: 1000",
			"Failed: 5",
			"Rate: 200 ops/s",
			"[spt] Test Status: RUNNING",
			"[spt] Container: spt-test",
			"[spt] Started: " + time.Now().Format("15:04:05"),
		}
	}

	model.startTime = time.Now().Add(-30 * time.Second)
	model.containerStatus = "Running"

	return model
}

// PrintLayoutCalculations prints detailed layout calculations for debugging
func PrintLayoutCalculations(width, height int) {
	fmt.Printf("=== LAYOUT CALCULATIONS DEBUG ===\n")
	fmt.Printf("Terminal: %dx%d\n", width, height)
	fmt.Printf("----------------------------------\n")

	// Create a temporary model to calculate heights
	model := InitialModel()
	model.width = width
	model.height = height

	liveHeight := model.getLiveViewHeight()
	processHeight := model.getProcessViewportHeight()
	chartHeight := 8
	statusHeight := 1
	borderPadding := 6 // Estimated

	// Show if process height is capped
	dynamicHeight := height - 21
	isCapped := processHeight == 25 && dynamicHeight > 25

	fmt.Printf("Live View:      %3d lines\n", liveHeight)
	if isCapped {
		fmt.Printf("Process Output: %3d lines (capped at 25, dynamic would be %d)\n", processHeight, dynamicHeight)
	} else {
		fmt.Printf("Process Output: %3d lines\n", processHeight)
	}
	fmt.Printf("Charts:         %3d lines (fixed)\n", chartHeight)
	fmt.Printf("Status:         %3d lines (fixed)\n", statusHeight)
	fmt.Printf("Borders/Pad:    %3d lines (estimated)\n", borderPadding)
	fmt.Printf("----------------------------------\n")

	total := liveHeight + processHeight + chartHeight + statusHeight + borderPadding
	fmt.Printf("Total Used:    %3d lines\n", total)
	fmt.Printf("Available:     %3d lines\n", height)

	if total > height {
		fmt.Printf("❌ OVERFLOW:   %3d lines\n", total-height)
	} else {
		fmt.Printf("✅ Remaining:  %3d lines\n", height-total)
	}
	fmt.Printf("==================================\n")
}

// RenderSampleTUIToFile renders a sample TUI layout to a file for QA purposes
func RenderSampleTUIToFile(filename string, width, height int, withData bool) error {
	renderer := NewDebugLayoutRenderer(width, height)
	content := renderer.RenderLayoutToString(withData)

	// Write to file
	err := os.WriteFile(filename, []byte(content), 0600)
	if err != nil {
		return fmt.Errorf("failed to write layout to file: %w", err)
	}

	fmt.Printf("TUI layout rendered to: %s\n", filename)
	return nil
}

// Helper function to truncate strings for display
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
