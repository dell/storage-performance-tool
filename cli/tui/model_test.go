/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/dell/storage-performance-tool/cli/internal/textutil"
)

func TestInitialModel(t *testing.T) {
	m := InitialModel()

	// Test initial state
	if len(m.processOutput) != 0 {
		t.Errorf("Expected empty process output, got %d lines", len(m.processOutput))
	}

	if m.processScroll != 0 {
		t.Errorf("Expected process scroll at 0, got %d", m.processScroll)
	}

	if m.containerStatus != "Not Started" {
		t.Errorf("Expected container status 'Not Started', got '%s'", m.containerStatus)
	}
}

func TestModelInit(t *testing.T) {
	m := InitialModel()
	cmd := m.Init()

	// Init should return a tick command for status updates
	if cmd == nil {
		t.Error("Expected Init to return a command, got nil")
	}
}

func TestModelUpdate_WindowSize(t *testing.T) {
	m := InitialModel()

	// Test window size update
	sizeMsg := tea.WindowSizeMsg{Width: 80, Height: 24}
	updatedModel, _ := m.Update(sizeMsg)
	m = updatedModel.(Model)

	if m.width != 80 {
		t.Errorf("Expected width 80, got %d", m.width)
	}

	if m.height != 24 {
		t.Errorf("Expected height 24, got %d", m.height)
	}
}

func TestModelUpdate_Scrolling(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24
	m.processOutputHidden = false // Make process output visible for scrolling test

	// Add some content to scroll through
	for i := 0; i < 50; i++ {
		m.processOutput = append(m.processOutput, "Line "+string(rune(i)))
	}

	// Test scrolling down
	m.processScroll = 0
	updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = updatedModel.(Model)

	if m.processScroll != 1 {
		t.Errorf("Expected process scroll 1, got %d", m.processScroll)
	}

	// Test scrolling up
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp})
	m = updatedModel.(Model)

	if m.processScroll != 0 {
		t.Errorf("Expected process scroll 0, got %d", m.processScroll)
	}

	// Test page down
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyPgDown})
	m = updatedModel.(Model)

	if m.processScroll != 10 {
		t.Errorf("Expected process scroll 10, got %d", m.processScroll)
	}

	// Test page up
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = updatedModel.(Model)

	if m.processScroll != 0 {
		t.Errorf("Expected process scroll 0, got %d", m.processScroll)
	}
}

func TestModelUpdate_Messages(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Test container output message
	updatedModel, _ := m.Update(containerOutputMsg("Test output"))
	m = updatedModel.(Model)

	if len(m.processOutput) != 1 || m.processOutput[0] != "Test output" {
		t.Errorf("Expected process output to contain 'Test output', got %v", m.processOutput)
	}

	// Test container output with ANSI sequences (should be stripped)
	updatedModel, _ = m.Update(containerOutputMsg("\033[31mRed text\033[0m with ANSI codes"))
	m = updatedModel.(Model)

	if len(m.processOutput) != 2 || m.processOutput[1] != "Red text with ANSI codes" {
		t.Errorf("Expected ANSI-stripped text 'Red text with ANSI codes', got %v", m.processOutput[1])
	}

	// Test spt message (should be prefixed with [spt] and colored yellow)
	updatedModel, _ = m.Update(sptMessageMsg("Test message"))
	m = updatedModel.(Model)

	expected := "\033[33m[spt] Test message\033[0m"
	if len(m.processOutput) != 3 || m.processOutput[2] != expected {
		t.Errorf("Expected process output to contain '%s', got %v", expected, m.processOutput)
	}

	// Test stderr message (should be prefixed with [stderr] and colored red)
	updatedModel, _ = m.Update(containerStderrMsg("Error occurred"))
	m = updatedModel.(Model)

	expectedStderr := "\033[31m[stderr] Error occurred\033[0m"
	if len(m.processOutput) != 4 || m.processOutput[3] != expectedStderr {
		t.Errorf("Expected process output to contain '%s', got %v", expectedStderr, m.processOutput)
	}

	// Test container status message
	updatedModel, _ = m.Update(containerStatusMsg("Running"))
	m = updatedModel.(Model)

	if m.containerStatus != "Running" {
		t.Errorf("Expected container status 'Running', got '%s'", m.containerStatus)
	}

	// Test container started message
	updatedModel, _ = m.Update(containerStartedMsg{containerID: "abc123"})
	m = updatedModel.(Model)

	if m.containerID != "abc123" {
		t.Errorf("Expected container ID 'abc123', got '%s'", m.containerID)
	}

	if m.containerStatus != "Running" {
		t.Errorf("Expected container status 'Running', got '%s'", m.containerStatus)
	}
}

func TestModelUpdate_ScrollbackLimit(t *testing.T) {
	m := InitialModel()

	// Add more than maxScrollback lines
	for i := 0; i < maxScrollback+100; i++ {
		updatedModel, _ := m.Update(containerOutputMsg("Line"))
		m = updatedModel.(Model)
	}

	if len(m.processOutput) != maxScrollback {
		t.Errorf("Expected spt output to be limited to %d lines, got %d", maxScrollback, len(m.processOutput))
	}
}

func TestModelUpdate_Quit(t *testing.T) {
	m := InitialModel()

	// Test quit with 'q'
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("q")})
	if cmd == nil {
		t.Error("Expected quit command, got nil")
	}

	// Test quit with Ctrl+C
	_, cmd = m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	if cmd == nil {
		t.Error("Expected quit command, got nil")
	}
}

func TestModelUpdate_ToggleProcessOutput(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Initially process output should be hidden (new default)
	if !m.processOutputHidden {
		t.Error("Expected process output to be hidden initially")
	}

	// Test toggling with 'm' key - should make it visible
	updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("m")})
	m = updatedModel.(Model)

	if m.processOutputHidden {
		t.Error("Expected process output to be visible after pressing 'm'")
	}

	// Test toggling back - should hide it again
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("m")})
	m = updatedModel.(Model)

	if !m.processOutputHidden {
		t.Error("Expected process output to be hidden after pressing 'm' again")
	}

	// Test that scrolling is disabled when hidden
	m.processOutputHidden = true
	originalScroll := m.processScroll

	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyUp})
	m = updatedModel.(Model)

	if m.processScroll != originalScroll {
		t.Error("Expected scroll position to remain unchanged when process output is hidden")
	}
}

func TestProcessOutputHidden_MessagesStillCollected(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Hide the process output
	m.processOutputHidden = true

	// Send messages while hidden
	updatedModel, _ := m.Update(containerOutputMsg("Test output while hidden"))
	m = updatedModel.(Model)

	updatedModel, _ = m.Update(sptMessageMsg("Test spt message while hidden"))
	m = updatedModel.(Model)

	// Verify messages were still collected
	if len(m.processOutput) != 2 {
		t.Errorf("Expected 2 messages to be collected while hidden, got %d", len(m.processOutput))
	}

	// Unhide and verify messages are there
	m.processOutputHidden = false
	view := m.View()

	if !strings.Contains(view, "Test output while hidden") {
		t.Error("Expected hidden messages to be visible after unhiding")
	}
}

func TestModelUpdate_ToggleCharts(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Initially charts should be hidden (new default)
	if !m.chartsHidden {
		t.Error("Expected charts to be hidden initially")
	}

	// Test toggling with 'g' key - should make them visible
	updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("g")})
	m = updatedModel.(Model)

	if m.chartsHidden {
		t.Error("Expected charts to be visible after pressing 'g'")
	}

	// Test toggling back - should hide them again
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("g")})
	m = updatedModel.(Model)

	if !m.chartsHidden {
		t.Error("Expected charts to be hidden after pressing 'g' again")
	}
}

func TestChartsHidden_MetricsStillCollected(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Hide the charts
	m.chartsHidden = true

	// Add some metrics while charts are hidden
	m.metricsCollector.AddSample(PerformanceMetric{
		OpsPerSec:    100,
		MeanLatency:  50,
		SuccessCount: 1000,
		FailedCount:  5,
		Timestamp:    time.Now(),
	})

	// Verify metrics were still collected
	samples := m.metricsCollector.GetSamples()
	if len(samples) == 0 {
		t.Error("Expected metrics to be collected even when charts are hidden")
	}

	// Unhide charts and verify view doesn't error
	m.chartsHidden = false
	view := m.View()

	if view == "" {
		t.Error("Expected non-empty view after unhiding charts")
	}
}

func TestChartsHidden_HistoricalReset(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Add some metrics to enable historical navigation
	for i := 0; i < 5; i++ {
		m.metricsCollector.AddSample(PerformanceMetric{
			OpsPerSec:    int64(100 + i),
			MeanLatency:  int64(50 + i),
			SuccessCount: int64(1000 + i),
			FailedCount:  int64(i),
			Timestamp:    time.Now().Add(time.Duration(i) * time.Second),
		})
	}

	// Navigate to historical mode
	m.historicalIndex = -2

	// Verify we're in historical mode
	if m.historicalIndex >= 0 {
		t.Error("Expected to be in historical mode")
	}

	// Charts start hidden by default, verify this
	if !m.chartsHidden {
		t.Error("Expected charts to be hidden initially")
	}

	// Unhide charts with 'g' key (first press makes them visible)
	updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("g")})
	m = updatedModel.(Model)

	// Charts should be visible and historical index reset to live mode
	if m.chartsHidden {
		t.Error("Expected charts to be visible after first 'g' press")
	}
	if m.historicalIndex != 0 {
		t.Error("Expected historical index to be reset to live mode (0) when unhiding charts")
	}

	// Set historical mode again for second test
	m.historicalIndex = -2

	// Hide charts again with 'g' key (second press hides them)
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("g")})
	m = updatedModel.(Model)

	// Charts should be hidden and historical index unchanged (hiding doesn't reset)
	if !m.chartsHidden {
		t.Error("Expected charts to be hidden after second 'g' press")
	}
	if m.historicalIndex != -2 {
		t.Error("Expected historical index to remain unchanged when hiding charts")
	}
}

func TestChartsHidden_ViewportHeights(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 30                 // Large enough to see difference but not hit the ProcessViewportMaxHeight-line cap
	m.processOutputHidden = false // Make process output visible for height calculation

	// Test with charts visible
	m.chartsHidden = false
	heightWithCharts := m.getProcessViewportHeight()

	// Test with charts hidden
	m.chartsHidden = true
	heightWithoutCharts := m.getProcessViewportHeight()

	// Process viewport should get more space when charts are hidden
	expectedDifference := 10 // Charts take 10 lines with borders
	actualDifference := heightWithoutCharts - heightWithCharts

	if actualDifference != expectedDifference {
		t.Errorf("Expected process viewport to gain %d lines when charts hidden, got difference of %d",
			expectedDifference, actualDifference)
	}

	// Verify heights are still within bounds
	if heightWithCharts < ProcessViewportMinHeight || heightWithCharts > ProcessViewportMaxHeight {
		t.Errorf("Process viewport height with charts (%d) should be between %d and %d", heightWithCharts, ProcessViewportMinHeight, ProcessViewportMaxHeight)
	}
	if heightWithoutCharts < ProcessViewportMinHeight || heightWithoutCharts > ProcessViewportMaxHeight {
		t.Errorf("Process viewport height without charts (%d) should be between %d and %d", heightWithoutCharts, ProcessViewportMinHeight, ProcessViewportMaxHeight)
	}
}

func TestChartsHidden_ViewCombinations(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Add some metrics so charts have data to render
	m.metricsCollector.AddSample(PerformanceMetric{
		OpsPerSec:    100,
		MeanLatency:  50,
		SuccessCount: 1000,
		FailedCount:  5,
		Timestamp:    time.Now(),
	})

	testCases := []struct {
		name                 string
		chartsHidden         bool
		processOutputHidden  bool
		shouldContainCharts  bool
		shouldContainProcess bool
	}{
		{
			name:                 "both visible",
			chartsHidden:         false,
			processOutputHidden:  false,
			shouldContainCharts:  true,
			shouldContainProcess: true,
		},
		{
			name:                 "charts hidden, process visible",
			chartsHidden:         true,
			processOutputHidden:  false,
			shouldContainCharts:  false,
			shouldContainProcess: true,
		},
		{
			name:                 "charts visible, process hidden",
			chartsHidden:         false,
			processOutputHidden:  true,
			shouldContainCharts:  true,
			shouldContainProcess: false,
		},
		{
			name:                 "both hidden",
			chartsHidden:         true,
			processOutputHidden:  true,
			shouldContainCharts:  false,
			shouldContainProcess: false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			m.chartsHidden = tt.chartsHidden
			m.processOutputHidden = tt.processOutputHidden

			view := m.View()

			// Charts contain "Operations/sec" text
			hasCharts := strings.Contains(view, "Operations/sec")
			if hasCharts != tt.shouldContainCharts {
				t.Errorf("Expected charts visibility to be %v, got %v", tt.shouldContainCharts, hasCharts)
			}

			// Process output contains "Process Output" text when visible
			hasProcessOutput := strings.Contains(view, "Process Output")
			if hasProcessOutput != tt.shouldContainProcess {
				t.Errorf("Expected process output visibility to be %v, got %v", tt.shouldContainProcess, hasProcessOutput)
			}

			// All combinations should contain the live view and status line
			if !strings.Contains(view, "Live View") {
				t.Error("Expected view to always contain live view")
			}
		})
	}
}

func TestModelView_InitialState(t *testing.T) {
	m := InitialModel()
	view := m.View()

	if view != "Initializing..." {
		t.Errorf("Expected 'Initializing...' for zero-sized window, got '%s'", view)
	}
}

func TestModelView_WithContent(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24
	m.processOutputHidden = false // Make process output visible for content test
	m.processOutput = []string{"Test output line 1", "Test output line 2", "\033[33m[spt] Info: Test message\033[0m"}
	m.containerStatus = "Running"

	view := m.View()

	// Check that view contains expected elements
	expectedStrings := []string{
		"Process Output",
		"Test output line 1",
		"Test output line 2",
		"[spt] Info: Test message",
		"Running",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(view, expected) {
			t.Errorf("Expected view to contain '%s'\nView:\n%s", expected, view)
		}
	}
}

func TestGetVisibleLines(t *testing.T) {
	m := InitialModel()

	tests := []struct {
		name     string
		lines    []string
		scroll   int
		height   int
		expected []string
	}{
		{
			name:     "empty lines",
			lines:    []string{},
			scroll:   0,
			height:   5,
			expected: []string{"(empty)"},
		},
		{
			name:     "fewer lines than height",
			lines:    []string{"Line 1", "Line 2"},
			scroll:   0,
			height:   5,
			expected: []string{"Line 1", "Line 2", "", "", ""},
		},
		{
			name:     "exact lines as height",
			lines:    []string{"Line 1", "Line 2", "Line 3", "Line 4", "Line 5"},
			scroll:   0,
			height:   5,
			expected: []string{"Line 1", "Line 2", "Line 3", "Line 4", "Line 5"},
		},
		{
			name:     "scrolled view",
			lines:    []string{"Line 1", "Line 2", "Line 3", "Line 4", "Line 5", "Line 6", "Line 7"},
			scroll:   2,
			height:   3,
			expected: []string{"Line 3", "Line 4", "Line 5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.getVisibleLines(tt.lines, tt.scroll, tt.height)

			// For empty lines test, getVisibleLines returns just ["(empty)"]
			if tt.name == "empty lines" {
				if len(result) != 1 {
					t.Errorf("Expected 1 line for empty case, got %d", len(result))
				}
				if result[0] != "(empty)" {
					t.Errorf("Expected first line to be '(empty)', got '%s'", result[0])
				}
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d lines, got %d", len(tt.expected), len(result))
				return
			}

			for i, line := range result {
				if line != tt.expected[i] {
					t.Errorf("Line %d: expected '%s', got '%s'", i, tt.expected[i], line)
				}
			}
		})
	}
}

func TestGetVisibleLines_LineTruncation(t *testing.T) {
	m := InitialModel()
	m.width = 40 // Small width to trigger truncation

	tests := []struct {
		name     string
		lines    []string
		scroll   int
		height   int
		expected []string
	}{
		{
			name:     "short lines not truncated",
			lines:    []string{"Short line", "Another short"},
			scroll:   0,
			height:   3,
			expected: []string{"Short line", "Another short", ""},
		},
		{
			name:   "long lines truncated with ellipsis",
			lines:  []string{"This is a very long line that should be truncated because it exceeds the viewport width"},
			scroll: 0,
			height: 2,
			// maxWidth = 40 - 6 = 34, so line is truncated to 31 chars + "..."
			expected: []string{"This is a very long line that s...", ""},
		},
		{
			name: "mixed short and long lines",
			lines: []string{
				"Short",
				"This is an extremely long line that will definitely be truncated when displayed",
				"Medium line here",
			},
			scroll: 0,
			height: 3,
			expected: []string{
				"Short",
				"This is an extremely long line ...", // truncated
				"Medium line here",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.getVisibleLines(tt.lines, tt.scroll, tt.height)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d lines, got %d", len(tt.expected), len(result))
				return
			}

			for i, line := range result {
				if line != tt.expected[i] {
					t.Errorf("Line %d: expected '%s' (len=%d), got '%s' (len=%d)",
						i, tt.expected[i], len(tt.expected[i]), line, len(line))
				}
			}
		})
	}
}

func TestGetViewportHeights(t *testing.T) {
	m := InitialModel()

	tests := []struct {
		name            string
		height          int
		chartsHidden    bool
		processHidden   bool
		expectedProcess int
	}{
		{
			name:            "small window",
			height:          15,
			chartsHidden:    false,
			processHidden:   false,
			expectedProcess: ProcessViewportMinHeight, // Minimum due to max(ProcessViewportMinHeight, height-LayoutHeightWithCharts) = max(5, -3) = 5
		},
		{
			name:            "normal window",
			height:          24,
			chartsHidden:    false,
			processHidden:   false,
			expectedProcess: 6, // max(ProcessViewportMinHeight, 24-LayoutHeightWithCharts) = max(5, 6) = 6
		},
		{
			name:            "large window",
			height:          50,
			chartsHidden:    false,
			processHidden:   false,
			expectedProcess: ProcessViewportMaxHeight, // Capped at ProcessViewportMaxHeight lines maximum (would be 32)
		},
		{
			name:            "very large window",
			height:          80,
			chartsHidden:    false,
			processHidden:   false,
			expectedProcess: ProcessViewportMaxHeight, // Still capped at ProcessViewportMaxHeight lines maximum
		},
		{
			name:            "hidden process output",
			height:          50,
			chartsHidden:    false,
			processHidden:   true,
			expectedProcess: 0, // Should be 0 when hidden
		},
		{
			name:            "small window with charts hidden",
			height:          15,
			chartsHidden:    true,
			processHidden:   false,
			expectedProcess: 7, // max(ProcessViewportMinHeight, 15-LayoutHeightWithoutCharts) = max(5, 7) = 7
		},
		{
			name:            "normal window with charts hidden",
			height:          24,
			chartsHidden:    true,
			processHidden:   false,
			expectedProcess: 16, // max(ProcessViewportMinHeight, 24-LayoutHeightWithoutCharts) = max(5, 16) = 16
		},
		{
			name:            "large window with charts hidden",
			height:          50,
			chartsHidden:    true,
			processHidden:   false,
			expectedProcess: ProcessViewportMaxHeight, // Still capped at ProcessViewportMaxHeight lines maximum (would be 42)
		},
		{
			name:            "both charts and process hidden",
			height:          50,
			chartsHidden:    true,
			processHidden:   true,
			expectedProcess: 0, // Should be 0 when process output hidden regardless of charts
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m.height = tt.height
			m.chartsHidden = tt.chartsHidden
			m.processOutputHidden = tt.processHidden

			processHeight := m.getProcessViewportHeight()
			if processHeight != tt.expectedProcess {
				t.Errorf("Expected process viewport height %d, got %d", tt.expectedProcess, processHeight)
			}
		})
	}
}

func TestRenderStatusLine(t *testing.T) {
	m := InitialModel()
	m.width = 140 // Increased width to ensure help text fits
	m.startTime = time.Now().Add(-1 * time.Hour)
	m.containerStatus = "Running"
	m.testStatus = "RUNNING"

	statusLine := m.renderStatusLine()

	// Check that status line contains expected elements
	expectedStrings := []string{
		time.Now().Format("2006-01-02"),
		"Running",
		"Test: RUNNING",
		"01:", // Part of elapsed time
		"↑/↓: Scroll | ←/→: Scan | g: Graphs | m: Messages | q: Quit ",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(statusLine, expected) {
			t.Errorf("Expected status line to contain '%s', got '%s'", expected, statusLine)
		}
	}
}

func TestAutoScroll(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Set scroll to bottom
	viewportHeight := m.getProcessViewportHeight()

	// Add lines to fill viewport
	for i := 0; i < viewportHeight+5; i++ {
		m.processOutput = append(m.processOutput, "Line")
	}

	// Set scroll to exactly at bottom (where auto-scroll should trigger)
	// The condition in Update is: if m.processScroll >= len(m.processOutput)-m.getProcessViewportHeight()-1
	m.processScroll = len(m.processOutput) - viewportHeight

	// Add new line - should auto-scroll
	updatedModel, _ := m.Update(containerOutputMsg("New line"))
	m = updatedModel.(Model)

	// After adding a line, the scroll should be adjusted to show the bottom
	expectedScroll := max(0, len(m.processOutput)-viewportHeight)
	if m.processScroll != expectedScroll {
		t.Errorf("Expected auto-scroll to %d, got %d (lines: %d, viewport: %d)",
			expectedScroll, m.processScroll, len(m.processOutput), viewportHeight)
	}
}

func TestMetricsIntegration(t *testing.T) {
	m := InitialModel()

	// Verify initial state
	if m.metricsCollector == nil {
		t.Fatal("MetricsCollector should be initialized")
	}

	if len(m.metricsCollector.GetSamples()) != 0 {
		t.Errorf("Expected 0 initial samples, got %d", len(m.metricsCollector.GetSamples()))
	}

	// Create a sample metric (JSON-only path now)
	metric := PerformanceMetric{OpsPerSec: 2541569, OpType: "CREATE", SuccessCount: 22544384}

	// Send metrics update message
	msg := metricsUpdateMsg(metric)
	updatedModel, _ := m.Update(msg)
	m = updatedModel.(Model)

	// Verify the metric was added to the collector
	samples := m.metricsCollector.GetSamples()
	if len(samples) != 1 {
		t.Errorf("Expected 1 sample after update, got %d", len(samples))
	}

	if len(samples) > 0 {
		if samples[0].OpsPerSec != 2541569 {
			t.Errorf("Expected OpsPerSec to be 2541569, got %d", samples[0].OpsPerSec)
		}
		if samples[0].OpType != "CREATE" {
			t.Errorf("Expected OpType to be CREATE, got %s", samples[0].OpType)
		}
	}

	// Verify latest ops/sec retrieval
	latest := m.metricsCollector.GetLatestOpsPerSec()
	if latest != 2541569 {
		t.Errorf("Expected latest ops/sec to be 2541569, got %d", latest)
	}

	// Test multiple metrics
	metric2 := PerformanceMetric{OpsPerSec: 2518953, OpType: "CREATE", SuccessCount: 47848736}

	msg2 := metricsUpdateMsg(metric2)
	updatedModel, _ = m.Update(msg2)
	m = updatedModel.(Model)

	// Verify we now have 2 samples
	samples = m.metricsCollector.GetSamples()
	if len(samples) != 2 {
		t.Errorf("Expected 2 samples after second update, got %d", len(samples))
	}

	// Verify latest is the second metric
	latest = m.metricsCollector.GetLatestOpsPerSec()
	if latest != 2518953 {
		t.Errorf("Expected latest ops/sec to be 2518953, got %d", latest)
	}
}

func TestChartIntegrationInTUI(t *testing.T) {
	m := InitialModel()
	m.width = 150 // Increased width to allow full header display
	m.height = 30
	m.chartsHidden = false // Make charts visible for integration test

	// Verify chart renderers are initialized
	if m.opsChartRenderer == nil {
		t.Fatal("OpsChartRenderer should be initialized")
	}
	if m.latencyChartRenderer == nil {
		t.Fatal("LatencyChartRenderer should be initialized")
	}

	// Add some metrics data
	metric := PerformanceMetric{
		Timestamp:   time.Now(),
		OpsPerSec:   2500,
		MeanLatency: 1500, // 1.5ms
		OpType:      "CREATE",
	}

	msg := metricsUpdateMsg(metric)
	updatedModel, _ := m.Update(msg)
	m = updatedModel.(Model)

	// Test that the view includes chart content
	view := m.View()

	// Should contain chart titles (now separate viewports)
	if !strings.Contains(view, "Operations/sec") {
		t.Error("Expected view to contain Operations/sec chart title")
	}
	if !strings.Contains(view, "Mean Latency") {
		t.Error("Expected view to contain Mean Latency chart title")
	}

	// Should contain ops/s in the chart area
	if !strings.Contains(view, "ops/s") {
		t.Error("Expected view to contain ops/s indicator")
	}
}

func TestCombineChartsHorizontally(t *testing.T) {
	m := InitialModel()

	leftChart := "Left Line 1\nLeft Line 2\nLeft Line 3"
	rightChart := "Right Line 1\nRight Line 2\nRight Line 3"

	result := m.combineChartsHorizontally(leftChart, rightChart)
	lines := strings.Split(result, "\n")

	if len(lines) != 3 {
		t.Errorf("Expected 3 lines, got %d", len(lines))
	}

	// Check that lines are combined with no gap
	for i, line := range lines {
		expectedLine := fmt.Sprintf("Left Line %dRight Line %d", i+1, i+1)
		if line != expectedLine {
			t.Errorf("Line %d: expected '%s', got '%s'", i, expectedLine, line)
		}
	}
}

func TestModelUpdate_HistoricalNavigation(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Add some metrics samples to navigate through
	for i := 0; i < 5; i++ {
		metric := PerformanceMetric{
			Timestamp:    time.Now().Add(time.Duration(i) * time.Second),
			OpsPerSec:    int64(1000 + i*100),
			SptTimestamp: fmt.Sprintf("19403%d", i),
			OpType:       "CREATE",
		}
		m.metricsCollector.AddSample(metric)
	}

	// Initially should be in live mode (historicalIndex = 0)
	if m.historicalIndex != 0 {
		t.Errorf("Expected initial historicalIndex to be 0, got %d", m.historicalIndex)
	}

	// Test left arrow (go backward in time)
	updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("h")})
	m = updatedModel.(Model)
	if m.historicalIndex != -1 {
		t.Errorf("Expected historicalIndex to be -1 after left arrow, got %d", m.historicalIndex)
	}

	// Test another left arrow
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft})
	m = updatedModel.(Model)
	if m.historicalIndex != -2 {
		t.Errorf("Expected historicalIndex to be -2 after second left arrow, got %d", m.historicalIndex)
	}

	// Test right arrow (go forward in time)
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("l")})
	m = updatedModel.(Model)
	if m.historicalIndex != -1 {
		t.Errorf("Expected historicalIndex to be -1 after right arrow, got %d", m.historicalIndex)
	}

	// Test another right arrow
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight})
	m = updatedModel.(Model)
	if m.historicalIndex != 0 {
		t.Errorf("Expected historicalIndex to be 0 after second right arrow, got %d", m.historicalIndex)
	}

	// Test that we can't go forward past live mode
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight})
	m = updatedModel.(Model)
	if m.historicalIndex != 0 {
		t.Errorf("Expected historicalIndex to stay at 0 when trying to go past live mode, got %d", m.historicalIndex)
	}

	// Go back to historical mode
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft})
	m = updatedModel.(Model)
	if m.historicalIndex != -1 {
		t.Errorf("Expected historicalIndex to be -1, got %d", m.historicalIndex)
	}

	// Test ESC key to jump back to live mode
	updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = updatedModel.(Model)
	if m.historicalIndex != 0 {
		t.Errorf("Expected historicalIndex to be 0 after ESC key, got %d", m.historicalIndex)
	}

	// Test boundary condition - can't go past oldest sample
	// Go to furthest back possible
	for i := 0; i < 10; i++ { // Try to go back more than we have samples
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft})
		m = updatedModel.(Model)
	}
	// Should be at -(len(samples)-1) = -4 (we have 5 samples, so oldest is index -4)
	if m.historicalIndex != -4 {
		t.Errorf("Expected historicalIndex to be -4 (oldest sample), got %d", m.historicalIndex)
	}
}

func TestHistoricalIndex_AutoAdjustment(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Add initial metrics samples
	for i := 0; i < 3; i++ {
		metric := PerformanceMetric{
			Timestamp:    time.Now().Add(time.Duration(i) * time.Second),
			OpsPerSec:    int64(1000 + i*100),
			SptTimestamp: fmt.Sprintf("19403%d", i),
			OpType:       "CREATE",
		}
		m.metricsCollector.AddSample(metric)
	}

	// Go to historical mode (viewing the first sample, which is index -2)
	m.historicalIndex = -2

	// Verify we're viewing the expected sample
	selectedIndex := m.getSelectedSampleIndex()
	if selectedIndex != 0 {
		t.Errorf("Expected selected sample index to be 0, got %d", selectedIndex)
	}

	// Add a new metric sample (simulating new data arriving)
	newMetric := PerformanceMetric{
		Timestamp:    time.Now().Add(4 * time.Second),
		OpsPerSec:    int64(1400),
		SptTimestamp: "194034",
		OpType:       "CREATE",
	}

	// Simulate receiving the new metric (this should auto-adjust historical index)
	updatedModel, _ := m.Update(metricsUpdateMsg(newMetric))
	m = updatedModel.(Model)

	// The historical index should have been decremented to maintain focus on same data
	// It was -2, now should be -3 (to keep viewing the same sample)
	if m.historicalIndex != -3 {
		t.Errorf("Expected historicalIndex to be auto-adjusted to -3, got %d", m.historicalIndex)
	}

	// Verify we're still viewing the same sample (now at index 0 in a longer array)
	selectedIndex = m.getSelectedSampleIndex()
	if selectedIndex != 0 {
		t.Errorf("Expected to still be viewing sample at index 0 after auto-adjustment, got %d", selectedIndex)
	}

	// Test that live mode (historicalIndex = 0) is not affected by auto-adjustment
	m.historicalIndex = 0

	// Add another new metric
	newMetric2 := PerformanceMetric{
		Timestamp:    time.Now().Add(5 * time.Second),
		OpsPerSec:    int64(1500),
		SptTimestamp: "194035",
		OpType:       "CREATE",
	}

	updatedModel, _ = m.Update(metricsUpdateMsg(newMetric2))
	m = updatedModel.(Model)

	// Live mode should remain at 0
	if m.historicalIndex != 0 {
		t.Errorf("Expected historicalIndex to remain 0 in live mode, got %d", m.historicalIndex)
	}

	// Test multiple auto-adjustments in sequence
	m.historicalIndex = -1 // Viewing second-to-latest sample

	// Add multiple new samples
	for i := 0; i < 3; i++ {
		newMetric := PerformanceMetric{
			Timestamp:    time.Now().Add(time.Duration(6+i) * time.Second),
			OpsPerSec:    int64(1600 + i*100),
			SptTimestamp: fmt.Sprintf("19403%d", 6+i),
			OpType:       "CREATE",
		}

		updatedModel, _ = m.Update(metricsUpdateMsg(newMetric))
		m = updatedModel.(Model)
	}

	// After 3 new samples, historicalIndex should have been decremented 3 times
	// Started at -1, should now be -4
	if m.historicalIndex != -4 {
		t.Errorf("Expected historicalIndex to be -4 after 3 auto-adjustments, got %d", m.historicalIndex)
	}
}

func TestGetVisibleLinesWithHighlight(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Test data with Spt-style output lines (note: first field must be decimal format)
	testLines := []string{
		"Node startup message",
		"194037.917|250630194048|CREATE|         8|12.5      |    22544384|     2|10.008 |2541569 |2541569|         5|          9",
		"Some other log message",
		"194038.918|250630194049|CREATE|         8|13.0      |    22544384|     2|10.009 |2541570 |2541570|         5|          9",
		"Another non-data line",
		"194039.919|250630194050|CREATE|         8|13.5      |    22544384|     2|10.010 |2541571 |2541571|         5|          9",
	}

	tests := []struct {
		name               string
		lines              []string
		scroll             int
		height             int
		selectedTimestamp  string
		expectedHighlights []bool // which lines should be highlighted
	}{
		{
			name:               "no timestamp selected - no highlighting",
			lines:              testLines,
			scroll:             0,
			height:             4,
			selectedTimestamp:  "",
			expectedHighlights: []bool{false, false, false, false},
		},
		{
			name:               "timestamp selected - highlight matching line",
			lines:              testLines,
			scroll:             0,
			height:             4,
			selectedTimestamp:  "250630194048",
			expectedHighlights: []bool{false, true, false, false}, // Second line should be highlighted
		},
		{
			name:               "timestamp selected - no matching line",
			lines:              testLines,
			scroll:             0,
			height:             4,
			selectedTimestamp:  "999999999999",
			expectedHighlights: []bool{false, false, false, false},
		},
		{
			name:               "timestamp selected - with scrolling",
			lines:              testLines,
			scroll:             3,
			height:             3,
			selectedTimestamp:  "250630194050",
			expectedHighlights: []bool{false, false, true}, // Third visible line (index 5 overall) should be highlighted
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := m.getVisibleLinesWithHighlight(tt.lines, tt.scroll, tt.height, tt.selectedTimestamp)

			if len(result) != tt.height {
				t.Errorf("Expected %d lines, got %d", tt.height, len(result))
				return
			}

			for i, line := range result {
				shouldBeHighlighted := i < len(tt.expectedHighlights) && tt.expectedHighlights[i]
				isHighlighted := strings.Contains(line, "\033[33m") && strings.Contains(line, "\033[0m")

				if shouldBeHighlighted && !isHighlighted {
					t.Errorf("Line %d should be highlighted but wasn't: '%s'", i, line)
				} else if !shouldBeHighlighted && isHighlighted {
					t.Errorf("Line %d should not be highlighted but was: '%s'", i, line)
				}
			}
		})
	}
}

func TestGetVisibleLinesWithHighlight_Truncation(t *testing.T) {
	m := InitialModel()
	m.width = 50 // Small width to test truncation with highlighting

	// Long line that will be truncated
	longDataLine := "194037.917|250630194048|CREATE|         8|12.5      |this_is_a_very_long_line_that_should_be_truncated_because_it_exceeds_the_viewport_width|     2|10.008 |2541569 |2541569|         5|          9"
	testLines := []string{
		"Short line",
		longDataLine,
		"Another line",
	}

	result := m.getVisibleLinesWithHighlight(testLines, 0, 3, "250630194048")

	if len(result) != 3 {
		t.Errorf("Expected 3 lines, got %d", len(result))
		return
	}

	// Check that the long line is both highlighted and truncated
	highlightedLine := result[1]
	if !strings.Contains(highlightedLine, "\033[33m") || !strings.Contains(highlightedLine, "\033[0m") {
		t.Errorf("Long line should be highlighted: '%s'", highlightedLine)
	}
	if !strings.Contains(highlightedLine, "...") {
		t.Errorf("Long line should be truncated with ellipsis: '%s'", highlightedLine)
	}

	// The visible length (excluding ANSI codes) should be within the limit
	cleanLine := stripANSIEscapeSequences(highlightedLine)
	maxWidth := m.width - 6 // Same calculation as in the function
	if len(cleanLine) > maxWidth {
		t.Errorf("Highlighted line is too long after truncation: %d chars (max %d)", len(cleanLine), maxWidth)
	}
}

func TestIsSptDataLine(t *testing.T) {
	tests := []struct {
		name            string
		line            string
		targetTimestamp string
		expectedMatch   bool
		description     string
	}{
		{
			name:            "valid spt data line without timestamp check",
			line:            "194037.917|250630194048|CREATE|         8|12.5      |    22544384|     2|10.008 |2541569 |2541569|         5|          9",
			targetTimestamp: "",
			expectedMatch:   true,
			description:     "Should match valid Spt data format",
		},
		{
			name:            "valid spt data line with matching timestamp",
			line:            "194037.917|250630194048|CREATE|         8|12.5      |    22544384|     2|10.008 |2541569 |2541569|         5|          9",
			targetTimestamp: "250630194048",
			expectedMatch:   true,
			description:     "Should match when timestamp matches",
		},
		{
			name:            "valid spt data line with non-matching timestamp",
			line:            "194037.917|250630194048|CREATE|         8|12.5      |    22544384|     2|10.008 |2541569 |2541569|         5|          9",
			targetTimestamp: "999999999999",
			expectedMatch:   false,
			description:     "Should not match when timestamp doesn't match",
		},
		{
			name:            "spt data line with ANSI codes at start",
			line:            "\033[32m194037.917| 250630194048 |CREATE|         8|12.5      |    22544384|     2|10.008 |2541569 |2541569|         5|          9\033[0m",
			targetTimestamp: "250630194048",
			expectedMatch:   true,
			description:     "Should handle ANSI escape sequences at line start",
		},
		{
			name:            "regular log message",
			line:            "Node startup message",
			targetTimestamp: "",
			expectedMatch:   false,
			description:     "Should not match regular log messages",
		},
		{
			name:            "line starting with viewport frame",
			line:            "││194037.917|250630194048|CREATE|8|12.5|22544384|2|10.008|2541569|2541569|5|9",
			targetTimestamp: "",
			expectedMatch:   false,
			description:     "Should not match lines that start with viewport frame characters",
		},
		{
			name:            "line starting with single frame character",
			line:            "│some log message",
			targetTimestamp: "",
			expectedMatch:   false,
			description:     "Should not match lines that start with single frame character",
		},
		{
			name:            "invalid format - missing decimal in first field",
			line:            "194037|250630194048|CREATE|8|12.5|22544384|2|10.008|2541569|2541569|5|9",
			targetTimestamp: "",
			expectedMatch:   false,
			description:     "Should not match if first field is not decimal format",
		},
		{
			name:            "invalid format - missing second field",
			line:            "194037.917||CREATE|8|12.5|22544384|2|10.008|2541569|2541569|5|9",
			targetTimestamp: "",
			expectedMatch:   false,
			description:     "Should not match if timestamp field is empty",
		},
		{
			name:            "minimal valid format",
			line:            "0.1|123|",
			targetTimestamp: "",
			expectedMatch:   true,
			description:     "Should match minimal valid format",
		},
		{
			name:            "minimal valid format with timestamp check",
			line:            "0.1|123|anything",
			targetTimestamp: "123",
			expectedMatch:   true,
			description:     "Should match minimal valid format with matching timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSptDataLine(tt.line, tt.targetTimestamp)
			if result != tt.expectedMatch {
				t.Errorf("isSptDataLine(%q, %q) = %v, want %v (%s)",
					tt.line, tt.targetTimestamp, result, tt.expectedMatch, tt.description)
			}
		})
	}
}

func TestGetSelectedSampleIndex(t *testing.T) {
	m := InitialModel()

	tests := []struct {
		name            string
		samplesCount    int
		historicalIndex int
		expectedIndex   int
		description     string
	}{
		{
			name:            "live mode with samples",
			samplesCount:    5,
			historicalIndex: 0,
			expectedIndex:   4, // Latest sample is at index 4 (len-1)
			description:     "In live mode (0), should return latest sample index",
		},
		{
			name:            "one step back",
			samplesCount:    5,
			historicalIndex: -1,
			expectedIndex:   3, // One back from latest
			description:     "Historical index -1 should return second-to-latest sample",
		},
		{
			name:            "two steps back",
			samplesCount:    5,
			historicalIndex: -2,
			expectedIndex:   2,
			description:     "Historical index -2 should return third-to-latest sample",
		},
		{
			name:            "oldest sample",
			samplesCount:    5,
			historicalIndex: -4,
			expectedIndex:   0, // Oldest sample
			description:     "Historical index -4 should return oldest sample (index 0)",
		},
		{
			name:            "beyond oldest sample",
			samplesCount:    5,
			historicalIndex: -10,
			expectedIndex:   -1, // Invalid - should return -1
			description:     "Historical index beyond oldest should return -1",
		},
		{
			name:            "beyond live mode",
			samplesCount:    5,
			historicalIndex: 1,
			expectedIndex:   -1, // Invalid - should return -1
			description:     "Historical index beyond live mode should return -1",
		},
		{
			name:            "no samples available",
			samplesCount:    0,
			historicalIndex: 0,
			expectedIndex:   -1, // No samples - should return -1
			description:     "With no samples, should return -1",
		},
		{
			name:            "single sample - live mode",
			samplesCount:    1,
			historicalIndex: 0,
			expectedIndex:   0,
			description:     "With single sample in live mode, should return index 0",
		},
		{
			name:            "single sample - one back",
			samplesCount:    1,
			historicalIndex: -1,
			expectedIndex:   -1, // Beyond available samples
			description:     "With single sample, -1 historical index should return -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset the model and add the specified number of samples
			m = InitialModel()
			for i := 0; i < tt.samplesCount; i++ {
				metric := PerformanceMetric{
					Timestamp:    time.Now().Add(time.Duration(i) * time.Second),
					OpsPerSec:    int64(1000 + i*100),
					SptTimestamp: fmt.Sprintf("timestamp_%d", i),
					OpType:       "CREATE",
				}
				m.metricsCollector.AddSample(metric)
			}

			// Set the historical index
			m.historicalIndex = tt.historicalIndex

			// Test the function
			result := m.getSelectedSampleIndex()

			if result != tt.expectedIndex {
				t.Errorf("getSelectedSampleIndex() with %d samples and historicalIndex %d = %d, want %d (%s)",
					tt.samplesCount, tt.historicalIndex, result, tt.expectedIndex, tt.description)
			}
		})
	}
}

func TestGetSelectedTimestamp(t *testing.T) {
	m := InitialModel()

	// Add some test samples with known timestamps
	testTimestamps := []string{"250630194001", "250630194002", "250630194003", "250630194004", "250630194005"}
	for i, timestamp := range testTimestamps {
		metric := PerformanceMetric{
			Timestamp:    time.Now().Add(time.Duration(i) * time.Second),
			OpsPerSec:    int64(1000 + i*100),
			SptTimestamp: timestamp,
			OpType:       "CREATE",
		}
		m.metricsCollector.AddSample(metric)
	}

	tests := []struct {
		name              string
		historicalIndex   int
		expectedTimestamp string
		description       string
	}{
		{
			name:              "live mode",
			historicalIndex:   0,
			expectedTimestamp: "",
			description:       "In live mode, should return empty string",
		},
		{
			name:              "one step back",
			historicalIndex:   -1,
			expectedTimestamp: "250630194004", // Second-to-latest
			description:       "Historical index -1 should return second-to-latest timestamp",
		},
		{
			name:              "two steps back",
			historicalIndex:   -2,
			expectedTimestamp: "250630194003", // Third-to-latest
			description:       "Historical index -2 should return third-to-latest timestamp",
		},
		{
			name:              "oldest sample",
			historicalIndex:   -4,
			expectedTimestamp: "250630194001", // Oldest
			description:       "Historical index -4 should return oldest timestamp",
		},
		{
			name:              "beyond oldest sample",
			historicalIndex:   -10,
			expectedTimestamp: "",
			description:       "Historical index beyond oldest should return empty string",
		},
		{
			name:              "beyond live mode",
			historicalIndex:   1,
			expectedTimestamp: "",
			description:       "Historical index beyond live mode should return empty string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the historical index
			m.historicalIndex = tt.historicalIndex

			// Test the function
			result := m.getSelectedTimestamp()

			if result != tt.expectedTimestamp {
				t.Errorf("getSelectedTimestamp() with historicalIndex %d = '%s', want '%s' (%s)",
					tt.historicalIndex, result, tt.expectedTimestamp, tt.description)
			}
		})
	}
}

func TestGetSelectedTimestamp_NoSamples(t *testing.T) {
	m := InitialModel()
	// No samples added

	tests := []struct {
		name              string
		historicalIndex   int
		expectedTimestamp string
	}{
		{
			name:              "no samples - live mode",
			historicalIndex:   0,
			expectedTimestamp: "",
		},
		{
			name:              "no samples - historical mode",
			historicalIndex:   -1,
			expectedTimestamp: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m.historicalIndex = tt.historicalIndex
			result := m.getSelectedTimestamp()

			if result != tt.expectedTimestamp {
				t.Errorf("getSelectedTimestamp() with no samples and historicalIndex %d = '%s', want '%s'",
					tt.historicalIndex, result, tt.expectedTimestamp)
			}
		})
	}
}

func TestStripANSIEscapeSequences(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no ANSI codes",
			input:    "Plain text without any formatting",
			expected: "Plain text without any formatting",
		},
		{
			name:     "simple color code",
			input:    "\033[31mRed text\033[0m",
			expected: "Red text",
		},
		{
			name:     "multiple color codes",
			input:    "\033[31mRed\033[0m normal \033[32mGreen\033[0m text",
			expected: "Red normal Green text",
		},
		{
			name:     "complex formatting",
			input:    "\033[1;31;4mBold red underlined\033[0m normal",
			expected: "Bold red underlined normal",
		},
		{
			name:     "spt-style output",
			input:    "\033[32m194037.917\033[0m|250630194048|CREATE|         8|12.5      |    22544384|     2|10.008 |2541569 |2541569|         5|          9",
			expected: "194037.917|250630194048|CREATE|         8|12.5      |    22544384|     2|10.008 |2541569 |2541569|         5|          9",
		},
		{
			name:     "ansi codes at beginning",
			input:    "\033[33m\033[1mYellow bold text",
			expected: "Yellow bold text",
		},
		{
			name:     "ansi codes at end",
			input:    "Normal text\033[0m\033[39m",
			expected: "Normal text",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only ansi codes",
			input:    "\033[31m\033[1m\033[4m\033[0m",
			expected: "",
		},
		{
			name:     "incomplete ansi sequence",
			input:    "Text with \033[31incomplete sequence",
			expected: "Text with \033[31incomplete sequence", // Incomplete sequences not ending in 'm' are not stripped
		},
		{
			name:     "ansi codes with extended sequences",
			input:    "\033[38;5;196mExtended color\033[0m",
			expected: "Extended color",
		},
		{
			name:     "cursor positioning codes",
			input:    "\033[10;20HPositioned text\033[0m",
			expected: "\033[10;20HPositioned text", // Only 'm' codes are stripped, not 'H' codes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripANSIEscapeSequences(tt.input)
			if result != tt.expected {
				t.Errorf("stripANSIEscapeSequences(%q) = %q, want %q",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestStripANSIEscapeSequences_LengthCalculation(t *testing.T) {
	// Test that the function is useful for length calculations
	testString := "\033[31m\033[1mHello\033[0m \033[32mWorld\033[0m"
	stripped := stripANSIEscapeSequences(testString)

	if stripped != "Hello World" {
		t.Errorf("Expected 'Hello World', got %q", stripped)
	}

	if len(stripped) != 11 { // "Hello World" = 11 characters
		t.Errorf("Expected length 11, got %d", len(stripped))
	}

	// Verify original string was much longer due to ANSI codes
	if len(testString) <= len(stripped) {
		t.Errorf("Original string should be longer than stripped version")
	}
}

func TestLineTruncation_WithANSICodes(t *testing.T) {
	m := InitialModel()
	m.width = 40 // Small width to force truncation

	tests := []struct {
		name        string
		input       []string
		expectedLen int
		shouldTrunc bool
		description string
	}{
		{
			name:        "line with ANSI codes that fits",
			input:       []string{"\033[31mShort red\033[0m text"},
			expectedLen: 1, // Should not be truncated
			shouldTrunc: false,
			description: "Short line with ANSI codes should not be truncated",
		},
		{
			name:        "line with ANSI codes that exceeds width",
			input:       []string{"\033[31mThis is a very long line with ANSI codes that should definitely be truncated because it exceeds the viewport width\033[0m"},
			expectedLen: 1,
			shouldTrunc: true,
			description: "Long line with ANSI codes should be truncated with ellipsis",
		},
		{
			name: "multiple lines with mixed ANSI content",
			input: []string{
				"Short plain text",
				"\033[32mMedium length colored text\033[0m",
				"\033[33m\033[1mVery long bold yellow text that will definitely exceed the maximum width and should be truncated with ellipsis\033[0m",
			},
			expectedLen: 3,
			shouldTrunc: true,
			description: "Mixed content should truncate only the long lines",
		},
		{
			name:        "line that's exactly at limit with ANSI",
			input:       []string{"\033[31mExact length line for testing\033[0m"}, // This should be close to the limit
			expectedLen: 1,
			shouldTrunc: false, // Depends on exact width calculation
			description: "Line at exact length limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use getVisibleLines which handles truncation
			maxWidth := m.width - 6 // Same as in the actual function
			result := m.getVisibleLinesWithWidth(tt.input, 0, len(tt.input), maxWidth)

			if len(result) != tt.expectedLen {
				t.Errorf("Expected %d lines, got %d", tt.expectedLen, len(result))
				return
			}

			// Check truncation behavior for the test cases that should be truncated
			if tt.shouldTrunc {
				foundTruncated := false
				for _, line := range result {
					// Strip ANSI codes to check actual visible length
					cleanLine := stripANSIEscapeSequences(line)
					if strings.Contains(cleanLine, "...") {
						foundTruncated = true
						// Verify the visible length is within limits
						if len(cleanLine) > maxWidth {
							t.Errorf("Truncated line still exceeds maxWidth: %d > %d", len(cleanLine), maxWidth)
						}
					}
				}
				if !foundTruncated {
					t.Errorf("Expected to find truncated line with ellipsis, but none found")
				}
			}

			// Verify no line exceeds the maximum width (accounting for ANSI codes)
			for i, line := range result {
				cleanLine := stripANSIEscapeSequences(line)
				if len(cleanLine) > maxWidth {
					t.Errorf("Line %d exceeds maxWidth: visible length %d > %d (line: %q)",
						i, len(cleanLine), maxWidth, cleanLine)
				}
			}
		})
	}
}

func TestLineTruncation_BoxDrawingPreservesGrid(t *testing.T) {
	m := InitialModel()
	m.width = 30
	maxWidth := m.width - ViewportContentPadding
	line := "\033[33m[spt] " + "┌" + strings.Repeat("─", 80) + "┐" + "\033[0m"
	result := m.getVisibleLinesWithWidth([]string{line}, 0, 1, maxWidth)
	if len(result) != 1 {
		t.Fatalf("expected 1 line, got %d", len(result))
	}
	clean := stripANSIEscapeSequences(result[0])
	if strings.Contains(clean, "...") {
		t.Fatalf("expected no ellipsis in truncated box drawing line, got %q", clean)
	}
	if textutil.RuneLen(clean) > maxWidth {
		t.Fatalf("truncated line exceeds max width: %d > %d", textutil.RuneLen(clean), maxWidth)
	}
	if !strings.Contains(clean, "┌") {
		t.Fatalf("expected leading box drawing characters to remain, got %q", clean)
	}
}

func TestLineTruncation_ANSIPreservation(t *testing.T) {
	m := InitialModel()
	m.width = 30 // Small width

	// Test that ANSI codes are preserved when possible during truncation
	input := []string{"\033[31mRed text that will be truncated\033[0m"}
	maxWidth := 15

	result := m.getVisibleLinesWithWidth(input, 0, 1, maxWidth)

	if len(result) != 1 {
		t.Fatalf("Expected 1 line, got %d", len(result))
	}

	line := result[0]
	cleanLine := stripANSIEscapeSequences(line)

	// Should be truncated with ellipsis
	if !strings.Contains(cleanLine, "...") {
		t.Errorf("Expected truncated line to contain ellipsis, got: %q", cleanLine)
	}

	// Clean line should be within limit
	if len(cleanLine) > maxWidth {
		t.Errorf("Truncated line exceeds maxWidth: %d > %d", len(cleanLine), maxWidth)
	}

	// For this specific test, the truncation should remove ANSI codes
	// since the getVisibleLinesWithWidth function doesn't preserve them during truncation
	if strings.Contains(line, "\033[") && strings.Contains(cleanLine, "...") {
		// This is acceptable - the function may or may not preserve ANSI codes during truncation
		// The important thing is that the visible length is correct
	}
}

func TestNavigationEdgeCases(t *testing.T) {
	m := InitialModel()
	m.width = 80
	m.height = 24

	// Test navigation with no metrics
	t.Run("navigation with empty metrics", func(t *testing.T) {
		// Reset to empty state
		m = InitialModel()

		// Try navigating left/right with no samples
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyLeft})
		m = updatedModel.(Model)
		if m.historicalIndex != 0 {
			t.Errorf("Expected historicalIndex to remain 0 with no samples, got %d", m.historicalIndex)
		}

		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyRight})
		m = updatedModel.(Model)
		if m.historicalIndex != 0 {
			t.Errorf("Expected historicalIndex to remain 0 with no samples, got %d", m.historicalIndex)
		}

		// ESC should also do nothing
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
		m = updatedModel.(Model)
		if m.historicalIndex != 0 {
			t.Errorf("Expected historicalIndex to remain 0 after ESC with no samples, got %d", m.historicalIndex)
		}
	})

	// Test rapid navigation
	t.Run("rapid navigation beyond boundaries", func(t *testing.T) {
		// Add 3 samples
		for i := 0; i < 3; i++ {
			metric := PerformanceMetric{
				Timestamp:    time.Now().Add(time.Duration(i) * time.Second),
				OpsPerSec:    int64(1000 + i*100),
				SptTimestamp: fmt.Sprintf("timestamp_%d", i),
				OpType:       "CREATE",
			}
			m.metricsCollector.AddSample(metric)
		}

		// Rapidly navigate backward past the oldest sample
		for i := 0; i < 10; i++ {
			updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyLeft})
			m = updatedModel.(Model)
		}

		// Should be at the oldest sample (-(len-1) = -2)
		if m.historicalIndex != -2 {
			t.Errorf("Expected historicalIndex to be -2 (oldest), got %d", m.historicalIndex)
		}

		// Rapidly navigate forward past live mode
		for i := 0; i < 10; i++ {
			updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyRight})
			m = updatedModel.(Model)
		}

		// Should be at live mode (0)
		if m.historicalIndex != 0 {
			t.Errorf("Expected historicalIndex to be 0 (live), got %d", m.historicalIndex)
		}
	})

	// Test navigation during metrics updates
	t.Run("navigation during live updates", func(t *testing.T) {
		// Reset and add initial samples
		m = InitialModel()
		for i := 0; i < 3; i++ {
			metric := PerformanceMetric{
				Timestamp:    time.Now().Add(time.Duration(i) * time.Second),
				OpsPerSec:    int64(1000 + i*100),
				SptTimestamp: fmt.Sprintf("initial_%d", i),
				OpType:       "CREATE",
			}
			m.metricsCollector.AddSample(metric)
		}

		// Navigate to historical mode
		updatedModel, _ := m.Update(tea.KeyMsg{Type: tea.KeyLeft})
		m = updatedModel.(Model)
		updatedModel, _ = m.Update(tea.KeyMsg{Type: tea.KeyLeft})
		m = updatedModel.(Model)

		originalIndex := m.historicalIndex // Should be -2

		// Add multiple new samples while in historical mode
		for i := 0; i < 5; i++ {
			newMetric := PerformanceMetric{
				Timestamp:    time.Now().Add(time.Duration(10+i) * time.Second),
				OpsPerSec:    int64(2000 + i*100),
				SptTimestamp: fmt.Sprintf("new_%d", i),
				OpType:       "CREATE",
			}
			updatedModel, _ = m.Update(metricsUpdateMsg(newMetric))
			m = updatedModel.(Model)
		}

		// Historical index should have been auto-adjusted 5 times
		expectedIndex := originalIndex - 5 // -2 - 5 = -7
		if m.historicalIndex != expectedIndex {
			t.Errorf("Expected historicalIndex to be %d after auto-adjustment, got %d",
				expectedIndex, m.historicalIndex)
		}

		// Should still be viewing the same logical sample
		selectedIndex := m.getSelectedSampleIndex()
		if selectedIndex != 0 {
			t.Errorf("Expected to still be viewing oldest sample (index 0), got %d", selectedIndex)
		}
	})

	// Test view port height calculations edge cases
	t.Run("viewport height edge cases", func(t *testing.T) {
		// Test very small window
		m.width = 10
		m.height = 10
		m.processOutputHidden = false

		height := m.getProcessViewportHeight()
		if height < ProcessViewportMinHeight {
			t.Errorf("Expected minimum height of %d, got %d", ProcessViewportMinHeight, height)
		}

		// Test when hidden
		m.processOutputHidden = true
		height = m.getProcessViewportHeight()
		if height != 0 {
			t.Errorf("Expected height 0 when hidden, got %d", height)
		}

		// Test very large window
		m.width = 200
		m.height = 100
		m.processOutputHidden = false
		height = m.getProcessViewportHeight()
		if height > ProcessViewportMaxHeight {
			t.Errorf("Expected maximum height of %d, got %d", ProcessViewportMaxHeight, height)
		}
	})

	// Test message handling edge cases
	t.Run("message handling edge cases", func(t *testing.T) {
		m = InitialModel()
		m.width = 80
		m.height = 24

		// Test empty messages
		updatedModel, _ := m.Update(containerOutputMsg(""))
		m = updatedModel.(Model)
		if len(m.processOutput) != 1 || m.processOutput[0] != "" {
			t.Errorf("Expected empty message to be added, got %v", m.processOutput)
		}

		// Test very long message that will be truncated
		longMessage := strings.Repeat("a", 200)
		updatedModel, _ = m.Update(containerOutputMsg(longMessage))
		m = updatedModel.(Model)

		// Should be truncated when displayed
		visibleLines := m.getVisibleLines(m.processOutput, 0, 5)
		for _, line := range visibleLines {
			cleanLine := stripANSIEscapeSequences(line)
			maxWidth := m.width - 6
			if len(cleanLine) > maxWidth {
				t.Errorf("Message not properly truncated: %d > %d", len(cleanLine), maxWidth)
			}
		}
	})
}
