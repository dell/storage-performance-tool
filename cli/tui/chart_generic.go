/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	chartHeight     = 6 // Bars take 6 lines, leaving 2 for labels/info
	chartRefreshSec = 2 // Recalculate scale every 2 seconds for more responsive updates
)

// MetricExtractor is a function that extracts a metric value from a PerformanceMetric
type MetricExtractor func(PerformanceMetric) int64

// ValueFormatter is a function that formats a metric value for display
type ValueFormatter func(int64) string

// GenericChartRenderer handles rendering of any performance metric chart
type GenericChartRenderer struct {
	title           string
	unit            string
	extractor       MetricExtractor
	formatter       ValueFormatter
	lastScaleUpdate time.Time
	currentScale    int64
	maxValue        int64
}

// NewGenericChartRenderer creates a new generic chart renderer
func NewGenericChartRenderer(title, unit string, extractor MetricExtractor, formatter ValueFormatter) *GenericChartRenderer {
	return &GenericChartRenderer{
		title:           title,
		unit:            unit,
		extractor:       extractor,
		formatter:       formatter,
		lastScaleUpdate: time.Now(),
		currentScale:    100, // Default scale
		maxValue:        0,
	}
}

// NewGenericChartRendererWithScale creates a new generic chart renderer with a custom initial scale
func NewGenericChartRendererWithScale(title, unit string, extractor MetricExtractor, formatter ValueFormatter, initialScale int64) *GenericChartRenderer {
	return &GenericChartRenderer{
		title:           title,
		unit:            unit,
		extractor:       extractor,
		formatter:       formatter,
		lastScaleUpdate: time.Now(),
		currentScale:    initialScale,
		maxValue:        0,
	}
}

// RenderChart renders the performance bar chart
func (cr *GenericChartRenderer) RenderChart(collector *MetricsCollector, width int) string {
	return cr.RenderChartWithSelection(collector, width, -1, false)
}

// RenderChartWithSelection renders the performance bar chart with optional selection highlighting
func (cr *GenericChartRenderer) RenderChartWithSelection(collector *MetricsCollector, width int, selectedIndex int, isHistorical bool) string {
	if width <= 10 { // Need minimum width for labels
		return "Chart too narrow"
	}

	samples := collector.GetSamples()
	if len(samples) == 0 {
		return cr.renderEmptyChart(width)
	}

	// Update scale if needed
	cr.updateScale(collector)

	// Calculate display window and get samples
	chartWidth := width - 8 // Reserve 8 chars for Y-axis labels
	displaySamples, startIndex := cr.getDisplaySamplesForSelection(samples, chartWidth, selectedIndex, isHistorical)

	// Calculate which bar to highlight (if any)
	highlightBar := -1
	if selectedIndex >= 0 && isHistorical {
		// The bar to highlight is the selected index minus the start of our display window
		if selectedIndex >= startIndex && selectedIndex < startIndex+len(displaySamples) {
			highlightBar = selectedIndex - startIndex
		}
	}

	// Render the chart components
	header := cr.renderChartHeaderWithSelection(collector, width, selectedIndex, isHistorical)
	bars := cr.renderBarsWithHighlight(displaySamples, width-8, highlightBar)
	timestamps := cr.renderTimestamps(displaySamples, width-8)

	// Combine all parts
	result := header + "\n" + bars + "\n" + timestamps
	return result
}

// updateScale recalculates the chart scale every 5 seconds
func (cr *GenericChartRenderer) updateScale(collector *MetricsCollector) {
	now := time.Now()
	if now.Sub(cr.lastScaleUpdate) < chartRefreshSec*time.Second {
		return
	}

	_, maxVal := collector.GetMetricRange(cr.extractor)
	if maxVal > cr.maxValue {
		cr.maxValue = maxVal
	}

	// Calculate nice upper bound
	cr.currentScale = CalculateNiceUpperBound(cr.maxValue)
	cr.lastScaleUpdate = now
}

// getDisplaySamples returns the last N samples to display
// getDisplaySamples moved to test helpers (unused in production)

// getDisplaySamplesForSelection returns samples to display, adjusting window to keep selection visible
// Returns the samples to display and the starting index in the full samples array
func (cr *GenericChartRenderer) getDisplaySamplesForSelection(samples []PerformanceMetric, maxSamples int, selectedIndex int, isHistorical bool) ([]PerformanceMetric, int) {
	if len(samples) <= maxSamples {
		// All samples fit, display them all
		return samples, 0
	}

	if !isHistorical || selectedIndex < 0 {
		// Live mode or no selection - show the most recent samples
		startIndex := len(samples) - maxSamples
		return samples[startIndex:], startIndex
	}

	// Historical mode with selection - ensure selected sample is visible
	// Try to center the selected sample in the display window
	idealStart := selectedIndex - maxSamples/2

	// Clamp to valid range
	if idealStart < 0 {
		idealStart = 0
	} else if idealStart > len(samples)-maxSamples {
		idealStart = len(samples) - maxSamples
	}

	endIndex := idealStart + maxSamples
	if endIndex > len(samples) {
		endIndex = len(samples)
	}

	return samples[idealStart:endIndex], idealStart
}

// renderChartHeaderWithSelection renders the header with either latest or selected sample info
func (cr *GenericChartRenderer) renderChartHeaderWithSelection(collector *MetricsCollector, width int, selectedIndex int, isHistorical bool) string {
	samples := collector.GetSamples()

	// Build header with title on left and value on right
	var header string
	if len(samples) == 0 {
		header = cr.title
	} else {
		var displaySample PerformanceMetric
		var timeStr string
		var valueStr string

		if isHistorical && selectedIndex >= 0 && selectedIndex < len(samples) {
			// Show historical value
			displaySample = samples[selectedIndex]
			timeStr = displaySample.Timestamp.Format("15:04:05")
			valueStr = cr.formatter(cr.extractor(displaySample))
		} else {
			// Show latest value
			displaySample = samples[len(samples)-1]
			timeStr = displaySample.Timestamp.Format("15:04:05")
			valueStr = cr.formatter(cr.extractor(displaySample))
		}

		// Build info string - only include unit if it's not empty
		var info string
		if cr.unit != "" {
			info = fmt.Sprintf("%s %s %s", timeStr, valueStr, cr.unit)
		} else {
			info = fmt.Sprintf("%s %s", timeStr, valueStr)
		}

		// Apply yellow color if historical
		if isHistorical {
			info = "\033[33m" + info + "\033[0m" // ANSI yellow
		}

		// Calculate padding to right-align the info
		// Account for ANSI escape sequences if present
		visibleInfoLen := len(info)
		if isHistorical {
			visibleInfoLen = len(timeStr) + len(valueStr) + 2 // 2 spaces
			if cr.unit != "" {
				visibleInfoLen += len(cr.unit) + 1 // unit + space
			}
		}

		padding := width - len(cr.title) - visibleInfoLen - 1
		if padding < 0 {
			// Not enough space, just show title
			header = cr.title
			if len(header) > width {
				header = header[:width]
			}
		} else {
			header = cr.title + strings.Repeat(" ", padding) + info
		}
	}

	// Don't truncate if we have ANSI codes
	return header
}

// renderBarsWithHighlight renders the bar chart with optional bar highlighting
func (cr *GenericChartRenderer) renderBarsWithHighlight(samples []PerformanceMetric, chartWidth int, highlightBar int) string {
	if len(samples) == 0 {
		return cr.renderEmptyBars(chartWidth)
	}

	var lines []string

	// Render from top to bottom (highest values first)
	for row := chartHeight - 1; row >= 0; row-- {
		line := cr.renderBarRowWithHighlight(samples, chartWidth, row, highlightBar)
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

// renderBarRowWithHighlight renders a single row of the bar chart with optional highlighting
func (cr *GenericChartRenderer) renderBarRowWithHighlight(samples []PerformanceMetric, chartWidth int, row int, highlightBar int) string {
	// Y-axis label (8 characters wide)
	threshold := cr.currentScale * int64(row+1) / int64(chartHeight)
	yLabel := fmt.Sprintf("%7s|", cr.formatter(threshold))

	// Bar data
	var bars strings.Builder
	for i := 0; i < chartWidth; i++ {
		if i < len(samples) {
			sample := samples[i]
			barHeight := cr.calculateBarHeight(cr.extractor(sample))

			if barHeight > row {
				if i == highlightBar {
					// Yellow highlighted bar
					bars.WriteString("\033[33m█\033[0m")
				} else {
					bars.WriteString("█")
				}
			} else {
				bars.WriteString(" ")
			}
		} else {
			bars.WriteString(" ")
		}
	}

	return yLabel + bars.String()
}

// calculateBarHeight determines how many rows tall a bar should be
func (cr *GenericChartRenderer) calculateBarHeight(value int64) int {
	if cr.currentScale == 0 {
		return 0
	}

	height := int(float64(value) / float64(cr.currentScale) * float64(chartHeight))
	if height > chartHeight {
		height = chartHeight
	}
	return height
}

// renderTimestamps renders the bottom timestamp row
func (cr *GenericChartRenderer) renderTimestamps(samples []PerformanceMetric, chartWidth int) string {
	// Y-axis label space
	prefix := "       |"

	if len(samples) == 0 {
		return prefix + strings.Repeat(" ", chartWidth)
	}

	// Show timestamps at intervals
	var timestamps strings.Builder
	for i := 0; i < chartWidth; i++ {
		if i < len(samples) && i%10 == 0 { // Show every 10th timestamp
			sample := samples[i]
			ts := sample.Timestamp.Format("04:05") // MM:SS format
			if len(ts) <= chartWidth-i {
				timestamps.WriteString(ts)
				i += len(ts) - 1 // Skip ahead by timestamp length
			} else {
				timestamps.WriteString(" ")
			}
		} else {
			timestamps.WriteString(" ")
		}
	}

	return prefix + timestamps.String()
}

// renderEmptyChart renders a chart when no data is available
func (cr *GenericChartRenderer) renderEmptyChart(width int) string {
	var lines []string

	// Header
	lines = append(lines, strings.Repeat(" ", width))

	// Empty bars with Y-axis
	for i := chartHeight - 1; i >= 0; i-- {
		threshold := cr.currentScale * int64(i+1) / int64(chartHeight)
		yLabel := fmt.Sprintf("%7s|", cr.formatter(threshold))
		line := yLabel + strings.Repeat(" ", width-8)
		lines = append(lines, line)
	}

	// Empty timestamp line
	prefix := "       |"
	lines = append(lines, prefix+strings.Repeat(" ", width-8))

	return strings.Join(lines, "\n")
}

// renderEmptyBars renders empty bars when no samples
func (cr *GenericChartRenderer) renderEmptyBars(chartWidth int) string {
	var lines []string

	for row := chartHeight - 1; row >= 0; row-- {
		threshold := cr.currentScale * int64(row+1) / int64(chartHeight)
		yLabel := fmt.Sprintf("%7s|", cr.formatter(threshold))
		line := yLabel + strings.Repeat(" ", chartWidth)
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

// formatValue formats a numeric value for display in Y-axis labels
func formatValue(value int64) string {
	if value >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(value)/1000000)
	} else if value >= 1000 {
		return fmt.Sprintf("%.1fK", float64(value)/1000)
	}
	return strconv.FormatInt(value, 10)
}

// Predefined formatters

// FormatOpsValue formats operations per second values
func FormatOpsValue(value int64) string {
	return formatValue(value)
}

// FormatLatencyValue formats latency values in microseconds
func FormatLatencyValue(value int64) string {
	if value < 0 {
		return notAvailableValue
	}
	if value >= 1000000 {
		return fmt.Sprintf("%.1fs", float64(value)/1000000)
	} else if value >= 1000 {
		return fmt.Sprintf("%.1fms", float64(value)/1000)
	}
	return fmt.Sprintf("%dμs", value)
}

// Predefined extractors

// ExtractOpsPerSec extracts the ops/sec metric
func ExtractOpsPerSec(m PerformanceMetric) int64 {
	return m.OpsPerSec
}

// ExtractDisplayLatency extracts the latency value selected for UI display.
func ExtractDisplayLatency(m PerformanceMetric) int64 {
	if !timingDisplayAvailable(m.HasLatency, m.MeanLatency) {
		return -1
	}
	return m.MeanLatency
}

// ExtractDisplayDuration extracts the duration value selected for UI display.
func ExtractDisplayDuration(m PerformanceMetric) int64 {
	if !timingDisplayAvailable(m.HasDuration, m.MeanDuration) {
		return -1
	}
	return m.MeanDuration
}

func timingDisplayAvailable(explicit bool, value int64) bool {
	return explicit || value > 0
}

// ExtractMeanLatency is kept for older tests/helpers that still refer to the legacy name.
func ExtractMeanLatency(m PerformanceMetric) int64 {
	return ExtractDisplayLatency(m)
}

// ExtractFailureIncrement extracts the incremental failure count
func ExtractFailureIncrement(m PerformanceMetric) int64 {
	return m.FailureIncrement
}

// FormatFailureValue formats failure count values
func FormatFailureValue(value int64) string {
	// Delegate to ops value formatter to avoid duplicate logic
	return FormatOpsValue(value)
}
