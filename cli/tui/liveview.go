/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
	"github.com/dell/storage-performance-tool/cli/internal/textutil"
)

// LiveViewRenderer handles formatting and rendering of live performance statistics
type LiveViewRenderer struct {
	cpuMonitor     *CPUMonitor
	scenarioParams *scenario.ScenarioParams
	startTime      time.Time
}

type tableLabels struct {
	progress string
	rate     string
	ops      string
	colSix   string
}

// NewLiveViewRenderer creates a new live view renderer with CPU monitoring
func NewLiveViewRenderer(params *scenario.ScenarioParams) *LiveViewRenderer {
	renderer := &LiveViewRenderer{
		cpuMonitor:     NewCPUMonitor(time.Second),
		scenarioParams: params,
		startTime:      time.Now(),
	}

	// Start CPU monitoring
	if err := renderer.cpuMonitor.Start(); err != nil {
		logging.LogError("liveview", "Failed to start CPU monitoring", err)
	}

	return renderer
}

// Stop cleanly shuts down the live view renderer
func (l *LiveViewRenderer) Stop() {
	if l.cpuMonitor != nil {
		l.cpuMonitor.Stop()
	}
}

// RenderLiveView generates the complete live view content (without borders - handled by TUI)
func (l *LiveViewRenderer) RenderLiveView(metrics *MetricsCollector, width int) string {
	if metrics == nil {
		logging.LogDebug("liveview", "render with nil metrics; using empty content")
		return l.renderEmptyContent(width)
	}

	// Check if we have aggregated samples for the header
	samples := metrics.GetSamples()
	if len(samples) == 0 {
		// If we have known node statuses, render table with placeholders so hosts are visible
		nodeCount := len(metrics.GetNodeStatus())
		if nodeCount > 0 {
			logging.LogDebug("liveview", "no samples yet; rendering placeholder rows", "nodes", nodeCount)
			empty := &PerformanceMetric{}
			header := l.formatHeader(empty)
			table := l.formatMultiNodeTable(metrics, empty, width)
			return header + "\n" + table
		}
		logging.LogDebug("liveview", "no samples and no node status; using empty content")
		return l.renderEmptyContent(width)
	}

	// Use the most recent aggregated sample for header display
	latestSample := samples[len(samples)-1]
	displayAggregate := l.aggregateForDisplay(metrics, &latestSample)

	// Generate header and multi-node table
	header := l.formatHeader(displayAggregate)
	table := l.formatMultiNodeTable(metrics, displayAggregate, width)

	// Return content without additional styling (TUI handles borders)
	return header + "\n" + table
}

// renderEmptyContent shows content when no metrics are available
func (l *LiveViewRenderer) renderEmptyContent(width int) string {
	header := l.formatEmptyHeader()
	table := l.formatEmptyTable(width)

	return header + "\n" + table
}

// formatHeader generates the header line with phase, CPU, active threads, and elapsed time
func (l *LiveViewRenderer) formatHeader(metric *PerformanceMetric) string {
	phaseRaw := strings.TrimSpace(metric.OpType)
	phase := strings.ToUpper(phaseRaw)
	if strings.EqualFold(phaseRaw, "none") || (metric.TestState == 0 && phaseRaw == "") {
		phase = "Idle—waiting for run"
	} else if phase == "" {
		phase = constants.UIStateUnknown
	}

	cpuPercent := l.cpuMonitor.GetCPUPercent()
	active := metric.ConcurrencyCurrent
	elapsed := l.calculateElapsed(metric)

	return fmt.Sprintf("Phase: %s  CPU: %.0f%%  Active: %d  Elapsed: %s",
		phase, cpuPercent, active, formatDuration(elapsed))
}

// formatEmptyHeader generates a header when no metrics are available
func (l *LiveViewRenderer) formatEmptyHeader() string {
	elapsed := time.Since(l.startTime)
	return fmt.Sprintf("Phase: INIT  CPU: %.0f%%  Active: 0  Elapsed: %s",
		l.cpuMonitor.GetCPUPercent(), formatDuration(elapsed))
}

// formatTable generates the performance metrics table without grid lines (single-node legacy method)
// formatTable moved to test helpers (unused in production)

// formatMultiNodeTable generates the performance metrics table with multi-node support
func (l *LiveViewRenderer) formatMultiNodeTable(metrics *MetricsCollector, aggregatedMetric *PerformanceMetric, width int) string {
	// Calculate derived values for aggregated total
	progressValue, rateValue := l.transferCells(aggregatedMetric)
	colSixValue := aggregatedMetric.SuccessCount
	if l.isListWorkload() {
		progressValue = fmt.Sprintf("%d", l.calculateDoneMiB(aggregatedMetric.SuccessCount))
		rateValue = fmt.Sprintf("%d", aggregatedMetric.OpsPerSec)
		colSixValue = aggregatedMetric.ConcurrencyCurrent
	}

	// Build data rows first so we can size the header highlight to the longest row
	var dataRows []string

	// Total row (aggregated metrics); Host column blank
	totalPrefix := fmt.Sprintf("%5s %3s %9s %8s %6d %7d %8d",
		"Total", l.formatPercentCell(aggregatedMetric), progressValue, rateValue, aggregatedMetric.OpsPerSec, colSixValue, aggregatedMetric.FailedCount)
	dataRows = append(dataRows, l.appendHostColumn(totalPrefix, "", NodeConnectionStatus{}, width))

	// Determine node list: include all nodes from NodeStatus (show even if unreachable)
	nodeStatuses := metrics.GetNodeStatus()
	nodeIDs := make([]string, 0, len(nodeStatuses))
	for nodeID := range nodeStatuses {
		nodeIDs = append(nodeIDs, nodeID)
	}
	// Sort node IDs for consistent display
	for i := 0; i < len(nodeIDs); i++ {
		for j := i + 1; j < len(nodeIDs); j++ {
			if nodeIDs[i] > nodeIDs[j] {
				nodeIDs[i], nodeIDs[j] = nodeIDs[j], nodeIDs[i]
			}
		}
	}

	// If no nodes known yet, show a placeholder row
	if len(nodeIDs) == 0 {
		placeholderPrefix := fmt.Sprintf("%5s %3s %9s %8s %6s %7s %8s",
			"0", l.formatPercentCell(nil), "-", "-", "-", "-", "-")
		dataRows = append(dataRows, l.appendHostColumn(placeholderPrefix, "", NodeConnectionStatus{}, width))
		// Now render header sized to longest row
		header := l.renderTableHeader(width, dataRows)
		return header + "\n" + strings.Join(dataRows, "\n")
	}

	// Display each node with Host in rightmost column
	for i, nodeID := range nodeIDs {
		nodeSamples := metrics.GetNodeSamples(nodeID)
		status := nodeStatuses[nodeID]
		if len(nodeSamples) > 0 {
			// Use the latest sample for this node
			latestNodeSample := nodeSamples[len(nodeSamples)-1]
			nodeProgress, rate := l.transferCells(&latestNodeSample)
			colSix := latestNodeSample.SuccessCount
			if l.isListWorkload() {
				nodeProgress = fmt.Sprintf("%d", l.calculateDoneMiB(latestNodeSample.SuccessCount))
				rate = fmt.Sprintf("%d", latestNodeSample.OpsPerSec)
				colSix = latestNodeSample.ConcurrencyCurrent
			}

			// Percentage cell uses latest node sample percent or ∞/—
			prefix := fmt.Sprintf("%5d %3s %9s %8s %6d %7d %8d",
				i, l.formatPercentCell(&latestNodeSample), nodeProgress, rate, latestNodeSample.OpsPerSec, colSix, latestNodeSample.FailedCount)
			dataRows = append(dataRows, l.appendHostColumn(prefix, nodeID, status, width))
		} else {
			// Node with no samples yet: use placeholders and status-only host
			prefix := fmt.Sprintf("%5d %3s %9s %8s %6s %7s %8s",
				i, l.formatPercentCell(nil), "-", "-", "-", "-", "-")
			dataRows = append(dataRows, l.appendHostColumn(prefix, nodeID, status, width))
		}
	}
	header := l.renderTableHeader(width, dataRows)
	return header + "\n" + strings.Join(dataRows, "\n")
}

func (l *LiveViewRenderer) transferCells(metric *PerformanceMetric) (string, string) {
	if metric != nil && metric.Delete != nil &&
		strings.EqualFold(metric.Delete.Performance.DataMoved, "not_applicable") &&
		strings.EqualFold(metric.Delete.Performance.Bandwidth, "not_applicable") {
		return notAvailableValue, notAvailableValue
	}
	if metric == nil {
		return "0", "0"
	}
	return fmt.Sprintf("%d", l.calculateDoneMiB(metric.SuccessCount)),
		fmt.Sprintf("%d", metric.MiBPerSec)
}

func (l *LiveViewRenderer) aggregateForDisplay(metrics *MetricsCollector, fallback *PerformanceMetric) *PerformanceMetric {
	if metrics == nil {
		return fallback
	}

	nodeStatuses := metrics.GetNodeStatus()
	if len(nodeStatuses) == 0 {
		return fallback
	}

	nodeMetrics := make(map[string]*PerformanceMetric, len(nodeStatuses))
	for nodeID := range nodeStatuses {
		samples := metrics.GetNodeSamples(nodeID)
		if len(samples) == 0 {
			continue
		}
		latest := samples[len(samples)-1]
		copySample := latest
		nodeMetrics[nodeID] = &copySample
	}

	if len(nodeMetrics) == 0 {
		return fallback
	}

	aggregated := NewMetricsAggregator().Aggregate(nodeMetrics)
	if aggregated == nil {
		return fallback
	}

	display := *aggregated
	if fallback != nil {
		if display.StepID == "" {
			display.StepID = fallback.StepID
		}
		if display.OpType == "" {
			display.OpType = fallback.OpType
		}
		if display.RunID == "" {
			display.RunID = fallback.RunID
		}
		if display.ClusterID == "" {
			display.ClusterID = fallback.ClusterID
		}
		if display.SptTimestamp == "" {
			display.SptTimestamp = fallback.SptTimestamp
		}
		if display.Timestamp.IsZero() {
			display.Timestamp = fallback.Timestamp
		}
		if display.SampleTimestamp.IsZero() {
			display.SampleTimestamp = fallback.SampleTimestamp
			display.SampleTimestampRaw = fallback.SampleTimestampRaw
		}
		// Preserve overall test state so the header reflects current phase.
		display.TestState = fallback.TestState
	}

	return &display
}

// appendHostColumn appends the rightmost Host column to a given prefix row, respecting terminal width.
func (l *LiveViewRenderer) appendHostColumn(prefix, host string, status NodeConnectionStatus, totalWidth int) string {
	// Compute remaining width; if too small, omit Host column entirely for this row
	prefixLen := textutil.RuneLen(prefix)
	remaining := totalWidth - prefixLen - 1 // space before Host
	if remaining < 6 {
		return prefix
	}

	// Totals/placeholder rows have an empty host; suppress connection icon entirely.
	if strings.TrimSpace(host) == "" {
		return prefix
	}

	// Icon coloring (apply color to icon only)
	icon := selectHostIcon(status)
	// Note: emoji are colored glyphs; wrapping in ANSI is unnecessary and may be ignored.
	// We keep icon as-is to match test expectations and user preference for per-host rows.
	hostCell := fmt.Sprintf("%s %s", icon, host)

	// Truncate host cell if needed based on remaining width only
	hostCell = textutil.TruncateWithEllipsis(hostCell, remaining)

	return prefix + " " + hostCell
}

func selectHostIcon(status NodeConnectionStatus) string {
	if status.Phase == NodePhaseMetricsFlowing || (status.IsConnected && status.Phase == NodePhaseAPIReady) {
		return "✅"
	}
	if status.Phase == NodePhaseContacted || status.Phase == NodePhaseContainerStarting || status.Phase == NodePhaseAPIReady {
		return "🔵"
	}
	if status.IsConnected {
		return "🔵"
	}
	return "❌"
}

// truncateWithEllipsis truncates s to at most max columns, adding ... when truncated.
// formatPercentCell returns a 3-char cell value for the % column
// - "∞" if unbounded
// - integer percent (rounded) when provided
// - "—" when metric is nil (no data)
func (l *LiveViewRenderer) formatPercentCell(metric *PerformanceMetric) string {
	if metric == nil {
		return "—"
	}
	if metric.Unbounded {
		return "∞"
	}
	// Round to nearest integer for display
	v := int(math.Round(metric.CompletionPercent))
	if v < 0 {
		v = 0
	}
	if v > 100 {
		v = 100
	}
	return fmt.Sprintf("%d", v)
}

// formatEmptyTable generates a table when no metrics are available
func (l *LiveViewRenderer) formatEmptyTable(width int) string {
	rows := make([]string, 0, 2)
	totalPrefix := fmt.Sprintf("%5s %3s %9d %8d %6d %7d %8d",
		"Total", l.formatPercentCell(nil), 0, 0, 0, 0, 0)
	rows = append(rows, l.appendHostColumn(totalPrefix, "", NodeConnectionStatus{}, width))
	nodePrefix := fmt.Sprintf("%5s %3s %9d %8d %6d %7d %8d",
		"0", l.formatPercentCell(nil), 0, 0, 0, 0, 0)
	rows = append(rows, l.appendHostColumn(nodePrefix, "", NodeConnectionStatus{}, width))
	header := l.renderTableHeader(width, rows)
	return header + "\n" + strings.Join(rows, "\n")
}

// renderTableHeader returns a reverse-video header padded to the longest data row (capped by width).
func (l *LiveViewRenderer) renderTableHeader(width int, dataRows []string) string {
	labels := l.tableLabels()
	headerLine := fmt.Sprintf("%5s %3s %9s %8s %6s %7s %8s %s",
		"Rank", "%", labels.progress, labels.rate, labels.ops, labels.colSix, "Errors", "Host")
	maxLen := textutil.RuneLen(headerLine)
	for _, r := range dataRows {
		if rl := textutil.RuneLen(r); rl > maxLen {
			maxLen = rl
		}
	}
	if maxLen > width {
		maxLen = width
	}
	return "\033[7m" + textutil.PadRight(headerLine, maxLen) + "\033[0m"
}

// calculateElapsed computes the elapsed time for the current test
func (l *LiveViewRenderer) calculateElapsed(metric *PerformanceMetric) time.Duration {
	// Use the step time from the metric if available, otherwise use wall clock time
	if metric.StepTime > 0 {
		return time.Duration(metric.StepTime * float64(time.Second))
	}

	return time.Since(l.startTime)
}

// calculateDoneMiB estimates the amount of data processed based on successful operations
func (l *LiveViewRenderer) calculateDoneMiB(successCount int64) int64 {
	if l.isListWorkload() {
		return successCount
	}

	if l.scenarioParams == nil || successCount == 0 {
		return 0
	}

	// Parse object size from scenario params
	objectSizeBytes := l.parseObjectSize(l.scenarioParams.ObjectSize)
	if objectSizeBytes == 0 {
		return 0
	}

	// Calculate total bytes and convert to MiB
	totalBytes := successCount * objectSizeBytes
	return totalBytes / (1024 * 1024) // Convert to MiB
}

func (l *LiveViewRenderer) isListWorkload() bool {
	if l.scenarioParams == nil {
		return false
	}
	return strings.EqualFold(l.scenarioParams.WorkloadType, scenario.WorkloadTypeList)
}

func (l *LiveViewRenderer) tableLabels() tableLabels {
	labels := tableLabels{
		progress: "DoneMiB",
		rate:     constants.UnitMiBPerSecond,
		ops:      "IOPs",
		colSix:   "Files",
	}
	if l.isListWorkload() {
		labels.progress = "Objects"
		labels.rate = "Ops/s"
		labels.colSix = "Threads"
	}
	return labels
}

// parseObjectSize converts object size string to bytes
func (l *LiveViewRenderer) parseObjectSize(sizeStr string) int64 {
	trimmed := strings.TrimSpace(sizeStr)
	sign := int64(1)
	if strings.HasPrefix(trimmed, "-") {
		sign = -1
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
	}

	sizeBytes, err := sizeparse.Parse(trimmed)
	if err != nil {
		if strings.TrimSpace(sizeStr) != "" {
			logging.LogError("liveview", "Failed to parse object size", err, "size", sizeStr)
		}
		return 0
	}
	return sign * sizeBytes
}
