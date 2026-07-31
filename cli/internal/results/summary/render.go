package summary

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/textutil"
)

// Renderer converts RunSummary structures into human-readable reports.
type Renderer struct {
	MaxWidth       int
	SnippetLineCap int
}

// RenderOptions controls renderer behaviour; zero values fall back to defaults.
type RenderOptions struct {
	MaxWidth       int
	SnippetLineCap int
}

const (
	defaultMaxWidth       = 100
	defaultSnippetLineCap = 40
	headerIOPSAvg         = "IOPS Avg"
	headerLatencyP50      = "Latency P50"
	headerTTFBP50         = "TTFB P50"
	headerBandwidthAvg    = "Bandwidth Avg"
)

// NewRenderer builds a Renderer using the provided options.
func NewRenderer(opts RenderOptions) *Renderer {
	r := &Renderer{
		MaxWidth:       opts.MaxWidth,
		SnippetLineCap: opts.SnippetLineCap,
	}
	if r.MaxWidth <= 0 {
		r.MaxWidth = defaultMaxWidth
	}
	if r.SnippetLineCap <= 0 {
		r.SnippetLineCap = defaultSnippetLineCap
	}
	return r
}

// FullReport renders the entire run summary.
func (r *Renderer) FullReport(summary *RunSummary) string {
	if summary == nil {
		return ""
	}
	b := &strings.Builder{}

	r.renderTitle(b, summary)
	r.renderEnvironment(b, summary)
	r.renderWorkload(b, summary)
	r.renderPerformance(b, summary)
	r.renderIntegrity(b, summary)
	r.renderMixedBreakdowns(b, summary)
	r.renderTotals(b, summary)
	r.renderArtifactsAndWarnings(b, summary)

	return strings.TrimRight(b.String(), "\n") + "\n"
}

// ConsoleSnippet renders a shorter report suitable for console messages.
func (r *Renderer) ConsoleSnippet(summary *RunSummary) string {
	full := r.FullReport(summary)
	lines := strings.Split(full, "\n")
	lineCap := r.SnippetLineCap
	if hasMixedSteps(summary) && lineCap < 60 {
		lineCap = 60
	}
	if len(lines) <= lineCap {
		return full
	}
	truncated := lines[:lineCap-1]
	truncated = append(truncated, "… (report truncated; see file for full details)")
	return strings.Join(truncated, "\n") + "\n"
}

// CompactSnippet renders a simplified, word-wrapped summary designed for
// narrow terminals (e.g. the TUI messages pane).
func (r *Renderer) CompactSnippet(summary *RunSummary) string {
	if summary == nil {
		return ""
	}
	wrapWidth := r.MaxWidth
	if wrapWidth <= 0 {
		wrapWidth = defaultMaxWidth
	}
	sb := &strings.Builder{}
	timestamp := summary.GeneratedAt
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	title := fmt.Sprintf("Run %s (%s)", summary.RunID, timestamp.Format("2006-01-02 15:04:05"))
	for _, line := range textutil.WrapWords(title, wrapWidth) {
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	sb.WriteString("Performance by Phase\n")
	sb.WriteString(r.performanceTable(summary))
	sb.WriteByte('\n')
	r.renderCompactIntegrity(sb, summary)
	r.renderCompactMixedBreakdowns(sb, summary)
	fmt.Fprintf(sb, "Totals: duration %s, data moved %s\n", summary.Totals.DurationHuman, formatBytesHuman(summary.Totals.DataBytes))
	if len(summary.Warnings) > 0 {
		sb.WriteString("Warnings:\n")
		sorted := append([]string(nil), summary.Warnings...)
		sort.Strings(sorted)
		for _, w := range sorted {
			for _, line := range textutil.WrapWords(w, wrapWidth-4) {
				sb.WriteString("    ")
				sb.WriteString(line)
				sb.WriteByte('\n')
			}
		}
	}
	return sb.String()
}

func (r *Renderer) renderTitle(b *strings.Builder, summary *RunSummary) {
	timestamp := summary.GeneratedAt
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	title := fmt.Sprintf(" SPT Run Summary  •  %s  •  %s", summary.RunID, timestamp.Format("2006-01-02 15:04:05 MST"))
	width := textutil.RuneLen(title) + 2
	if width < 60 {
		width = 60
	}
	if width > r.MaxWidth {
		width = r.MaxWidth
		title = textutil.TruncateWithEllipsis(title, width-2)
	}
	border := strings.Repeat("=", width)
	fmt.Fprintf(b, "%s\n%s\n%s\n\n", border, title, border)
}

func (r *Renderer) renderEnvironment(b *strings.Builder, summary *RunSummary) {
	env := summary.Environment
	fmt.Fprintf(b, "Environment\n")
	r.writeBullet(b, "Spt image", env.SptImage)
	r.writeBullet(b, "API endpoint", env.BaseURL)
	hostList := formatHostList(env.Hosts)
	if hostList != "" {
		label := fmt.Sprintf("Hosts (%d)", len(env.Hosts))
		r.writeBullet(b, label, hostList)
	}
	runtimeFlags := formatRuntimeFlags(env)
	if runtimeFlags != "" {
		r.writeBullet(b, "Runtime", runtimeFlags)
	}
	if env.ScenarioStoredPath != "" {
		r.writeBullet(b, "Scenario file", env.ScenarioStoredPath)
	}
	b.WriteString("\n")
}

func (r *Renderer) renderWorkload(b *strings.Builder, summary *RunSummary) {
	work := summary.Workload
	fmt.Fprintf(b, "Workload Configuration\n")
	r.writeBullet(b, "Workload", titleize(work.Type))
	isList := strings.EqualFold(work.Type, workloadTypeList)
	if work.ObjectSizeHuman != "" && !isList {
		r.writeBullet(b, "Object size", formatObjectSizeBullet(work))
	} else if isList {
		r.writeBullet(b, "Object size", "not applicable")
	}
	if work.Threads > 0 {
		r.writeBullet(b, "Threads", fmt.Sprintf("%d", work.Threads))
	}
	if len(work.Endpoints) > 0 {
		r.writeBullet(b, "Endpoints", strings.Join(work.Endpoints, ", "))
	}
	if work.Bucket != "" {
		r.writeBullet(b, "Bucket", work.Bucket)
	}
	if isList {
		prefixValue := "(not set)"
		if strings.TrimSpace(work.Prefix) != "" {
			prefixValue = work.Prefix
		}
		r.writeBullet(b, "Prefix", prefixValue)
	}
	if work.DurationRequest != "" {
		r.writeBullet(b, "Requested duration", work.DurationRequest)
	}
	toggles := formatWorkloadToggles(work)
	if toggles != "" {
		r.writeBullet(b, "Options", toggles)
	}
	b.WriteString("\n")
}

func formatObjectSizeBullet(work WorkloadSummary) string {
	if work.ObjectCount > 0 {
		return fmt.Sprintf("%s (%d objects)", work.ObjectSizeHuman, work.ObjectCount)
	}
	return work.ObjectSizeHuman
}

func (r *Renderer) renderPerformance(b *strings.Builder, summary *RunSummary) {
	fmt.Fprintf(b, "Performance by Phase\n")
	b.WriteString(r.performanceTable(summary))
	b.WriteString("\n\n")
}

func (r *Renderer) renderIntegrity(b *strings.Builder, summary *RunSummary) {
	if summary.Integrity == nil {
		return
	}
	integrity := summary.Integrity
	digest := integrity.DigestPerformance
	fmt.Fprintf(b, "Integrity Verification\n")
	r.writeBullet(b, "Selection", fmt.Sprintf("selected %d, attempted %d, verified %d, remaining %d, corrupt %d",
		integrity.SelectionCount, integrity.VerificationAttemptedCount, integrity.VerifiedCount,
		integrity.RemainingCount, integrity.CorruptCount))
	r.writeBullet(b, "Digest work", fmt.Sprintf("%d objects, %s", digest.Objects, formatBytesHuman(digest.Bytes)))
	r.writeBullet(b, "Digest worker time", fmt.Sprintf("%.6f s cumulative worker time", digest.HashWorkerSeconds))
	r.writeBullet(b, "Mean worker rate", fmt.Sprintf("%.3f MiB/s", digest.MeanWorkerHashMiBPerSecond))
	if digest.InitialWriteDelaySecondsMaxNode != nil {
		r.writeBullet(b, "Initial write delay", fmt.Sprintf("%.6f s (maximum node interval)", *digest.InitialWriteDelaySecondsMaxNode))
	}
	r.writeBullet(b, "Additional passes", fmt.Sprintf("%d full payload passes", digest.AdditionalPayloadPasses))
	b.WriteString("\n")
}

func (r *Renderer) renderCompactIntegrity(b *strings.Builder, summary *RunSummary) {
	if summary.Integrity == nil {
		return
	}
	i := summary.Integrity
	d := i.DigestPerformance
	fmt.Fprintf(b, "Integrity: selected %d, attempted %d, verified %d, remaining %d, corrupt %d\n",
		i.SelectionCount, i.VerificationAttemptedCount, i.VerifiedCount, i.RemainingCount, i.CorruptCount)
	fmt.Fprintf(b, "Digest work: %d objects, %s, %.6f cumulative worker seconds, %.3f MiB/s mean worker rate",
		d.Objects, formatBytesHuman(d.Bytes), d.HashWorkerSeconds, d.MeanWorkerHashMiBPerSecond)
	if d.InitialWriteDelaySecondsMaxNode != nil {
		fmt.Fprintf(b, ", %.6f s maximum-node initial write delay", *d.InitialWriteDelaySecondsMaxNode)
	}
	fmt.Fprintf(b, ", %d additional full payload passes\n", d.AdditionalPayloadPasses)
}

func (r *Renderer) performanceTable(summary *RunSummary) string {
	headers := []string{"Phase", "Object Size", "Success", "Data Moved", headerIOPSAvg, headerLatencyP50, headerTTFBP50, headerBandwidthAvg}
	isList := strings.EqualFold(summary.Workload.Type, workloadTypeList)
	if isList {
		headers[4] = "Ops/s Avg"
	}
	rows := make([][]string, 0, len(summary.Steps))
	for _, step := range summary.Steps {
		if step.Metrics == nil {
			continue
		}
		m := step.Metrics
		sizeCell := nonEmpty(m.ObjectSizeHuman, summary.Workload.ObjectSizeHuman)
		if strings.TrimSpace(sizeCell) == "" {
			sizeCell = "—"
		}
		row := []string{
			step.PhaseLabel,
			sizeCell,
			formatInt(m.SuccessCount),
			formatBytesHuman(m.DataBytes),
			formatNumber(m.ThroughputAvgOps, "ops/s"),
			mixedLatencyCell(step, m),
			mixedTTFBCell(step, m),
			formatNumber(m.BandwidthAvgMiBps, constants.UnitMiBPerSecond),
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		rows = append(rows, []string{"—", "—", "—", "—", "—", "—", "—", "—"})
	}
	return renderUnicodeTable(headers, rows, []Alignment{AlignLeft, AlignLeft, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight})
}

func (r *Renderer) renderCompactMixedBreakdowns(b *strings.Builder, summary *RunSummary) {
	if !hasMixedSteps(summary) {
		return
	}
	mixedStepCount := 0
	for _, step := range summary.Steps {
		if step.IsMixed && len(step.OperationBreakdown) > 0 {
			mixedStepCount++
		}
	}
	b.WriteString("Mixed Operation Breakdown\n")
	first := true
	for _, step := range summary.Steps {
		if !step.IsMixed || len(step.OperationBreakdown) == 0 {
			continue
		}
		if !first {
			b.WriteByte('\n')
		}
		first = false
		if mixedStepCount > 1 && step.StepID != "" {
			fmt.Fprintf(b, "Step: %s\n", step.StepID)
		}
		headers := []string{"Operation", "Configured", "Actual Ops", headerIOPSAvg, headerLatencyP50, headerTTFBP50}
		rows := make([][]string, 0, len(step.OperationBreakdown))
		for _, op := range step.OperationBreakdown {
			rows = append(rows, []string{
				op.Operation,
				formatConfiguredShare(op.ConfiguredShare),
				formatActualOps(op.ActualShare, op.ActualOps),
				formatNumber(op.Metrics.ThroughputAvgOps, "ops/s"),
				formatNumber(op.Metrics.LatencyMedianMs, "ms"),
				formatTTFBNumber(op.Metrics.TTFBMedianMs),
			})
		}
		b.WriteString(renderUnicodeTable(headers, rows, []Alignment{AlignLeft, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight}))
		b.WriteByte('\n')
	}
}

func (r *Renderer) renderMixedBreakdowns(b *strings.Builder, summary *RunSummary) {
	if !hasMixedSteps(summary) {
		return
	}
	b.WriteString("Mixed Operation Breakdown\n")
	for _, step := range summary.Steps {
		if !step.IsMixed || len(step.OperationBreakdown) == 0 {
			continue
		}
		fmt.Fprintf(b, "Step: %s\n", step.StepID)
		if configured := configuredDistributionText(summary.Workload.MixedDistribution); configured != "" {
			fmt.Fprintf(b, "Configured distribution: %s\n", configured)
		}
		headers := []string{"Operation", "Configured", "Actual Ops", "Success", "Failure", "Data Moved", headerIOPSAvg, headerBandwidthAvg, headerLatencyP50, headerTTFBP50}
		rows := make([][]string, 0, len(step.OperationBreakdown))
		for _, op := range step.OperationBreakdown {
			rows = append(rows, []string{
				op.Operation,
				formatConfiguredShare(op.ConfiguredShare),
				formatActualOps(op.ActualShare, op.ActualOps),
				formatInt(op.Metrics.SuccessCount),
				formatInt(op.Metrics.FailureCount),
				formatBytesHuman(op.Metrics.DataBytes),
				formatNumber(op.Metrics.ThroughputAvgOps, "ops/s"),
				formatNumber(op.Metrics.BandwidthAvgMiBps, constants.UnitMiBPerSecond),
				formatNumber(op.Metrics.LatencyMedianMs, "ms"),
				formatTTFBNumber(op.Metrics.TTFBMedianMs),
			})
		}
		b.WriteString(renderUnicodeTable(headers, rows, []Alignment{AlignLeft, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight}))
		b.WriteString("\n\n")
	}
}

func (r *Renderer) renderTotals(b *strings.Builder, summary *RunSummary) {
	totals := summary.Totals
	fmt.Fprintf(b, "Run Totals\n")
	r.writeBullet(b, "Duration", totals.DurationHuman)
	r.writeBullet(b, "Data moved", formatBytesHuman(totals.DataBytes))
	if len(summary.MissingExpected) > 0 {
		r.writeBullet(b, "Missing steps", strings.Join(summary.MissingExpected, ", "))
	}
	b.WriteString("\n")
}

func (r *Renderer) renderArtifactsAndWarnings(b *strings.Builder, summary *RunSummary) {
	if len(summary.Warnings) == 0 && len(summary.Steps) == 0 {
		return
	}
	if len(summary.Warnings) > 0 {
		fmt.Fprintf(b, "Warnings\n")
		sorted := append([]string(nil), summary.Warnings...)
		sort.Strings(sorted)
		for _, w := range sorted {
			r.writeBullet(b, "", w)
		}
		b.WriteString("\n")
	}

	incomplete := collectIncompleteSteps(summary)
	if len(incomplete) > 0 {
		fmt.Fprintf(b, "Artifact Health\n")
		for _, line := range incomplete {
			r.writeBullet(b, "", line)
		}
		b.WriteString("\n")
	}
}

func (r *Renderer) writeBullet(b *strings.Builder, label, value string) {
	if value == "" {
		return
	}
	if label != "" {
		fmt.Fprintf(b, "  • %-18s %s\n", label, value)
	} else {
		fmt.Fprintf(b, "  • %s\n", value)
	}
}

func formatHostList(hosts []HostSummary) string {
	if len(hosts) == 0 {
		return ""
	}
	names := make([]string, 0, len(hosts))
	for _, h := range hosts {
		if h.Original != "" {
			names = append(names, h.Original)
		} else if h.Host != "" {
			names = append(names, h.Host)
		}
	}
	return strings.Join(names, ", ")
}

func formatRuntimeFlags(env EnvironmentSummary) string {
	flags := make([]string, 0, 4)
	if env.AutoResults {
		flags = append(flags, "auto-results on")
	}
	if env.ShutdownOnComplete {
		flags = append(flags, fmt.Sprintf("shutdown-on-complete (linger %ds)", env.ShutdownLingerSec))
	}
	if env.DebugEnabled {
		flags = append(flags, "debug")
	}
	return strings.Join(flags, ", ")
}

func formatWorkloadToggles(work WorkloadSummary) string {
	toggles := make([]string, 0, 3)
	if work.CleanupEnabled {
		toggles = append(toggles, "cleanup")
	}
	if work.KeepScenario {
		toggles = append(toggles, "keep-scenario")
	}
	if work.SliceEndpoints {
		toggles = append(toggles, "slice-endpoints")
	}
	return strings.Join(toggles, ", ")
}

func nonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func formatInt(v int64) string {
	if v == 0 {
		return "0"
	}
	return fmt.Sprintf("%d", v)
}

func formatNumber(value float64, suffix string) string {
	if value == 0 {
		return "0 " + suffix
	}
	switch {
	case value >= 100:
		return fmt.Sprintf("%.0f %s", value, suffix)
	case value >= 10:
		return fmt.Sprintf("%.1f %s", value, suffix)
	default:
		return fmt.Sprintf("%.2f %s", value, suffix)
	}
}

func formatPercent(value float64) string {
	if value == 0 {
		return "0%"
	}
	if value == math.Trunc(value) {
		return fmt.Sprintf("%.0f%%", value)
	}
	return fmt.Sprintf("%.1f%%", value)
}

func formatBytesHuman(bytes int64) string {
	return formatBytes(bytes)
}

func hasMixedSteps(summary *RunSummary) bool {
	if summary == nil {
		return false
	}
	for _, step := range summary.Steps {
		if step.IsMixed && len(step.OperationBreakdown) > 0 {
			return true
		}
	}
	return false
}

func mixedLatencyCell(step StepSummary, metrics *PhaseMetrics) string {
	if step.IsMixed {
		return "see ops"
	}
	return formatNumber(metrics.LatencyHeadlineMs, "ms")
}

func mixedTTFBCell(step StepSummary, metrics *PhaseMetrics) string {
	if step.IsMixed {
		return "see ops"
	}
	return formatTTFBNumber(metrics.TTFBMedianMs)
}

func formatTTFBNumber(value float64) string {
	if value <= 0 {
		return "—"
	}
	return formatNumber(value, "ms")
}

func configuredDistributionText(dist MixedDistribution) string {
	if !dist.Available {
		return ""
	}
	parts := make([]string, 0, 4)
	for _, item := range []struct {
		label string
		value int
	}{
		{label: "READ", value: dist.ReadPercent},
		{label: "STAT", value: dist.StatPercent},
		{label: "CREATE", value: dist.CreatePercent},
		{label: "DELETE", value: dist.DeletePercent},
	} {
		if item.value <= 0 {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %d%%", item.label, item.value))
	}
	return strings.Join(parts, ", ")
}

func formatConfiguredShare(value *float64) string {
	if value == nil {
		return "-"
	}
	return formatPercent(*value)
}

func formatActualOps(share *float64, count int64) string {
	if share == nil {
		return formatInt(count)
	}
	return fmt.Sprintf("%s (%s)", formatPercent(*share), formatInt(count))
}

func collectIncompleteSteps(summary *RunSummary) []string {
	var out []string
	for _, step := range summary.Steps {
		if len(step.MissingRequired) > 0 {
			out = append(out, fmt.Sprintf("%s missing required artifacts: %s", step.StepID, strings.Join(step.MissingRequired, ", ")))
		}
	}
	return out
}

// Alignment determines horizontal alignment for table cells.
type Alignment int

const (
	// AlignLeft left-justifies table cell content.
	AlignLeft Alignment = iota
	// AlignRight right-justifies table cell content.
	AlignRight
)

func renderUnicodeTable(headers []string, rows [][]string, align []Alignment) string {
	if len(align) != len(headers) {
		align = make([]Alignment, len(headers))
	}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = textutil.RuneLen(h)
	}
	for _, row := range rows {
		for c := range headers {
			if c < len(row) {
				if rl := textutil.RuneLen(row[c]); rl > widths[c] {
					widths[c] = rl
				}
			}
		}
	}

	cellPad := func(value string, column int) string {
		width := widths[column]
		if column < len(align) && align[column] == AlignRight {
			return textutil.PadLeft(value, width)
		}
		return textutil.PadRight(value, width)
	}

	var sb strings.Builder
	sb.WriteString(drawTableBorder("┌", "┬", "┐", widths))
	sb.WriteString("\n")
	sb.WriteString(drawTableRow(headers, widths, cellPad))
	sb.WriteString("\n")
	sb.WriteString(drawTableBorder("├", "┼", "┤", widths))
	sb.WriteString("\n")
	for i, row := range rows {
		padded := make([]string, len(headers))
		for c := range headers {
			value := ""
			if c < len(row) {
				value = row[c]
			} else {
				value = ""
			}
			padded[c] = cellPad(value, c)
		}
		sb.WriteString("│ " + strings.Join(padded, " │ ") + " │")
		if i < len(rows)-1 {
			sb.WriteString("\n")
		}
	}
	sb.WriteString("\n")
	sb.WriteString(drawTableBorder("└", "┴", "┘", widths))
	return sb.String()
}

func drawTableBorder(left, middle, right string, widths []int) string {
	parts := make([]string, len(widths))
	for i, w := range widths {
		parts[i] = strings.Repeat("─", w+2)
	}
	return left + strings.Join(parts, middle) + right
}

func drawTableRow(values []string, widths []int, pad func(string, int) string) string {
	padded := make([]string, len(widths))
	for i, width := range widths {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		padded[i] = pad(value, i)
		if textutil.RuneLen(padded[i]) > width {
			padded[i] = textutil.TruncateWithEllipsis(padded[i], width)
		}
	}
	return "│ " + strings.Join(padded, " │ ") + " │"
}
