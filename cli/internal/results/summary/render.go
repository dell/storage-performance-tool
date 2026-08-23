package summary

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
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
	notApplicableCell     = "—"
	objectSizeDiscovered  = "discovered at runtime"
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
	r.renderDeleteDetails(b, summary)
	r.renderIntegrity(b, summary)
	r.renderMixedBreakdowns(b, summary)
	r.renderTotals(b, summary)
	r.renderArtifactsAndWarnings(b, summary)

	return strings.TrimRight(b.String(), "\n") + "\n"
}

func (r *Renderer) renderDeleteDetails(b *strings.Builder, summary *RunSummary) {
	for _, step := range summary.Steps {
		d := step.Delete
		if d == nil {
			continue
		}
		fmt.Fprintf(b, "DELETE Results — %s\n", step.PhaseLabel)
		r.writeBullet(b, "Units", fmt.Sprintf("requests=%s, objects=%s, batches=%s", d.Units.Requests, d.Units.Objects, d.Units.Batches))
		r.writeBullet(b, "Requests", fmt.Sprintf("attempted %d, full success %d, partial %d, failed %d, unresolved %d, %.3f requests/s", d.Requests.Attempted, d.Requests.FullSuccess, d.Requests.Partial, d.Requests.Failed, d.Requests.Unresolved, d.Requests.PerSecond))
		r.writeBullet(b, "Objects", fmt.Sprintf("selected %d, attempted %d, accepted %d, failed %d, unattempted %d, unresolved %d, %.3f objects/s", d.Objects.Selected, d.Objects.Attempted, d.Objects.Accepted, d.Objects.Failed, d.Objects.Unattempted, d.Objects.Unresolved, d.Objects.PerSecond))
		r.writeBullet(b, "Batches", fmt.Sprintf("configured %d, actual requests %d, actual objects %d, mean %.3f objects/request, full %d, partial %d, full %.3f%%", d.Batches.ConfiguredSize, d.Batches.ActualRequestCount, d.Batches.ActualObjectCount, d.Batches.MeanObjectsPerRequest, d.Batches.FullBatchCount, d.Batches.PartialBatchCount, d.Batches.FullBatchPercent))
		r.writeBullet(b, "Versions", fmt.Sprintf("current key %d, exact version %d", d.Versions.CurrentKey, d.Versions.ExactVersion))
		r.writeBullet(b, "Completion", fmt.Sprintf("requests %.3f%%, objects %.3f%%, reconciled %t", d.Completion.RequestPercent, d.Completion.ObjectPercent, d.TerminalReconciled))
		r.writeBullet(b, "Identity", fmt.Sprintf("mode %s, configured batch %d, selection %s", d.Identity.Mode, d.Identity.ConfiguredBatchSize, d.Identity.SelectionOrder))
		r.writeBullet(b, "Outcome", strings.ReplaceAll(d.FailurePolicy.Outcome, "_", " "))
		r.writeBullet(b, "Failure policy", fmt.Sprintf("mode %s, max objects %d, max percent %.3f, grace %ds, operational %d, excluded %d, observed %.3f%%", d.FailurePolicy.Mode, d.FailurePolicy.MaxFailedObjects, d.FailurePolicy.MaxFailurePercent, d.FailurePolicy.GraceSeconds, d.FailurePolicy.OperationalFailedObjects, d.FailurePolicy.ExcludedFailedObjects, d.FailurePolicy.ObservedFailurePercent))
		r.writeBullet(b, "Phases", formatDeletePhases(d.Phases))
		r.writeBullet(b, "Request latency", formatDeleteTiming(d.Timing.LatencyDefinition, d.Timing.Latency))
		r.writeBullet(b, "Request duration", formatDeleteTiming(d.Timing.DurationDefinition, d.Timing.Duration))
		r.writeBullet(b, "Not applicable", "object size, data moved, bandwidth, TTFB, object latency")
		r.writeBullet(b, "Outcome terminology", d.OutcomeTerminology)
		r.writeBullet(b, "Verification", d.Verification.Notice)
		if len(d.Buckets) > 0 {
			for _, bucket := range d.Buckets {
				r.writeBullet(b, "Bucket "+bucket.Bucket, fmt.Sprintf("selected %d, attempted %d, accepted %d, failed %d", bucket.Selected, bucket.Attempted, bucket.Accepted, bucket.Failed))
			}
		}
		b.WriteByte('\n')
	}
}

func formatDeleteTiming(definition string, stat *deletemetrics.TimingStat) string {
	if stat == nil || stat.Count <= 0 {
		return definition + "; N/A"
	}
	return fmt.Sprintf(
		"%s; count %d, mean %.3f us, p50 %d us, p90 %d us, p99 %d us, p99.9 %d us",
		definition, stat.Count, stat.MeanUs, stat.P50Us, stat.P90Us, stat.P99Us, stat.P999Us)
}

func formatDeletePhases(phases deletemetrics.Phases) string {
	values := []struct {
		name  string
		value *float64
	}{
		{"seed", phases.SeedSeconds},
		{"discovery", phases.DiscoverySeconds},
		{"pre-validation", phases.PreValidationSeconds},
		{"scheduled DELETE", phases.ScheduledDeleteSeconds},
		{"drain", phases.DrainSeconds},
		{"post-verification", phases.PostVerificationSeconds},
		{"cleanup", phases.CleanupSeconds},
		{"total wall", phases.TotalWallSeconds},
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		formatted := "N/A"
		if value.value != nil {
			formatted = fmt.Sprintf("%.6fs", *value.value)
		}
		parts = append(parts, value.name+" "+formatted)
	}
	return strings.Join(parts, ", ")
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
	r.renderIntegrity(sb, summary)
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
	isReadVerify := strings.EqualFold(work.Type, workloadTypeReadVerify)
	if work.ObjectSizeHuman != "" && !isList {
		r.writeBullet(b, "Object size", formatObjectSizeBullet(work))
	} else if isList {
		r.writeBullet(b, "Object size", "not applicable")
	} else if isReadVerify {
		r.writeBullet(b, "Object size", objectSizeDiscovered)
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
	r.writeBullet(b, "Finalization", map[bool]string{true: "complete", false: "incomplete"}[integrity.Complete])
	if integrity.VerificationDeferred {
		r.writeBullet(b, "Verification", "deferred")
	}
	if integrity.SelectionCountsValid && hasListStep(summary) {
		r.writeBullet(b, "Discovery", fmt.Sprintf("source %d, unique %d, selected %d, delete markers excluded %d",
			integrity.SelectionSourceCount, integrity.SelectionUniqueCount, integrity.SelectionCount,
			integrity.ExcludedDeleteMarkerCount))
	}
	if integrity.FinalizationError != "" {
		r.writeBullet(b, "Finalization error", integrity.FinalizationError)
	}
	r.writeBullet(b, "Selection", fmt.Sprintf("selected %d, attempted %d, verified %d, remaining %d, corrupt %d",
		integrity.SelectionCount, integrity.VerificationAttemptedCount, integrity.VerifiedCount,
		integrity.RemainingCount, integrity.CorruptCount))
	r.writeBullet(b, "Empty selection", fmt.Sprintf("%t (allowed: %t)",
		integrity.EmptySelection, integrity.EmptyAllowed))
	r.writeBullet(b, "Digest work", fmt.Sprintf("%d objects, %s", digest.Objects, formatBytesHuman(digest.Bytes)))
	r.writeBullet(b, "Digest worker time", fmt.Sprintf("%.6f s cumulative worker time", digest.HashWorkerSeconds))
	r.writeBullet(b, "Mean worker rate", fmt.Sprintf("%.3f MiB/s", digest.MeanWorkerHashMiBPerSecond))
	if digest.InitialWriteDelaySecondsMaxNode != nil {
		r.writeBullet(b, "Initial write delay", fmt.Sprintf("%.6f s (maximum node interval)", *digest.InitialWriteDelaySecondsMaxNode))
	}
	r.writeBullet(b, "Additional passes", fmt.Sprintf("%d full payload passes", digest.AdditionalPayloadPasses))
	b.WriteString("\n")
}

func (r *Renderer) performanceTable(summary *RunSummary) string {
	headers := []string{"Phase", "Object Size", "Success", "Data Moved", headerIOPSAvg, headerLatencyP50, headerTTFBP50, headerBandwidthAvg}
	if hasListStep(summary) {
		headers[4] = "Rate Avg"
	}
	rows := make([][]string, 0, len(summary.Steps))
	for _, step := range summary.Steps {
		if step.Metrics == nil {
			continue
		}
		m := step.Metrics
		sizeCell := nonEmpty(m.ObjectSizeHuman, summary.Workload.ObjectSizeHuman)
		dataCell := formatBytesHuman(m.DataBytes)
		bandwidthCell := formatNumber(m.BandwidthAvgMiBps, constants.UnitMiBPerSecond)
		rateCell := formatNumber(m.ThroughputAvgOps, "ops/s")
		if strings.EqualFold(step.Operation, workloadTypeList) {
			sizeCell, dataCell, bandwidthCell = notApplicableCell, notApplicableCell, notApplicableCell
			rateCell = formatNumber(m.ThroughputAvgOps, "objects/s")
		}
		if strings.EqualFold(step.Operation, "DELETE") {
			sizeCell, dataCell, bandwidthCell = notApplicableCell, notApplicableCell, notApplicableCell
		}
		if strings.TrimSpace(sizeCell) == "" {
			sizeCell = notApplicableCell
		}
		row := []string{
			step.PhaseLabel,
			sizeCell,
			formatInt(m.SuccessCount),
			dataCell,
			rateCell,
			mixedLatencyCell(step, m),
			mixedTTFBCell(step, m),
			bandwidthCell,
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		rows = append(rows, []string{notApplicableCell, notApplicableCell, notApplicableCell, notApplicableCell, notApplicableCell, notApplicableCell, notApplicableCell, notApplicableCell})
	}
	return renderUnicodeTable(headers, rows, []Alignment{AlignLeft, AlignLeft, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight, AlignRight})
}

func hasListStep(summary *RunSummary) bool {
	if summary == nil {
		return false
	}
	for _, step := range summary.Steps {
		if strings.EqualFold(step.Operation, workloadTypeList) {
			return true
		}
	}
	return false
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
	if strings.EqualFold(step.Operation, "DELETE") && step.Delete != nil {
		if step.Delete.Timing.Latency == nil || step.Delete.Timing.Latency.Count <= 0 {
			return notApplicableCell
		}
		return formatNumber(float64(step.Delete.Timing.Latency.P50Us)/1000.0, "ms")
	}
	latency := metrics.LatencyHeadlineMs
	if strings.EqualFold(step.Operation, "DELETE") && latency <= 0 {
		latency = metrics.LatencyMedianMs
	}
	return formatNumber(latency, "ms")
}

func mixedTTFBCell(step StepSummary, metrics *PhaseMetrics) string {
	if step.IsMixed {
		return "see ops"
	}
	if strings.EqualFold(step.Operation, "DELETE") {
		return notApplicableCell
	}
	return formatTTFBNumber(metrics.TTFBMedianMs)
}

func formatTTFBNumber(value float64) string {
	if value <= 0 {
		return notApplicableCell
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
