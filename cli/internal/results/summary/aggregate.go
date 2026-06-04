package summary

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// RunSummary represents the aggregated data set used when rendering result summaries.
type RunSummary struct {
	RunID             string
	GeneratedAt       time.Time
	ManifestPath      string
	MetadataPath      string
	Environment       EnvironmentSummary
	Workload          WorkloadSummary
	Steps             []StepSummary
	Totals            RunTotals
	MissingExpected   []string
	ActualStepIDs     []string
	DiscoveredStepIDs []string
	Warnings          []string
}

// EnvironmentSummary captures host, image, and runtime metadata for the run.
type EnvironmentSummary struct {
	BaseURL            string
	APIPort            string
	SptImage           string
	Label              string
	ResultsDir         string
	ResultsRoot        string
	ScenarioFile       string
	ScenarioStoredPath string
	Hosts              []HostSummary
	AutoResults        bool
	ShutdownOnComplete bool
	ShutdownLingerSec  int
	DebugEnabled       bool
}

// HostSummary describes a single orchestrated host.
type HostSummary struct {
	Host       string
	User       string
	IsLocal    bool
	Original   string
	DockerHost string
}

// WorkloadSummary records key scenario parameters.
type WorkloadSummary struct {
	Type            string
	ObjectSizeBytes int64
	ObjectSizeMB    float64
	ObjectSizeGiB   float64
	ObjectSizeHuman string
	ObjectCount     int64
	Threads         int
	Endpoints       []string
	Bucket          string
	Prefix          string
	DurationRequest string
	CleanupEnabled  bool
	KeepScenario    bool
	SliceEndpoints  bool
	MixedDistribution MixedDistribution
}

// StepSummary aggregates per-step metrics and artifact health.
type StepSummary struct {
	Ordinal         int
	StepID          string
	PhaseLabel      string
	Operation       string
	Status          StepStatus
	Metrics         *PhaseMetrics
	IsMixed         bool
	OperationBreakdown []OperationBreakdown
	MixedLatencyNote string
	MissingRequired []string
	MissingOptional []string
	Notes           []string
}

type MixedDistribution struct {
	Available     bool
	ReadPercent   int
	StatPercent   int
	CreatePercent int
	DeletePercent int
}

type OperationBreakdown struct {
	Operation       string
	ConfiguredShare *float64
	ActualShare     *float64
	ActualOps       int64
	Metrics         PhaseMetrics
}

// PhaseMetrics holds derived statistics for a single run phase.
type PhaseMetrics struct {
	SuccessCount      int64
	FailureCount      int64
	DataBytes         int64
	DataMB            float64
	DataGiB           float64
	HasDataTransfer   bool
	DurationSeconds   float64
	DurationHuman     string
	ThroughputAvgOps  float64
	ThroughputLastOps float64
	BandwidthAvgMBps  float64
	BandwidthLastMBps float64
	LatencyHeadlineMs float64
	LatencyMedianMs   float64
	LatencyP90Ms      float64
	LatencyP99Ms      float64
	LatencyP999Ms     float64
	TTFBMedianMs      float64
	TTFBP90Ms         float64
	TTFBP99Ms         float64
	TTFBP999Ms        float64
	ObjectSizeBytes   int64
	ObjectSizeMB      float64
	ObjectSizeGiB     float64
	ObjectSizeHuman   string
	Concurrency       float64
	ConcurrencyMean   float64
	NodeCount         int64
	SampleTimestamp   string
}

// RunTotals aggregates run-wide duration and data transfer metrics.
type RunTotals struct {
	DurationSeconds float64
	DurationHuman   string
	DataBytes       int64
	DataMB          float64
	DataGiB         float64
}

const (
	bytesInKB = 1024
	bytesInMB = bytesInKB * 1024
	bytesInGB = bytesInMB * 1024
)

// Aggregate builds a RunSummary from Loader output.
func Aggregate(data *RunData) (*RunSummary, error) {
	if data == nil {
		return nil, errors.New("run data not provided")
	}
	if data.Params == nil {
		return nil, errors.New("run parameters not loaded")
	}
	summary := &RunSummary{
		RunID:             data.RunID,
		GeneratedAt:       data.Params.GeneratedAt,
		ManifestPath:      data.ManifestPath,
		MetadataPath:      data.MetadataPath,
		MissingExpected:   append([]string(nil), data.MissingExpectedSteps...),
		ActualStepIDs:     append([]string(nil), data.Params.ActualStepIDs...),
		DiscoveredStepIDs: append([]string(nil), data.Params.DiscoveredStepIDs...),
	}

	summary.Environment = buildEnvironmentSummary(data)
	workload, workloadWarnings := buildWorkloadSummary(data)
	summary.Workload = workload
	summary.Warnings = append(summary.Warnings, workloadWarnings...)

	steps, totals, stepWarnings := buildStepSummaries(data, workload)
	summary.Steps = steps
	summary.Totals = totals
	summary.Warnings = append(summary.Warnings, stepWarnings...)

	if len(summary.MissingExpected) > 1 {
		sort.Strings(summary.MissingExpected)
	}
	if len(summary.Warnings) > 1 {
		summary.Warnings = dedupeStrings(summary.Warnings)
		sort.Strings(summary.Warnings)
	}
	return summary, nil
}

func buildEnvironmentSummary(data *RunData) EnvironmentSummary {
	params := data.Params
	env := EnvironmentSummary{
		BaseURL:            params.BaseURL,
		APIPort:            params.APIPort,
		SptImage:           params.SptImage,
		Label:              params.Label,
		ResultsDir:         params.ResultsDir,
		ResultsRoot:        params.ResultsRoot,
		ScenarioFile:       params.ScenarioFile,
		ScenarioStoredPath: params.ScenarioStoredPath,
		AutoResults:        params.ResultsOptions.AutoResults,
		ShutdownOnComplete: params.ResultsOptions.ShutdownOnComplete,
		ShutdownLingerSec:  params.ResultsOptions.ShutdownLingerSeconds,
		DebugEnabled:       params.ResultsOptions.Debug,
	}
	hosts := make([]HostSummary, 0, len(params.Hosts))
	for _, h := range params.Hosts {
		hosts = append(hosts, HostSummary(h))
	}
	env.Hosts = hosts
	return env
}

const (
	workloadTypeList = "list"
)

func buildWorkloadSummary(data *RunData) (WorkloadSummary, []string) {
	params := data.Params.ScenarioParams
	warnings := make([]string, 0, 1)
	sizeBytes, sizeWarn := parseSizeString(params.ObjectSize)
	if sizeWarn != nil {
		warnings = append(warnings, fmt.Sprintf("object size parse error: %v", sizeWarn))
	}
	isList := strings.EqualFold(data.Params.WorkloadType, workloadTypeList)
	summary := WorkloadSummary{
		Type:            strings.ToLower(data.Params.WorkloadType),
		ObjectSizeBytes: sizeBytes,
		ObjectSizeMB:    bytesToMB(sizeBytes),
		ObjectSizeGiB:   bytesToGiB(sizeBytes),
		ObjectSizeHuman: formatBytes(sizeBytes),
		ObjectCount:     params.ObjectCount,
		Threads:         params.Threads,
		Endpoints:       append([]string(nil), params.Endpoints...),
		Bucket:          params.Bucket,
		Prefix:          params.Prefix,
		DurationRequest: params.Duration,
		CleanupEnabled:  params.Cleanup,
		KeepScenario:    params.KeepScenario,
		SliceEndpoints:  params.SliceEndpoints,
		MixedDistribution: mixedDistributionFromParams(data.Params.WorkloadType, params),
	}
	if isList {
		summary.ObjectSizeHuman = ""
	}
	return summary, warnings
}

func buildStepSummaries(data *RunData, workload WorkloadSummary) ([]StepSummary, RunTotals, []string) {
	steps := make([]StepSummary, 0, len(data.StepOrder))
	totals := RunTotals{}
	warnings := make([]string, 0)

	for idx, stepID := range data.StepOrder {
		stepData := data.Steps[stepID]
		if stepData == nil {
			continue
		}
		summary := StepSummary{
			Ordinal:         idx + 1,
			StepID:          stepData.StepID,
			PhaseLabel:      phaseLabelFromStep(stepData.StepID),
			Operation:       operationFromStep(stepData.StepID, stepData.Metrics),
			Status:          stepData.Status,
			MissingRequired: append([]string(nil), stepData.MissingRequired...),
			MissingOptional: append([]string(nil), stepData.MissingOptional...),
			Notes:           append([]string(nil), stepData.Notes...),
		}

		var metrics *PhaseMetrics
		if isMixedStep(stepData.StepID, workload, stepData.Metrics) {
			var mixedWarnings []string
			metrics, summary.OperationBreakdown, mixedWarnings = deriveMixedMetrics(stepData, workload, workload.ObjectSizeBytes)
			summary.IsMixed = metrics != nil
			if summary.IsMixed {
				summary.Operation = "MIXED"
				summary.MixedLatencyNote = "Mixed latency is shown per operation; no combined p50 is derived from per-op quantiles."
			}
			warnings = append(warnings, mixedWarnings...)
		} else {
			metrics = deriveMetrics(stepData, workload.ObjectSizeBytes)
		}
		if metrics != nil {
			summary.Metrics = metrics
			totals.DurationSeconds += metrics.DurationSeconds
			totals.DataBytes += metrics.DataBytes
		} else {
			statusLabel := string(stepData.Status)
			if statusLabel == "" {
				statusLabel = string(StepStatusUnknown)
			}
			label := summary.PhaseLabel
			if label == "" {
				label = stepData.StepID
			}
			warnings = append(warnings, fmt.Sprintf("%s metrics unavailable (status: %s)", label, statusLabel))
		}

		steps = append(steps, summary)
	}

	totals.DurationHuman = formatSeconds(totals.DurationSeconds)
	totals.DataMB = bytesToMB(totals.DataBytes)
	totals.DataGiB = bytesToGiB(totals.DataBytes)
	return steps, totals, warnings
}

func mixedDistributionFromParams(workloadType string, params ScenarioParams) MixedDistribution {
	if !strings.EqualFold(workloadType, "mixed") && !strings.EqualFold(params.WorkloadType, "mixed") {
		return MixedDistribution{}
	}
	return MixedDistribution{
		Available:     true,
		ReadPercent:   params.GetDistrib,
		StatPercent:   params.StatDistrib,
		CreatePercent: params.PutDistrib,
		DeletePercent: params.DeleteDistrib,
	}
	}

func isMixedStep(stepID string, workload WorkloadSummary, totals *MetricsTotals) bool {
	if totals == nil || len(totals.Rows) < 2 {
		return false
	}
	if !strings.EqualFold(workload.Type, "mixed") {
		return false
	}
	if !strings.HasSuffix(strings.ToLower(stepID), "-mixed") {
		return false
	}
	seen := make(map[string]struct{}, len(totals.Rows))
	for _, row := range totals.Rows {
		op := normalizeReportOperation(row.Operation)
		if op == "" {
			continue
		}
		seen[op] = struct{}{}
	}
	return len(seen) >= 2
}

func deriveMixedMetrics(stepData *StepData, workload WorkloadSummary, objectSizeBytes int64) (*PhaseMetrics, []OperationBreakdown, []string) {
	if stepData == nil || stepData.Metrics == nil || len(stepData.Metrics.Rows) == 0 {
		return nil, nil, nil
	}
	groups := make(map[string][]MetricsTotalsRow, len(stepData.Metrics.Rows))
	for _, row := range stepData.Metrics.Rows {
		op := normalizeReportOperation(row.Operation)
		if op == "" {
			op = strings.ToUpper(strings.TrimSpace(row.Operation))
		}
		groups[op] = append(groups[op], row)
	}

	warnings := make([]string, 0, 1)
	if workload.MixedDistribution.Available {
		sum := workload.MixedDistribution.ReadPercent + workload.MixedDistribution.StatPercent + workload.MixedDistribution.CreatePercent + workload.MixedDistribution.DeletePercent
		if sum != 100 {
			warnings = append(warnings, fmt.Sprintf("mixed configured distribution sums to %d%%, expected 100%%", sum))
		}
	}

	orderedOps := orderedOperationKeys(groups)
	breakdown := make([]OperationBreakdown, 0, len(orderedOps))
	metrics := &PhaseMetrics{}
	var totalActualOps int64
	for _, op := range orderedOps {
		opMetrics := deriveOperationMetrics(groups[op], objectSizeBytes)
		actualOps := opMetrics.SuccessCount + opMetrics.FailureCount
		totalActualOps += actualOps
		breakdown = append(breakdown, OperationBreakdown{
			Operation:       op,
			ConfiguredShare: operationConfiguredShare(op, workload.MixedDistribution),
			ActualOps:       actualOps,
			Metrics:         opMetrics,
		})
		metrics.SuccessCount += opMetrics.SuccessCount
		metrics.FailureCount += opMetrics.FailureCount
		metrics.DataBytes += opMetrics.DataBytes
		metrics.ThroughputAvgOps += opMetrics.ThroughputAvgOps
		metrics.ThroughputLastOps += opMetrics.ThroughputLastOps
		metrics.BandwidthAvgMBps += opMetrics.BandwidthAvgMBps
		metrics.BandwidthLastMBps += opMetrics.BandwidthLastMBps
		if opMetrics.DurationSeconds > metrics.DurationSeconds {
			metrics.DurationSeconds = opMetrics.DurationSeconds
		}
		if opMetrics.Concurrency > metrics.Concurrency {
			metrics.Concurrency = opMetrics.Concurrency
		}
		if opMetrics.ConcurrencyMean > metrics.ConcurrencyMean {
			metrics.ConcurrencyMean = opMetrics.ConcurrencyMean
		}
		if opMetrics.NodeCount > metrics.NodeCount {
			metrics.NodeCount = opMetrics.NodeCount
		}
		if opMetrics.SampleTimestamp > metrics.SampleTimestamp {
			metrics.SampleTimestamp = opMetrics.SampleTimestamp
		}
	}
	metrics.DurationHuman = formatSeconds(metrics.DurationSeconds)
	metrics.DataMB = bytesToMB(metrics.DataBytes)
	metrics.DataGiB = bytesToGiB(metrics.DataBytes)
	metrics.HasDataTransfer = metrics.DataBytes > 0
	if objectSizeBytes > 0 {
		metrics.ObjectSizeBytes = objectSizeBytes
		metrics.ObjectSizeMB = bytesToMB(objectSizeBytes)
		metrics.ObjectSizeGiB = bytesToGiB(objectSizeBytes)
		metrics.ObjectSizeHuman = formatBytes(objectSizeBytes)
	}
	if totalActualOps > 0 {
		for i := range breakdown {
			share := float64(breakdown[i].ActualOps) * 100 / float64(totalActualOps)
			breakdown[i].ActualShare = &share
		}
	}
	return metrics, breakdown, warnings
}

func deriveOperationMetrics(rows []MetricsTotalsRow, objectSizeBytes int64) PhaseMetrics {
	if len(rows) == 0 {
		return PhaseMetrics{}
	}
	best := rows[0]
	metrics := PhaseMetrics{}
	for _, row := range rows {
		metrics.SuccessCount += row.SuccessCount
		metrics.FailureCount += row.FailureCount
		metrics.DataBytes += row.SizeBytes
		metrics.ThroughputAvgOps += row.ThroughputAvgOps
		metrics.ThroughputLastOps += row.ThroughputLastOps
		metrics.BandwidthAvgMBps += row.BandwidthAvgMBps
		metrics.BandwidthLastMBps += row.BandwidthLastMBps
		if row.StepDurationSeconds > metrics.DurationSeconds {
			metrics.DurationSeconds = row.StepDurationSeconds
		}
		if row.Concurrency > metrics.Concurrency {
			metrics.Concurrency = row.Concurrency
		}
		if row.ConcurrencyMean > metrics.ConcurrencyMean {
			metrics.ConcurrencyMean = row.ConcurrencyMean
		}
		if row.NodeCount > metrics.NodeCount {
			metrics.NodeCount = row.NodeCount
		}
		if row.SampleTimestamp > metrics.SampleTimestamp {
			metrics.SampleTimestamp = row.SampleTimestamp
		}
		if row.SuccessCount+row.FailureCount > best.SuccessCount+best.FailureCount {
			best = row
		}
	}
	metrics.DataMB = bytesToMB(metrics.DataBytes)
	metrics.DataGiB = bytesToGiB(metrics.DataBytes)
	metrics.HasDataTransfer = metrics.DataBytes > 0
	metrics.DurationHuman = formatSeconds(metrics.DurationSeconds)
	metrics.LatencyHeadlineMs = preferredLatencyMicros(best) / 1000.0
	metrics.LatencyMedianMs = best.LatencyP50Micros / 1000.0
	metrics.LatencyP90Ms = best.LatencyP90Micros / 1000.0
	metrics.LatencyP99Ms = best.LatencyP99Micros / 1000.0
	metrics.LatencyP999Ms = best.LatencyP999Micros / 1000.0
	metrics.TTFBMedianMs = best.TTFBP50Micros / 1000.0
	metrics.TTFBP90Ms = best.TTFBP90Micros / 1000.0
	metrics.TTFBP99Ms = best.TTFBP99Micros / 1000.0
	metrics.TTFBP999Ms = best.TTFBP999Micros / 1000.0
	if objectSizeBytes > 0 {
		metrics.ObjectSizeBytes = objectSizeBytes
		metrics.ObjectSizeMB = bytesToMB(objectSizeBytes)
		metrics.ObjectSizeGiB = bytesToGiB(objectSizeBytes)
		metrics.ObjectSizeHuman = formatBytes(objectSizeBytes)
	}
	return metrics
}

func operationConfiguredShare(op string, dist MixedDistribution) *float64 {
	if !dist.Available {
		return nil
	}
	var value float64
	switch normalizeReportOperation(op) {
	case "READ":
		value = float64(dist.ReadPercent)
	case "STAT":
		value = float64(dist.StatPercent)
	case "CREATE":
		value = float64(dist.CreatePercent)
	case "DELETE":
		value = float64(dist.DeletePercent)
	default:
		return nil
	}
	return &value
}

func orderedOperationKeys(groups map[string][]MetricsTotalsRow) []string {
	ordered := make([]string, 0, len(groups))
	for _, op := range []string{"READ", "STAT", "CREATE", "DELETE"} {
		if _, ok := groups[op]; ok {
			ordered = append(ordered, op)
		}
	}
	unknown := make([]string, 0, len(groups))
	for op := range groups {
		switch op {
		case "READ", "STAT", "CREATE", "DELETE":
			continue
		default:
			unknown = append(unknown, op)
		}
	}
	if len(unknown) > 1 {
		sort.Strings(unknown)
	}
	return append(ordered, unknown...)
}

func normalizeReportOperation(op string) string {
	switch strings.ToUpper(strings.TrimSpace(op)) {
	case "GET", "READ":
		return "READ"
	case "HEAD", "STAT":
		return "STAT"
	case "PUT", "CREATE", "WRITE":
		return "CREATE"
	case "DELETE":
		return "DELETE"
	default:
		return strings.ToUpper(strings.TrimSpace(op))
	}
}

func deriveMetrics(stepData *StepData, objectSizeBytes int64) *PhaseMetrics {
	if stepData == nil || stepData.Metrics == nil || len(stepData.Metrics.Rows) == 0 {
		return nil
	}
	operationHint := operationFromStep(stepData.StepID, stepData.Metrics)
	row := selectMetricsRow(stepData.Metrics, operationHint)

	metrics := &PhaseMetrics{
		SuccessCount:      row.SuccessCount,
		FailureCount:      row.FailureCount,
		DataBytes:         row.SizeBytes,
		DataMB:            bytesToMB(row.SizeBytes),
		DataGiB:           bytesToGiB(row.SizeBytes),
		HasDataTransfer:   row.SizeBytes > 0,
		DurationSeconds:   row.StepDurationSeconds,
		DurationHuman:     formatSeconds(row.StepDurationSeconds),
		ThroughputAvgOps:  row.ThroughputAvgOps,
		ThroughputLastOps: row.ThroughputLastOps,
		BandwidthAvgMBps:  row.BandwidthAvgMBps,
		BandwidthLastMBps: row.BandwidthLastMBps,
		LatencyHeadlineMs: preferredLatencyMicros(row) / 1000.0,
		LatencyMedianMs:   row.LatencyP50Micros / 1000.0,
		LatencyP90Ms:      row.LatencyP90Micros / 1000.0,
		LatencyP99Ms:      row.LatencyP99Micros / 1000.0,
		LatencyP999Ms:     row.LatencyP999Micros / 1000.0,
		TTFBMedianMs:      row.TTFBP50Micros / 1000.0,
		TTFBP90Ms:         row.TTFBP90Micros / 1000.0,
		TTFBP99Ms:         row.TTFBP99Micros / 1000.0,
		TTFBP999Ms:        row.TTFBP999Micros / 1000.0,
		Concurrency:       row.Concurrency,
		ConcurrencyMean:   row.ConcurrencyMean,
		NodeCount:         row.NodeCount,
		SampleTimestamp:   row.SampleTimestamp,
	}

	if objectSizeBytes > 0 {
		metrics.ObjectSizeBytes = objectSizeBytes
		metrics.ObjectSizeMB = bytesToMB(objectSizeBytes)
		metrics.ObjectSizeGiB = bytesToGiB(objectSizeBytes)
		metrics.ObjectSizeHuman = formatBytes(objectSizeBytes)
	}
	return metrics
}

func preferredLatencyMicros(row MetricsTotalsRow) float64 {
	if row.LatencyP50Micros > 0 {
		return row.LatencyP50Micros
	}
	return row.LatencyAvgMicros
}

func selectMetricsRow(totals *MetricsTotals, operationHint string) MetricsTotalsRow {
	if totals == nil || len(totals.Rows) == 0 {
		return MetricsTotalsRow{}
	}
	if operationHint != "" {
		upperHint := strings.ToUpper(operationHint)
		for _, row := range totals.Rows {
			if strings.ToUpper(strings.TrimSpace(row.Operation)) == upperHint {
				return row
			}
		}
	}
	// Fallback: choose the row with the highest success count, otherwise the first row.
	best := totals.Rows[0]
	for _, row := range totals.Rows[1:] {
		if row.SuccessCount > best.SuccessCount {
			best = row
		}
	}
	return best
}

func phaseLabelFromStep(stepID string) string {
	if stepID == "" {
		return ""
	}
	lowerStepID := strings.ToLower(stepID)
	switch {
	case strings.HasSuffix(lowerStepID, "-cleanup-seed"):
		return "Cleanup seed"
	case strings.HasSuffix(lowerStepID, "-cleanup-put"):
		return "Cleanup CREATE"
	case strings.HasSuffix(lowerStepID, "-mixed"):
		return "Mixed"
	}
	parts := strings.Split(stepID, "-")
	if len(parts) == 0 {
		return ""
	}
	label := parts[len(parts)-1]
	if label == "" && len(parts) > 1 {
		label = parts[len(parts)-2]
	}
	return titleize(label)
}

func operationFromStep(stepID string, totals *MetricsTotals) string {
	if strings.HasSuffix(strings.ToLower(stepID), "-mixed") {
		return "MIXED"
	}
	if totals != nil && len(totals.Rows) == 1 {
		op := normalizeReportOperation(totals.Rows[0].Operation)
		if op != "" {
			return op
		}
	}
	if stepID != "" {
		parts := strings.Split(stepID, "-")
		if len(parts) > 0 {
			op := parts[len(parts)-1]
			if op != "" {
				return normalizeReportOperation(op)
			}
		}
	}
	if totals != nil && len(totals.Rows) > 0 {
		return normalizeReportOperation(totals.Rows[0].Operation)
	}
	return ""
}

func parseSizeString(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, nil
	}
	upper := strings.ToUpper(trimmed)
	idx := strings.IndexFunc(upper, func(r rune) bool {
		return (r < '0' || r > '9') && r != '.'
	})
	var numPart, unitPart string
	if idx == -1 {
		numPart = upper
	} else {
		numPart = strings.TrimSpace(upper[:idx])
		unitPart = strings.TrimSpace(upper[idx:])
	}
	if numPart == "" {
		return 0, fmt.Errorf("size value %q missing numeric component", value)
	}
	f, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size %q: %w", value, err)
	}
	multiplier := sizeUnitMultiplier(unitPart)
	if multiplier == 0 {
		return 0, fmt.Errorf("unknown size unit %q", unitPart)
	}
	bytes := f * multiplier
	if math.IsNaN(bytes) || math.IsInf(bytes, 0) {
		return 0, fmt.Errorf("invalid size value %q", value)
	}
	return int64(bytes + 0.5), nil
}

func sizeUnitMultiplier(unit string) float64 {
	switch unit {
	case "", "B":
		return 1
	case "K", "KB", "KIB":
		return bytesInKB
	case "M", "MB", "MIB":
		return bytesInMB
	case "G", "GB", "GIB":
		return bytesInGB
	case "T", "TB", "TIB":
		return bytesInGB * 1024
	default:
		return 0
	}
}

func bytesToMB(bytes int64) float64 {
	if bytes == 0 {
		return 0
	}
	return float64(bytes) / float64(bytesInMB)
}

func bytesToGiB(bytes int64) float64 {
	if bytes == 0 {
		return 0
	}
	return float64(bytes) / float64(bytesInGB)
}

func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}
	abs := math.Abs(float64(bytes))
	switch {
	case abs >= float64(bytesInGB):
		return fmt.Sprintf("%.2f GiB", float64(bytes)/float64(bytesInGB))
	case abs >= float64(bytesInMB):
		return fmt.Sprintf("%.2f MiB", float64(bytes)/float64(bytesInMB))
	case abs >= float64(bytesInKB):
		return fmt.Sprintf("%.2f KiB", float64(bytes)/float64(bytesInKB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func formatSeconds(seconds float64) string {
	if seconds <= 0 {
		return "0s"
	}
	if seconds < 1 {
		return fmt.Sprintf("%dms", int(math.Round(seconds*1000)))
	}
	if seconds < 60 {
		return formatFloat(seconds, 2) + "s"
	}
	minutes := int(seconds / 60)
	remaining := seconds - float64(minutes*60)
	if minutes < 60 {
		if remaining < 0.01 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm %ss", minutes, formatFloat(remaining, 2))
	}
	hours := minutes / 60
	minutes = minutes % 60
	if remaining < 0.01 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dh %dm %ss", hours, minutes, formatFloat(remaining, 2))
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func titleize(value string) string {
	if value == "" {
		return ""
	}
	lower := strings.ToLower(value)
	return strings.ToUpper(lower[:1]) + lower[1:]
}

func formatFloat(value float64, precision int) string {
	format := fmt.Sprintf("%%.%df", precision)
	str := fmt.Sprintf(format, value)
	if strings.Contains(str, ".") {
		str = strings.TrimRight(str, "0")
		str = strings.TrimRight(str, ".")
	}
	if str == "" {
		return "0"
	}
	return str
}
