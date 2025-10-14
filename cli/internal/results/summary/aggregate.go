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
}

// StepSummary aggregates per-step metrics and artifact health.
type StepSummary struct {
	Ordinal         int
	StepID          string
	PhaseLabel      string
	Operation       string
	Status          StepStatus
	Metrics         *PhaseMetrics
	MissingRequired []string
	MissingOptional []string
	Notes           []string
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
	LatencyMeanMs     float64
	LatencyMedianMs   float64
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

	steps, totals, stepWarnings := buildStepSummaries(data, workload.ObjectSizeBytes)
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
	}
	if isList {
		summary.ObjectSizeHuman = ""
	}
	return summary, warnings
}

func buildStepSummaries(data *RunData, objectSizeBytes int64) ([]StepSummary, RunTotals, []string) {
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

		metrics := deriveMetrics(stepData, objectSizeBytes)
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
		LatencyMeanMs:     row.LatencyAvgMicros / 1000.0,
		LatencyMedianMs:   row.LatencyP50Micros / 1000.0,
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
	if stepID != "" {
		parts := strings.Split(stepID, "-")
		if len(parts) > 0 {
			op := parts[len(parts)-1]
			if op != "" {
				return strings.ToUpper(op)
			}
		}
	}
	if totals != nil && len(totals.Rows) > 0 {
		return strings.ToUpper(strings.TrimSpace(totals.Rows[0].Operation))
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
