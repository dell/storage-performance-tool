package summary

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	workloadreg "github.com/dell/storage-performance-tool/cli/internal/workload"
)

// RunSummary represents the aggregated data set used when rendering result summaries.
type RunSummary struct {
	RunID             string
	GeneratedAt       time.Time
	ManifestPath      string
	MetadataPath      string
	Environment       EnvironmentSummary
	Workload          WorkloadSummary
	Integrity         *results.IntegritySummary
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
	Type              string
	ObjectSizeBytes   int64
	ObjectSizeMiB     float64
	ObjectSizeGiB     float64
	ObjectSizeHuman   string
	ObjectCount       int64
	Threads           int
	Endpoints         []string
	Bucket            string
	Prefix            string
	DurationRequest   string
	CleanupEnabled    bool
	KeepScenario      bool
	SliceEndpoints    bool
	MixedDistribution MixedDistribution
}

// StepSummary aggregates per-step metrics and artifact health.
type StepSummary struct {
	Ordinal            int
	StepID             string
	PhaseLabel         string
	Operation          string
	Status             StepStatus
	Metrics            *PhaseMetrics
	IsMixed            bool
	OperationBreakdown []OperationBreakdown
	MixedLatencyNote   string
	MissingRequired    []string
	MissingOptional    []string
	Notes              []string
	Delete             *deletemetrics.Metrics
	DeleteEvidence     *DeleteArtifactEvidence
}

// MixedDistribution stores the configured mixed-workload share for each known operation.
type MixedDistribution struct {
	Available     bool
	ReadPercent   int
	StatPercent   int
	CreatePercent int
	DeletePercent int
}

// OperationBreakdown holds one operation-specific row within a mixed step summary.
type OperationBreakdown struct {
	Operation       string
	ConfiguredShare *float64
	ActualShare     *float64
	ActualOps       int64
	Metrics         PhaseMetrics
}

// PhaseMetrics holds derived statistics for a single run phase.
type PhaseMetrics struct {
	SuccessCount       int64
	FailureCount       int64
	CorruptCount       int64
	HasCorruptCount    bool
	DataBytes          int64
	DataMiB            float64
	DataGiB            float64
	HasDataTransfer    bool
	DurationSeconds    float64
	DurationHuman      string
	ThroughputAvgOps   float64
	ThroughputLastOps  float64
	BandwidthAvgMiBps  float64
	BandwidthLastMiBps float64
	LatencyHeadlineMs  float64
	LatencyMedianMs    float64
	LatencyP90Ms       float64
	LatencyP99Ms       float64
	LatencyP999Ms      float64
	TTFBMedianMs       float64
	TTFBP90Ms          float64
	TTFBP99Ms          float64
	TTFBP999Ms         float64
	ObjectSizeBytes    int64
	ObjectSizeMiB      float64
	ObjectSizeGiB      float64
	ObjectSizeHuman    string
	Concurrency        float64
	ConcurrencyMean    float64
	NodeCount          int64
	SampleTimestamp    string
}

// RunTotals aggregates run-wide duration and data transfer metrics.
type RunTotals struct {
	DurationSeconds float64
	DurationHuman   string
	DataBytes       int64
	DataMiB         float64
	DataGiB         float64
}

const (
	bytesInKiB            = constants.BytesPerKiB
	bytesInMiB            = constants.BytesPerMiB
	bytesInGiB            = constants.BytesPerGiB
	reportOperationRead   = "READ"
	reportOperationStat   = "STAT"
	reportOperationCreate = "CREATE"
	reportOperationDelete = "DELETE"
)

var mixedOperationOrder = []string{
	reportOperationRead,
	reportOperationStat,
	reportOperationCreate,
	reportOperationDelete,
}

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
	if data.Manifest != nil {
		summary.Integrity = data.Manifest.Integrity
	}

	summary.Environment = buildEnvironmentSummary(data)
	workload, workloadWarnings := buildWorkloadSummary(data)
	summary.Workload = workload
	summary.Warnings = append(summary.Warnings, workloadWarnings...)

	steps, totals, stepWarnings := buildStepSummaries(data, workload, summary.Integrity)
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
	workloadTypeList        = workloadreg.List
	workloadTypeReadVerify  = workloadreg.ReadVerify
	objectSizeAverageSuffix = " avg"
)

func buildWorkloadSummary(data *RunData) (WorkloadSummary, []string) {
	params := data.Params.ScenarioParams
	warnings := make([]string, 0, 1)
	sizeBytes, sizeWarn := parseSizeString(params.ObjectSize)
	if sizeWarn != nil {
		warnings = append(warnings, fmt.Sprintf("object size parse error: %v", sizeWarn))
	}
	workloadType := strings.ToLower(strings.TrimSpace(data.Params.WorkloadType))
	if workloadType == "" {
		workloadType = strings.ToLower(strings.TrimSpace(params.WorkloadType))
	}
	isList := strings.EqualFold(workloadType, workloadreg.List)
	objectSizeHuman := ""
	if strings.TrimSpace(params.ObjectSize) != "" {
		objectSizeHuman = formatBytes(sizeBytes)
	}
	summary := WorkloadSummary{
		Type:              workloadType,
		ObjectSizeBytes:   sizeBytes,
		ObjectSizeMiB:     bytesToMiB(sizeBytes),
		ObjectSizeGiB:     bytesToGiB(sizeBytes),
		ObjectSizeHuman:   objectSizeHuman,
		ObjectCount:       params.ObjectCount,
		Threads:           params.Threads,
		Endpoints:         append([]string(nil), params.Endpoints...),
		Bucket:            params.Bucket,
		Prefix:            params.Prefix,
		DurationRequest:   params.Duration,
		CleanupEnabled:    params.Cleanup,
		KeepScenario:      params.KeepScenario,
		SliceEndpoints:    params.SliceEndpoints,
		MixedDistribution: mixedDistributionFromParams(workloadType, params),
	}
	if isList {
		summary.ObjectSizeHuman = ""
	}
	return summary, warnings
}

func buildStepSummaries(data *RunData, workload WorkloadSummary, integrity *results.IntegritySummary) ([]StepSummary, RunTotals, []string) {
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
			Delete:          stepData.Delete,
			DeleteEvidence:  stepData.DeleteEvidence,
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
		if metrics != nil && strings.EqualFold(summary.Operation, workloadreg.List) {
			normalizeListMetrics(metrics, integrity)
		}
		if metrics != nil {
			summary.Metrics = metrics
			totals.DurationSeconds += metrics.DurationSeconds
			totals.DataBytes += metrics.DataBytes
		} else if !stepData.MetricsSuppressed {
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
	appendSeededDeleteCleanupPhase(steps, workload)

	totals.DurationHuman = formatSeconds(totals.DurationSeconds)
	totals.DataMiB = bytesToMiB(totals.DataBytes)
	totals.DataGiB = bytesToGiB(totals.DataBytes)
	return steps, totals, warnings
}

func appendSeededDeleteCleanupPhase(steps []StepSummary, workload WorkloadSummary) {
	if !workload.CleanupEnabled || !strings.EqualFold(workload.Type, workloadreg.Delete) {
		return
	}
	deleteIndex := -1
	cleanupSeconds := 0.0
	cleanupFound := false
	for i := range steps {
		if steps[i].Delete != nil {
			deleteIndex = i
		}
		if scenario.IsSeededDeleteCleanupStepID(steps[i].StepID) &&
			steps[i].Metrics != nil {
			cleanupSeconds = steps[i].Metrics.DurationSeconds
			cleanupFound = true
		}
	}
	if deleteIndex < 0 || !cleanupFound || steps[deleteIndex].Delete.Phases.CleanupSeconds != nil {
		return
	}

	deleteMetrics := *steps[deleteIndex].Delete
	phases := deleteMetrics.Phases
	phases.CleanupSeconds = &cleanupSeconds
	if phases.TotalWallSeconds != nil {
		totalWallSeconds := *phases.TotalWallSeconds + cleanupSeconds
		phases.TotalWallSeconds = &totalWallSeconds
	}
	deleteMetrics.Phases = phases
	steps[deleteIndex].Delete = &deleteMetrics
}

func mixedDistributionFromParams(workloadType string, params ScenarioParams) MixedDistribution {
	if !strings.EqualFold(workloadType, workloadreg.Mixed) && !strings.EqualFold(params.WorkloadType, workloadreg.Mixed) {
		return MixedDistribution{}
	}
	if params.GetDistrib == 0 && params.StatDistrib == 0 && params.PutDistrib == 0 && params.DeleteDistrib == 0 {
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
	seen, allKnown := mixedOperationSet(totals.Rows)
	if len(seen) < 2 {
		return false
	}
	if strings.HasSuffix(strings.ToLower(stepID), "-mixed") {
		return true
	}
	if strings.EqualFold(workload.Type, workloadreg.Mixed) {
		return true
	}
	return allKnown
}

func deriveMixedMetrics(stepData *StepData, workload WorkloadSummary, objectSizeBytes int64) (*PhaseMetrics, []OperationBreakdown, []string) {
	if stepData == nil || stepData.Metrics == nil || len(stepData.Metrics.Rows) == 0 {
		return nil, nil, nil
	}
	groups := make(map[string][]MetricsTotalsRow, len(stepData.Metrics.Rows))
	for _, row := range stepData.Metrics.Rows {
		op := normalizeReportOperation(row.Operation)
		if op == "" {
			continue
		}
		groups[op] = append(groups[op], row)
	}
	if len(groups) == 0 {
		return nil, nil, nil
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
		metrics.BandwidthAvgMiBps += opMetrics.BandwidthAvgMiBps
		metrics.BandwidthLastMiBps += opMetrics.BandwidthLastMiBps
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
	metrics.DataMiB = bytesToMiB(metrics.DataBytes)
	metrics.DataGiB = bytesToGiB(metrics.DataBytes)
	metrics.HasDataTransfer = metrics.DataBytes > 0
	if objectSizeBytes > 0 {
		metrics.ObjectSizeBytes = objectSizeBytes
		metrics.ObjectSizeMiB = bytesToMiB(objectSizeBytes)
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
	metrics := PhaseMetrics{HasCorruptCount: true}
	for _, row := range rows {
		metrics.SuccessCount += row.SuccessCount
		metrics.FailureCount += row.FailureCount
		metrics.CorruptCount += row.CorruptCount
		metrics.HasCorruptCount = metrics.HasCorruptCount && row.HasCorruptCount
		metrics.DataBytes += row.SizeBytes
		metrics.ThroughputAvgOps += row.ThroughputAvgOps
		metrics.ThroughputLastOps += row.ThroughputLastOps
		metrics.BandwidthAvgMiBps += row.BandwidthAvgMiBps
		metrics.BandwidthLastMiBps += row.BandwidthLastMiBps
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
	metrics.DataMiB = bytesToMiB(metrics.DataBytes)
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
		metrics.ObjectSizeMiB = bytesToMiB(objectSizeBytes)
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
	case reportOperationRead:
		value = float64(dist.ReadPercent)
	case reportOperationStat:
		value = float64(dist.StatPercent)
	case reportOperationCreate:
		value = float64(dist.CreatePercent)
	case reportOperationDelete:
		value = float64(dist.DeletePercent)
	default:
		return nil
	}
	return &value
}

func orderedOperationKeys(groups map[string][]MetricsTotalsRow) []string {
	ordered := make([]string, 0, len(groups))
	for _, op := range mixedOperationOrder {
		if _, ok := groups[op]; ok {
			ordered = append(ordered, op)
		}
	}
	unknown := make([]string, 0, len(groups))
	for op := range groups {
		if !isKnownMixedOperation(op) {
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
	case "GET", reportOperationRead:
		return reportOperationRead
	case "HEAD", reportOperationStat:
		return reportOperationStat
	case "PUT", reportOperationCreate, "WRITE":
		return reportOperationCreate
	case reportOperationDelete:
		return reportOperationDelete
	default:
		return strings.ToUpper(strings.TrimSpace(op))
	}
}

func mixedOperationSet(rows []MetricsTotalsRow) (map[string]struct{}, bool) {
	seen := make(map[string]struct{}, len(rows))
	allKnown := true
	for _, row := range rows {
		op := normalizeReportOperation(row.Operation)
		if op == "" {
			continue
		}
		seen[op] = struct{}{}
		if !isKnownMixedOperation(op) {
			allKnown = false
		}
	}
	return seen, allKnown && len(seen) > 0
}

func isKnownMixedOperation(op string) bool {
	switch op {
	case reportOperationRead, reportOperationStat, reportOperationCreate, reportOperationDelete:
		return true
	default:
		return false
	}
}

func deriveMetrics(stepData *StepData, objectSizeBytes int64) *PhaseMetrics {
	if stepData == nil || stepData.Metrics == nil || len(stepData.Metrics.Rows) == 0 {
		return nil
	}
	operationHint := operationFromStep(stepData.StepID, stepData.Metrics)
	row := selectMetricsRow(stepData.Metrics, operationHint)

	metrics := &PhaseMetrics{
		SuccessCount:       row.SuccessCount,
		FailureCount:       row.FailureCount,
		CorruptCount:       row.CorruptCount,
		HasCorruptCount:    row.HasCorruptCount,
		DataBytes:          row.SizeBytes,
		DataMiB:            bytesToMiB(row.SizeBytes),
		DataGiB:            bytesToGiB(row.SizeBytes),
		HasDataTransfer:    row.SizeBytes > 0,
		DurationSeconds:    row.StepDurationSeconds,
		DurationHuman:      formatSeconds(row.StepDurationSeconds),
		ThroughputAvgOps:   row.ThroughputAvgOps,
		ThroughputLastOps:  row.ThroughputLastOps,
		BandwidthAvgMiBps:  row.BandwidthAvgMiBps,
		BandwidthLastMiBps: row.BandwidthLastMiBps,
		LatencyHeadlineMs:  preferredLatencyMicros(row) / 1000.0,
		LatencyMedianMs:    row.LatencyP50Micros / 1000.0,
		LatencyP90Ms:       row.LatencyP90Micros / 1000.0,
		LatencyP99Ms:       row.LatencyP99Micros / 1000.0,
		LatencyP999Ms:      row.LatencyP999Micros / 1000.0,
		TTFBMedianMs:       row.TTFBP50Micros / 1000.0,
		TTFBP90Ms:          row.TTFBP90Micros / 1000.0,
		TTFBP99Ms:          row.TTFBP99Micros / 1000.0,
		TTFBP999Ms:         row.TTFBP999Micros / 1000.0,
		Concurrency:        row.Concurrency,
		ConcurrencyMean:    row.ConcurrencyMean,
		NodeCount:          row.NodeCount,
		SampleTimestamp:    row.SampleTimestamp,
	}

	if objectSizeBytes > 0 {
		metrics.ObjectSizeBytes = objectSizeBytes
		metrics.ObjectSizeMiB = bytesToMiB(objectSizeBytes)
		metrics.ObjectSizeGiB = bytesToGiB(objectSizeBytes)
		metrics.ObjectSizeHuman = formatBytes(objectSizeBytes)
	} else if strings.EqualFold(row.Operation, reportOperationRead) && row.SuccessCount > 0 && row.SizeBytes > 0 {
		averageSizeBytes := int64(math.Round(float64(row.SizeBytes) / float64(row.SuccessCount)))
		metrics.ObjectSizeBytes = averageSizeBytes
		metrics.ObjectSizeMiB = bytesToMiB(averageSizeBytes)
		metrics.ObjectSizeGiB = bytesToGiB(averageSizeBytes)
		metrics.ObjectSizeHuman = formatBytes(averageSizeBytes) + objectSizeAverageSuffix
	}
	return metrics
}

// normalizeListMetrics keeps discovery cardinality separate from payload-transfer accounting.
// Engine LIST totals count distributed object emissions; once canonical selection evidence is
// available, the run summary reports the deduplicated, post-cap candidate count instead.
func normalizeListMetrics(metrics *PhaseMetrics, integrity *results.IntegritySummary) {
	metrics.DataBytes = 0
	metrics.DataMiB = 0
	metrics.DataGiB = 0
	metrics.HasDataTransfer = false
	metrics.BandwidthAvgMiBps = 0
	metrics.BandwidthLastMiBps = 0
	if integrity == nil || !integrity.SelectionCountsValid {
		return
	}
	metrics.SuccessCount = integrity.SelectionCount
	metrics.ThroughputAvgOps = 0
	metrics.ThroughputLastOps = 0
	if metrics.DurationSeconds > 0 {
		metrics.ThroughputAvgOps = float64(integrity.SelectionCount) / metrics.DurationSeconds
	}
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
		return float64(bytesInKiB)
	case "M", "MB", "MIB":
		return float64(bytesInMiB)
	case "G", "GB", "GIB":
		return float64(bytesInGiB)
	case "T", "TB", "TIB":
		return float64(constants.BytesPerTiB)
	default:
		return 0
	}
}

func bytesToMiB(bytes int64) float64 {
	if bytes == 0 {
		return 0
	}
	return float64(bytes) / float64(bytesInMiB)
}

func bytesToGiB(bytes int64) float64 {
	if bytes == 0 {
		return 0
	}
	return float64(bytes) / float64(bytesInGiB)
}

func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}
	abs := math.Abs(float64(bytes))
	switch {
	case abs >= float64(constants.BytesPerTiB):
		return fmt.Sprintf("%.2f %s", float64(bytes)/float64(constants.BytesPerTiB), constants.UnitTiB)
	case abs >= float64(bytesInGiB):
		return fmt.Sprintf("%.2f %s", float64(bytes)/float64(bytesInGiB), constants.UnitGiB)
	case abs >= float64(bytesInMiB):
		return fmt.Sprintf("%.2f %s", float64(bytes)/float64(bytesInMiB), constants.UnitMiB)
	case abs >= float64(bytesInKiB):
		return fmt.Sprintf("%.2f %s", float64(bytes)/float64(bytesInKiB), constants.UnitKiB)
	default:
		return fmt.Sprintf("%d %s", bytes, constants.UnitByte)
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
