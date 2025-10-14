/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"sort"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// MetricsAggregator produces client-side totals from per-node metrics.
type MetricsAggregator struct{}

// NewMetricsAggregator creates a new metrics aggregator.
func NewMetricsAggregator() *MetricsAggregator {
	return &MetricsAggregator{}
}

// Aggregate combines node-scoped metrics into a single aggregate view.
// Returns nil if no node-scoped samples are available.
func (ma *MetricsAggregator) Aggregate(nodeMetrics map[string]*PerformanceMetric) *PerformanceMetric {
	if len(nodeMetrics) == 0 {
		return nil
	}

	filtered := make(map[string]*PerformanceMetric)
	for nodeID, metric := range nodeMetrics {
		if metric == nil {
			continue
		}
		if strings.EqualFold(metric.Scope, "node") && metric.MetricsSchema >= 2 {
			filtered[nodeID] = metric
		}
	}

	if len(filtered) == 0 {
		logging.LogWarn("metrics-aggregator", "no node-scoped metrics to aggregate", "original_count", len(nodeMetrics))
		return nil
	}

	logging.LogDebug("metrics-aggregator", "aggregating node metrics", "node_count", len(filtered))
	return ma.aggregateNodeMetrics(filtered)
}

func (ma *MetricsAggregator) aggregateNodeMetrics(nodeMetrics map[string]*PerformanceMetric) *PerformanceMetric {
	var base *PerformanceMetric
	for _, metric := range nodeMetrics {
		base = metric
		break
	}
	if base == nil {
		return nil
	}

	nodesPresent := make([]string, 0, len(nodeMetrics))
	var (
		totalOpsPerSec          int64
		totalMBPerSec           int64
		totalSuccess            int64
		totalFailed             int64
		totalLatencyOps         int64
		totalDurationOps        int64
		weightedLatency         int64
		weightedDuration        int64
		totalConcurrencyCurrent int64
		totalConcurrencyMean    float64
		allUnbounded            = true
		allOverallUnbounded     = true
		completionSum           float64
		completionWeight        float64
		overallCompletionSum    float64
		overallCompletionCount  int
		latestSample            time.Time
		latestTimestamp         time.Time
		limitType               string
		limitMixed              bool
		totalLimitOps           int64
		maxLimitTime            int64
		limitPresent            bool
		maxTestState            int
	)

	for nodeID, metric := range nodeMetrics {
		nodesPresent = append(nodesPresent, nodeID)

		totalOpsPerSec += metric.OpsPerSec
		totalMBPerSec += metric.MBPerSec
		totalSuccess += metric.SuccessCount
		totalFailed += metric.FailedCount

		opsWeight := metric.OpsPerSec
		if opsWeight <= 0 {
			opsWeight = 1
		}
		weightedLatency += metric.MeanLatency * opsWeight
		weightedDuration += metric.MeanDuration * opsWeight
		totalLatencyOps += opsWeight
		totalDurationOps += opsWeight

		totalConcurrencyCurrent += metric.ConcurrencyCurrent
		totalConcurrencyMean += metric.ConcurrencyMean

		if !metric.Unbounded {
			allUnbounded = false
			completionSum += metric.CompletionPercent * float64(metric.OpsPerSec)
			completionWeight += float64(metric.OpsPerSec)
		}
		if !metric.OverallUnbounded {
			allOverallUnbounded = false
		}
		overallCompletionSum += metric.OverallCompletionPercent
		overallCompletionCount++

		if metric.SampleTimestamp.After(latestSample) {
			latestSample = metric.SampleTimestamp
		}
		if metric.Timestamp.After(latestTimestamp) {
			latestTimestamp = metric.Timestamp
		}

		if metric.HasLimit {
			limitPresent = true
			if limitType == "" {
				limitType = metric.LimitType
			} else if limitType != metric.LimitType {
				limitMixed = true
			}
			if metric.LimitType == "op_count" {
				totalLimitOps += metric.LimitOpCount
			}
			if metric.LimitType == "time" && metric.LimitTimeSec > maxLimitTime {
				maxLimitTime = metric.LimitTimeSec
			}
		}

		if metric.TestState > maxTestState {
			maxTestState = metric.TestState
		}
	}

	sort.Strings(nodesPresent)

	aggregated := &PerformanceMetric{
		MetricsSchema: base.MetricsSchema,
		Scope:         "aggregate",
		Role:          "aggregate",
		ClusterID:     base.ClusterID,
		RunID:         base.RunID,
		StepID:        base.StepID,
		OpType:        base.OpType,
		TestState:     maxTestState,
		SptTimestamp:  base.SptTimestamp,
		NodesCount:    len(nodesPresent),
		NodesPresent:  nodesPresent,
	}

	aggregated.OpsPerSec = totalOpsPerSec
	aggregated.MBPerSec = totalMBPerSec
	aggregated.SuccessCount = totalSuccess
	aggregated.FailedCount = totalFailed

	if totalLatencyOps > 0 {
		aggregated.MeanLatency = weightedLatency / totalLatencyOps
	}
	if totalDurationOps > 0 {
		aggregated.MeanDuration = weightedDuration / totalDurationOps
	}

	aggregated.ConcurrencyCurrent = totalConcurrencyCurrent
	aggregated.ConcurrencyMean = totalConcurrencyMean

	aggregated.Unbounded = allUnbounded
	aggregated.OverallUnbounded = allOverallUnbounded

	if completionWeight > 0 {
		aggregated.CompletionPercent = completionSum / completionWeight
	}
	if overallCompletionCount > 0 {
		aggregated.OverallCompletionPercent = overallCompletionSum / float64(overallCompletionCount)
	}

	if !limitMixed && limitPresent {
		aggregated.HasLimit = true
		aggregated.LimitType = limitType
		switch limitType {
		case "op_count":
			aggregated.LimitOpCount = totalLimitOps
			if totalLimitOps > 0 {
				totalWork := aggregated.SuccessCount + aggregated.FailedCount
				aggregated.CompletionPercent = (float64(totalWork) / float64(totalLimitOps)) * 100
			}
			aggregated.Unbounded = false
		case "time":
			aggregated.LimitTimeSec = maxLimitTime
		}
	}

	aggregated.CompletionPercent = clampPercentage(aggregated.CompletionPercent)
	aggregated.OverallCompletionPercent = clampPercentage(aggregated.OverallCompletionPercent)

	if latestSample.IsZero() {
		latestSample = time.Now()
	}
	aggregated.SampleTimestamp = latestSample
	aggregated.SampleTimestampRaw = latestSample.Format(time.RFC3339Nano)
	if latestTimestamp.IsZero() {
		latestTimestamp = latestSample
	}
	aggregated.Timestamp = latestTimestamp

	return aggregated
}

func clampPercentage(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}
