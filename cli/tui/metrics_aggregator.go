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
type MetricsAggregator struct {
	entryNodeID string
}

// NewMetricsAggregator creates a new metrics aggregator.
func NewMetricsAggregator() *MetricsAggregator {
	return &MetricsAggregator{}
}

// NewMetricsAggregatorWithEntry creates a metrics aggregator that treats the
// provided node identifier as the aggregate (entry) source.
func NewMetricsAggregatorWithEntry(entryNodeID string) *MetricsAggregator {
	return &MetricsAggregator{entryNodeID: entryNodeID}
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
	if len(nodeMetrics) == 0 {
		return nil
	}

	nodesPresent := make([]string, 0, len(nodeMetrics))
	for nodeID := range nodeMetrics {
		nodesPresent = append(nodesPresent, nodeID)
	}
	sort.Strings(nodesPresent)

	if aggregateMetric := ma.selectAggregateMetric(nodeMetrics, nodesPresent); aggregateMetric != nil {
		clone := *aggregateMetric
		clone.Scope = "aggregate"
		clone.Role = "aggregate"
		clone.NodesCount = aggregateMetric.NodesCount
		if clone.NodesCount == 0 {
			clone.NodesCount = len(nodesPresent)
		}
		if len(aggregateMetric.NodesPresent) > 0 {
			clone.NodesPresent = append([]string(nil), aggregateMetric.NodesPresent...)
		} else {
			clone.NodesPresent = append([]string(nil), nodesPresent...)
		}
		if clone.SampleTimestamp.IsZero() {
			clone.SampleTimestamp = time.Now()
			clone.SampleTimestampRaw = clone.SampleTimestamp.Format(time.RFC3339Nano)
		}
		if clone.Timestamp.IsZero() {
			clone.Timestamp = clone.SampleTimestamp
		}
		return &clone
	}

	return ma.sumWorkerMetrics(nodeMetrics, nodesPresent)
}

func (ma *MetricsAggregator) selectAggregateMetric(nodeMetrics map[string]*PerformanceMetric, nodesPresent []string) *PerformanceMetric {
	if ma.entryNodeID != "" {
		if metric, ok := nodeMetrics[ma.entryNodeID]; ok && metric != nil {
			return metric
		}
	}

	var candidate *PerformanceMetric
	for _, metric := range nodeMetrics {
		if metric == nil {
			continue
		}
		if strings.EqualFold(metric.Role, "entry") || metric.NodesCount > 0 || len(metric.NodesPresent) > 0 {
			if candidate == nil || metric.Timestamp.After(candidate.Timestamp) || (metric.Timestamp.Equal(candidate.Timestamp) && metric.SampleTimestamp.After(candidate.SampleTimestamp)) {
				candidate = metric
			}
		}
	}
	return candidate
}

func (ma *MetricsAggregator) sumWorkerMetrics(nodeMetrics map[string]*PerformanceMetric, nodesPresent []string) *PerformanceMetric {
	var result PerformanceMetric
	result.Scope = "aggregate"
	result.Role = "aggregate"
	result.NodesCount = len(nodesPresent)
	result.NodesPresent = append([]string(nil), nodesPresent...)

	var (
		totalLatencyWeight   int64
		totalDurationWeight  int64
		completionSum        float64
		completionWeight     float64
		overallCompletionSum float64
		overallCount         int
		allUnbounded         = true
		allOverallUnbounded  = true
		latestSample         time.Time
		latestTimestamp      time.Time
		limitPresent         bool
		limitMixed           bool
		limitType            string
		totalLimitOps        int64
		maxLimitTime         int64
	)

	for _, metric := range nodeMetrics {
		if metric == nil {
			continue
		}

		result.OpsPerSec += metric.OpsPerSec
		result.MBPerSec += metric.MBPerSec
		result.SuccessCount += metric.SuccessCount
		result.FailedCount += metric.FailedCount
		result.ConcurrencyCurrent += metric.ConcurrencyCurrent
		result.ConcurrencyMean += metric.ConcurrencyMean

		opsWeight := metric.OpsPerSec
		if opsWeight <= 0 {
			opsWeight = 1
		}
		result.MeanLatency += metric.MeanLatency * opsWeight
		result.MeanDuration += metric.MeanDuration * opsWeight
		totalLatencyWeight += opsWeight
		totalDurationWeight += opsWeight

		if !metric.Unbounded {
			allUnbounded = false
			completionSum += metric.CompletionPercent * float64(metric.OpsPerSec)
			completionWeight += float64(metric.OpsPerSec)
		}
		if !metric.OverallUnbounded {
			allOverallUnbounded = false
		}
		overallCompletionSum += metric.OverallCompletionPercent
		overallCount++

		if metric.SampleTimestamp.After(latestSample) {
			latestSample = metric.SampleTimestamp
		}
		if metric.Timestamp.After(latestTimestamp) {
			latestTimestamp = metric.Timestamp
		}

		if result.StepID == "" {
			result.StepID = metric.StepID
		}
		if result.OpType == "" {
			result.OpType = metric.OpType
		}
		if metric.TestState > result.TestState {
			result.TestState = metric.TestState
		}
		if result.RunID == "" {
			result.RunID = metric.RunID
		}
		if result.ClusterID == "" {
			result.ClusterID = metric.ClusterID
		}
		if result.SptTimestamp == "" {
			result.SptTimestamp = metric.SptTimestamp
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
	}

	if totalLatencyWeight > 0 {
		result.MeanLatency /= totalLatencyWeight
		result.MeanDuration /= totalDurationWeight
	}

	if completionWeight > 0 {
		result.CompletionPercent = completionSum / completionWeight
	}
	if overallCount > 0 {
		result.OverallCompletionPercent = overallCompletionSum / float64(overallCount)
	}
	result.Unbounded = allUnbounded
	result.OverallUnbounded = allOverallUnbounded

	if !limitMixed && limitPresent {
		result.HasLimit = true
		result.LimitType = limitType
		switch limitType {
		case "op_count":
			result.LimitOpCount = totalLimitOps
			if totalLimitOps > 0 {
				totalWork := result.SuccessCount + result.FailedCount
				result.CompletionPercent = (float64(totalWork) / float64(totalLimitOps)) * 100
			}
			result.Unbounded = false
		case "time":
			result.LimitTimeSec = maxLimitTime
		}
	}

	result.CompletionPercent = clampPercentage(result.CompletionPercent)
	result.OverallCompletionPercent = clampPercentage(result.OverallCompletionPercent)

	if latestSample.IsZero() {
		latestSample = time.Now()
	}
	result.SampleTimestamp = latestSample
	result.SampleTimestampRaw = latestSample.Format(time.RFC3339Nano)
	if latestTimestamp.IsZero() {
		latestTimestamp = latestSample
	}
	result.Timestamp = latestTimestamp

	return &result
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
