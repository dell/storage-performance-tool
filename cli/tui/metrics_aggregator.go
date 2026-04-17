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

const aggregateLabel = "aggregate"

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

	nodeCount := len(nodeMetrics)
	nodesPresent := make([]string, 0, nodeCount)
	for nodeID := range nodeMetrics {
		nodesPresent = append(nodesPresent, nodeID)
	}
	sort.Strings(nodesPresent)

	ma.warnOnAggregateSamples(nodeMetrics, nodeCount)

	result := ma.sumWorkerMetrics(nodeMetrics, nodesPresent)
	if result != nil {
		logging.LogDebug("metrics-aggregator", "summed node metrics",
			"success", result.SuccessCount,
			"failed", result.FailedCount,
			"ops_per_sec", result.OpsPerSec,
			"nodes", len(nodesPresent))
	}
	return result
}

func (ma *MetricsAggregator) sumWorkerMetrics(nodeMetrics map[string]*PerformanceMetric, nodesPresent []string) *PerformanceMetric {
	var result PerformanceMetric
	result.Scope = aggregateLabel
	result.Role = aggregateLabel
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

func (ma *MetricsAggregator) warnOnAggregateSamples(nodeMetrics map[string]*PerformanceMetric, nodeCount int) {
	if ma.entryNodeID != "" {
		if metric, ok := nodeMetrics[ma.entryNodeID]; ok && looksAggregate(metric, nodeCount) {
			logging.LogWarn("metrics-aggregator", "unexpected aggregate metrics detected on entry node",
				"node", ma.entryNodeID,
				"success", metric.SuccessCount,
				"failed", metric.FailedCount,
				"nodes_count", metric.NodesCount,
				"nodes_present", len(metric.NodesPresent))
		}
	}
	for nodeID, metric := range nodeMetrics {
		if nodeID == ma.entryNodeID {
			continue
		}
		if looksAggregate(metric, nodeCount) {
			logging.LogWarn("metrics-aggregator", "unexpected aggregate metrics detected on worker node",
				"node", nodeID,
				"success", metric.SuccessCount,
				"failed", metric.FailedCount,
				"nodes_count", metric.NodesCount,
				"nodes_present", len(metric.NodesPresent))
		}
	}
}

func looksAggregate(metric *PerformanceMetric, expectedNodes int) bool {
	if metric == nil {
		return false
	}
	switch {
	case strings.EqualFold(metric.Role, aggregateLabel):
		return true
	case expectedNodes > 1 && metric.NodesCount >= expectedNodes && metric.NodesCount > 1:
		return true
	case expectedNodes > 1 && len(metric.NodesPresent) >= expectedNodes && len(metric.NodesPresent) > 1:
		return true
	default:
		return false
	}
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

// AggregateByOpType groups a slice of metrics by OpType and produces a combined
// aggregate plus a per-op-type map. For non-mixed workloads (single metric) the
// aggregate is the metric itself and the map has one entry. For mixed workloads
// the aggregate sums additive fields (ops/s, MB/s, counts) across op types and
// uses ops-weighted averages for latency/duration.
func AggregateByOpType(metrics []*PerformanceMetric) (combined *PerformanceMetric, perOp map[string]*PerformanceMetric) {
	if len(metrics) == 0 {
		return nil, nil
	}
	if len(metrics) == 1 {
		m := metrics[0]
		return m, map[string]*PerformanceMetric{m.OpType: m}
	}

	perOp = make(map[string]*PerformanceMetric, len(metrics))
	for _, m := range metrics {
		perOp[m.OpType] = m
	}

	// Build combined aggregate by summing additive fields.
	var agg PerformanceMetric
	var totalLatencyWeight, totalDurationWeight int64
	first := metrics[0]

	// Copy identity fields from the first (latest) metric.
	agg.StepID = first.StepID
	agg.OpType = "MIXED"
	agg.Scope = first.Scope
	agg.Role = first.Role
	agg.RunID = first.RunID
	agg.ClusterID = first.ClusterID
	agg.NodeID = first.NodeID
	agg.MetricsSchema = first.MetricsSchema
	agg.NodesCount = first.NodesCount
	agg.NodesPresent = append([]string(nil), first.NodesPresent...)
	agg.Timestamp = first.Timestamp
	agg.SampleTimestamp = first.SampleTimestamp
	agg.SampleTimestampRaw = first.SampleTimestampRaw
	agg.SptTimestamp = first.SptTimestamp

	for _, m := range metrics {
		agg.OpsPerSec += m.OpsPerSec
		agg.MBPerSec += m.MBPerSec
		agg.SuccessCount += m.SuccessCount
		agg.FailedCount += m.FailedCount
		agg.ConcurrencyCurrent += m.ConcurrencyCurrent
		agg.ConcurrencyMean += m.ConcurrencyMean

		w := m.OpsPerSec
		if w <= 0 {
			w = 1
		}
		agg.MeanLatency += m.MeanLatency * w
		agg.MeanDuration += m.MeanDuration * w
		totalLatencyWeight += w
		totalDurationWeight += w

		if m.TestState > agg.TestState {
			agg.TestState = m.TestState
		}
		if m.StepTime > agg.StepTime {
			agg.StepTime = m.StepTime
		}
		if m.HasLimit {
			agg.HasLimit = true
			agg.LimitType = m.LimitType
			agg.LimitTimeSec = m.LimitTimeSec
		}
	}

	if totalLatencyWeight > 0 {
		agg.MeanLatency /= totalLatencyWeight
		agg.MeanDuration /= totalDurationWeight
	}

	// Completion: use the first metric's values (all ops in a mixed step
	// share the same time limit and overall completion).
	agg.CompletionPercent = first.CompletionPercent
	agg.OverallCompletionPercent = first.OverallCompletionPercent
	agg.Unbounded = first.Unbounded
	agg.OverallUnbounded = first.OverallUnbounded

	return &agg, perOp
}
