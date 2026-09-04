/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// MetricsAggregator produces client-side totals from per-node metrics.
type MetricsAggregator struct {
	entryNodeID                string
	incompatibleDeleteWarnOnce sync.Once
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
	deleteMetrics, deleteCompatible, deleteIncomplete := aggregateDeleteMetrics(nodeMetrics, nodesPresent)
	if !deleteCompatible {
		ma.incompatibleDeleteWarnOnce.Do(func() {
			logging.LogWarn("metrics-aggregator", "refusing to merge incompatible DELETE result identities")
		})
		return nil
	}
	var result PerformanceMetric
	result.Scope = aggregateLabel
	result.Role = aggregateLabel
	result.NodesCount = len(nodesPresent)
	result.NodesPresent = append([]string(nil), nodesPresent...)
	result.ContributorsPresent = append([]string(nil), nodesPresent...)
	result.Delete = deleteMetrics
	result.Partial = deleteIncomplete

	var (
		totalLatencyWeight   int64
		totalDurationWeight  int64
		totalTTFBWeight      int64
		metricCount          int
		ttfbMetricCount      int
		firstTTFBOpType      string
		allSameTTFBOpType    = true
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
		allHaveCorruptCount  = true
	)

	for _, metric := range nodeMetrics {
		if metric == nil {
			continue
		}

		metricCount++
		result.Partial = result.Partial || metric.Partial
		result.DeleteDetailExpected = result.DeleteDetailExpected || metric.DeleteDetailExpected
		if firstTTFBOpType == "" {
			firstTTFBOpType = metric.OpType
		} else if !strings.EqualFold(firstTTFBOpType, metric.OpType) {
			allSameTTFBOpType = false
		}
		result.OpsPerSec += metric.OpsPerSec
		result.MiBPerSec += metric.MiBPerSec
		result.SuccessCount += metric.SuccessCount
		result.FailedCount += metric.FailedCount
		if metric.HasCorruptCount {
			result.CorruptCount += metric.CorruptCount
		} else {
			allHaveCorruptCount = false
		}
		result.ConcurrencyCurrent += metric.ConcurrencyCurrent
		result.ConcurrencyMean += metric.ConcurrencyMean

		opsWeight := metric.OpsPerSec
		if opsWeight <= 0 {
			opsWeight = 1
		}
		if timingDisplayAvailable(metric.HasLatency, metric.MeanLatency) {
			result.MeanLatency += metric.MeanLatency * opsWeight
			totalLatencyWeight += opsWeight
		}
		if timingDisplayAvailable(metric.HasDuration, metric.MeanDuration) {
			result.MeanDuration += metric.MeanDuration * opsWeight
			totalDurationWeight += opsWeight
		}
		if metric.HasTTFB {
			result.MeanTTFB += metric.MeanTTFB * opsWeight
			totalTTFBWeight += opsWeight
			ttfbMetricCount++
		}

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
			result.NodeID = metric.NodeID
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
		if metric.MetricsSchema > result.MetricsSchema {
			result.MetricsSchema = metric.MetricsSchema
		}
		if metric.StepTime > result.StepTime {
			result.StepTime = metric.StepTime
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
		result.HasLatency = true
	}
	if totalDurationWeight > 0 {
		result.MeanDuration /= totalDurationWeight
		result.HasDuration = true
	}
	if totalTTFBWeight > 0 && ttfbMetricCount == metricCount && allSameTTFBOpType && isTTFBEligibleOpType(firstTTFBOpType) {
		result.MeanTTFB /= totalTTFBWeight
		result.HasTTFB = true
	} else {
		result.MeanTTFB = 0
		result.HasTTFB = false
	}

	if completionWeight > 0 {
		result.CompletionPercent = completionSum / completionWeight
	}
	if overallCount > 0 {
		result.OverallCompletionPercent = overallCompletionSum / float64(overallCount)
	}
	result.Unbounded = allUnbounded
	result.OverallUnbounded = allOverallUnbounded
	result.HasCorruptCount = metricCount > 0 && allHaveCorruptCount

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

	nodeList := make([]*PerformanceMetric, 0, len(nodeMetrics))
	for _, metric := range nodeMetrics {
		nodeList = append(nodeList, metric)
	}
	applyProgressAndLimitFields(&result, nodeList)

	return &result
}

const maxDeleteBucketMetrics = deletemetrics.MaxBucketMetrics

func aggregateDeleteMetrics(
	nodeMetrics map[string]*PerformanceMetric,
	nodesPresent []string,
) (*DeleteMetrics, bool, bool) {
	var first *DeleteMetrics
	contributors := 0
	incomplete := false
	for _, nodeID := range nodesPresent {
		metric := nodeMetrics[nodeID]
		if metric == nil {
			continue
		}
		if metric.Delete == nil {
			incomplete = incomplete || (metric.MetricsSchema >= deletemetrics.SchemaVersion &&
				metric.DeleteDetailExpected && strings.EqualFold(metric.OpType, "DELETE"))
			continue
		}
		contributors++
		if first == nil {
			first = metric.Delete
			continue
		}
		if first.Identity != metric.Delete.Identity || first.Units != metric.Delete.Units ||
			!compatibleDeleteFailurePolicy(first.FailurePolicy, metric.Delete.FailurePolicy) ||
			first.FailurePolicy.Outcome != metric.Delete.FailurePolicy.Outcome ||
			first.OutcomeTerminology != metric.Delete.OutcomeTerminology ||
			!compatibleDeleteVerification(first.Verification, metric.Delete.Verification) {
			return nil, false, false
		}
	}
	if first == nil {
		return nil, true, incomplete // generic v2/v3 DELETE metrics remain compatible
	}
	if contributors != len(nodesPresent) {
		if incomplete {
			return nil, true, true
		}
		return nil, false, false
	}

	result := &DeleteMetrics{
		Units:         first.Units,
		Identity:      first.Identity,
		FailurePolicy: first.FailurePolicy,
		Timing: DeleteTimingMetrics{
			LatencyDefinition:  first.Timing.LatencyDefinition,
			DurationDefinition: first.Timing.DurationDefinition,
		},
		Performance:        first.Performance,
		OutcomeTerminology: first.OutcomeTerminology,
		Verification:       first.Verification,
		TerminalReconciled: true,
	}
	if contributors == 1 {
		result.Timing = first.Timing
	}
	result.FailurePolicy.OperationalFailedObjects = 0
	result.FailurePolicy.ExcludedFailedObjects = 0
	result.FailurePolicy.ObservedFailurePercent = 0
	resetDeleteVerificationCounters(&result.Verification)
	result.Verification.RemovalConfirmed = true
	buckets := make(map[string]DeleteBucketMetrics)
	for _, nodeID := range nodesPresent {
		item := nodeMetrics[nodeID].Delete
		result.Requests.Attempted += item.Requests.Attempted
		result.Requests.FullSuccess += item.Requests.FullSuccess
		result.Requests.Partial += item.Requests.Partial
		result.Requests.Failed += item.Requests.Failed
		result.Requests.Unresolved += item.Requests.Unresolved
		result.Requests.PerSecond += item.Requests.PerSecond
		result.Objects.Selected += item.Objects.Selected
		result.Objects.Attempted += item.Objects.Attempted
		result.Objects.Accepted += item.Objects.Accepted
		result.Objects.Failed += item.Objects.Failed
		result.Objects.Unattempted += item.Objects.Unattempted
		result.Objects.Unresolved += item.Objects.Unresolved
		result.Objects.PerSecond += item.Objects.PerSecond
		result.Batches.ActualRequestCount += item.Batches.ActualRequestCount
		result.Batches.ActualObjectCount += item.Batches.ActualObjectCount
		result.Batches.FullBatchCount += item.Batches.FullBatchCount
		result.Batches.PartialBatchCount += item.Batches.PartialBatchCount
		result.Versions.CurrentKey += item.Versions.CurrentKey
		result.Versions.ExactVersion += item.Versions.ExactVersion
		result.FailurePolicy.OperationalFailedObjects += item.FailurePolicy.OperationalFailedObjects
		result.FailurePolicy.ExcludedFailedObjects += item.FailurePolicy.ExcludedFailedObjects
		addDeleteVerification(&result.Verification, item.Verification)
		result.Phases.SeedSeconds = maxDeletePhase(result.Phases.SeedSeconds, item.Phases.SeedSeconds)
		result.Phases.DiscoverySeconds = maxDeletePhase(result.Phases.DiscoverySeconds, item.Phases.DiscoverySeconds)
		result.Phases.PreValidationSeconds = maxDeletePhase(result.Phases.PreValidationSeconds, item.Phases.PreValidationSeconds)
		result.Phases.ScheduledDeleteSeconds = maxDeletePhase(result.Phases.ScheduledDeleteSeconds, item.Phases.ScheduledDeleteSeconds)
		result.Phases.DrainSeconds = maxDeletePhase(result.Phases.DrainSeconds, item.Phases.DrainSeconds)
		result.Phases.PostVerificationSeconds = maxDeletePhase(result.Phases.PostVerificationSeconds, item.Phases.PostVerificationSeconds)
		result.Phases.CleanupSeconds = maxDeletePhase(result.Phases.CleanupSeconds, item.Phases.CleanupSeconds)
		result.Phases.TotalWallSeconds = maxDeletePhase(result.Phases.TotalWallSeconds, item.Phases.TotalWallSeconds)
		result.TerminalReconciled = result.TerminalReconciled && item.TerminalReconciled
		for _, bucket := range item.Buckets {
			sum := buckets[bucket.Bucket]
			sum.Bucket = bucket.Bucket
			sum.Selected += bucket.Selected
			sum.Attempted += bucket.Attempted
			sum.Accepted += bucket.Accepted
			sum.Failed += bucket.Failed
			buckets[bucket.Bucket] = sum
		}
	}
	result.Batches.ConfiguredSize = result.Identity.ConfiguredBatchSize
	if result.Batches.ActualRequestCount > 0 {
		result.Batches.MeanObjectsPerRequest = float64(result.Batches.ActualObjectCount) /
			float64(result.Batches.ActualRequestCount)
		result.Batches.FullBatchPercent = float64(result.Batches.FullBatchCount) * 100 /
			float64(result.Batches.ActualRequestCount)
	}
	failureOutcomes := result.Objects.Accepted + result.FailurePolicy.OperationalFailedObjects
	if failureOutcomes > 0 {
		result.FailurePolicy.ObservedFailurePercent =
			float64(result.FailurePolicy.OperationalFailedObjects) * 100 / float64(failureOutcomes)
	}
	terminalRequests := result.Requests.FullSuccess + result.Requests.Partial +
		result.Requests.Failed + result.Requests.Unresolved
	if result.Requests.Attempted > 0 {
		result.Completion.RequestPercent = float64(terminalRequests) * 100 /
			float64(result.Requests.Attempted)
	} else if result.TerminalReconciled {
		result.Completion.RequestPercent = 100
	}
	accountedObjects := result.Objects.Accepted + result.Objects.Failed +
		result.Objects.Unattempted + result.Objects.Unresolved
	if result.Objects.Selected > 0 {
		result.Completion.ObjectPercent = float64(accountedObjects) * 100 /
			float64(result.Objects.Selected)
	}
	result.Completion.TerminalReconciled = result.TerminalReconciled
	result.Buckets = boundedDeleteBuckets(buckets)
	return result, true, false
}

func compatibleDeleteVerification(first, next DeleteVerificationMetrics) bool {
	return first.Enabled == next.Enabled &&
		first.PreValidationEnabled == next.PreValidationEnabled &&
		first.PostVerificationEnabled == next.PostVerificationEnabled &&
		first.PreValidationComplete == next.PreValidationComplete &&
		first.PostVerificationComplete == next.PostVerificationComplete &&
		first.PostVerificationSkipped == next.PostVerificationSkipped &&
		first.TimeoutSeconds == next.TimeoutSeconds && first.Notice == next.Notice
}

func resetDeleteVerificationCounters(verification *DeleteVerificationMetrics) {
	verification.PreValidationFailures = 0
	verification.VerifiedAbsent = 0
	verification.StillPresent = 0
	verification.Unresolved = 0
	verification.AcceptedAbsent = 0
	verification.AcceptedPresent = 0
	verification.AcceptedUnresolved = 0
	verification.FailedAbsent = 0
	verification.FailedPresent = 0
	verification.FailedUnresolved = 0
	verification.OperationalUnresolvedAbsent = 0
	verification.OperationalUnresolvedPresent = 0
	verification.OperationalUnresolvedUnresolved = 0
	verification.UnattemptedAbsent = 0
	verification.UnattemptedPresent = 0
	verification.UnattemptedUnresolved = 0
	verification.CorrectnessFailures = 0
	verification.InconclusiveFailures = 0
	verification.Residual = 0
}

func addDeleteVerification(result *DeleteVerificationMetrics, item DeleteVerificationMetrics) {
	result.PreValidationFailures += item.PreValidationFailures
	result.VerifiedAbsent += item.VerifiedAbsent
	result.StillPresent += item.StillPresent
	result.Unresolved += item.Unresolved
	result.AcceptedAbsent += item.AcceptedAbsent
	result.AcceptedPresent += item.AcceptedPresent
	result.AcceptedUnresolved += item.AcceptedUnresolved
	result.FailedAbsent += item.FailedAbsent
	result.FailedPresent += item.FailedPresent
	result.FailedUnresolved += item.FailedUnresolved
	result.OperationalUnresolvedAbsent += item.OperationalUnresolvedAbsent
	result.OperationalUnresolvedPresent += item.OperationalUnresolvedPresent
	result.OperationalUnresolvedUnresolved += item.OperationalUnresolvedUnresolved
	result.UnattemptedAbsent += item.UnattemptedAbsent
	result.UnattemptedPresent += item.UnattemptedPresent
	result.UnattemptedUnresolved += item.UnattemptedUnresolved
	result.CorrectnessFailures += item.CorrectnessFailures
	result.InconclusiveFailures += item.InconclusiveFailures
	result.Residual += item.Residual
	result.RemovalConfirmed = result.RemovalConfirmed && item.RemovalConfirmed
}

func compatibleDeleteFailurePolicy(left, right DeleteFailurePolicy) bool {
	return left.Mode == right.Mode &&
		left.MaxFailedObjects == right.MaxFailedObjects &&
		left.MaxFailurePercent == right.MaxFailurePercent &&
		left.GraceSeconds == right.GraceSeconds
}

func maxDeletePhase(current, candidate *float64) *float64 {
	if candidate == nil {
		return current
	}
	if current == nil || *candidate > *current {
		value := *candidate
		return &value
	}
	return current
}

func boundedDeleteBuckets(byName map[string]DeleteBucketMetrics) []DeleteBucketMetrics {
	names := make([]string, 0, len(byName))
	overflow := byName[deletemetrics.OverflowBucket]
	delete(byName, deletemetrics.OverflowBucket)
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]DeleteBucketMetrics, 0, min(len(names), maxDeleteBucketMetrics)+1)
	for index, name := range names {
		bucket := byName[name]
		if index < maxDeleteBucketMetrics {
			result = append(result, bucket)
			continue
		}
		overflow.Bucket = deletemetrics.OverflowBucket
		overflow.Selected += bucket.Selected
		overflow.Attempted += bucket.Attempted
		overflow.Accepted += bucket.Accepted
		overflow.Failed += bucket.Failed
	}
	if overflow.Bucket != "" {
		result = append(result, overflow)
	}
	return result
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

func isTTFBEligibleOpType(opType string) bool {
	return strings.EqualFold(opType, "READ") || strings.EqualFold(opType, "LIST")
}

// AggregateByOpType groups a slice of metrics by OpType and produces a combined
// aggregate plus a per-op-type map. For non-mixed workloads (single metric) the
// aggregate is the metric itself and the map has one entry. For mixed workloads
// the aggregate sums additive fields (ops/s, MiB/s, counts) across op types and
// uses ops-weighted averages for latency/duration. Combined TTFB is populated only
// when every contributor has TTFB for the same READ or LIST operation type.
func AggregateByOpType(metrics []*PerformanceMetric) (combined *PerformanceMetric, perOp map[string]*PerformanceMetric) {
	groups := make(map[string][]*PerformanceMetric, len(metrics))
	groupKeys := make(map[string]string, len(metrics))
	groupOrder := make([]string, 0, len(metrics))
	var identity *PerformanceMetric
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		if identity == nil {
			identity = metric
		} else if metric.StepID != identity.StepID ||
			(identity.RunID != "" && metric.RunID != "" && metric.RunID != identity.RunID) ||
			(identity.ClusterID != "" && metric.ClusterID != "" && metric.ClusterID != identity.ClusterID) {
			return nil, nil
		}
		groupID := strings.ToUpper(metric.OpType)
		if _, ok := groups[groupID]; !ok {
			groupOrder = append(groupOrder, groupID)
			groupKeys[groupID] = metric.OpType
		}
		groups[groupID] = append(groups[groupID], metric)
	}
	if len(groupOrder) == 0 {
		return nil, nil
	}

	perOp = make(map[string]*PerformanceMetric, len(groups))
	groupedMetrics := make([]*PerformanceMetric, 0, len(groups))
	for _, groupID := range groupOrder {
		group := groups[groupID]
		metric := group[0]
		if len(group) > 1 {
			nodeMetrics := make(map[string]*PerformanceMetric, len(group))
			for _, nodeMetric := range group {
				nodeID := strings.TrimSpace(nodeMetric.NodeID)
				if nodeID == "" {
					return nil, nil
				}
				if _, duplicate := nodeMetrics[nodeID]; duplicate {
					return nil, nil
				}
				nodeMetrics[nodeID] = nodeMetric
			}
			metric = NewMetricsAggregator().aggregateNodeMetrics(nodeMetrics)
			if metric == nil {
				return nil, nil
			}
		}
		perOp[groupKeys[groupID]] = metric
		groupedMetrics = append(groupedMetrics, metric)
	}
	if len(groupedMetrics) == 1 {
		return groupedMetrics[0], perOp
	}

	// Build combined aggregate by summing additive fields.
	var agg PerformanceMetric
	var totalLatencyWeight, totalDurationWeight, totalTTFBWeight int64
	var ttfbMetricCount int
	allHaveCorruptCount := true
	first := groupedMetrics[0]
	allSameTTFBOpType := true

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
	agg.ContributorsPresent = append([]string(nil), first.ContributorsPresent...)
	agg.Timestamp = first.Timestamp
	agg.SampleTimestamp = first.SampleTimestamp
	agg.SampleTimestampRaw = first.SampleTimestampRaw
	agg.SptTimestamp = first.SptTimestamp

	for _, m := range groupedMetrics {
		if !strings.EqualFold(first.OpType, m.OpType) {
			allSameTTFBOpType = false
		}
		agg.OpsPerSec += m.OpsPerSec
		agg.MiBPerSec += m.MiBPerSec
		agg.SuccessCount += m.SuccessCount
		agg.FailedCount += m.FailedCount
		if m.HasCorruptCount {
			agg.CorruptCount += m.CorruptCount
		} else {
			allHaveCorruptCount = false
		}
		agg.ConcurrencyCurrent += m.ConcurrencyCurrent
		agg.ConcurrencyMean += m.ConcurrencyMean

		w := m.OpsPerSec
		if w <= 0 {
			w = 1
		}
		if timingDisplayAvailable(m.HasLatency, m.MeanLatency) {
			agg.MeanLatency += m.MeanLatency * w
			totalLatencyWeight += w
		}
		if timingDisplayAvailable(m.HasDuration, m.MeanDuration) {
			agg.MeanDuration += m.MeanDuration * w
			totalDurationWeight += w
		}
		if m.HasTTFB {
			agg.MeanTTFB += m.MeanTTFB * w
			totalTTFBWeight += w
			ttfbMetricCount++
		}

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
		agg.HasLatency = true
	}
	if totalDurationWeight > 0 {
		agg.MeanDuration /= totalDurationWeight
		agg.HasDuration = true
	}
	if totalTTFBWeight > 0 && ttfbMetricCount == len(groupedMetrics) && allSameTTFBOpType && isTTFBEligibleOpType(first.OpType) {
		agg.MeanTTFB /= totalTTFBWeight
		agg.HasTTFB = true
	} else {
		agg.MeanTTFB = 0
		agg.HasTTFB = false
	}

	agg.HasCorruptCount = allHaveCorruptCount

	// Completion: use the first metric's values (all ops in a mixed step
	// share the same time limit and overall completion).
	agg.CompletionPercent = first.CompletionPercent
	agg.OverallCompletionPercent = first.OverallCompletionPercent
	agg.Unbounded = first.Unbounded
	agg.OverallUnbounded = first.OverallUnbounded

	applyProgressAndLimitFields(&agg, groupedMetrics)

	return &agg, perOp
}

// applyProgressAndLimitFields propagates per-step progress and limit metadata
// from the per-node metrics onto an aggregate result so that completion
// detection (shouldSignalCompletion) has the data it needs.
//
//   - StepTime: MAX across nodes (the most-progressed elapsed time).
//   - HasLimit/LimitType/LimitTimeSec/LimitOpCount: filled from a node only when
//     the aggregate has not already computed them.
func applyProgressAndLimitFields(result *PerformanceMetric, metrics []*PerformanceMetric) {
	if result == nil || len(metrics) == 0 {
		return
	}

	var (
		maxStepTime float64
		limitSet    = result.HasLimit
	)

	for _, m := range metrics {
		if m == nil {
			continue
		}
		if m.StepTime > maxStepTime {
			maxStepTime = m.StepTime
		}
		// Limit metadata is identical per-step across nodes. Preserve any aggregate
		// values already computed by the caller, such as summed op-count limits.
		if !limitSet && m.HasLimit {
			result.HasLimit = m.HasLimit
			result.LimitType = m.LimitType
			result.LimitTimeSec = m.LimitTimeSec
			result.LimitOpCount = m.LimitOpCount
			limitSet = true
		}
	}

	result.StepTime = maxStepTime
}
