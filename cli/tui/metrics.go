/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

const (
	maxMetricsSamples = 10000
)

// PerformanceMetric represents a single performance data point from Spt
type PerformanceMetric struct {
	Timestamp          time.Time
	SampleTimestamp    time.Time
	SampleTimestampRaw string
	StepID             string
	SptTimestamp       string  // Raw Spt timestamp (column 1)
	OpType             string  // Operation type (CREATE, READ, etc.)
	TestState          int     // Spt test state (0=idle, 1=running, 2=completed)
	ConcurrencyCurrent int64   // Current concurrency level
	ConcurrencyMean    float64 // Mean concurrency level
	SuccessCount       int64   // Successful operations count
	FailedCount        int64   // Failed operations count
	StepTime           float64 // Step time in seconds
	OpsPerSec          int64   // Last Rate [op/s] - primary chart metric
	MBPerSec           int64   // Last Rate [MB/s] - secondary chart metric
	MeanLatency        int64   // Display latency [us]; schema 3 prefers p50 with mean fallback
	MeanDuration       int64   // Display duration [us]; schema 3 prefers p50 with mean fallback
	MeanTTFB           int64   // Display TTFB [us]
	HasTTFB            bool    // Whether TTFB was present in the metrics sample
	FailureIncrement   int64   // Incremental failures since last sample

	MetricsSchema int
	Scope         string
	Role          string
	ClusterID     string
	NodeID        string
	RunID         string
	NodesCount    int
	NodesPresent  []string
	Partial       bool

	LimitType    string
	LimitOpCount int64
	LimitTimeSec int64
	HasLimit     bool

	// Completion tracking (per-phase)
	// CompletionPercent is a 0-100 value reported by the node JSON API.
	// When Unbounded is true (no time/op-count limit), UI should display "∞" instead of a number.
	CompletionPercent        float64
	OverallCompletionPercent float64
	Unbounded                bool
	OverallUnbounded         bool
}

// NodePhase represents the lifecycle stage of a node from spt's perspective.
type NodePhase string

const (
	// NodePhasePending indicates the orchestrator has not contacted the node yet.
	NodePhasePending NodePhase = "pending"
	// NodePhaseContacted indicates basic connectivity (e.g., SSH/Docker) succeeded.
	NodePhaseContacted NodePhase = "contacted"
	// NodePhaseContainerStarting indicates container launch has been initiated.
	NodePhaseContainerStarting NodePhase = "container_starting"
	// NodePhaseAPIReady indicates the node's API reported readiness.
	NodePhaseAPIReady NodePhase = "api_ready"
	// NodePhaseMetricsFlowing indicates metrics are being successfully collected.
	NodePhaseMetricsFlowing NodePhase = "metrics_flowing"
)

// NodeConnectionStatus tracks the health and connection status of individual nodes
type NodeConnectionStatus struct {
	LastSeen    time.Time
	IsConnected bool  // Whether the node is reachable
	IsActive    bool  // Whether the node is actively processing
	Error       error // Last error if any
	Phase       NodePhase
}

// MultiNodeMetricsUpdate contains all metrics data for multi-node tests
type MultiNodeMetricsUpdate struct {
	Timestamp  time.Time
	Aggregated *PerformanceMetric              // Aggregated metrics for charts (sum of all op types)
	PerNode    map[string]*PerformanceMetric   // Individual node metrics (primary/first metric per node)
	PerOpType  map[string]*PerformanceMetric   // Per-operation-type aggregates (e.g. "CREATE", "READ", "DELETE")
	NodeStatus map[string]NodeConnectionStatus // Connection health per node
}

// MetricsPoller interface for testable metrics collection
type MetricsPoller interface {
	PollMetrics(ctx context.Context, nodeID string) (*PerformanceMetric, error)
}

// MetricsCollector manages the collection and storage of performance metrics
type MetricsCollector struct {
	mu             sync.RWMutex
	samples        []PerformanceMetric             // Aggregated metrics (existing)
	perNodeSamples map[string][]PerformanceMetric  // Per-node metric history
	nodeStatus     map[string]NodeConnectionStatus // Node connection status
	maxSize        int
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		samples:        make([]PerformanceMetric, 0, maxMetricsSamples),
		perNodeSamples: make(map[string][]PerformanceMetric),
		nodeStatus:     make(map[string]NodeConnectionStatus),
		maxSize:        maxMetricsSamples,
	}
}

// AddSample adds a new performance metric sample (aggregated metrics for charts)
func (mc *MetricsCollector) AddSample(metric PerformanceMetric) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Calculate incremental failures if we have a previous sample
	if len(mc.samples) > 0 {
		prevSample := mc.samples[len(mc.samples)-1]
		// Calculate the increment (new failures - previous failures)
		metric.FailureIncrement = metric.FailedCount - prevSample.FailedCount
		// Handle case where count might reset or decrease (shouldn't happen but defensive programming)
		if metric.FailureIncrement < 0 {
			// Log error when failure count decreases
			logging.LogError("metrics", "failure count decreased unexpectedly", nil,
				"previous", prevSample.FailedCount,
				"current", metric.FailedCount)
			metric.FailureIncrement = 0
		}

		// No console/JSON interleaving anymore; keep raw JSON completion values.
	} else {
		// First sample - the increment is 0 as specified
		metric.FailureIncrement = 0
	}

	mc.samples = append(mc.samples, metric)

	// Keep only the last maxSize samples
	if len(mc.samples) > mc.maxSize {
		// Remove the oldest sample
		copy(mc.samples, mc.samples[1:])
		mc.samples = mc.samples[:mc.maxSize]
	}
}

// GetSamples returns all collected samples (aggregated metrics for charts)
func (mc *MetricsCollector) GetSamples() []PerformanceMetric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Return a copy to avoid race conditions
	result := make([]PerformanceMetric, len(mc.samples))
	copy(result, mc.samples)
	return result
}

// GetLatestOpsPerSec returns the most recent ops/sec value, or 0 if no samples
func (mc *MetricsCollector) GetLatestOpsPerSec() int64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if len(mc.samples) == 0 {
		return 0
	}
	return mc.samples[len(mc.samples)-1].OpsPerSec
}

// GetOpsPerSecRange returns the min and max ops/sec values across all samples
func (mc *MetricsCollector) GetOpsPerSecRange() (minVal, maxVal int64) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if len(mc.samples) == 0 {
		return 0, 0
	}

	minVal = mc.samples[0].OpsPerSec
	maxVal = mc.samples[0].OpsPerSec

	for _, sample := range mc.samples {
		if sample.OpsPerSec < minVal {
			minVal = sample.OpsPerSec
		}
		if sample.OpsPerSec > maxVal {
			maxVal = sample.OpsPerSec
		}
	}

	return minVal, maxVal
}

// GetMetricRange returns the min and max values for a given metric extractor
func (mc *MetricsCollector) GetMetricRange(extractor func(PerformanceMetric) int64) (minVal, maxVal int64) {
	if len(mc.samples) == 0 {
		return 0, 0
	}

	minVal = extractor(mc.samples[0])
	maxVal = extractor(mc.samples[0])

	for _, sample := range mc.samples {
		value := extractor(sample)
		if value < minVal {
			minVal = value
		}
		if value > maxVal {
			maxVal = value
		}
	}

	return minVal, maxVal
}

// AddMultiNodeSample processes a multi-node metrics update
func (mc *MetricsCollector) AddMultiNodeSample(update *MultiNodeMetricsUpdate) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Store aggregated metrics (feeds existing charts)
	if update.Aggregated != nil {
		mc.addAggregatedSampleLocked(*update.Aggregated)
	}

	// Store per-node samples
	for nodeID, metrics := range update.PerNode {
		if metrics != nil {
			mc.addNodeSampleLocked(nodeID, *metrics)
		}
	}

	// Update node status
	for nodeID, status := range update.NodeStatus {
		mc.nodeStatus[nodeID] = status
	}
}

// addAggregatedSampleLocked adds aggregated sample (assumes lock is held)
func (mc *MetricsCollector) addAggregatedSampleLocked(metric PerformanceMetric) {
	// Calculate incremental failures if we have a previous sample
	if len(mc.samples) > 0 {
		prevSample := mc.samples[len(mc.samples)-1]
		metric.FailureIncrement = metric.FailedCount - prevSample.FailedCount
		if metric.FailureIncrement < 0 {
			logging.LogError("metrics", "failure count decreased unexpectedly", nil,
				"previous", prevSample.FailedCount,
				"current", metric.FailedCount)
			metric.FailureIncrement = 0
		}

		// No smoothing required under JSON-only metrics.
	} else {
		metric.FailureIncrement = 0
	}

	mc.samples = append(mc.samples, metric)

	// Keep only the last maxSize samples
	if len(mc.samples) > mc.maxSize {
		copy(mc.samples, mc.samples[1:])
		mc.samples = mc.samples[:mc.maxSize]
	}
}

// addNodeSampleLocked adds per-node sample (assumes lock is held)
func (mc *MetricsCollector) addNodeSampleLocked(nodeID string, metric PerformanceMetric) {
	if mc.perNodeSamples[nodeID] == nil {
		mc.perNodeSamples[nodeID] = make([]PerformanceMetric, 0, mc.maxSize)
	}

	nodeSamples := mc.perNodeSamples[nodeID]

	// Calculate incremental failures for this node
	if len(nodeSamples) > 0 {
		prevSample := nodeSamples[len(nodeSamples)-1]
		metric.FailureIncrement = metric.FailedCount - prevSample.FailedCount
		if metric.FailureIncrement < 0 {
			logging.LogError("metrics", "node failure count decreased unexpectedly", nil,
				"node", nodeID,
				"previous", prevSample.FailedCount,
				"current", metric.FailedCount)
			metric.FailureIncrement = 0
		}

		// No smoothing required under JSON-only metrics.
	} else {
		metric.FailureIncrement = 0
	}

	nodeSamples = append(nodeSamples, metric)

	// Keep only the last maxSize samples
	if len(nodeSamples) > mc.maxSize {
		copy(nodeSamples, nodeSamples[1:])
		nodeSamples = nodeSamples[:mc.maxSize]
	}

	mc.perNodeSamples[nodeID] = nodeSamples
}

// GetNodeSamples returns metrics history for a specific node
func (mc *MetricsCollector) GetNodeSamples(nodeID string) []PerformanceMetric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	samples := mc.perNodeSamples[nodeID]
	if samples == nil {
		return []PerformanceMetric{}
	}

	// Return a copy to avoid race conditions
	result := make([]PerformanceMetric, len(samples))
	copy(result, samples)
	return result
}

// GetNodeStatus returns connection status for all nodes
func (mc *MetricsCollector) GetNodeStatus() map[string]NodeConnectionStatus {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// Return a copy to avoid race conditions
	result := make(map[string]NodeConnectionStatus)
	for nodeID, status := range mc.nodeStatus {
		result[nodeID] = status
	}
	return result
}

// GetActiveNodeCount returns the number of currently active nodes
func (mc *MetricsCollector) GetActiveNodeCount() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	count := 0
	for _, status := range mc.nodeStatus {
		if status.IsActive && status.IsConnected {
			count++
		}
	}
	return count
}

// GetLatestSample returns the most recent aggregated sample, or nil if no samples
func (mc *MetricsCollector) GetLatestSample() *PerformanceMetric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if len(mc.samples) == 0 {
		return nil
	}

	// Return a copy to avoid race conditions
	latest := mc.samples[len(mc.samples)-1]
	return &latest
}

// GetLatestNodeSample returns the most recent sample for a specific node
func (mc *MetricsCollector) GetLatestNodeSample(nodeID string) *PerformanceMetric {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	samples := mc.perNodeSamples[nodeID]
	if len(samples) == 0 {
		return nil
	}

	// Return a copy to avoid race conditions
	latest := samples[len(samples)-1]
	return &latest
}

// CalculateNiceUpperBound calculates a "nice" round number for chart scaling
// Given a maximum value, returns a round number that's >= max
func CalculateNiceUpperBound(maxVal int64) int64 {
	if maxVal <= 0 {
		return 100 // Default minimum scale
	}

	// Find the order of magnitude
	magnitude := int64(1)
	for magnitude <= maxVal {
		magnitude *= 10
	}
	magnitude /= 10 // Step back one order

	// Calculate nice multiples
	candidates := []int64{
		magnitude,      // 1x10^n
		magnitude * 2,  // 2x10^n
		magnitude * 5,  // 5x10^n
		magnitude * 10, // 1x10^(n+1)
	}

	// Return the first candidate that's >= max
	for _, candidate := range candidates {
		if candidate >= maxVal {
			return candidate
		}
	}

	// Fallback - shouldn't reach here
	return maxVal * 2
}
