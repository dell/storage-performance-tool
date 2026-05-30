package tui

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// timeBoundedNode builds a per-node metric for a time-bounded run.
func timeBoundedNode(nodeID string, state int, stepTime float64, limitSec int64) *PerformanceMetric {
	m := newNodeMetric(100, 0, 1000, 0, 0, 0, 5, 5.0)
	m.NodeID = nodeID
	m.TestState = state
	m.HasLimit = true
	m.LimitType = constants.LimitTypeTime
	m.LimitTimeSec = limitSec
	m.StepTime = stepTime
	return m
}

// opCountBoundedNode builds a per-node metric for an op-count-bounded run.
func opCountBoundedNode(nodeID string, state int, completionPercent float64, limitOps int64) *PerformanceMetric {
	completedOps := int64((completionPercent / 100) * float64(limitOps))
	m := newNodeMetric(100, 0, completedOps, 0, 0, 0, 5, 5.0)
	m.NodeID = nodeID
	m.TestState = state
	m.HasLimit = true
	m.LimitType = constants.LimitTypeOpCount
	m.LimitOpCount = limitOps
	m.CompletionPercent = completionPercent
	return m
}

// Finding 1 regression: a genuinely-completed, time-bounded run must be
// detected as complete after running through the real aggregator. Before the
// aggregator propagated StepTime/limit fields, the aggregate had StepTime=0 and
// HasLimit=false, so this hung (or never required the time guard at all).
func TestShouldSignalCompletion_TimeBounded_GenuineThroughAggregate(t *testing.T) {
	agg := NewMetricsAggregator()

	result := agg.Aggregate(map[string]*PerformanceMetric{
		"node1": timeBoundedNode("node1", constants.TestStateCompleted, 120, 120),
		"node2": timeBoundedNode("node2", constants.TestStateCompleted, 121, 120),
	})
	if result == nil {
		t.Fatal("expected non-nil aggregate")
	}
	if !shouldSignalCompletion(result) {
		t.Errorf("expected completion to be signalled for genuine time-bounded run; aggregate StepTime=%v HasLimit=%v LimitType=%q LimitTimeSec=%d",
			result.StepTime, result.HasLimit, result.LimitType, result.LimitTimeSec)
	}
}

// Finding 1: a transient Completed state before the duration elapses must NOT
// signal completion, through the real aggregator.
func TestShouldSignalCompletion_TimeBounded_TransientThroughAggregate(t *testing.T) {
	agg := NewMetricsAggregator()

	result := agg.Aggregate(map[string]*PerformanceMetric{
		"node1": timeBoundedNode("node1", constants.TestStateCompleted, 5, 120),
		"node2": timeBoundedNode("node2", constants.TestStateCompleted, 4, 120),
	})
	if result == nil {
		t.Fatal("expected non-nil aggregate")
	}
	if shouldSignalCompletion(result) {
		t.Errorf("expected completion NOT to be signalled for transient time-bounded run; aggregate StepTime=%v (limit %d)",
			result.StepTime, result.LimitTimeSec)
	}
}

// Finding 2 (CLI defense-in-depth): an op-count run that reports Completed but
// has not reached 100% across the fleet must NOT signal completion.
func TestShouldSignalCompletion_OpCount_TransientThroughAggregate(t *testing.T) {
	agg := NewMetricsAggregator()
	// node1 fully done, node2 lagging -> fleet completion remains below 100%.

	result := agg.Aggregate(map[string]*PerformanceMetric{
		"node1": opCountBoundedNode("node1", constants.TestStateCompleted, 100, 1000),
		"node2": opCountBoundedNode("node2", constants.TestStateCompleted, 40, 1000),
	})
	if result == nil {
		t.Fatal("expected non-nil aggregate")
	}
	if shouldSignalCompletion(result) {
		t.Errorf("expected completion NOT to be signalled for transient op-count run; aggregate CompletionPercent=%v",
			result.CompletionPercent)
	}
}

// Finding 2 (CLI defense-in-depth): a genuinely-complete op-count run (every
// node at 100%) must signal completion.
func TestShouldSignalCompletion_OpCount_GenuineThroughAggregate(t *testing.T) {
	agg := NewMetricsAggregator()

	result := agg.Aggregate(map[string]*PerformanceMetric{
		"node1": opCountBoundedNode("node1", constants.TestStateCompleted, 100, 1000),
		"node2": opCountBoundedNode("node2", constants.TestStateCompleted, 100, 1000),
	})
	if result == nil {
		t.Fatal("expected non-nil aggregate")
	}
	if !shouldSignalCompletion(result) {
		t.Errorf("expected completion to be signalled for genuine op-count run; aggregate CompletionPercent=%v",
			result.CompletionPercent)
	}
}
