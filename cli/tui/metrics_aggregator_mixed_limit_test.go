package tui

import (
	"math/rand/v2"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// mixedLimitIterations repeats each case so that Go's randomised map order
// (and the shuffled input order) gets many chances to pick a different node
// first. The defect this guards picked the limit from whichever node came
// first, about one time in five for the losing node.
const mixedLimitIterations = 2000

// limitNode builds a per-node metric with the given limit type. An op_count
// node is half done unless Completed; a time node is short of its duration
// unless Completed.
func limitNode(nodeID, opType, limitType string, state int) *PerformanceMetric {
	m := newNodeMetric(100, 0, 50, 0, 0, 0, 5, 5.0)
	m.NodeID = nodeID
	m.OpType = opType
	m.TestState = state
	m.HasLimit = true
	m.LimitType = limitType
	m.CompletionPercent = 50
	m.StepTime = 30
	switch limitType {
	case constants.LimitTypeOpCount:
		m.LimitOpCount = 100
		if state == constants.TestStateCompleted {
			m.SuccessCount = 100
			m.CompletionPercent = 100
		}
	case constants.LimitTypeTime:
		m.LimitTimeSec = 60
		if state == constants.TestStateCompleted {
			m.StepTime = 61
			m.CompletionPercent = 100
		}
	case constants.LimitTypeNone:
		m.Unbounded = true
		m.CompletionPercent = 0
	}
	return m
}

type aggregateOutcome struct {
	hasLimit     bool
	limitType    string
	limitOpCount int64
	limitTimeSec int64
	testState    int
	signal       bool
}

func outcomeOf(m *PerformanceMetric) aggregateOutcome {
	return aggregateOutcome{
		hasLimit:     m.HasLimit,
		limitType:    m.LimitType,
		limitOpCount: m.LimitOpCount,
		limitTimeSec: m.LimitTimeSec,
		testState:    m.TestState,
		signal:       shouldSignalCompletion(m),
	}
}

// opNode describes one node's sample for one op type.
type opNode struct {
	node, op, limit string
	state           int
}

func buildOpNodes(nodes []opNode) []*PerformanceMetric {
	metrics := make([]*PerformanceMetric, 0, len(nodes))
	for _, n := range nodes {
		metrics = append(metrics, limitNode(n.node, n.op, n.limit, n.state))
	}
	return metrics
}

// The reachable mixed-limit case (weighted, pipeline or appended mixed steps
// whose base count slices to 0 on one engine) reports several op types, so
// collectAllMetrics takes AggregateByOpType's combined metric. It must not
// signal while any op type is still running on any node, whatever the order.
func TestAggregateByOpTypeMixedLimitsSignalsOnlyWhenEveryNodeCompleted(t *testing.T) {
	const (
		running   = constants.TestStateRunning
		completed = constants.TestStateCompleted
		count     = constants.LimitTypeOpCount
		none      = constants.LimitTypeNone
	)
	cases := []struct {
		name       string
		nodes      []opNode
		wantSignal bool
		wantState  int
	}{
		{
			name: "CREATE completed on the count node only",
			nodes: []opNode{
				{"n1", "CREATE", count, completed},
				{"n2", "CREATE", none, running},
				{"n1", "READ", count, running},
				{"n2", "READ", none, running},
			},
			wantState: running,
		},
		{
			name: "CREATE completed on the unlimited node only",
			nodes: []opNode{
				{"n1", "CREATE", count, running},
				{"n2", "CREATE", none, completed},
				{"n1", "READ", count, running},
				{"n2", "READ", none, running},
			},
			wantState: running,
		},
		{
			name: "CREATE completed everywhere, READ running everywhere",
			nodes: []opNode{
				{"n1", "CREATE", count, completed},
				{"n2", "CREATE", none, completed},
				{"n1", "READ", count, running},
				{"n2", "READ", none, running},
			},
			wantState: running,
		},
		{
			name: "every op type completed on every node",
			nodes: []opNode{
				{"n1", "CREATE", count, completed},
				{"n2", "CREATE", none, completed},
				{"n1", "READ", count, completed},
				{"n2", "READ", none, completed},
			},
			wantSignal: true,
			wantState:  completed,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := aggregateOutcome{testState: tc.wantState, signal: tc.wantSignal}
			for i := 0; i < mixedLimitIterations; i++ {
				metrics := buildOpNodes(tc.nodes)
				rand.Shuffle(len(metrics), func(a, b int) { metrics[a], metrics[b] = metrics[b], metrics[a] })
				combined, perOp := AggregateByOpType(metrics)
				if combined == nil || len(perOp) != 2 {
					t.Fatalf("iteration %d: combined=%v perOp=%d, want a combined metric over 2 op types", i, combined, len(perOp))
				}
				if got := outcomeOf(combined); got != want {
					t.Fatalf("iteration %d: got %+v, want %+v", i, got, want)
				}
				for op, m := range perOp {
					if m.HasLimit || m.LimitType != "" {
						t.Fatalf("iteration %d: %s group kept limit %q from one node", i, op, m.LimitType)
					}
				}
			}
		})
	}
}

// A step with several op types completes only when every op type has, even
// when the engines agree on each op type's limit.
func TestAggregateByOpTypeUniformLimitsWaitsForEveryOpType(t *testing.T) {
	const (
		running   = constants.TestStateRunning
		completed = constants.TestStateCompleted
	)
	for _, limit := range []string{constants.LimitTypeOpCount, constants.LimitTypeTime, constants.LimitTypeNone} {
		t.Run(limit, func(t *testing.T) {
			for _, readState := range []int{running, completed} {
				metrics := buildOpNodes([]opNode{
					{"n1", "CREATE", limit, completed},
					{"n2", "CREATE", limit, completed},
					{"n1", "READ", limit, readState},
					{"n2", "READ", limit, readState},
				})
				combined, _ := AggregateByOpType(metrics)
				if combined == nil {
					t.Fatal("expected a combined metric")
				}
				if combined.TestState != readState {
					t.Errorf("READ %d: combined TestState %d, want %d", readState, combined.TestState, readState)
				}
				if got, want := shouldSignalCompletion(combined), readState == completed; got != want {
					t.Errorf("READ %d: signal %v, want %v", readState, got, want)
				}
				if !combined.HasLimit || combined.LimitType != limit {
					t.Errorf("READ %d: combined limit %v %q, want %q", readState, combined.HasLimit, combined.LimitType, limit)
				}
			}
		})
	}
}

// Aggregate (single op type) with op_count, none and time nodes must give the
// same result on every iteration and no signal until every node is Completed.
func TestAggregateMixedLimitsIsDeterministicAndConservative(t *testing.T) {
	limits := map[string]string{
		"n1": constants.LimitTypeOpCount,
		"n2": constants.LimitTypeNone,
		"n3": constants.LimitTypeTime,
	}
	cases := []struct {
		name       string
		completed  map[string]bool
		wantSignal bool
		wantState  int
	}{
		{name: "count node completed", completed: map[string]bool{"n1": true}, wantState: constants.TestStateRunning},
		{name: "unlimited node completed", completed: map[string]bool{"n2": true}, wantState: constants.TestStateRunning},
		{name: "time node completed", completed: map[string]bool{"n3": true}, wantState: constants.TestStateRunning},
		{name: "two nodes completed", completed: map[string]bool{"n1": true, "n2": true}, wantState: constants.TestStateRunning},
		{
			name:       "every node completed",
			completed:  map[string]bool{"n1": true, "n2": true, "n3": true},
			wantSignal: true,
			wantState:  constants.TestStateCompleted,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := aggregateOutcome{testState: tc.wantState, signal: tc.wantSignal}
			for i := 0; i < mixedLimitIterations; i++ {
				nodes := make(map[string]*PerformanceMetric, len(limits))
				for id, limit := range limits {
					state := constants.TestStateRunning
					if tc.completed[id] {
						state = constants.TestStateCompleted
					}
					nodes[id] = limitNode(id, "CREATE", limit, state)
				}
				result := NewMetricsAggregator().Aggregate(nodes)
				if result == nil {
					t.Fatalf("iteration %d: expected an aggregate", i)
				}
				if got := outcomeOf(result); got != want {
					t.Fatalf("iteration %d: got %+v, want %+v", i, got, want)
				}
			}
		})
	}
}

// Steps whose engines agree on the limit type keep today's behaviour.
func TestAggregateUniformLimitsUnchanged(t *testing.T) {
	const (
		running   = constants.TestStateRunning
		completed = constants.TestStateCompleted
	)
	cases := []struct {
		name       string
		limit      string
		states     [2]int
		wantSignal bool
		wantState  int
		wantOps    int64
		wantSec    int64
	}{
		{name: "op_count, one node completed", limit: constants.LimitTypeOpCount, states: [2]int{completed, running}, wantState: completed, wantOps: 200},
		{name: "op_count, every node completed", limit: constants.LimitTypeOpCount, states: [2]int{completed, completed}, wantSignal: true, wantState: completed, wantOps: 200},
		{name: "time, one node completed", limit: constants.LimitTypeTime, states: [2]int{completed, running}, wantSignal: true, wantState: completed, wantSec: 60},
		{name: "time, every node running", limit: constants.LimitTypeTime, states: [2]int{running, running}, wantState: running, wantSec: 60},
		// A uniform unlimited step still completes on the first Completed node;
		// .scratch/unlimited-step-first-node-completion covers that separately.
		{name: "none, one node completed", limit: constants.LimitTypeNone, states: [2]int{completed, running}, wantSignal: true, wantState: completed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{
				"n1": limitNode("n1", "CREATE", tc.limit, tc.states[0]),
				"n2": limitNode("n2", "CREATE", tc.limit, tc.states[1]),
			})
			if result == nil {
				t.Fatal("expected an aggregate")
			}
			want := aggregateOutcome{
				hasLimit:     true,
				limitType:    tc.limit,
				limitOpCount: tc.wantOps,
				limitTimeSec: tc.wantSec,
				testState:    tc.wantState,
				signal:       tc.wantSignal,
			}
			if got := outcomeOf(result); got != want {
				t.Errorf("got %+v, want %+v", got, want)
			}
		})
	}
}
