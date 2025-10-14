package tui

import (
	"strings"
	"testing"
	"time"
)

// Verifies Host column is rightmost with icons and placeholders for missing stats.
func TestLiveView_HostColumn_WithIconsAndPlaceholders(t *testing.T) {
	r := NewLiveViewRenderer(nil)
	defer r.Stop()

	mc := NewMetricsCollector()
	now := time.Now()

	// Aggregated sample to ensure header/time render
	agg := PerformanceMetric{Timestamp: now, OpType: "create", ConcurrencyCurrent: 1}
	mc.AddSample(agg)

	// Node A: has metrics and connected
	nodeA := &PerformanceMetric{Timestamp: now, OpsPerSec: 1000, MBPerSec: 100, SuccessCount: 500}
	// Node B: no metrics, disconnected
	update := &MultiNodeMetricsUpdate{
		Aggregated: &agg,
		PerNode:    map[string]*PerformanceMetric{"hostA": nodeA},
		NodeStatus: map[string]NodeConnectionStatus{
			"hostA": {IsConnected: true, IsActive: true, LastSeen: now, Phase: NodePhaseMetricsFlowing},
			"hostB": {IsConnected: false, IsActive: false, LastSeen: now, Phase: NodePhasePending},
		},
	}
	mc.AddMultiNodeSample(update)

	out := r.RenderLiveView(mc, 120)
	if !strings.Contains(out, " Host") {
		t.Fatalf("expected Host header in output; got:\n%s", out)
	}
	if !strings.Contains(out, "✅ hostA") {
		t.Errorf("expected connected icon for hostA; got:\n%s", out)
	}
	if !strings.Contains(out, "❌ hostB") {
		t.Errorf("expected disconnected icon for hostB; got:\n%s", out)
	}
	// hostB row should contain placeholders
	// Percent '—' and numeric '-' appear together in the placeholder row
	if !strings.Contains(out, " — ") {
		t.Errorf("expected em dash for percent in placeholder row; got:\n%s", out)
	}
}

func TestLiveView_HostColumn_TruncationAndNarrowWidth(t *testing.T) {
	r := NewLiveViewRenderer(nil)
	defer r.Stop()

	mc := NewMetricsCollector()
	now := time.Now()
	agg := PerformanceMetric{Timestamp: now, OpType: "read", ConcurrencyCurrent: 1}
	mc.AddSample(agg)

	longHost := strings.Repeat("x", 80)
	update := &MultiNodeMetricsUpdate{
		Aggregated: &agg,
		PerNode:    map[string]*PerformanceMetric{},
		NodeStatus: map[string]NodeConnectionStatus{
			longHost: {IsConnected: false, LastSeen: now, Phase: NodePhasePending},
		},
	}
	mc.AddMultiNodeSample(update)

	// Moderate width: expect ellipsis
	out := r.RenderLiveView(mc, 80)
	if !strings.Contains(out, "❌ ") {
		t.Fatalf("expected host icon present; got:\n%s", out)
	}
	if !strings.Contains(out, "...") {
		t.Errorf("expected host to be truncated with ellipsis; got:\n%s", out)
	}

	// Extremely narrow width: host column should be omitted (no icon visible)
	outNarrow := r.RenderLiveView(mc, 40)
	if strings.Contains(outNarrow, "✅") || strings.Contains(outNarrow, "❌") {
		t.Errorf("expected host column omitted on narrow width; got:\n%s", outNarrow)
	}
}

func TestLiveView_HostColumn_BlueIconForContactedPhase(t *testing.T) {
	r := NewLiveViewRenderer(nil)
	defer r.Stop()

	mc := NewMetricsCollector()
	now := time.Now()
	agg := PerformanceMetric{Timestamp: now, OpType: "create", ConcurrencyCurrent: 1}
	mc.AddSample(agg)

	update := &MultiNodeMetricsUpdate{
		Aggregated: &agg,
		PerNode:    map[string]*PerformanceMetric{},
		NodeStatus: map[string]NodeConnectionStatus{
			"hostC": {IsConnected: false, IsActive: false, LastSeen: now, Phase: NodePhaseContacted},
		},
	}
	mc.AddMultiNodeSample(update)

	out := r.RenderLiveView(mc, 120)
	if !strings.Contains(out, "🔵 hostC") {
		t.Fatalf("expected contacted hosts to render with blue icon; got:\n%s", out)
	}
	if strings.Contains(out, "✅ hostC") || strings.Contains(out, "❌ hostC") {
		t.Errorf("expected only blue icon for contacted host; got:\n%s", out)
	}
}
