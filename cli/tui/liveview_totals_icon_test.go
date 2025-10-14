package tui

import (
	"strings"
	"testing"
	"time"
)

// Ensures the Totals row (empty host) does not render a connection icon.
func TestLiveView_TotalsRow_NoIcon(t *testing.T) {
	r := NewLiveViewRenderer(nil)
	defer r.Stop()

	mc := NewMetricsCollector()
	now := time.Now()

	// Add an aggregated sample so the live view renders a Totals row
	agg := PerformanceMetric{Timestamp: now, OpType: "create", ConcurrencyCurrent: 1}
	mc.AddSample(agg)

	// Seed NodeStatus to also render at least one host row
	update := &MultiNodeMetricsUpdate{
		Aggregated: &agg,
		PerNode:    map[string]*PerformanceMetric{},
		NodeStatus: map[string]NodeConnectionStatus{"hostA": {IsConnected: true, IsActive: true, LastSeen: now, Phase: NodePhaseMetricsFlowing}},
	}
	mc.AddMultiNodeSample(update)

	out := r.RenderLiveView(mc, 120)

	// The Totals line begins with "Total" and should NOT contain any icon glyphs.
	var totalsLine string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "Total") {
			totalsLine = line
			break
		}
	}
	if totalsLine == "" {
		t.Fatalf("expected Totals row in output; got:\n%s", out)
	}
	if strings.Contains(totalsLine, "✅") || strings.Contains(totalsLine, "❌") {
		t.Errorf("Totals row should not include connection icon; got:\n%s", totalsLine)
	}
}
