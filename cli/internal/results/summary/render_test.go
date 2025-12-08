package summary

import (
	"strings"
	"testing"
	"time"
)

func TestRendererFullReportIncludesSections(t *testing.T) {
	t.Parallel()

	summary := &RunSummary{
		RunID:       "mt-20250926.172619.540",
		GeneratedAt: time.Date(2025, 9, 26, 17, 27, 28, 0, time.UTC),
		Environment: EnvironmentSummary{
			BaseURL:            "http://spt-test-1.cec.delllabs.net:9999",
			SptImage:           "ghcr.io/dell/storage-performance-tool",
			Hosts:              []HostSummary{{Original: "root@spt-test-1.cec.delllabs.net"}},
			AutoResults:        true,
			ShutdownOnComplete: true,
			ShutdownLingerSec:  5,
			ScenarioStoredPath: "spt-scenario-1758907579.js",
		},
		Workload: WorkloadSummary{
			Type:            "write",
			ObjectSizeHuman: "1.00 MiB",
			ObjectCount:     2500,
			Threads:         8,
			Endpoints:       []string{"http://10.247.70.222:9020", "http://10.247.70.223:9020"},
			Bucket:          "mh-testwrites",
			CleanupEnabled:  true,
			KeepScenario:    true,
		},
		Steps: []StepSummary{
			{
				PhaseLabel: "Create",
				Metrics: &PhaseMetrics{
					SuccessCount:     2499,
					DataBytes:        2620391424,
					ThroughputAvgOps: 334.6964,
					LatencyMeanMs:    79.9,
					BandwidthAvgMBps: 334.6964,
					ObjectSizeHuman:  "1.00 MiB",
				},
			},
			{
				PhaseLabel: "Delete",
				Metrics: &PhaseMetrics{
					SuccessCount:     2454,
					DataBytes:        0,
					ThroughputAvgOps: 1227,
					LatencyMeanMs:    23.4,
					BandwidthAvgMBps: 0,
				},
			},
		},
		Totals: RunTotals{
			DurationHuman: "9.7s",
			DataBytes:     2620391424,
		},
	}

	renderer := NewRenderer(RenderOptions{MaxWidth: 120})
	report := renderer.FullReport(summary)

	mustContain(t, report, "Environment")
	mustContain(t, report, "Workload Configuration")
	mustContain(t, report, "Performance by Phase")
	mustContain(t, report, "Run Totals")
	mustContain(t, report, "Create")
	mustContain(t, report, "Delete")

	lines := strings.Split(report, "\n")
	if len(lines) < 10 {
		t.Fatalf("expected report to have multiple lines, got %d", len(lines))
	}

	headerFound := false
	for _, line := range lines {
		if strings.Contains(line, "Phase") && strings.Contains(line, "Bandwidth Avg") {
			headerFound = true
			break
		}
	}
	if !headerFound {
		t.Fatalf("table header not found in report:\n%s", report)
	}
}

func TestRendererConsoleSnippetTruncates(t *testing.T) {
	t.Parallel()

	summary := &RunSummary{RunID: "run", Steps: make([]StepSummary, 0)}
	renderer := NewRenderer(RenderOptions{SnippetLineCap: 3})
	report := renderer.ConsoleSnippet(summary)
	lines := strings.Split(strings.TrimSpace(report), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}
	if !strings.Contains(lines[2], "truncated") {
		t.Fatalf("expected truncated marker in snippet, got %q", lines[2])
	}
}

func TestRendererCompactSnippetIncludesTable(t *testing.T) {
	t.Parallel()

	summary := &RunSummary{
		RunID:       "mt-20250926.201254.486",
		GeneratedAt: time.Date(2025, 9, 26, 20, 13, 59, 0, time.UTC),
		Workload: WorkloadSummary{
			ObjectSizeHuman: "1.00 MiB",
		},
		Steps: []StepSummary{
			{
				PhaseLabel: "Create",
				Metrics: &PhaseMetrics{
					ObjectSizeHuman:  "1.00 MiB",
					SuccessCount:     2499,
					DataBytes:        2620391424,
					ThroughputAvgOps: 372,
					LatencyMeanMs:    89.2,
					BandwidthAvgMBps: 372,
				},
			},
		},
		Totals: RunTotals{
			DurationHuman: "9.62s",
			DataBytes:     2620391424,
		},
	}

	renderer := NewRenderer(RenderOptions{MaxWidth: 90})
	snippet := renderer.CompactSnippet(summary)

	mustContain(t, snippet, "Performance by Phase")
	mustContain(t, snippet, "┌")
	mustContain(t, snippet, "│ Phase")
	mustContain(t, snippet, "│ Create")
	mustContain(t, snippet, "└")
	mustContain(t, snippet, "Totals: duration 9.62s")
}

func TestRendererPerformanceTableSkipsMissingMetrics(t *testing.T) {
	t.Parallel()

	renderer := NewRenderer(RenderOptions{MaxWidth: 90})
	summary := &RunSummary{
		Workload: WorkloadSummary{ObjectSizeHuman: "1.00 MiB"},
		Steps: []StepSummary{
			{PhaseLabel: "Create", Status: StepStatusPartial, Metrics: nil},
			{PhaseLabel: "Delete", Status: StepStatusComplete, Metrics: &PhaseMetrics{SuccessCount: 42, DataBytes: 0, ThroughputAvgOps: 10}},
		},
	}

	table := renderer.performanceTable(summary)
	if strings.Contains(table, "Create") {
		t.Fatalf("expected table to skip steps without metrics, got:\n%s", table)
	}
	if !strings.Contains(table, "Delete") {
		t.Fatalf("expected table to include populated steps, got:\n%s", table)
	}
}

func TestRendererListWorkloadDisplaysPrefix(t *testing.T) {
	t.Parallel()

	summary := &RunSummary{
		Workload: WorkloadSummary{
			Type:    "list",
			Prefix:  "daily/",
			Threads: 1,
		},
		Steps: []StepSummary{
			{
				PhaseLabel: "List",
				Metrics: &PhaseMetrics{
					SuccessCount:     128,
					ThroughputAvgOps: 256.4,
					LatencyMeanMs:    12.5,
					BandwidthAvgMBps: 0,
				},
			},
		},
		Totals: RunTotals{DurationHuman: "12.0s"},
	}

	renderer := NewRenderer(RenderOptions{MaxWidth: 100})
	report := renderer.FullReport(summary)

	mustContain(t, report, "• Object size        not applicable")
	mustContain(t, report, "• Prefix             daily/")
	mustContain(t, report, "Ops/s Avg")

	table := renderer.performanceTable(summary)
	if !strings.Contains(table, "—") {
		t.Fatalf("expected object size column to use em dash, got:\n%s", table)
	}
}

func mustContain(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("expected %q to contain %q", s, sub)
	}
}
