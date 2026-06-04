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
					SuccessCount:      2499,
					DataBytes:         2620391424,
					ThroughputAvgOps:  334.6964,
					LatencyHeadlineMs: 79.9,
					BandwidthAvgMBps:  334.6964,
					ObjectSizeHuman:   "1.00 MiB",
				},
			},
			{
				PhaseLabel: "Delete",
				Metrics: &PhaseMetrics{
					SuccessCount:      2454,
					DataBytes:         0,
					ThroughputAvgOps:  1227,
					LatencyHeadlineMs: 23.4,
					BandwidthAvgMBps:  0,
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
					ObjectSizeHuman:   "1.00 MiB",
					SuccessCount:      2499,
					DataBytes:         2620391424,
					ThroughputAvgOps:  372,
					LatencyHeadlineMs: 89.2,
					BandwidthAvgMBps:  372,
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
					SuccessCount:      128,
					ThroughputAvgOps:  256.4,
					LatencyHeadlineMs: 12.5,
					BandwidthAvgMBps:  0,
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

func TestRendererFullReportIncludesMixedBreakdown(t *testing.T) {
	t.Parallel()

	summary := mixedSummaryFixture()
	renderer := NewRenderer(RenderOptions{MaxWidth: 120})
	report := renderer.FullReport(summary)

	mustContain(t, report, "Mixed Operation Breakdown")
	mustContain(t, report, "Step: mt-002-20260604.180001.000-mixed")
	mustContain(t, report, "Configured distribution: READ 45%, STAT 30%, CREATE 15%, DELETE 10%")
	mustContain(t, report, "see ops")
	mustContain(t, report, "│ READ")
	mustContain(t, report, "│ DELETE")
}

func TestRendererCompactSnippetIncludesMixedOperationDetail(t *testing.T) {
	t.Parallel()

	renderer := NewRenderer(RenderOptions{MaxWidth: 100})
	snippet := renderer.CompactSnippet(mixedSummaryFixture())

	mustContain(t, snippet, "Mixed operations: READ")
	mustContain(t, snippet, "cfg 45%")
	mustContain(t, snippet, "CREATE")
	mustContain(t, snippet, "Totals: duration 30s")
}

func TestRendererMixedBreakdownOmitsConfiguredDistributionWhenUnavailable(t *testing.T) {
	t.Parallel()

	summary := mixedSummaryFixture()
	summary.Workload.MixedDistribution = MixedDistribution{}
	for i := range summary.Steps[0].OperationBreakdown {
		summary.Steps[0].OperationBreakdown[i].ConfiguredShare = nil
	}

	renderer := NewRenderer(RenderOptions{MaxWidth: 120})
	report := renderer.FullReport(summary)
	snippet := renderer.CompactSnippet(summary)

	mustNotContain(t, report, "Configured distribution:")
	mustNotContain(t, snippet, "cfg ")
}

func TestRendererConsoleSnippetKeepsMixedDetailWithElevatedCap(t *testing.T) {
	t.Parallel()

	summary := mixedSummaryFixture()
	for i := 0; i < 10; i++ {
		summary.Warnings = append(summary.Warnings, "warning line for mixed report truncation coverage")
	}

	renderer := NewRenderer(RenderOptions{MaxWidth: 120, SnippetLineCap: 40})
	snippet := renderer.ConsoleSnippet(summary)

	mustContain(t, snippet, "Mixed Operation Breakdown")
	mustContain(t, snippet, "Run Totals")
	if strings.Contains(snippet, "report truncated") {
		t.Fatalf("expected elevated mixed cap to avoid truncation, got:\n%s", snippet)
	}
}

func mixedSummaryFixture() *RunSummary {
	configuredRead := 45.0
	configuredStat := 30.0
	configuredCreate := 15.0
	configuredDelete := 10.0
	actualRead := 44.5
	actualStat := 29.8
	actualCreate := 15.1
	actualDelete := 10.6

	return &RunSummary{
		RunID:       "mt-20260604.180000.000",
		GeneratedAt: time.Date(2026, 6, 4, 18, 0, 0, 0, time.UTC),
		Workload: WorkloadSummary{
			Type:            "mixed",
			ObjectSizeHuman: "4.00 MiB",
			DurationRequest: "30s",
			MixedDistribution: MixedDistribution{
				Available:     true,
				ReadPercent:   45,
				StatPercent:   30,
				CreatePercent: 15,
				DeletePercent: 10,
			},
		},
		Steps: []StepSummary{
			{
				StepID:           "mt-002-20260604.180001.000-mixed",
				PhaseLabel:       "Mixed",
				Operation:        "MIXED",
				IsMixed:          true,
				MixedLatencyNote: "Mixed latency is shown per operation; no combined p50 is derived from per-op quantiles.",
				Metrics: &PhaseMetrics{
					SuccessCount:     1000,
					FailureCount:     12,
					DataBytes:        2499805184,
					ThroughputAvgOps: 33.5,
					BandwidthAvgMBps: 80.4,
					DurationSeconds:  30,
					DurationHuman:    "30s",
					ObjectSizeHuman:  "4.00 MiB",
				},
				OperationBreakdown: []OperationBreakdown{
					{Operation: "READ", ConfiguredShare: &configuredRead, ActualShare: &actualRead, ActualOps: 450, Metrics: PhaseMetrics{SuccessCount: 445, FailureCount: 5, DataBytes: 1866465280, ThroughputAvgOps: 14.8, BandwidthAvgMBps: 59.3, LatencyMedianMs: 6.2}},
					{Operation: "STAT", ConfiguredShare: &configuredStat, ActualShare: &actualStat, ActualOps: 302, Metrics: PhaseMetrics{SuccessCount: 301, FailureCount: 1, DataBytes: 0, ThroughputAvgOps: 10.0, BandwidthAvgMBps: 0, LatencyMedianMs: 3.1}},
					{Operation: "CREATE", ConfiguredShare: &configuredCreate, ActualShare: &actualCreate, ActualOps: 153, Metrics: PhaseMetrics{SuccessCount: 151, FailureCount: 2, DataBytes: 633339904, ThroughputAvgOps: 5.1, BandwidthAvgMBps: 21.1, LatencyMedianMs: 8.7}},
					{Operation: "DELETE", ConfiguredShare: &configuredDelete, ActualShare: &actualDelete, ActualOps: 107, Metrics: PhaseMetrics{SuccessCount: 103, FailureCount: 4, DataBytes: 0, ThroughputAvgOps: 3.6, BandwidthAvgMBps: 0, LatencyMedianMs: 5.2}},
				},
			},
		},
		Totals: RunTotals{DurationHuman: "30s", DataBytes: 2499805184},
	}
}

func mustContain(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("expected %q to contain %q", s, sub)
	}
}

func mustNotContain(t *testing.T, s, sub string) {
	t.Helper()
	if strings.Contains(s, sub) {
		t.Fatalf("expected %q not to contain %q", s, sub)
	}
}
