package summary

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
)

const (
	rdmaReadHeading   = "RDMA Data Path — Read"
	rdmaNothingMoved  = "Read: the S3-RDMA driver ran and no payload moved over RDMA"
	rdmaCaptureReason = "/metrics/fleet/json driver statistics step \"mt-001-create\": fleet total covers 1 of 2 contributors"
)

func rdmaStepStats(counters map[string]int64) *driverstats.Stats {
	return &driverstats.Stats{Driver: driverstats.DriverS3RDMA, Counters: counters, Reporting: 1}
}

// rdmaReadSummary is a one-step read run whose step carries the given driver statistics.
func rdmaReadSummary(stats *driverstats.Stats) *RunSummary {
	summary := &RunSummary{
		RunID:    "mt-20261013.100000.000",
		Workload: WorkloadSummary{Type: "read", S3Driver: "rdma"},
		Steps: []StepSummary{{
			StepID:      "mt-001-20261013.100000.000-read",
			PhaseLabel:  "Read",
			Operation:   "READ",
			Metrics:     &PhaseMetrics{SuccessCount: 500, DataBytes: 524288000, ThroughputAvgOps: 50},
			DriverStats: stats,
		}},
		Totals: RunTotals{DurationHuman: "10s", DataBytes: 524288000},
	}
	summary.Warnings = rdmaDataPathWarnings(summary.Steps, summary.Workload, "")
	return summary
}

// rdmaReports renders the summary in both forms that show the RDMA data path.
func rdmaReports(summary *RunSummary) map[string]string {
	return map[string]string{
		"full":    NewRenderer(RenderOptions{}).FullReport(summary),
		"compact": NewRenderer(RenderOptions{MaxWidth: 200}).CompactSnippet(summary),
	}
}

// rdmaSection returns the lines of the report from each RDMA data path heading to the blank
// line that ends it.
func rdmaSection(report string) string {
	var section []string
	inside := false
	for _, line := range strings.Split(report, "\n") {
		switch {
		case strings.HasPrefix(line, rdmaDataPathTitle):
			inside = true
		case strings.TrimSpace(line) == "":
			inside = false
		}
		if inside {
			section = append(section, line)
		}
	}
	return strings.Join(section, "\n")
}

func TestRendererShowsRDMADataPathCounts(t *testing.T) {
	tests := map[string]struct {
		counters     map[string]int64
		wantLine     string
		wantWarnings []string
	}{
		"every payload over RDMA": {
			counters: map[string]int64{"rdma_requests_sent": 500, "rdma_transferred": 500},
			wantLine: "500 transfers reported over RDMA, 0 payload requests sent over HTTP",
		},
		"both paths": {
			counters: map[string]int64{
				"rdma_requests_sent": 403, "rdma_transferred": 400, "rdma_declined_served_over_http": 3,
				"http_below_threshold": 90, "http_ineligible": 7,
			},
			wantLine: "400 transfers reported over RDMA, 100 payload requests sent over HTTP",
		},
		"nothing over RDMA": {
			counters:     map[string]int64{"http_rdma_unavailable": 500},
			wantLine:     "0 transfers reported over RDMA, 500 payload requests sent over HTTP",
			wantWarnings: []string{rdmaNothingMoved},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			summary := rdmaReadSummary(rdmaStepStats(tc.counters))
			if !reflect.DeepEqual(summary.Warnings, tc.wantWarnings) {
				t.Fatalf("warnings = %q, want %q", summary.Warnings, tc.wantWarnings)
			}
			for form, report := range rdmaReports(summary) {
				section := rdmaSection(report)
				if !strings.Contains(section, rdmaReadHeading) || !strings.Contains(section, tc.wantLine) {
					t.Fatalf("%s report lacks %q under %q:\n%s", form, tc.wantLine, rdmaReadHeading, report)
				}
				if strings.Contains(section, rdmaProblemLabel) {
					t.Fatalf("%s report shows a problem line without a problem:\n%s", form, section)
				}
				for _, warning := range tc.wantWarnings {
					mustContain(t, report, warning)
				}
			}
		})
	}
}

func TestRendererShowsEachRDMAProblemOnItsOwnLine(t *testing.T) {
	problems := map[string]string{
		"rdma_declined_failed":          "3 RDMA requests declined by the server and failed",
		"rdma_request_failed":           "3 RDMA requests ended in failure",
		"rdma_protocol_error":           "3 RDMA replies broke the reply contract",
		"rdma_timed_out":                "3 RDMA requests timed out",
		"prepare_failed":                "3 attempts failed in RDMA preparation",
		"registration_failed_pooled":    "3 pooled buffer registrations failed",
		"registration_failed_on_demand": "3 per-operation buffer registrations failed",
		"allocation_failed_pooled":      "3 pooled buffer allocations failed",
		"allocation_failed_on_demand":   "3 per-operation buffer allocations failed",
		"token_generation_failed":       "3 RDMA token generations failed",
		"rdma_requests_sent":            "3 RDMA requests had no outcome when the step ended",
	}
	for counter, wantLine := range problems {
		t.Run(counter, func(t *testing.T) {
			summary := rdmaReadSummary(rdmaStepStats(map[string]int64{counter: 3}))
			for form, report := range rdmaReports(summary) {
				section := rdmaSection(report)
				if !strings.Contains(section, wantLine) {
					t.Fatalf("%s report lacks %q:\n%s", form, wantLine, report)
				}
				if got := strings.Count(section, rdmaProblemLabel); got != 1 {
					t.Fatalf("%s report shows %d problem lines, want 1:\n%s", form, got, section)
				}
			}
		})
	}
}

func TestRendererShowsRDMADataPathOnceForAMixedStep(t *testing.T) {
	summary := mixedSummaryFixture()
	summary.Steps[0].DriverStats = rdmaStepStats(map[string]int64{
		"rdma_requests_sent": 600, "rdma_transferred": 598, "rdma_timed_out": 2, "http_below_threshold": 4,
	})
	const heading = "RDMA Data Path — Mixed (all payload operations of the step)"
	for form, report := range rdmaReports(summary) {
		if got := strings.Count(report, rdmaDataPathTitle); got != 1 {
			t.Fatalf("%s report shows the RDMA data path %d times, want once at step level:\n%s", form, got, report)
		}
		section := rdmaSection(report)
		for _, want := range []string{
			heading,
			"598 transfers reported over RDMA, 4 payload requests sent over HTTP",
			"2 RDMA requests timed out",
		} {
			if !strings.Contains(section, want) {
				t.Fatalf("%s report lacks %q:\n%s", form, want, report)
			}
		}
		breakdown := report[strings.Index(report, "Mixed Operation Breakdown"):]
		if before := strings.Index(report, heading); before > strings.Index(report, "Mixed Operation Breakdown") {
			t.Fatalf("%s report places the RDMA data path inside the operation breakdown:\n%s", form, report)
		}
		for _, operationRow := range []string{"READ", "CREATE"} {
			for _, line := range strings.Split(breakdown, "\n") {
				if strings.Contains(line, operationRow) && strings.Contains(line, "RDMA") {
					t.Fatalf("%s report places RDMA counts under %s:\n%s", form, operationRow, report)
				}
			}
		}
	}
}

func TestRendererShowsNoRDMADataPathWithoutSomethingToReport(t *testing.T) {
	tests := map[string]*driverstats.Stats{
		"no statistics":            nil,
		"another driver":           {Driver: "test-driver", Counters: map[string]int64{"rdma_transferred": 9}},
		"step without any payload": rdmaStepStats(map[string]int64{"rdma_transferred": 0, "pool_hits": 0}),
	}
	for name, stats := range tests {
		t.Run(name, func(t *testing.T) {
			summary := rdmaReadSummary(stats)
			if len(summary.Warnings) != 0 {
				t.Fatalf("warnings = %q, want none", summary.Warnings)
			}
			for form, report := range rdmaReports(summary) {
				if strings.Contains(report, "RDMA") {
					t.Fatalf("%s report mentions RDMA:\n%s", form, report)
				}
			}
		})
	}
}

func TestRDMADataPathWarnsOnlyWhenStatisticsOfASelectedDriverWereNotStored(t *testing.T) {
	tests := map[string]struct {
		s3Driver     string
		captureError string
		want         []string
	}{
		"refused by the capture":           {"rdma", rdmaCaptureReason, []string{"S3-RDMA driver statistics are not available: " + rdmaCaptureReason}},
		"custom scenario":                  {"", rdmaCaptureReason, nil},
		"another driver":                   {"aws", rdmaCaptureReason, nil},
		"results without the new keys":     {"rdma", "", nil},
		"older results without a driver":   {"", "", nil},
		"reason of only spaces":            {"rdma", "  ", nil},
		"selection in another letter case": {"RDMA", rdmaCaptureReason, []string{"S3-RDMA driver statistics are not available: " + rdmaCaptureReason}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			summary := rdmaReadSummary(nil)
			summary.Workload.S3Driver = tc.s3Driver
			summary.Warnings = rdmaDataPathWarnings(summary.Steps, summary.Workload, tc.captureError)
			if !reflect.DeepEqual(summary.Warnings, tc.want) {
				t.Fatalf("warnings = %q, want %q", summary.Warnings, tc.want)
			}
			for form, report := range rdmaReports(summary) {
				if strings.Contains(report, rdmaDataPathTitle) {
					t.Fatalf("%s report shows an RDMA data path without statistics:\n%s", form, report)
				}
				for _, warning := range tc.want {
					mustContain(t, report, warning)
				}
				if len(tc.want) == 0 && strings.Contains(report, "RDMA") {
					t.Fatalf("%s report mentions RDMA:\n%s", form, report)
				}
			}
		})
	}
}

func TestRDMADataPathRendersNoPercentage(t *testing.T) {
	everyCounter := map[string]int64{
		"rdma_requests_sent": 1000, "rdma_transferred": 600, "rdma_declined_served_over_http": 10,
		"rdma_declined_failed": 11, "rdma_request_failed": 12, "rdma_protocol_error": 13, "rdma_timed_out": 14,
		"http_rdma_unavailable": 15, "http_below_threshold": 16, "http_ineligible": 17, "http_oversize": 18,
		"http_fallback": 19, "prepare_failed": 20, "registration_failed_pooled": 21,
		"registration_failed_on_demand": 22, "allocation_failed_pooled": 23, "allocation_failed_on_demand": 24,
		"token_generation_failed": 25,
	}
	summaries := map[string]*RunSummary{
		"statistics":           rdmaReadSummary(rdmaStepStats(everyCounter)),
		"nothing over RDMA":    rdmaReadSummary(rdmaStepStats(map[string]int64{"http_below_threshold": 7})),
		"statistics refused":   rdmaReadSummary(nil),
		"statistics absent":    rdmaReadSummary(nil),
		"mixed step":           mixedSummaryFixture(),
		"step without payload": rdmaReadSummary(rdmaStepStats(nil)),
	}
	summaries["statistics refused"].Warnings = rdmaDataPathWarnings(
		summaries["statistics refused"].Steps, summaries["statistics refused"].Workload, rdmaCaptureReason)
	summaries["mixed step"].Steps[0].DriverStats = rdmaStepStats(everyCounter)
	summaries["mixed step"].Warnings = rdmaDataPathWarnings(
		summaries["mixed step"].Steps, summaries["mixed step"].Workload, "")
	for name, summary := range summaries {
		t.Run(name, func(t *testing.T) {
			for _, warning := range summary.Warnings {
				if strings.Contains(warning, "%") {
					t.Fatalf("warning carries a percentage: %q", warning)
				}
			}
			for form, report := range rdmaReports(summary) {
				if section := rdmaSection(report); strings.Contains(section, "%") {
					t.Fatalf("%s report renders a percentage for driver statistics:\n%s", form, section)
				}
			}
		})
	}
}

// The remaining tests go through the stored results, as the summary command does.

func TestStoredDriverStatsReachTheRenderedRDMADataPath(t *testing.T) {
	runDir := driverStatsRun(t, map[string]any{
		"workloadType":    "write",
		"expectedStepIds": []string{driverStatsStepID},
		"scenarioParams":  map[string]any{"WorkloadType": "write", "S3Driver": "rdma"},
		"driverStats": map[string]any{
			driverStatsStepID: map[string]any{
				"driver":    "s3-rdma",
				"counters":  map[string]any{"rdma_requests_sent": 100, "rdma_transferred": 97, "rdma_timed_out": 3},
				"reporting": 1,
			},
		},
	})
	aggregated := aggregateStoredRun(t, runDir)
	if len(aggregated.Warnings) != 0 {
		t.Fatalf("warnings = %q, want none", aggregated.Warnings)
	}
	for form, report := range rdmaReports(aggregated) {
		section := rdmaSection(report)
		for _, want := range []string{
			"RDMA Data Path — Create",
			"97 transfers reported over RDMA, 0 payload requests sent over HTTP",
			"3 RDMA requests timed out",
		} {
			if !strings.Contains(section, want) {
				t.Fatalf("%s report lacks %q:\n%s", form, want, report)
			}
		}
	}
}

func TestStoredRunWarnsWhenNoPayloadMovedOverRDMA(t *testing.T) {
	runDir := driverStatsRun(t, map[string]any{
		"workloadType":    "write",
		"expectedStepIds": []string{driverStatsStepID},
		"scenarioParams":  map[string]any{"WorkloadType": "write", "S3Driver": "rdma"},
		"driverStats": map[string]any{
			driverStatsStepID: map[string]any{
				"driver":   "s3-rdma",
				"counters": map[string]any{"http_rdma_unavailable": 100},
				"gauges":   map[string]any{"transport_available": 0},
			},
		},
	})
	aggregated := aggregateStoredRun(t, runDir)
	want := []string{"Create: the S3-RDMA driver ran and no payload moved over RDMA"}
	if !reflect.DeepEqual(aggregated.Warnings, want) {
		t.Fatalf("warnings = %q, want %q", aggregated.Warnings, want)
	}
	for form, report := range rdmaReports(aggregated) {
		mustContain(t, report, want[0])
		if !strings.Contains(rdmaSection(report), "0 transfers reported over RDMA, 100 payload requests sent over HTTP") {
			t.Fatalf("%s report lacks the counts:\n%s", form, report)
		}
	}
}

func TestStoredRunWarnsWhenStatisticsOfTheS3RDMADriverWereRefused(t *testing.T) {
	runDir := driverStatsRun(t, map[string]any{
		"workloadType":     "write",
		"expectedStepIds":  []string{driverStatsStepID},
		"scenarioParams":   map[string]any{"WorkloadType": "write", "S3Driver": "rdma"},
		"driverStatsError": rdmaCaptureReason,
	})
	aggregated := aggregateStoredRun(t, runDir)
	want := []string{"S3-RDMA driver statistics are not available: " + rdmaCaptureReason}
	if !reflect.DeepEqual(aggregated.Warnings, want) {
		t.Fatalf("warnings = %q, want %q", aggregated.Warnings, want)
	}
	for form, report := range rdmaReports(aggregated) {
		mustContain(t, report, "S3-RDMA driver statistics are not available")
		if strings.Contains(report, rdmaDataPathTitle) {
			t.Fatalf("%s report shows an RDMA data path for statistics that were refused:\n%s", form, report)
		}
	}
}

func TestStoredRunsWithoutDriverStatsRenderAsBefore(t *testing.T) {
	tests := map[string]map[string]any{
		"S3-RDMA results of an older CLI": {"WorkloadType": "write", "S3Driver": "rdma"},
		"another driver":                  {"WorkloadType": "write", "S3Driver": "aws"},
		"no driver stored":                {"WorkloadType": "write"},
	}
	for name, scenarioParams := range tests {
		t.Run(name, func(t *testing.T) {
			runDir := driverStatsRun(t, map[string]any{
				"workloadType":    "write",
				"expectedStepIds": []string{driverStatsStepID},
				"scenarioParams":  scenarioParams,
			})
			aggregated := aggregateStoredRun(t, runDir)
			if len(aggregated.Warnings) != 0 {
				t.Fatalf("warnings = %q, want none", aggregated.Warnings)
			}
			for form, report := range rdmaReports(aggregated) {
				if strings.Contains(report, "RDMA") {
					t.Fatalf("%s report mentions RDMA for results without driver statistics:\n%s", form, report)
				}
			}
		})
	}
}

func TestRejectedRunHasNoRDMADataPathWarning(t *testing.T) {
	runDir := driverStatsRun(t, map[string]any{
		"workloadType":     "write",
		"expectedStepIds":  []string{driverStatsStepID},
		"scenarioParams":   map[string]any{"WorkloadType": "write", "S3Driver": "rdma"},
		"driverStatsError": rdmaCaptureReason,
		"lifecycle": map[string]any{
			"workload": map[string]any{"state": "rejected", "error": "engine build identity mismatch"},
		},
	})
	aggregated := aggregateStoredRun(t, runDir)
	if !aggregated.Rejected {
		t.Fatal("the stored run was not loaded as rejected")
	}
	for form, report := range rdmaReports(aggregated) {
		if strings.Contains(report, "RDMA") {
			t.Fatalf("%s report of a rejected run mentions RDMA:\n%s", form, report)
		}
	}
}

func aggregateStoredRun(t *testing.T, runDir string) *RunSummary {
	t.Helper()
	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	aggregated, err := Aggregate(data)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	return aggregated
}
