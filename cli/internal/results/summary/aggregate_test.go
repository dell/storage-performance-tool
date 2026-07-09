package summary

import (
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestFormatBytesUsesIecLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		bytes int64
		want  string
	}{
		{name: "bytes", bytes: 512, want: "512 B"},
		{name: "kib", bytes: constants.BytesPerKiB, want: "1.00 KiB"},
		{name: "mib", bytes: constants.BytesPerMiB, want: "1.00 MiB"},
		{name: "gib", bytes: constants.BytesPerGiB, want: "1.00 GiB"},
		{name: "tib", bytes: constants.BytesPerTiB, want: "1.00 TiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := formatBytes(tt.bytes); got != tt.want {
				t.Fatalf("formatBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}

func TestAggregateBuildsSummary(t *testing.T) {
	t.Parallel()

	runData := &RunData{
		RunDir:               "results/mt-20250926.172619.540",
		RunID:                "mt-20250926.172619.540",
		ManifestPath:         "results/mt-20250926.172619.540/index.json",
		MetadataPath:         "results/mt-20250926.172619.540/spt_run_params.json",
		Steps:                make(map[string]*StepData),
		StepOrder:            []string{"mt-001-20250926.172621.307-create", "mt-002-20250926.172621.307-delete"},
		Manifest:             nil,
		MissingExpectedSteps: nil,
		Params: &RunParams{
			GeneratedAt:        time.Date(2025, 9, 26, 17, 27, 28, 739494359, time.UTC),
			WorkloadType:       "write",
			SptImage:           "ghcr.io/dell/storage-performance-tool",
			APIPort:            "9999",
			BaseURL:            "http://spt-test-1.cec.delllabs.net:9999",
			Label:              "mt",
			ResultsDir:         "./results",
			ResultsRoot:        "results/mt-20250926.172619.540",
			ScenarioFile:       "spt-scenario-1758907579.js",
			ScenarioStoredPath: "spt-scenario-1758907579.js",
			ScenarioParams: ScenarioParams{
				ObjectSize:     "1MB",
				ObjectCount:    2500,
				Threads:        8,
				Bucket:         "mh-testwrites",
				Cleanup:        true,
				KeepScenario:   true,
				Endpoints:      []string{"http://10.247.70.222:9020", "http://10.247.70.223:9020"},
				SliceEndpoints: false,
			},
			Hosts: []RunHost{
				{Host: "spt-test-1.cec.delllabs.net", User: "root", Original: "root@spt-test-1.cec.delllabs.net", DockerHost: "ssh://root@spt-test-1.cec.delllabs.net"},
			},
			ExpectedStepIDs:   []string{"mt-001-20250926.172621.307-create", "mt-002-20250926.172621.307-delete"},
			ActualStepIDs:     []string{"mt-001-20250926.172621.307-create", "mt-002-20250926.172621.307-delete"},
			DiscoveredStepIDs: []string{"mt-001-20250926.172621.307-create", "mt-002-20250926.172621.307-delete"},
			ResultsOptions: RunResultsOptions{
				AutoResults:           true,
				ResultsDir:            "./results",
				Label:                 "mt",
				Debug:                 false,
				ShutdownOnComplete:    true,
				ShutdownLingerSeconds: 5,
			},
		},
	}

	runData.Steps["mt-001-20250926.172621.307-create"] = &StepData{
		StepID: "mt-001-20250926.172621.307-create",
		Status: StepStatusComplete,
		Metrics: &MetricsTotals{
			StepID: "mt-001-20250926.172621.307-create",
			Rows: []MetricsTotalsRow{
				{
					Operation:           "CREATE",
					SuccessCount:        2499,
					FailureCount:        0,
					SizeBytes:           2620391424,
					StepDurationSeconds: 7.554,
					ThroughputAvgOps:    334.6964285714,
					ThroughputLastOps:   320.2810720618962,
					BandwidthAvgMiBps:   334.6964285714,
					BandwidthLastMiBps:  320.2810720618962,
					LatencyAvgMicros:    79900,
					LatencyP50Micros:    79876,
					Concurrency:         8,
					ConcurrencyMean:     8,
					NodeCount:           4,
					SampleTimestamp:     "2025-09-26T17:27:00Z",
				},
			},
		},
	}

	runData.Steps["mt-002-20250926.172621.307-delete"] = &StepData{
		StepID: "mt-002-20250926.172621.307-delete",
		Status: StepStatusComplete,
		Metrics: &MetricsTotals{
			StepID: "mt-002-20250926.172621.307-delete",
			Rows: []MetricsTotalsRow{
				{
					Operation:           "DELETE",
					SuccessCount:        2454,
					FailureCount:        0,
					SizeBytes:           0,
					StepDurationSeconds: 2.18,
					ThroughputAvgOps:    1227.0,
					ThroughputLastOps:   1144.0,
					BandwidthAvgMiBps:   0,
					BandwidthLastMiBps:  0,
					LatencyAvgMicros:    23400,
					LatencyP50Micros:    23000,
					Concurrency:         8,
					ConcurrencyMean:     8,
					NodeCount:           4,
					SampleTimestamp:     "2025-09-26T17:27:18Z",
				},
			},
		},
	}

	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	if summary == nil {
		t.Fatalf("Aggregate returned nil summary")
	}
	if summary.Workload.ObjectSizeBytes != 1<<20 {
		t.Fatalf("object size bytes = %d, want 1048576", summary.Workload.ObjectSizeBytes)
	}
	if len(summary.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(summary.Steps))
	}
	create := summary.Steps[0]
	if create.PhaseLabel != "Create" {
		t.Fatalf("phase label = %q, want Create", create.PhaseLabel)
	}
	if create.Operation != "CREATE" {
		t.Fatalf("operation = %q, want CREATE", create.Operation)
	}
	if create.Metrics == nil {
		t.Fatalf("create metrics missing")
	}
	if !approxEqual(create.Metrics.DataGiB, 2.44, 0.01) {
		t.Fatalf("create data GiB = %.2f", create.Metrics.DataGiB)
	}
	if !approxEqual(summary.Totals.DurationSeconds, 9.734, 0.001) {
		t.Fatalf("totals duration seconds = %.3f", summary.Totals.DurationSeconds)
	}
	if !approxEqual(summary.Totals.DataGiB, 2.44, 0.01) {
		t.Fatalf("totals data GiB = %.2f", summary.Totals.DataGiB)
	}
	if len(summary.Warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", summary.Warnings)
	}
}

func TestAggregateHandlesMissingMetrics(t *testing.T) {
	t.Parallel()

	runData := &RunData{
		RunID:        "run-1",
		StepOrder:    []string{"step-1"},
		Steps:        make(map[string]*StepData),
		Params:       &RunParams{},
		ManifestPath: "index.json",
		MetadataPath: "params.json",
	}
	runData.Params.ScenarioParams.ObjectSize = "1MB"

	runData.Steps["step-1"] = &StepData{
		StepID:          "step-1",
		Status:          StepStatusPartial,
		MissingRequired: []string{"metrics.total.csv"},
	}

	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	if len(summary.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(summary.Steps))
	}
	if summary.Steps[0].Metrics != nil {
		t.Fatalf("expected nil metrics when missing")
	}
	if len(summary.Warnings) == 0 {
		t.Fatalf("expected warning about missing metrics")
	}
	if !strings.Contains(summary.Warnings[0], "metrics unavailable") {
		t.Fatalf("warning %q does not mention metrics unavailable", summary.Warnings[0])
	}
}

func TestAggregateSuppressesMissingMetricsWarningForSuppressedStep(t *testing.T) {
	t.Parallel()

	runData := &RunData{
		RunID:        "run-1",
		StepOrder:    []string{"mt-001-20260604.195722.504-seed"},
		Steps:        make(map[string]*StepData),
		Params:       &RunParams{},
		ManifestPath: "index.json",
		MetadataPath: "params.json",
	}
	runData.Params.ScenarioParams.ObjectSize = "4k"
	runData.Steps["mt-001-20260604.195722.504-seed"] = &StepData{
		StepID:            "mt-001-20260604.195722.504-seed",
		Status:            StepStatusComplete,
		MetricsSuppressed: true,
	}

	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	if len(summary.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(summary.Steps))
	}
	if summary.Steps[0].Metrics != nil {
		t.Fatalf("expected nil metrics for suppressed step")
	}
	if len(summary.Warnings) != 0 {
		t.Fatalf("expected no warnings for suppressed metrics, got %v", summary.Warnings)
	}
}

func TestAggregateListWorkloadOmitsObjectSize(t *testing.T) {
	t.Parallel()

	stepID := "mt-001-20250930.120000.000-list"
	runData := &RunData{
		RunID:     "mt-20250930.120000.000",
		StepOrder: []string{stepID},
		Steps:     make(map[string]*StepData),
		Params: &RunParams{
			WorkloadType: "list",
			ScenarioParams: ScenarioParams{
				WorkloadType: "list",
				Bucket:       "reports",
				Prefix:       "daily/",
				Threads:      1,
			},
		},
	}

	runData.Steps[stepID] = &StepData{
		StepID: stepID,
		Status: StepStatusComplete,
		Metrics: &MetricsTotals{
			StepID: stepID,
			Rows: []MetricsTotalsRow{
				{
					Operation:           "LIST",
					SuccessCount:        1234,
					FailureCount:        0,
					SizeBytes:           0,
					StepDurationSeconds: 12.5,
					ThroughputAvgOps:    98.7,
					LatencyAvgMicros:    4500,
					LatencyP50Micros:    3200,
					Concurrency:         1,
					ConcurrencyMean:     1,
					NodeCount:           1,
					SampleTimestamp:     "2025-09-30T12:00:12Z",
				},
			},
		},
	}

	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	if summary == nil {
		t.Fatal("Aggregate returned nil summary")
	}
	if summary.Workload.Type != "list" {
		t.Fatalf("workload type = %q, want list", summary.Workload.Type)
	}
	if summary.Workload.ObjectSizeHuman != "" {
		t.Fatalf("expected empty object size, got %q", summary.Workload.ObjectSizeHuman)
	}
	if summary.Workload.Prefix != "daily/" {
		t.Fatalf("prefix = %q, want daily/", summary.Workload.Prefix)
	}
	if len(summary.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(summary.Steps))
	}
	listStep := summary.Steps[0]
	if listStep.PhaseLabel != "List" {
		t.Fatalf("phase label = %q, want List", listStep.PhaseLabel)
	}
	if listStep.Operation != "LIST" {
		t.Fatalf("operation = %q, want LIST", listStep.Operation)
	}
	if listStep.Metrics == nil {
		t.Fatal("metrics missing for list step")
	}
	if listStep.Metrics.ObjectSizeHuman != "" {
		t.Fatalf("expected empty per-step object size, got %q", listStep.Metrics.ObjectSizeHuman)
	}
}

func TestAggregateBuildsMixedSummary(t *testing.T) {
	t.Parallel()

	stepID := "mt-002-20260604.180001.000-mixed"
	runData := &RunData{
		RunID:     "mt-20260604.180000.000",
		StepOrder: []string{stepID},
		Steps:     make(map[string]*StepData),
		Params: &RunParams{
			WorkloadType: "mixed",
			ScenarioParams: ScenarioParams{
				WorkloadType:  "mixed",
				ObjectSize:    "4MB",
				Duration:      "30s",
				GetDistrib:    45,
				StatDistrib:   30,
				PutDistrib:    15,
				DeleteDistrib: 10,
			},
		},
	}

	runData.Steps[stepID] = &StepData{
		StepID: stepID,
		Status: StepStatusComplete,
		Metrics: &MetricsTotals{
			StepID: stepID,
			Rows: []MetricsTotalsRow{
				{Operation: "READ", SuccessCount: 445, FailureCount: 5, SizeBytes: 1866465280, StepDurationSeconds: 30, ThroughputAvgOps: 14.8, ThroughputLastOps: 13.9, BandwidthAvgMiBps: 59.3, BandwidthLastMiBps: 55.6, LatencyAvgMicros: 8100, LatencyP50Micros: 6200, Concurrency: 8, ConcurrencyMean: 8, NodeCount: 4, SampleTimestamp: "2026-06-04T18:00:10Z"},
				{Operation: "STAT", SuccessCount: 301, FailureCount: 1, SizeBytes: 0, StepDurationSeconds: 30, ThroughputAvgOps: 10.0, ThroughputLastOps: 9.5, BandwidthAvgMiBps: 0, BandwidthLastMiBps: 0, LatencyAvgMicros: 4200, LatencyP50Micros: 3100, Concurrency: 8, ConcurrencyMean: 8, NodeCount: 4, SampleTimestamp: "2026-06-04T18:00:10Z"},
				{Operation: "CREATE", SuccessCount: 151, FailureCount: 2, SizeBytes: 633339904, StepDurationSeconds: 30, ThroughputAvgOps: 5.1, ThroughputLastOps: 4.8, BandwidthAvgMiBps: 21.1, BandwidthLastMiBps: 20.0, LatencyAvgMicros: 11200, LatencyP50Micros: 8700, Concurrency: 8, ConcurrencyMean: 8, NodeCount: 4, SampleTimestamp: "2026-06-04T18:00:10Z"},
				{Operation: "DELETE", SuccessCount: 103, FailureCount: 4, SizeBytes: 0, StepDurationSeconds: 30, ThroughputAvgOps: 3.6, ThroughputLastOps: 3.1, BandwidthAvgMiBps: 0, BandwidthLastMiBps: 0, LatencyAvgMicros: 7300, LatencyP50Micros: 5200, Concurrency: 8, ConcurrencyMean: 8, NodeCount: 4, SampleTimestamp: "2026-06-04T18:00:10Z"},
			},
		},
	}

	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	if summary == nil {
		t.Fatal("Aggregate returned nil summary")
	}
	if len(summary.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(summary.Steps))
	}
	mixed := summary.Steps[0]
	if !mixed.IsMixed {
		t.Fatal("expected mixed step")
	}
	if mixed.PhaseLabel != "Mixed" {
		t.Fatalf("phase label = %q, want Mixed", mixed.PhaseLabel)
	}
	if mixed.Operation != "MIXED" {
		t.Fatalf("operation = %q, want MIXED", mixed.Operation)
	}
	if mixed.Metrics == nil {
		t.Fatal("mixed metrics missing")
	}
	if got := mixed.Metrics.DurationSeconds; got != 30 {
		t.Fatalf("mixed duration seconds = %.2f, want 30", got)
	}
	if got := mixed.Metrics.SuccessCount; got != 1000 {
		t.Fatalf("mixed success count = %d, want 1000", got)
	}
	if got := mixed.Metrics.FailureCount; got != 12 {
		t.Fatalf("mixed failure count = %d, want 12", got)
	}
	if got := mixed.Metrics.DataBytes; got != 2499805184 {
		t.Fatalf("mixed data bytes = %d", got)
	}
	if !approxEqual(mixed.Metrics.ThroughputAvgOps, 33.5, 0.001) {
		t.Fatalf("mixed avg throughput = %.3f", mixed.Metrics.ThroughputAvgOps)
	}
	if !approxEqual(mixed.Metrics.ThroughputLastOps, 31.3, 0.001) {
		t.Fatalf("mixed last throughput = %.3f", mixed.Metrics.ThroughputLastOps)
	}
	if !approxEqual(mixed.Metrics.BandwidthAvgMiBps, 80.4, 0.001) {
		t.Fatalf("mixed avg bandwidth = %.3f", mixed.Metrics.BandwidthAvgMiBps)
	}
	if !approxEqual(mixed.Metrics.BandwidthLastMiBps, 75.6, 0.001) {
		t.Fatalf("mixed last bandwidth = %.3f", mixed.Metrics.BandwidthLastMiBps)
	}
	if !approxEqual(summary.Totals.DurationSeconds, 30, 0.001) {
		t.Fatalf("totals duration seconds = %.3f, want 30", summary.Totals.DurationSeconds)
	}
	if got := summary.Totals.DataBytes; got != 2499805184 {
		t.Fatalf("totals data bytes = %d", got)
	}
	if len(mixed.OperationBreakdown) != 4 {
		t.Fatalf("expected 4 breakdown rows, got %d", len(mixed.OperationBreakdown))
	}
	gotOps := []string{
		mixed.OperationBreakdown[0].Operation,
		mixed.OperationBreakdown[1].Operation,
		mixed.OperationBreakdown[2].Operation,
		mixed.OperationBreakdown[3].Operation,
	}
	if strings.Join(gotOps, ",") != "READ,STAT,CREATE,DELETE" {
		t.Fatalf("unexpected breakdown order: %v", gotOps)
	}
	if mixed.OperationBreakdown[0].ConfiguredShare == nil || *mixed.OperationBreakdown[0].ConfiguredShare != 45 {
		t.Fatalf("read configured share = %v", mixed.OperationBreakdown[0].ConfiguredShare)
	}
	if mixed.OperationBreakdown[1].ConfiguredShare == nil || *mixed.OperationBreakdown[1].ConfiguredShare != 30 {
		t.Fatalf("stat configured share = %v", mixed.OperationBreakdown[1].ConfiguredShare)
	}
	if mixed.OperationBreakdown[2].ConfiguredShare == nil || *mixed.OperationBreakdown[2].ConfiguredShare != 15 {
		t.Fatalf("create configured share = %v", mixed.OperationBreakdown[2].ConfiguredShare)
	}
	if mixed.OperationBreakdown[3].ConfiguredShare == nil || *mixed.OperationBreakdown[3].ConfiguredShare != 10 {
		t.Fatalf("delete configured share = %v", mixed.OperationBreakdown[3].ConfiguredShare)
	}
	if got := mixed.OperationBreakdown[0].ActualOps; got != 450 {
		t.Fatalf("read actual ops = %d, want 450", got)
	}
	if !approxEqual(*mixed.OperationBreakdown[0].ActualShare, 44.47, 0.01) {
		t.Fatalf("read actual share = %.4f, want about 44.47", *mixed.OperationBreakdown[0].ActualShare)
	}
	if !approxEqual(*mixed.OperationBreakdown[3].ActualShare, 10.57, 0.01) {
		t.Fatalf("delete actual share = %.4f, want about 10.57", *mixed.OperationBreakdown[3].ActualShare)
	}
	var shareSum float64
	for _, row := range mixed.OperationBreakdown {
		if row.ActualShare == nil {
			t.Fatalf("actual share missing for %s", row.Operation)
		}
		shareSum += *row.ActualShare
	}
	if !approxEqual(shareSum, 100, 0.01) {
		t.Fatalf("actual share sum = %.4f", shareSum)
	}
	if mixed.MixedLatencyNote == "" {
		t.Fatal("expected mixed latency note")
	}
	if mixed.Metrics.LatencyMedianMs != 0 {
		t.Fatalf("expected no combined latency median, got %.3f", mixed.Metrics.LatencyMedianMs)
	}
	if len(summary.Warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", summary.Warnings)
	}
}

func TestAggregateBuildsMixedSummaryFromScenarioWorkloadType(t *testing.T) {
	t.Parallel()

	stepID := "mt-002-20260604.180001.000-mixed"
	runData := &RunData{
		RunID:     "mt-20260604.180000.000",
		StepOrder: []string{stepID},
		Steps:     make(map[string]*StepData),
		Params: &RunParams{
			ScenarioParams: ScenarioParams{
				WorkloadType: "mixed",
				ObjectSize:   "4MB",
				Duration:     "30s",
			},
		},
	}

	runData.Steps[stepID] = &StepData{
		StepID: stepID,
		Status: StepStatusComplete,
		Metrics: &MetricsTotals{
			StepID: stepID,
			Rows: []MetricsTotalsRow{
				{Operation: "READ", SuccessCount: 10, SizeBytes: 40960, StepDurationSeconds: 30, ThroughputAvgOps: 0.3, BandwidthAvgMiBps: 0.2, LatencyAvgMicros: 4000, LatencyP50Micros: 3000},
				{Operation: "CREATE", SuccessCount: 5, SizeBytes: 20480, StepDurationSeconds: 30, ThroughputAvgOps: 0.2, BandwidthAvgMiBps: 0.1, LatencyAvgMicros: 5000, LatencyP50Micros: 3500},
			},
		},
	}

	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	if summary.Workload.Type != "mixed" {
		t.Fatalf("workload type = %q, want mixed", summary.Workload.Type)
	}
	if len(summary.Steps) != 1 || !summary.Steps[0].IsMixed {
		t.Fatalf("expected mixed step summary, got %+v", summary.Steps)
	}
}

func TestAggregateMixedSummaryHidesZeroConfiguredDistribution(t *testing.T) {
	t.Parallel()

	stepID := "mt-002-20260604.180001.000-mixed"
	runData := &RunData{
		RunID:     "mt-20260604.180000.000",
		StepOrder: []string{stepID},
		Steps:     make(map[string]*StepData),
		Params: &RunParams{
			WorkloadType: "mixed",
			ScenarioParams: ScenarioParams{
				WorkloadType: "mixed",
				ObjectSize:   "4MB",
				Duration:     "30s",
			},
		},
	}

	runData.Steps[stepID] = &StepData{
		StepID: stepID,
		Status: StepStatusComplete,
		Metrics: &MetricsTotals{
			StepID: stepID,
			Rows: []MetricsTotalsRow{
				{Operation: "READ", SuccessCount: 12, SizeBytes: 49152, StepDurationSeconds: 30, ThroughputAvgOps: 0.4, BandwidthAvgMiBps: 0.2, LatencyAvgMicros: 4200, LatencyP50Micros: 3100},
				{Operation: "STAT", SuccessCount: 8, SizeBytes: 0, StepDurationSeconds: 30, ThroughputAvgOps: 0.3, BandwidthAvgMiBps: 0, LatencyAvgMicros: 2800, LatencyP50Micros: 2000},
			},
		},
	}

	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	if summary.Workload.MixedDistribution.Available {
		t.Fatalf("expected mixed distribution to be unavailable when all values are zero")
	}
	for _, row := range summary.Steps[0].OperationBreakdown {
		if row.ConfiguredShare != nil {
			t.Fatalf("expected nil configured share for %s, got %v", row.Operation, *row.ConfiguredShare)
		}
	}
}

func TestAggregateBuildsMixedSummaryOrdersKnownOpsBeforeUnknowns(t *testing.T) {
	t.Parallel()

	stepID := "mt-002-20260604.180001.000-mixed"
	runData := &RunData{
		RunID:     "mt-20260604.180000.000",
		StepOrder: []string{stepID},
		Steps:     make(map[string]*StepData),
		Params: &RunParams{
			WorkloadType: "mixed",
			ScenarioParams: ScenarioParams{
				WorkloadType: "mixed",
				ObjectSize:   "4MB",
				Duration:     "30s",
			},
		},
	}

	runData.Steps[stepID] = &StepData{
		StepID: stepID,
		Status: StepStatusComplete,
		Metrics: &MetricsTotals{
			StepID: stepID,
			Rows: []MetricsTotalsRow{
				{Operation: "DELETE", SuccessCount: 5, StepDurationSeconds: 30, ThroughputAvgOps: 0.1, LatencyAvgMicros: 3000, LatencyP50Micros: 2000},
				{Operation: "LIST", SuccessCount: 3, StepDurationSeconds: 30, ThroughputAvgOps: 0.1, LatencyAvgMicros: 2000, LatencyP50Micros: 1500},
				{Operation: "CREATE", SuccessCount: 7, SizeBytes: 28672, StepDurationSeconds: 30, ThroughputAvgOps: 0.2, BandwidthAvgMiBps: 0.1, LatencyAvgMicros: 5000, LatencyP50Micros: 3500},
				{Operation: "READ", SuccessCount: 11, SizeBytes: 45056, StepDurationSeconds: 30, ThroughputAvgOps: 0.3, BandwidthAvgMiBps: 0.2, LatencyAvgMicros: 4000, LatencyP50Micros: 3000},
				{Operation: "STAT", SuccessCount: 9, StepDurationSeconds: 30, ThroughputAvgOps: 0.2, LatencyAvgMicros: 2500, LatencyP50Micros: 1800},
			},
		},
	}

	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	gotOps := make([]string, 0, len(summary.Steps[0].OperationBreakdown))
	for _, row := range summary.Steps[0].OperationBreakdown {
		gotOps = append(gotOps, row.Operation)
	}
	if strings.Join(gotOps, ",") != "READ,STAT,CREATE,DELETE,LIST" {
		t.Fatalf("unexpected operation order: %v", gotOps)
	}
}

func TestPhaseLabelFromStepHandlesMixedCleanupSteps(t *testing.T) {
	t.Parallel()

	if got := phaseLabelFromStep("mt-003-20260604.180001.000-cleanup-seed"); got != "Cleanup seed" {
		t.Fatalf("cleanup-seed label = %q", got)
	}
	if got := phaseLabelFromStep("mt-004-20260604.180001.000-cleanup-put"); got != "Cleanup CREATE" {
		t.Fatalf("cleanup-put label = %q", got)
	}
	if got := phaseLabelFromStep("mt-002-20260604.180001.000-mixed"); got != "Mixed" {
		t.Fatalf("mixed label = %q", got)
	}
}

func TestParseSizeString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  int64
		ok    bool
	}{
		{"1MB", 1 << 20, true},
		{"2.5GB", int64(2.5 * float64(1<<30)), true},
		{"512", 512, true},
		{"10kB", 10 * 1024, true},
		{"1tb", int64(1) << 40, true},
		{"", 0, true},
		{"invalid", 0, false},
	}

	for _, tc := range tests {
		got, err := parseSizeString(tc.input)
		if tc.ok && err != nil {
			t.Fatalf("parseSizeString(%q) returned error %v", tc.input, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("parseSizeString(%q) expected error", tc.input)
		}
		if tc.ok && !approxEqual(float64(got), float64(tc.want), float64(tc.want)*0.001+1) {
			t.Fatalf("parseSizeString(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestFormatSeconds(t *testing.T) {
	t.Parallel()

	cases := map[float64]string{
		0:      "0s",
		0.25:   "250ms",
		7.554:  "7.55s",
		60:     "1m",
		75.3:   "1m 15.3s",
		3665.2: "1h 1m 5.2s",
	}
	for input, want := range cases {
		if got := formatSeconds(input); got != want {
			t.Fatalf("formatSeconds(%.3f) = %q, want %q", input, got, want)
		}
	}
}

func approxEqual(got, want, delta float64) bool {
	if got > want {
		return got-want <= delta
	}
	return want-got <= delta
}
