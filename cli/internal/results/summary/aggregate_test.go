package summary

import (
	"strings"
	"testing"
	"time"
)

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
			SptImage:           "dcr.octo.dell.com/spt/spt",
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
					BandwidthAvgMBps:    334.6964285714,
					BandwidthLastMBps:   320.2810720618962,
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
					BandwidthAvgMBps:    0,
					BandwidthLastMBps:   0,
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
