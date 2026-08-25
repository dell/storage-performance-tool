package summary

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

func TestLoaderLoadSuccess(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "mt-20250926.172619.540")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	manifest := makeManifest(t, runDir, []stepFixture{
		{
			ID: "mt-001-20250926.172621.307-create",
			MetricsContent: sampleMetricsCSV([]string{
				`"2025-09-26T17:27:00Z",CREATE,8,4,8,8,2499,1,2620391424,7.554,244.988254,334.69642857142856,320.2810720618962,334.69642857142856,320.2810720618962,98547.16572807723,28461,59543,79876,108873,623571,97646.28439259855,27941,58658,79128,108037,622443`,
			}),
		},
		{
			ID: "mt-002-20250926.172621.307-delete",
			MetricsContent: sampleMetricsCSV([]string{
				`"2025-09-26T17:27:18Z",DELETE,8,4,8,8,2454,0,0,2.18,20.0,1227.0,1144.0,0.0,0.0,23456,12000,18000,23000,40000,88000,20000,9000,14000,18000,30000,60000`,
			}),
		},
	})
	writeManifest(t, runDir, manifest)

	params := RunParams{
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
			WorkloadType:   "write",
			Bucket:         "mh-testwrites",
			Prefix:         "daily/",
			Threads:        8,
			ObjectSize:     "1MB",
			ObjectCount:    2500,
			Cleanup:        true,
			KeepScenario:   true,
			SliceEndpoints: false,
		},
		Hosts: []RunHost{{Host: "spt-test-1.cec.delllabs.net", User: "root"}},
		ExpectedStepIDs: []string{
			"mt-001-20250926.172621.307-create",
			"mt-002-20250926.172621.307-delete",
		},
	}
	writeParams(t, runDir, &params)

	loader := NewLoader()
	data, err := loader.Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if data == nil {
		t.Fatalf("Load returned nil data")
	}
	if data.RunID != "mt-20250926.172619.540" {
		t.Fatalf("RunID mismatch: got %q", data.RunID)
	}
	if len(data.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(data.Steps))
	}
	for _, stepID := range params.ExpectedStepIDs {
		step := data.Steps[stepID]
		if step == nil {
			t.Fatalf("step %s missing in RunData", stepID)
		}
		if step.Status != StepStatusComplete {
			t.Fatalf("step %s expected complete, got %s", stepID, step.Status)
		}
		if step.Metrics == nil || len(step.Metrics.Rows) != 1 {
			t.Fatalf("step %s expected metrics row", stepID)
		}
	}
	create := data.Steps["mt-001-20250926.172621.307-create"]
	if got := create.Metrics.Rows[0].SuccessCount; got != 2499 {
		t.Fatalf("create success count = %d", got)
	}
	if got := create.Metrics.Rows[0].BandwidthAvgMiBps; got <= 0 {
		t.Fatalf("create bandwidth avg not parsed: %f", got)
	}
	if len(data.MissingExpectedSteps) != 0 {
		t.Fatalf("unexpected missing expected steps: %v", data.MissingExpectedSteps)
	}
	if data.Params.ScenarioParams.Prefix != "daily/" {
		t.Fatalf("expected prefix 'daily/', got %q", data.Params.ScenarioParams.Prefix)
	}
}

func TestLoaderHydratesTerminalDeleteModelThroughAggregateAndRender(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "delete-run")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const stepID = "mt-001-delete"
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: stepID,
		MetricsContent: sampleMetricsCSV([]string{
			`"2026-08-23T10:00:00Z",DELETE,1,1,1,1,2,0,0,1,2,2,2,0,0,10,10,10,10,10,20,20,20,20,20,20`,
		}),
	}})
	writeManifest(t, runDir, manifest)
	stored := &deletemetrics.Metrics{
		Units:              deletemetrics.Units{Requests: deletemetrics.RequestUnit, Objects: deletemetrics.ObjectUnit, Batches: deletemetrics.RequestUnit},
		Requests:           deletemetrics.Requests{Attempted: 1, FullSuccess: 1},
		Objects:            deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 2},
		Identity:           deletemetrics.Identity{Mode: "batch", ConfiguredBatchSize: 2, SelectionOrder: "canonical"},
		OutcomeTerminology: deletemetrics.OutcomeAccepted,
		TerminalReconciled: true,
	}
	metadata, err := json.Marshal(map[string]any{
		"workloadType":    "delete",
		"expectedStepIds": []string{stepID},
		"deleteMetrics":   map[string]*deletemetrics.Metrics{stepID: stored},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsMetadataFileName), metadata, 0o644); err != nil {
		t.Fatal(err)
	}

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatal(err)
	}
	if data.Steps[stepID].Delete == nil {
		t.Fatal("loader dropped stored terminal DELETE model")
	}
	aggregated, err := Aggregate(data)
	if err != nil {
		t.Fatal(err)
	}
	report := NewRenderer(RenderOptions{}).FullReport(aggregated)
	if !strings.Contains(report, "DELETE Results") || !strings.Contains(report, "accepted 2") {
		t.Fatalf("stored terminal DELETE model did not reach rendered report:\n%s", report)
	}
}

func TestStoredListToDeleteSummaryLabelsSuccessUnitsWithoutChangingRates(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "delete-run")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	const (
		listStepID   = "mt-001-list"
		deleteStepID = "mt-002-delete"
	)
	manifest := makeManifest(t, runDir, []stepFixture{
		{
			ID: listStepID,
			MetricsContent: sampleMetricsCSV([]string{
				`"2026-08-25T10:00:00Z",LIST,1,1,1,1,200,0,0,4,4,50,50,0,0,10,10,10,10,10,20,20,20,20,20,20`,
			}),
		},
		{
			ID: deleteStepID,
			MetricsContent: sampleMetricsCSV([]string{
				`"2026-08-25T10:00:04Z",DELETE,1,1,1,1,2,0,0,4,4,0.5,0.5,0,0,10,10,10,10,10,20,20,20,20,20,20`,
			}),
		},
	})
	manifest.Integrity = &results.IntegritySummary{
		Complete:             true,
		SelectionCountsValid: true,
		SelectionSourceCount: 200,
		SelectionUniqueCount: 200,
		SelectionCount:       200,
	}
	writeManifest(t, runDir, manifest)
	storedDelete := &deletemetrics.Metrics{
		Units: deletemetrics.Units{
			Requests: deletemetrics.RequestUnit,
			Objects:  deletemetrics.ObjectUnit,
			Batches:  deletemetrics.RequestUnit,
		},
		Requests:           deletemetrics.Requests{Attempted: 2, FullSuccess: 2, PerSecond: 0.5},
		Objects:            deletemetrics.Objects{Selected: 200, Attempted: 200, Accepted: 200, PerSecond: 50},
		Batches:            deletemetrics.Batches{ConfiguredSize: 100, ActualRequestCount: 2, ActualObjectCount: 200, MeanObjectsPerRequest: 100, FullBatchCount: 2, FullBatchPercent: 100},
		Identity:           deletemetrics.Identity{Mode: "batch", ConfiguredBatchSize: 100, SelectionOrder: "canonical"},
		OutcomeTerminology: deletemetrics.OutcomeAccepted,
		TerminalReconciled: true,
	}
	writeParams(t, runDir, &RunParams{
		WorkloadType:    "delete",
		ExpectedStepIDs: []string{listStepID, deleteStepID},
		DeleteMetrics:   map[string]*deletemetrics.Metrics{deleteStepID: storedDelete},
	})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := data.Steps[listStepID].Metrics.Rows[0].SuccessCount; got != 200 {
		t.Fatalf("stored LIST success = %d, want 200 object identities", got)
	}
	if got := data.Steps[deleteStepID].Metrics.Rows[0].SuccessCount; got != 2 {
		t.Fatalf("stored DELETE success = %d, want 2 logical API requests", got)
	}
	if got := data.Steps[deleteStepID].Delete.Objects.Accepted; got != 200 {
		t.Fatalf("stored DELETE accepted objects = %d, want 200", got)
	}

	aggregated, err := Aggregate(data)
	if err != nil {
		t.Fatal(err)
	}
	if got := aggregated.Steps[0].Metrics.ThroughputAvgOps; got != 50 {
		t.Fatalf("LIST rate = %v, want unchanged 50 objects/s", got)
	}
	if got := aggregated.Steps[1].Metrics.ThroughputAvgOps; got != 0.5 {
		t.Fatalf("DELETE rate = %v, want unchanged 0.5 ops/s", got)
	}
	if got := aggregated.Steps[0].SuccessUnit; got != deletemetrics.ObjectUnit {
		t.Fatalf("LIST success unit = %q, want %q", got, deletemetrics.ObjectUnit)
	}
	if got := aggregated.Steps[1].SuccessUnit; got != deletemetrics.RequestUnit {
		t.Fatalf("DELETE success unit = %q, want %q", got, deletemetrics.RequestUnit)
	}

	table := NewRenderer(RenderOptions{}).performanceTable(aggregated)
	for _, want := range []string{
		"200 object identities",
		"2 logical API requests",
		"50.0 objects/s",
		"0.50 ops/s",
	} {
		if !strings.Contains(table, want) {
			t.Fatalf("stored LIST-to-DELETE summary omitted %q:\n%s", want, table)
		}
	}
}

func TestLoaderLoadMixedDistributionAndMultiRowMetrics(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "mt-20260604.180000.000")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	stepID := "mt-002-20260604.180001.000-mixed"
	manifest := makeManifest(t, runDir, []stepFixture{
		{
			ID: stepID,
			MetricsContent: sampleMetricsCSV([]string{
				`"2026-06-04T18:00:10Z",READ,8,4,8,8,445,5,1866465280,30.0,20.0,14.8,13.9,59.3,55.6,8100,2200,4300,6200,9100,18000,8100,2200,4300,6200,9100,18000`,
				`"2026-06-04T18:00:10Z",STAT,8,4,8,8,301,1,0,30.0,8.0,10.0,9.5,0.0,0.0,4200,1000,2100,3100,5000,9000,4200,1000,2100,3100,5000,9000`,
				`"2026-06-04T18:00:10Z",CREATE,8,4,8,8,151,2,633339904,30.0,11.0,5.1,4.8,21.1,20.0,11200,3000,6200,8700,13000,25000,11200,3000,6200,8700,13000,25000`,
				`"2026-06-04T18:00:10Z",DELETE,8,4,8,8,103,4,0,30.0,6.0,3.6,3.1,0.0,0.0,7300,1700,3000,5200,8600,14000,7300,1700,3000,5200,8600,14000`,
			}),
		},
	})
	writeManifest(t, runDir, manifest)

	params := RunParams{
		GeneratedAt:  time.Date(2026, 6, 4, 18, 0, 0, 0, time.UTC),
		WorkloadType: "mixed",
		ResultsRoot:  "results/mt-20260604.180000.000",
		ScenarioParams: ScenarioParams{
			WorkloadType:  "mixed",
			Bucket:        "mixed-bucket",
			Threads:       8,
			ObjectSize:    "4MB",
			Duration:      "30s",
			GetDistrib:    45,
			StatDistrib:   30,
			PutDistrib:    15,
			DeleteDistrib: 10,
		},
		ExpectedStepIDs: []string{stepID},
	}
	writeParams(t, runDir, &params)

	loader := NewLoader()
	data, err := loader.Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	step := data.Steps[stepID]
	if step == nil {
		t.Fatalf("step %s missing", stepID)
	}
	if step.Metrics == nil {
		t.Fatalf("expected metrics for %s", stepID)
	}
	if len(step.Metrics.Rows) != 4 {
		t.Fatalf("expected 4 mixed metrics rows, got %d", len(step.Metrics.Rows))
	}
	gotOps := []string{
		step.Metrics.Rows[0].Operation,
		step.Metrics.Rows[1].Operation,
		step.Metrics.Rows[2].Operation,
		step.Metrics.Rows[3].Operation,
	}
	if strings.Join(gotOps, ",") != "READ,STAT,CREATE,DELETE" {
		t.Fatalf("unexpected operation order: %v", gotOps)
	}
	if data.Params.ScenarioParams.GetDistrib != 45 ||
		data.Params.ScenarioParams.StatDistrib != 30 ||
		data.Params.ScenarioParams.PutDistrib != 15 ||
		data.Params.ScenarioParams.DeleteDistrib != 10 {
		t.Fatalf("unexpected mixed distribution: %+v", data.Params.ScenarioParams)
	}
	if got := step.Metrics.Rows[2].SizeBytes; got != 633339904 {
		t.Fatalf("create size bytes = %d", got)
	}
	if got := step.Metrics.Rows[3].FailureCount; got != 4 {
		t.Fatalf("delete failure count = %d", got)
	}
}

func TestParseMetricsTotalsAcceptsLegacyMBpsHeaders(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "metrics.total.csv")
	content := legacySampleMetricsCSV([]string{
		`"2026-06-04T18:00:10Z",READ,8,4,8,8,445,5,1866465280,30.0,20.0,14.8,13.9,59.3,55.6,8100,2200,4300,6200,9100,18000,8100,2200,4300,6200,9100,18000`,
	})
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write metrics: %v", err)
	}

	metrics, err := parseMetricsTotals("legacy-step", path)
	if err != nil {
		t.Fatalf("parseMetricsTotals returned error: %v", err)
	}
	if got := metrics.Rows[0].BandwidthAvgMiBps; got != 59.3 {
		t.Fatalf("legacy bandwidth avg = %f, want 59.3", got)
	}
}

func TestLoaderLoadMissingMetrics(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "mt-20250926.180000.000")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	manifest := &results.Manifest{
		BaseURL:     "http://example",
		OutputDir:   runDir,
		GeneratedAt: time.Now().UTC(),
		Steps: []results.StepManifest{
			{
				StepID: "step-1",
				Files: []results.FileStatus{
					{
						Name:   "step-1." + constants.ResultsArtifactSuffixMetricsTotal,
						Status: "missing",
					},
				},
			},
		},
	}
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{ExpectedStepIDs: []string{"step-1"}})

	loader := NewLoader()
	data, err := loader.Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	step := data.Steps["step-1"]
	if step == nil {
		t.Fatalf("step not found")
	}
	if step.Status != StepStatusPartial {
		t.Fatalf("expected partial status, got %s", step.Status)
	}
	if len(step.MissingRequired) == 0 || step.MissingRequired[0] != constants.ResultsArtifactSuffixMetricsTotal {
		t.Fatalf("expected missing metrics total suffix, got %v", step.MissingRequired)
	}
}

func TestLoaderLoadMetricsSuppressedStepDoesNotRequireMetricsTotal(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "mt-20260604.195722.505")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	stepID := "mt-001-20260604.195722.504-seed"
	configName := stepID + "." + constants.ResultsArtifactSuffixConfig
	configContent := []byte("output:\n  metrics:\n    summary:\n      persist: false\n")
	if err := os.WriteFile(filepath.Join(runDir, configName), configContent, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	manifest := &results.Manifest{
		BaseURL:     "http://example",
		OutputDir:   runDir,
		GeneratedAt: time.Now().UTC(),
		Steps: []results.StepManifest{
			{
				StepID: stepID,
				Files: []results.FileStatus{
					{
						Name:   stepID + "." + constants.ResultsArtifactSuffixMetricsTotal,
						Status: "missing",
						Error:  "not listed in index.json",
					},
					{
						Name:   configName,
						Status: fileStatusOK,
					},
				},
			},
		},
	}
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{ExpectedStepIDs: []string{stepID}})

	loader := NewLoader()
	data, err := loader.Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	step := data.Steps[stepID]
	if step == nil {
		t.Fatalf("step not found")
	}
	if !step.MetricsSuppressed {
		t.Fatalf("expected metrics to be marked suppressed")
	}
	if step.Status != StepStatusComplete {
		t.Fatalf("expected complete status for metrics-suppressed step, got %s", step.Status)
	}
	if len(step.MissingRequired) != 0 {
		t.Fatalf("metrics-suppressed step should not have missing required artifacts: %v", step.MissingRequired)
	}
}

func TestLoaderLoadMalformedMetrics(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runDir := filepath.Join(tempDir, "mt-20250926.190000.000")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	manifest := makeManifest(t, runDir, []stepFixture{
		{
			ID: "step-err",
			MetricsContent: sampleMetricsCSV([]string{
				`"2025-09-26T17:27:00Z",CREATE,8,4,8,8,not-a-number,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1`,
			}),
		},
	})
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, &RunParams{ExpectedStepIDs: []string{"step-err"}})

	loader := NewLoader()
	data, err := loader.Load(context.Background(), runDir)
	if err == nil {
		t.Fatalf("expected error due to malformed metrics")
	}
	if data == nil {
		t.Fatalf("expected data even on error")
	}
	step := data.Steps["step-err"]
	if step == nil {
		t.Fatalf("step missing")
	}
	if step.Status != StepStatusError {
		t.Fatalf("expected error status, got %s", step.Status)
	}
	if len(step.Notes) == 0 {
		t.Fatalf("expected notes explaining error")
	}
}

type stepFixture struct {
	ID             string
	MetricsContent string
}

func makeManifest(t *testing.T, runDir string, steps []stepFixture) *results.Manifest {
	t.Helper()
	manifestSteps := make([]results.StepManifest, 0, len(steps))
	for _, st := range steps {
		metricsName := st.ID + "." + constants.ResultsArtifactSuffixMetricsTotal
		if st.MetricsContent != "" {
			if err := os.WriteFile(filepath.Join(runDir, metricsName), []byte(st.MetricsContent), 0o644); err != nil {
				t.Fatalf("write metrics file: %v", err)
			}
		}
		manifestSteps = append(manifestSteps, results.StepManifest{
			StepID: st.ID,
			Files: []results.FileStatus{
				{
					Name:     metricsName,
					Status:   statusForContent(st.MetricsContent),
					Modified: time.Now().UTC().Format(time.RFC1123),
				},
			},
		})
	}
	return &results.Manifest{
		BaseURL:     "http://example",
		OutputDir:   runDir,
		GeneratedAt: time.Now().UTC(),
		Steps:       manifestSteps,
	}
}

func statusForContent(content string) string {
	if strings.TrimSpace(content) == "" {
		return fileStatusMissing
	}
	return fileStatusOK
}

func sampleMetricsCSV(rows []string) string {
	return sampleMetricsCSVWithHeader(rows, "BWAvg[MiB/s]", "BWLast[MiB/s]")
}

func legacySampleMetricsCSV(rows []string) string {
	return sampleMetricsCSVWithHeader(rows, "BWAvg[MB/s]", "BWLast[MB/s]")
}

func sampleMetricsCSVWithHeader(rows []string, bandwidthAvgColumn string, bandwidthLastColumn string) string {
	header := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s]," + bandwidthAvgColumn + "," + bandwidthLastColumn + ",DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us],DurationQ_0.75[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us],LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us]"
	builder := strings.Builder{}
	builder.WriteString(header)
	builder.WriteString("\n")
	for i, row := range rows {
		builder.WriteString(row)
		if i < len(rows)-1 {
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

func writeManifest(t *testing.T, runDir string, manifest *results.Manifest) {
	t.Helper()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsManifestFileName), data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

func writeParams(t *testing.T, runDir string, params *RunParams) {
	t.Helper()
	data, err := json.MarshalIndent(params, "", "  ")
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsMetadataFileName), data, 0o644); err != nil {
		t.Fatalf("write params: %v", err)
	}
}
