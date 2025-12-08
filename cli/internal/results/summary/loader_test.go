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
	if got := create.Metrics.Rows[0].BandwidthAvgMBps; got <= 0 {
		t.Fatalf("create bandwidth avg not parsed: %f", got)
	}
	if len(data.MissingExpectedSteps) != 0 {
		t.Fatalf("unexpected missing expected steps: %v", data.MissingExpectedSteps)
	}
	if data.Params.ScenarioParams.Prefix != "daily/" {
		t.Fatalf("expected prefix 'daily/', got %q", data.Params.ScenarioParams.Prefix)
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
	header := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MB/s],BWLast[MB/s],DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us],DurationQ_0.75[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us],LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us]"
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
