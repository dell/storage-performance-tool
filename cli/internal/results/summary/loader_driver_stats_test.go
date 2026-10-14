package summary

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
)

const driverStatsStepID = "mt-001-create"

// driverStatsRun writes a one-step run whose stored metadata is the given JSON object and
// returns its directory. The metadata is written as stored text, not through RunParams, so the
// tests bind the keys a finished run leaves on disk.
func driverStatsRun(t *testing.T, metadata map[string]any) string {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "driver-stats-run")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: driverStatsStepID,
		MetricsContent: sampleMetricsCSV([]string{
			`"2026-10-13T10:00:00Z",CREATE,8,1,8,8,100,0,104857600,1,8,100,100,100,100,1000,500,750,900,950,2000,800,400,650,800,900,1800`,
		}),
	}})
	writeManifest(t, runDir, manifest)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsMetadataFileName), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	return runDir
}

func TestLoaderCarriesStoredDriverStatsToStepSummary(t *testing.T) {
	runDir := driverStatsRun(t, map[string]any{
		"workloadType":    "write",
		"expectedStepIds": []string{driverStatsStepID},
		"scenarioParams":  map[string]any{"WorkloadType": "write", "S3Driver": "rdma"},
		"driverStats": map[string]any{
			driverStatsStepID: map[string]any{
				"driver":    "s3-rdma",
				"counters":  map[string]any{"rdma_transferred": 100, "name_from_a_newer_engine": 7},
				"gauges":    map[string]any{"pool_buffers": 4, "gauge_from_a_newer_engine": -1},
				"reporting": 2,
			},
		},
	})
	want := &driverstats.Stats{
		Driver:    "s3-rdma",
		Counters:  map[string]int64{"rdma_transferred": 100, "name_from_a_newer_engine": 7},
		Gauges:    map[string]int64{"pool_buffers": 4, "gauge_from_a_newer_engine": -1},
		Reporting: 2,
	}

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := data.Params.DriverStats[driverStatsStepID]; !reflect.DeepEqual(got, want) {
		t.Fatalf("run parameters carry %+v, want %+v", got, want)
	}
	if got := data.Steps[driverStatsStepID].DriverStats; !reflect.DeepEqual(got, want) {
		t.Fatalf("step data carries %+v, want %+v", got, want)
	}
	if got := data.Params.ScenarioParams.S3Driver; got != "rdma" {
		t.Fatalf("stored S3 driver = %q, want rdma", got)
	}

	aggregated, err := Aggregate(data)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(aggregated.Steps) != 1 {
		t.Fatalf("step summaries = %d, want 1", len(aggregated.Steps))
	}
	if got := aggregated.Steps[0].DriverStats; !reflect.DeepEqual(got, want) {
		t.Fatalf("step summary carries %+v, want %+v", got, want)
	}
}

func TestLoaderLoadsResultsStoredWithoutDriverStats(t *testing.T) {
	runDir := driverStatsRun(t, map[string]any{
		"workloadType":    "write",
		"expectedStepIds": []string{driverStatsStepID},
		"scenarioParams":  map[string]any{"WorkloadType": "write"},
	})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("results stored before driver statistics existed did not load: %v", err)
	}
	if data.Params.DriverStats != nil || data.Params.DriverStatsError != "" {
		t.Fatalf("run parameters invented driver statistics: %+v, error %q",
			data.Params.DriverStats, data.Params.DriverStatsError)
	}
	if data.Steps[driverStatsStepID].DriverStats != nil {
		t.Fatalf("step data invented driver statistics: %+v", data.Steps[driverStatsStepID].DriverStats)
	}
	if data.Params.ScenarioParams.S3Driver != "" {
		t.Fatalf("S3 driver = %q, want empty for results that did not store one", data.Params.ScenarioParams.S3Driver)
	}
	aggregated, err := Aggregate(data)
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(aggregated.Steps) != 1 || aggregated.Steps[0].DriverStats != nil {
		t.Fatalf("step summaries carry driver statistics that were never stored: %+v", aggregated.Steps)
	}
	if report := NewRenderer(RenderOptions{}).FullReport(aggregated); report == "" {
		t.Fatal("results stored without driver statistics did not render")
	}
}

func TestLoaderKeepsSummaryWhenDriverStatsWereNotStored(t *testing.T) {
	const reason = "capture terminal driver statistics: fleet total covers 1 of 2 contributors"
	runDir := driverStatsRun(t, map[string]any{
		"workloadType":     "write",
		"expectedStepIds":  []string{driverStatsStepID},
		"scenarioParams":   map[string]any{"WorkloadType": "write", "S3Driver": "rdma"},
		"driverStatsError": reason,
	})

	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("a failed capture of driver statistics failed the load: %v", err)
	}
	if data.Params.DriverStatsError != reason {
		t.Fatalf("stored reason = %q, want %q", data.Params.DriverStatsError, reason)
	}
	if data.Steps[driverStatsStepID].DriverStats != nil {
		t.Fatalf("step data carries statistics that were not stored: %+v", data.Steps[driverStatsStepID].DriverStats)
	}
	aggregated, err := Aggregate(data)
	if err != nil {
		t.Fatalf("a failed capture of driver statistics failed the summary: %v", err)
	}
	if aggregated.Rejected || len(aggregated.Steps) != 1 {
		t.Fatalf("summary changed because driver statistics were not stored: rejected=%v steps=%d",
			aggregated.Rejected, len(aggregated.Steps))
	}
	if aggregated.Steps[0].DriverStats != nil {
		t.Fatalf("step summary carries statistics that were not stored: %+v", aggregated.Steps[0].DriverStats)
	}
	if report := NewRenderer(RenderOptions{}).FullReport(aggregated); report == "" {
		t.Fatal("a run without stored driver statistics did not render")
	}
}
