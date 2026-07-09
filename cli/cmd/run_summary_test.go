package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestGenerateRunSummaryCreatesFileAndSnippet(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runID := "mt-20250926.172619.540"
	runDir := filepath.Join(tempDir, runID)
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	// Write metrics CSV
	stepID := "mt-001-20250926.172621.307-create"
	metricsName := fmt.Sprintf("%s.%s", stepID, constants.ResultsArtifactSuffixMetricsTotal)
	metricsCSV := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s],BWLast[MiB/s],DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us],DurationQ_0.75[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us],LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us]\n" +
		"\"2025-09-26T17:27:00Z\",CREATE,8,4,8,8,2499,0,2620391424,7.554,244.988254,334.69642857142856,320.2810720618962,334.69642857142856,320.2810720618962,79900,28000,59000,79876,108000,623000,79900,28000,59000,79876,108000,623000\n"
	if err := os.WriteFile(filepath.Join(runDir, metricsName), []byte(metricsCSV), 0o644); err != nil {
		t.Fatalf("write metrics csv: %v", err)
	}

	// Manifest referencing metrics file
	manifest := &results.Manifest{
		BaseURL:     "http://spt-test-1.cec.delllabs.net:9999",
		OutputDir:   runDir,
		GeneratedAt: time.Now().UTC(),
		Steps: []results.StepManifest{
			{
				StepID:      stepID,
				CompletedAt: time.Now().UTC(),
				Files: []results.FileStatus{
					{
						Name:   metricsName,
						Size:   int64(len(metricsCSV)),
						Status: "ok",
					},
				},
			},
		},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsManifestFileName), data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	meta := &runMetadata{
		GeneratedAt:        time.Date(2025, 9, 26, 17, 27, 28, 0, time.UTC),
		WorkloadType:       "write",
		SptImage:           "ghcr.io/dell/storage-performance-tool",
		APIPort:            "9999",
		BaseURL:            "http://spt-test-1.cec.delllabs.net:9999",
		Label:              "mt",
		ResultsDir:         "./results",
		ResultsRoot:        filepath.Base(runDir),
		ScenarioFile:       "scenario.js",
		ScenarioStoredPath: "scenario.js",
		ScenarioParams: scenario.Params{
			ObjectSize:   "1MB",
			ObjectCount:  2500,
			Threads:      8,
			Cleanup:      true,
			KeepScenario: true,
		},
		Hosts: []runHostMetadata{
			{Host: "spt-test-1.cec.delllabs.net", User: "root", Original: "root@spt-test-1.cec.delllabs.net"},
		},
		ExpectedStepIDs: []string{stepID},
		ActualStepIDs:   []string{stepID},
		ResultsOptions:  resultsOptionsSnapshot{AutoResults: true, ResultsDir: "./results", Label: "mt", ShutdownOnComplete: true, ShutdownLingerSec: 5},
	}
	if err := writeRunMetadata(meta, runDir); err != nil {
		t.Fatalf("write run metadata: %v", err)
	}

	var buf strings.Builder
	if err := generateRunSummary(context.Background(), runDir, &buf); err != nil {
		t.Fatalf("generateRunSummary error: %v", err)
	}

	snippet := buf.String()
	if !strings.Contains(snippet, "SPT Run Summary") {
		t.Fatalf("snippet missing title: %q", snippet)
	}

	summaryPath := filepath.Join(runDir, fmt.Sprintf(constants.ResultsSummaryFilePattern, runID))
	content, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary file: %v", err)
	}
	if !strings.Contains(string(content), "Performance by Phase") {
		t.Fatalf("summary file missing table section: %s", string(content))
	}
}

func TestGenerateRunSummaryHandlesListWorkload(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runID := "mt-20250930.120000.000"
	runDir := filepath.Join(tempDir, runID)
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	stepID := "mt-001-20250930.120000.000-list"
	metricsName := fmt.Sprintf("%s.%s", stepID, constants.ResultsArtifactSuffixMetricsTotal)
	metricsCSV := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s],BWLast[MiB/s],DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us],DurationQ_0.75[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us],LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us]\n" +
		"\"2025-09-30T12:00:05Z\",LIST,1,1,1,1,1200,0,0,12.5,12.5,96.0,94.0,0.0,0.0,4500,3000,3800,4200,4700,6000,4500,3000,3800,4200,4700,6000\n"
	if err := os.WriteFile(filepath.Join(runDir, metricsName), []byte(metricsCSV), 0o644); err != nil {
		t.Fatalf("write metrics csv: %v", err)
	}

	manifest := &results.Manifest{
		BaseURL:     "http://localhost:9999",
		OutputDir:   runDir,
		GeneratedAt: time.Now().UTC(),
		Steps: []results.StepManifest{
			{
				StepID:      stepID,
				CompletedAt: time.Now().UTC(),
				Files: []results.FileStatus{
					{
						Name:   metricsName,
						Size:   int64(len(metricsCSV)),
						Status: "ok",
					},
				},
			},
		},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsManifestFileName), manifestBytes, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	meta := &runMetadata{
		GeneratedAt:        time.Date(2025, 9, 30, 12, 0, 0, 0, time.UTC),
		WorkloadType:       "list",
		SptImage:           "ghcr.io/dell/storage-performance-tool",
		APIPort:            "9999",
		BaseURL:            "http://localhost:9999",
		Label:              "mt",
		ResultsDir:         "./results",
		ResultsRoot:        filepath.Base(runDir),
		ScenarioFile:       "scenario.js",
		ScenarioStoredPath: "scenario.js",
		ScenarioParams: scenario.Params{
			WorkloadType: "list",
			Bucket:       "reports",
			Prefix:       "daily/",
			Threads:      1,
		},
		Hosts:           []runHostMetadata{{Host: "localhost", User: "tester", Original: "tester@localhost"}},
		ExpectedStepIDs: []string{stepID},
		ActualStepIDs:   []string{stepID},
	}
	if err := writeRunMetadata(meta, runDir); err != nil {
		t.Fatalf("write run metadata: %v", err)
	}

	var buf strings.Builder
	if err := generateRunSummary(context.Background(), runDir, &buf); err != nil {
		t.Fatalf("generateRunSummary error: %v", err)
	}

	snippet := buf.String()
	if !strings.Contains(snippet, "• Object size        not applicable") {
		t.Fatalf("snippet missing list object size message: %q", snippet)
	}
	if !strings.Contains(snippet, "• Prefix             daily/") {
		t.Fatalf("snippet missing prefix line: %q", snippet)
	}
}

func TestGenerateRunSummaryMixedWorkloadIncludesBreakdown(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runID := "mt-20260604.180000.000"
	runDir := filepath.Join(tempDir, runID)
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	stepID := "mt-002-20260604.180001.000-mixed"
	metricsName := fmt.Sprintf("%s.%s", stepID, constants.ResultsArtifactSuffixMetricsTotal)
	metricsCSV := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s],BWLast[MiB/s],DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us],DurationQ_0.75[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us],LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us]\n" +
		"\"2026-06-04T18:00:10Z\",READ,8,4,8,8,445,5,1866465280,30,20,14.8,13.9,59.3,55.6,8100,2200,4300,6200,9100,18000,8100,2200,4300,6200,9100,18000\n" +
		"\"2026-06-04T18:00:10Z\",STAT,8,4,8,8,301,1,0,30,8,10.0,9.5,0,0,4200,1000,2100,3100,5000,9000,4200,1000,2100,3100,5000,9000\n" +
		"\"2026-06-04T18:00:10Z\",CREATE,8,4,8,8,151,2,633339904,30,11,5.1,4.8,21.1,20.0,11200,3000,6200,8700,13000,25000,11200,3000,6200,8700,13000,25000\n" +
		"\"2026-06-04T18:00:10Z\",DELETE,8,4,8,8,103,4,0,30,6,3.6,3.1,0,0,7300,1700,3000,5200,8600,14000,7300,1700,3000,5200,8600,14000\n"
	if err := os.WriteFile(filepath.Join(runDir, metricsName), []byte(metricsCSV), 0o644); err != nil {
		t.Fatalf("write metrics csv: %v", err)
	}

	manifest := &results.Manifest{
		BaseURL:     "http://localhost:9999",
		OutputDir:   runDir,
		GeneratedAt: time.Now().UTC(),
		Steps: []results.StepManifest{
			{
				StepID:      stepID,
				CompletedAt: time.Now().UTC(),
				Files:       []results.FileStatus{{Name: metricsName, Size: int64(len(metricsCSV)), Status: "ok"}},
			},
		},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsManifestFileName), manifestBytes, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	meta := &runMetadata{
		GeneratedAt:        time.Date(2026, 6, 4, 18, 0, 0, 0, time.UTC),
		WorkloadType:       "mixed",
		SptImage:           "ghcr.io/dell/storage-performance-tool",
		APIPort:            "9999",
		BaseURL:            "http://localhost:9999",
		Label:              "mt",
		ResultsDir:         "./results",
		ResultsRoot:        filepath.Base(runDir),
		ScenarioFile:       "scenario.js",
		ScenarioStoredPath: "scenario.js",
		ScenarioParams: scenario.Params{
			WorkloadType:  "mixed",
			ObjectSize:    "4MB",
			Threads:       8,
			Duration:      "30s",
			GetDistrib:    45,
			StatDistrib:   30,
			PutDistrib:    15,
			DeleteDistrib: 10,
		},
		Hosts:           []runHostMetadata{{Host: "localhost", User: "tester", Original: "tester@localhost"}},
		ExpectedStepIDs: []string{stepID},
		ActualStepIDs:   []string{stepID},
	}
	if err := writeRunMetadata(meta, runDir); err != nil {
		t.Fatalf("write run metadata: %v", err)
	}

	var buf strings.Builder
	if err := generateRunSummary(context.Background(), runDir, &buf); err != nil {
		t.Fatalf("generateRunSummary error: %v", err)
	}

	snippet := buf.String()
	if !strings.Contains(snippet, "Mixed Operation Breakdown") {
		t.Fatalf("snippet missing mixed section: %q", snippet)
	}
	if !strings.Contains(snippet, "Configured distribution: READ 45%, STAT 30%, CREATE 15%, DELETE 10%") {
		t.Fatalf("snippet missing configured mixed detail: %q", snippet)
	}

	summaryPath := filepath.Join(runDir, fmt.Sprintf(constants.ResultsSummaryFilePattern, runID))
	content, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary file: %v", err)
	}
	report := string(content)
	if !strings.Contains(report, "Mixed Operation Breakdown") {
		t.Fatalf("summary file missing mixed section: %s", report)
	}
	if !strings.Contains(report, "Configured distribution: READ 45%, STAT 30%, CREATE 15%, DELETE 10%") {
		t.Fatalf("summary file missing configured distribution: %s", report)
	}
	if !strings.Contains(report, "see ops") {
		t.Fatalf("summary file missing mixed latency marker: %s", report)
	}
}

func TestGenerateRunSummaryMixedWorkloadPropagatesDistributionWarning(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	runID := "mt-20260604.190000.000"
	runDir := filepath.Join(tempDir, runID)
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}

	stepID := "mt-002-20260604.190001.000-mixed"
	metricsName := fmt.Sprintf("%s.%s", stepID, constants.ResultsArtifactSuffixMetricsTotal)
	metricsCSV := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s],BWLast[MiB/s],DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us],DurationQ_0.75[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us],LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us]\n" +
		"\"2026-06-04T19:00:10Z\",READ,8,4,8,8,445,5,1866465280,30,20,14.8,13.9,59.3,55.6,8100,2200,4300,6200,9100,18000,8100,2200,4300,6200,9100,18000\n" +
		"\"2026-06-04T19:00:10Z\",STAT,8,4,8,8,301,1,0,30,8,10.0,9.5,0,0,4200,1000,2100,3100,5000,9000,4200,1000,2100,3100,5000,9000\n" +
		"\"2026-06-04T19:00:10Z\",CREATE,8,4,8,8,151,2,633339904,30,11,5.1,4.8,21.1,20.0,11200,3000,6200,8700,13000,25000,11200,3000,6200,8700,13000,25000\n" +
		"\"2026-06-04T19:00:10Z\",DELETE,8,4,8,8,103,4,0,30,6,3.6,3.1,0,0,7300,1700,3000,5200,8600,14000,7300,1700,3000,5200,8600,14000\n"
	if err := os.WriteFile(filepath.Join(runDir, metricsName), []byte(metricsCSV), 0o644); err != nil {
		t.Fatalf("write metrics csv: %v", err)
	}

	manifest := &results.Manifest{
		BaseURL:     "http://localhost:9999",
		OutputDir:   runDir,
		GeneratedAt: time.Now().UTC(),
		Steps: []results.StepManifest{
			{
				StepID:      stepID,
				CompletedAt: time.Now().UTC(),
				Files:       []results.FileStatus{{Name: metricsName, Size: int64(len(metricsCSV)), Status: "ok"}},
			},
		},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsManifestFileName), manifestBytes, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	meta := &runMetadata{
		GeneratedAt:        time.Date(2026, 6, 4, 19, 0, 0, 0, time.UTC),
		WorkloadType:       "mixed",
		SptImage:           "ghcr.io/dell/storage-performance-tool",
		APIPort:            "9999",
		BaseURL:            "http://localhost:9999",
		Label:              "mt",
		ResultsDir:         "./results",
		ResultsRoot:        filepath.Base(runDir),
		ScenarioFile:       "scenario.js",
		ScenarioStoredPath: "scenario.js",
		ScenarioParams: scenario.Params{
			WorkloadType:  "mixed",
			ObjectSize:    "4MB",
			Threads:       8,
			Duration:      "30s",
			GetDistrib:    45,
			StatDistrib:   30,
			PutDistrib:    10,
			DeleteDistrib: 5,
		},
		Hosts:           []runHostMetadata{{Host: "localhost", User: "tester", Original: "tester@localhost"}},
		ExpectedStepIDs: []string{stepID},
		ActualStepIDs:   []string{stepID},
	}
	if err := writeRunMetadata(meta, runDir); err != nil {
		t.Fatalf("write run metadata: %v", err)
	}

	var buf strings.Builder
	if err := generateRunSummary(context.Background(), runDir, &buf); err != nil {
		t.Fatalf("generateRunSummary error: %v", err)
	}

	const wantWarning = "mixed configured distribution sums to 90%, expected 100%"
	snippet := buf.String()
	if !strings.Contains(snippet, wantWarning) {
		t.Fatalf("snippet missing warning %q: %q", wantWarning, snippet)
	}

	summaryPath := filepath.Join(runDir, fmt.Sprintf(constants.ResultsSummaryFilePattern, runID))
	content, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatalf("read summary file: %v", err)
	}
	if !strings.Contains(string(content), wantWarning) {
		t.Fatalf("summary file missing warning %q: %s", wantWarning, string(content))
	}
}

func TestGenerateRunSummaryErrorOnMissingManifest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "empty"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	err := generateRunSummary(context.Background(), filepath.Join(dir, "empty"), &strings.Builder{})
	if err == nil {
		t.Fatalf("expected error when manifest missing")
	}
}
