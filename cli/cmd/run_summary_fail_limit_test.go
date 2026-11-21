package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/results/summary"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestSummaryRunEnd(t *testing.T) {
	t.Parallel()
	tracked := func(state string) *portcheck.RunResult { return &portcheck.RunResult{FinalState: state} }
	cases := []struct {
		name    string
		outcome *autoResultsOutcome
		want    summary.RunEnd
	}{
		{"completed", &autoResultsOutcome{Tracker: tracked(constants.StateCompleted)}, summary.RunEndCompleted},
		{"idle fallback", &autoResultsOutcome{Tracker: tracked(constants.StateIdle)}, summary.RunEndCompleted},
		{"failed", &autoResultsOutcome{Tracker: tracked(constants.StateFailed)}, summary.RunEndFailed},
		{"stopped elsewhere", &autoResultsOutcome{Tracker: tracked(constants.StateStopped)}, summary.RunEndFailed},
		{"auto-terminated", &autoResultsOutcome{AutoTerminated: true, Tracker: tracked(constants.StateCompleted)}, summary.RunEndAutoTerminated},
		{"interrupted", &autoResultsOutcome{TrackerErr: context.Canceled, Tracker: tracked(constants.StateRunning)}, summary.RunEndInterrupted},
		{"tracking failed", &autoResultsOutcome{TrackerErr: errors.New("lost"), Tracker: tracked(constants.StateCompleted)}, summary.RunEndInterrupted},
		{"never tracked", &autoResultsOutcome{}, summary.RunEndUnknown},
		{"no outcome", nil, summary.RunEndUnknown},
	}
	for _, tc := range cases {
		if got := summaryRunEnd(tc.outcome); got != tc.want {
			t.Errorf("%s: summaryRunEnd = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// A write phase that the engine stopped at load.op.limit.fail.count=20 after 0.694 s of a 12 s
// limit, as in the triage's write-puthalf-count20 run: only a run the engine ended on its own
// names the stop, in the console snippet and the saved summary file.
func TestGenerateRunSummaryNamesFailureLimitStop(t *testing.T) {
	t.Parallel()
	const warning = "Create: stopped at its failure limit after 28 failed requests (load.op.limit.fail.count 20), 694ms into its 12s time limit"
	for _, end := range []summary.RunEnd{
		summary.RunEndCompleted, summary.RunEndAutoTerminated, summary.RunEndInterrupted, summary.RunEndFailed,
	} {
		t.Run(string(end), func(t *testing.T) {
			t.Parallel()
			runID := "mt-20261121.002914.831"
			runDir := filepath.Join(t.TempDir(), runID)
			writeFailLimitRunDir(t, runDir, "0.694")
			var buf strings.Builder
			if _, err := generateRunSummary(context.Background(), runDir, &buf, end); err != nil {
				t.Fatalf("generateRunSummary: %v", err)
			}
			saved, err := os.ReadFile(filepath.Join(runDir, fmt.Sprintf(constants.ResultsSummaryFilePattern, runID)))
			if err != nil {
				t.Fatalf("read summary file: %v", err)
			}
			want := end == summary.RunEndCompleted
			for name, text := range map[string]string{"snippet": buf.String(), "file": string(saved)} {
				if strings.Contains(text, warning) != want {
					t.Errorf("%s shows the failure-limit warning = %v, want %v:\n%s", name, !want, want, text)
				}
				if !strings.Contains(text, "Create: 28 requests failed") {
					t.Errorf("%s lost the failed-count warning:\n%s", name, text)
				}
			}
		})
	}
}

func writeFailLimitRunDir(t *testing.T, runDir, stepSeconds string) {
	t.Helper()
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run dir: %v", err)
	}
	stepID := "mt-001-20261121.002914.829-create"
	metricsName := stepID + "." + constants.ResultsArtifactSuffixMetricsTotal
	configName := stepID + "." + constants.ResultsArtifactSuffixConfig
	metricsCSV := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s],BWLast[MiB/s],DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us],DurationQ_0.75[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us],LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us]\n" +
		"\"2026-11-21T00:29:19Z\",CREATE,1,1,0,0.2,24,28,6291456," + stepSeconds + ",0.05,34.6,0,8.6,0,2263,931,1399,1719,2735,4930,928,209,503,631,771,3588\n"
	configYAML := "load:\n  op:\n    limit:\n      count: 0\n      fail:\n        count: 20\n        rate: false\n  step:\n    limit:\n      time: \"12s\"\n"
	for name, content := range map[string]string{metricsName: metricsCSV, configName: configYAML} {
		if err := os.WriteFile(filepath.Join(runDir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	manifest := &results.Manifest{
		BaseURL: "http://127.0.0.1:19700", OutputDir: runDir, GeneratedAt: time.Now().UTC(),
		Steps: []results.StepManifest{{
			StepID: stepID, CompletedAt: time.Now().UTC(),
			Files: []results.FileStatus{
				{Name: metricsName, Size: int64(len(metricsCSV)), Status: "ok"},
				{Name: configName, Size: int64(len(configYAML)), Status: "ok"},
			},
		}},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsManifestFileName), data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	meta := &runMetadata{
		GeneratedAt:     time.Date(2026, 11, 21, 0, 29, 20, 0, time.UTC),
		WorkloadType:    "write",
		BaseURL:         "http://127.0.0.1:19700",
		Label:           "mt",
		ResultsRoot:     filepath.Base(runDir),
		ScenarioParams:  scenario.Params{ObjectSize: "256KiB", Threads: 1, Duration: "12s"},
		ExpectedStepIDs: []string{stepID},
		ActualStepIDs:   []string{stepID},
		ResultsOptions:  resultsOptionsSnapshot{AutoResults: true, Label: "mt"},
	}
	if err := writeRunMetadata(meta, runDir); err != nil {
		t.Fatalf("write run metadata: %v", err)
	}
}
