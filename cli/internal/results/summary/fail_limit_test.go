package summary

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

func TestFailureLimitStopWarningRule(t *testing.T) {
	t.Parallel()

	countLimited := func(timeSeconds float64) *StepLimits {
		return &StepLimits{FailCount: 20, TimeSeconds: timeSeconds}
	}
	phase := func(success, failed int64, seconds float64) *PhaseMetrics {
		return &PhaseMetrics{SuccessCount: success, FailureCount: failed, DurationSeconds: seconds}
	}
	const countWarning = "Create: stopped at its failure limit after 28 failed requests (load.op.limit.fail.count 20), 694ms into its 12s time limit"
	cases := []struct {
		name      string
		end       RunEnd
		step      *StepData
		operation string
		metrics   *PhaseMetrics
		want      string
	}{
		{"count above the limit, well short of the time limit", RunEndCompleted,
			&StepData{Limits: countLimited(12)}, "CREATE", phase(24, 28, 0.694), countWarning},
		{"within 1 s of the time limit", RunEndCompleted,
			&StepData{Limits: countLimited(12)}, "CREATE", phase(2400, 28, 11.2), ""},
		{"over the time limit", RunEndCompleted,
			&StepData{Limits: countLimited(8)}, "CREATE", phase(2400, 28, 8.34), ""},
		{"one-hour phase stopped two minutes early", RunEndCompleted,
			&StepData{Limits: countLimited(3600)}, "CREATE", phase(90000, 21, 3480),
			"Create: stopped at its failure limit after 21 failed requests (load.op.limit.fail.count 20), 58m into its 1h 0m time limit"},
		{"count at the limit", RunEndCompleted,
			&StepData{Limits: countLimited(12)}, "CREATE", phase(24, 20, 0.694), ""},
		{"no failures", RunEndCompleted,
			&StepData{Limits: &StepLimits{FailCount: 20, FailRate: true, TimeSeconds: 12}}, "CREATE", phase(24, 0, 0.5), ""},
		{"rate on, failures, ended early", RunEndCompleted,
			&StepData{Limits: &StepLimits{FailCount: 100000, FailRate: true, TimeSeconds: 12}}, "CREATE", phase(2700, 2714, 8.85),
			"Create: ended 8.85s into its 12s time limit with the failure-rate limit on (load.op.limit.fail.rate) and 2714 failed requests; the engine stops a phase when failures outpace successes"},
		{"rate on, ran to the time limit", RunEndCompleted,
			&StepData{Limits: &StepLimits{FailCount: 100000, FailRate: true, TimeSeconds: 12}}, "CREATE", phase(2700, 2714, 12.1), ""},
		{"rate on, no own limit", RunEndCompleted,
			&StepData{Limits: &StepLimits{FailCount: 100000, FailRate: true}}, "CREATE", phase(2700, 2714, 8.85), ""},
		{"count-bounded phase short of its count", RunEndCompleted,
			&StepData{Limits: &StepLimits{FailCount: 20, OpCount: 1000}}, "CREATE", phase(30, 21, 0.4),
			"Create: stopped at its failure limit after 21 failed requests (load.op.limit.fail.count 20), after 51 of its 1000 requests"},
		{"count-bounded phase at its count", RunEndCompleted,
			&StepData{Limits: &StepLimits{FailCount: 20, OpCount: 1000}}, "CREATE", phase(979, 21, 4), ""},
		{"ran to its count before its time limit", RunEndCompleted,
			&StepData{Limits: &StepLimits{FailCount: 20, OpCount: 1000, TimeSeconds: 60}}, "CREATE", phase(979, 21, 4), ""},
		{"neither own limit, count rule alone", RunEndCompleted,
			&StepData{Limits: &StepLimits{FailCount: 20}}, "CREATE", phase(24, 21, 0.4),
			"Create: stopped at its failure limit after 21 failed requests (load.op.limit.fail.count 20)"},
		{"unparsable time treated as absent", RunEndCompleted,
			&StepData{Limits: (&stepConfigProbe{}).withLimits(t, 20, false, 0, "12 seconds")}, "CREATE", phase(24000, 21, 40),
			"Create: stopped at its failure limit after 21 failed requests (load.op.limit.fail.count 20)"},
		{"standalone DELETE", RunEndCompleted,
			&StepData{Limits: countLimited(12), Delete: &deletemetrics.Metrics{}}, "DELETE", phase(24, 28, 0.694), ""},
		{"DELETE operation", RunEndCompleted,
			&StepData{Limits: countLimited(12)}, "DELETE", phase(24, 28, 0.694), ""},
		{"suppressed metrics", RunEndCompleted,
			&StepData{Limits: countLimited(12), MetricsSuppressed: true}, "CREATE", phase(24, 28, 0.694), ""},
		{"no config.yaml", RunEndCompleted,
			&StepData{}, "CREATE", phase(24, 28, 0.694), ""},
		{"auto-terminated run", RunEndAutoTerminated,
			&StepData{Limits: countLimited(90)}, "CREATE", phase(24, 2632, 15.18), ""},
		{"interrupted run", RunEndInterrupted,
			&StepData{Limits: countLimited(12)}, "CREATE", phase(24, 28, 0.694), ""},
		{"failed run", RunEndFailed,
			&StepData{Limits: countLimited(12)}, "CREATE", phase(24, 28, 0.694), ""},
		{"unknown end", RunEndUnknown,
			&StepData{Limits: countLimited(12)}, "CREATE", phase(24, 28, 0.694), ""},
		{"all failed keeps the warning", RunEndCompleted,
			&StepData{Limits: countLimited(12)}, "CREATE", phase(0, 21, 0.3),
			"Create: stopped at its failure limit after 21 failed requests (load.op.limit.fail.count 20), 300ms into its 12s time limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := failureLimitStopWarning("Create", tc.end, tc.step, tc.operation, tc.metrics); got != tc.want {
				t.Fatalf("warning = %q, want %q", got, tc.want)
			}
		})
	}
}

// withLimits builds StepLimits through the same parse the loader uses.
func (c *stepConfigProbe) withLimits(t *testing.T, failCount int64, rate bool, opCount int64, stepTime any) *StepLimits {
	t.Helper()
	c.Load.Op.Limit.Fail.Count = &failCount
	c.Load.Op.Limit.Fail.Rate = &rate
	c.Load.Op.Limit.Count = &opCount
	c.Load.Step.Limit.Time = stepTime
	return c.stepLimits()
}

func TestEngineTimeSeconds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		raw  any
		want float64
		ok   bool
	}{
		{"12s", 12, true}, {"5m", 300, true}, {"1h", 3600, true}, {"2d", 172800, true},
		{"90", 90, true}, {"0", 0, true}, {"10S", 10, true}, {"3.minutes", 180, true},
		{90, 90, true}, {float64(7), 7, true},
		{"12 seconds", 0, false}, {"1.5h", 0, false}, {"12x", 0, false}, {nil, 0, false},
		{"5.milliseconds", 0, false},
	}
	for _, tc := range cases {
		got, ok := engineTimeSeconds(tc.raw)
		if got != tc.want || ok != tc.ok {
			t.Errorf("engineTimeSeconds(%#v) = %v, %v; want %v, %v", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

func TestLoaderReadsStepLimitsFromConfig(t *testing.T) {
	t.Parallel()

	fixture, err := os.ReadFile(filepath.Join("testdata", "fail_limit_step.config.yaml"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	load := func(t *testing.T, config []byte) *StepData {
		t.Helper()
		runDir := t.TempDir()
		stepID := "mt-002-20261121.002840.821-mixed"
		configName := stepID + "." + constants.ResultsArtifactSuffixConfig
		files := []results.FileStatus{{Name: configName, Status: fileStatusMissing}}
		if config != nil {
			if err := os.WriteFile(filepath.Join(runDir, configName), config, 0o644); err != nil {
				t.Fatalf("write config: %v", err)
			}
			files[0].Status = fileStatusOK
		}
		writeManifest(t, runDir, &results.Manifest{
			BaseURL: "http://example", OutputDir: runDir, GeneratedAt: time.Now().UTC(),
			Steps: []results.StepManifest{{StepID: stepID, Files: files}},
		})
		writeParams(t, runDir, &RunParams{ExpectedStepIDs: []string{stepID}})
		data, _ := NewLoader().Load(context.Background(), runDir)
		if data == nil || data.Steps[stepID] == nil {
			t.Fatal("step not loaded")
		}
		return data.Steps[stepID]
	}

	t.Run("fixture", func(t *testing.T) {
		t.Parallel()
		got := load(t, fixture).Limits
		want := StepLimits{FailCount: 20, TimeSeconds: 12}
		if got == nil || *got != want {
			t.Fatalf("limits = %+v, want %+v", got, want)
		}
	})
	t.Run("all four values", func(t *testing.T) {
		t.Parallel()
		config := strings.NewReplacer(
			"count: 0\n", "count: 5000\n", "rate: false", "rate: true", `time: "12s"`, `time: "5m"`,
		).Replace(string(fixture))
		got := load(t, []byte(config)).Limits
		want := StepLimits{FailCount: 20, FailRate: true, OpCount: 5000, TimeSeconds: 300}
		if got == nil || *got != want {
			t.Fatalf("limits = %+v, want %+v", got, want)
		}
	})
	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		if got := load(t, nil).Limits; got != nil {
			t.Fatalf("limits = %+v, want nil", got)
		}
	})
	t.Run("unreadable file", func(t *testing.T) {
		t.Parallel()
		if got := load(t, []byte("load: [unclosed\n")).Limits; got != nil {
			t.Fatalf("limits = %+v, want nil", got)
		}
	})
}

// failLimitMixedRun is the mixed step of cli-mixed-put50-stat50-puthalf-fc20: put 50 / stat 50,
// every second PUT failing, load.op.limit.fail.count=20, --duration 12s.
func failLimitMixedRun(createSeconds, statSeconds float64) *RunData {
	stepID := "mt-002-20261121.002840.821-mixed"
	return &RunData{
		RunID:     "mt-20261121.002840.823",
		StepOrder: []string{stepID},
		End:       RunEndCompleted,
		Params: &RunParams{
			WorkloadType: "mixed",
			ScenarioParams: ScenarioParams{
				WorkloadType: "mixed", ObjectSize: "256KiB", Duration: "12s", PutDistrib: 50, StatDistrib: 50,
			},
		},
		Steps: map[string]*StepData{stepID: {
			StepID: stepID,
			Status: StepStatusComplete,
			Limits: &StepLimits{FailCount: 20, TimeSeconds: 12},
			Metrics: &MetricsTotals{StepID: stepID, Rows: []MetricsTotalsRow{
				{Operation: "CREATE", SuccessCount: 21, FailureCount: 22, SizeBytes: 5505024, StepDurationSeconds: createSeconds, ThroughputAvgOps: 66.7, NodeCount: 1},
				{Operation: "STAT", SuccessCount: 57, FailureCount: 1, StepDurationSeconds: statSeconds, ThroughputAvgOps: 180.4, NodeCount: 1},
			}},
		}},
	}
}

func TestAggregateMixedFailureLimitStopWarning(t *testing.T) {
	t.Parallel()

	const want = "Mixed: stopped at its failure limit after 23 failed requests (load.op.limit.fail.count 20), 322ms into its 12s time limit"
	cases := []struct {
		name string
		data *RunData
		want []string
	}{
		// The phase's duration is the longest of its operations' (0.322 s here, not 0.316 s).
		{"short duration", failLimitMixedRun(0.316, 0.322), []string{want}},
		{"full-length duration", failLimitMixedRun(12.05, 12.08), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			summary, err := Aggregate(tc.data)
			if err != nil {
				t.Fatalf("Aggregate: %v", err)
			}
			var got []string
			for _, warning := range summary.Warnings {
				if strings.Contains(warning, "failure limit") {
					got = append(got, warning)
				}
			}
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Fatalf("failure-limit warnings = %q, want %q", got, tc.want)
			}
			if !containsString(summary.Warnings, "Mixed: 23 requests failed") {
				t.Fatalf("failed-count warning missing: %q", summary.Warnings)
			}
			renders := map[string]string{
				"full":    NewRenderer(RenderOptions{MaxWidth: 110}).FullReport(summary),
				"console": NewRenderer(RenderOptions{MaxWidth: 100}).ConsoleSnippet(summary),
				"compact": NewRenderer(RenderOptions{MaxWidth: 72}).CompactSnippet(summary),
			}
			for name, text := range renders {
				// The compact TUI wraps long lines, so compare without line breaks and indentation.
				flat := strings.Join(strings.Fields(text), " ")
				has := strings.Contains(flat, "stopped at its failure limit after 23 failed requests")
				if has != (len(tc.want) > 0) {
					t.Fatalf("%s output shows the warning = %v, want %v:\n%s", name, has, len(tc.want) > 0, text)
				}
			}
		})
	}
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
