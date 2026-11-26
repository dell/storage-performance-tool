package summary

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// Lifecycle rows measured in the triage of mixed-seed-step-fail-limit-silent 01 (mcli-poc
// comparison/results/triage-20261121/mixed-seed-step-fail-limit-silent).
const (
	// Every second PUT failing, load.op.limit.fail.count=20 (out/cli-seedcut-get50-stat50-fc20).
	seedCutRow = "1,1795700418814,mt-001-seed,1470ae42-ad74-4e74-a980-4f49e1df5b59,1,true,2500,27,28,2445,0,55,0,0,0"
	// Every tenth PUT failing, default limit (out/cli-seed10pct-get50-stat50).
	seedTenthRow = "1,1795700418814,mt-001-seed,w0,1,true,2500,2250,250,0,0,2500,0,0,0"
	// Every seed PUT failing, fail count 20 (second-opinion-probe/t2-mixed-allfail-real.txt).
	seedAllFailedRow = "1,1795700418814,mt-001-seed,w0,1,true,2500,0,28,2472,0,28,0,0,0"
	// 75% PUT failures, load.op.limit.fail.rate=true (second-opinion-probe/t2-mixed-failrate.txt).
	seedRateRow = "1,1795700418814,mt-001-seed,w0,1,true,2500,88,263,2149,0,351,0,0,0"
	// --seed-objects 10000, above load.batch.size (second-opinion-probe/t2-mixed-seed10000-fc20.txt).
	seedTenThousandRow = "1,1795700418814,mt-001-seed,w0,1,true,10000,31,30,9939,0,61,0,0,0"
	// Two engines, count 500, fail count 20 (second-opinion-probe/t2-dist-seed-fc20.txt).
	seedDistRow0 = "1,1795271620154,t2-dist-seed,b12e65b9-67ab-479d-82a7-5ffc9961d29d,1,true,250,40,124,86,0,164,0,0,0"
	seedDistRow1 = "1,1795271620154,t2-dist-seed,26defe94-eb2e-43cc-8674-c5ad659ebb5d,1,true,250,6,14,230,0,20,0,0,0"
	// A healthy seed.
	seedFullRow = "1,1795700418814,mt-001-seed,w0,1,true,2500,2500,0,0,0,2500,0,0,0"
	// spt run write --duration 2s with summary persistence off: selected is the generator's
	// prefetch (second-opinion-probe/t2-write-duration-persist-false.txt).
	timeLimitedCreateRow = "1,1795700418814,spt-001-write,w0,1,true,786432,398,0,786034,0,398,0,0,0"

	seedStepID = "spt-001-20261126.120000.000-seed"
)

func seedLifecycle(t *testing.T, rows ...string) *OperationLifecycle {
	t.Helper()
	lifecycle, err := parseOperationLifecycle(strings.NewReader(lifecycleHeader + "\n" + strings.Join(rows, "\n") + "\n"))
	if err != nil {
		t.Fatalf("parse lifecycle: %v", err)
	}
	return lifecycle
}

func TestParseOperationLifecycleCounts(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		rows  []string
		rowsN int
		total LifecycleCounts
	}{
		{"seed cut at its failure limit", []string{seedCutRow}, 1, LifecycleCounts{Selected: 2500, Accepted: 27, Failed: 28, Unattempted: 2445}},
		{"two workers", []string{seedDistRow0, seedDistRow1}, 2, LifecycleCounts{Selected: 500, Accepted: 46, Failed: 138, Unattempted: 316}},
		{"seed above load.batch.size", []string{seedTenThousandRow}, 1, LifecycleCounts{Selected: 10000, Accepted: 31, Failed: 30, Unattempted: 9939}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := seedLifecycle(t, tc.rows...)
			if !got.CountsKnown || len(got.Rows) != tc.rowsN || got.Total != tc.total {
				t.Fatalf("lifecycle = %+v, want %d rows totalling %+v", got, tc.rowsN, tc.total)
			}
		})
	}
	t.Run("rows keep their own counts", func(t *testing.T) {
		t.Parallel()
		got := seedLifecycle(t, seedDistRow0, seedDistRow1)
		want := []LifecycleCounts{{Selected: 250, Accepted: 40, Failed: 124, Unattempted: 86}, {Selected: 250, Accepted: 6, Failed: 14, Unattempted: 230}}
		if !reflect.DeepEqual(got.Rows, want) {
			t.Fatalf("rows = %+v, want %+v", got.Rows, want)
		}
	})
	// A bad count column loses only the seed counts; the unresolved count is still exact.
	for name, content := range map[string]string{
		"only the unresolved column": "unresolved\n3\n",
		"bad accepted value":         strings.Replace(lifecycleCSV("3"), ",10,5,0,0,3,", ",10,x,0,0,3,", 1),
		"repeated failed column":     "selected,accepted,failed,failed,unattempted,unresolved\n10,5,1,1,0,3\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := parseOperationLifecycle(strings.NewReader(content))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got.CountsKnown || got.Total != (LifecycleCounts{Unresolved: 3}) || got.Rows != nil {
				t.Fatalf("lifecycle = %+v, want only unresolved 3", got)
			}
		})
	}
}

func TestSeedShortfallWarningRule(t *testing.T) {
	t.Parallel()
	seedLimits := func(failCount int64, rate bool) *StepLimits {
		return &StepLimits{OpType: "create", OpCount: 2500, FailCount: failCount, FailRate: rate}
	}
	seed := func(limits *StepLimits, rows ...string) *StepData {
		return &StepData{StepID: seedStepID, MetricsSuppressed: true, Limits: limits, Lifecycle: seedLifecycle(t, rows...)}
	}
	const lifecycleSource = " (operation.lifecycle.csv)"
	const later = "; later phases used only the objects written"
	cases := []struct {
		name  string
		end   RunEnd
		step  *StepData
		later bool
		want  string
	}{
		{"cut at its failure limit", RunEndCompleted, seed(seedLimits(20, false), seedCutRow), true,
			"Seed: wrote 27 of 2500 objects (28 failed, 2445 not attempted) and stopped at its failure limit (load.op.limit.fail.count 20)" + later + lifecycleSource},
		{"failed PUTs under the default limit", RunEndCompleted, seed(seedLimits(100000, false), seedTenthRow), true,
			"Seed: wrote 2250 of 2500 objects (250 failed, 0 not attempted)" + later + lifecycleSource},
		{"every PUT failed, on a failed run", RunEndFailed, seed(seedLimits(20, false), seedAllFailedRow), true,
			"Seed: wrote 0 of 2500 objects (28 failed, 2472 not attempted) and stopped at its failure limit (load.op.limit.fail.count 20)" + later + lifecycleSource},
		{"seed is the last step", RunEndCompleted, seed(seedLimits(20, false), seedCutRow), false,
			"Seed: wrote 27 of 2500 objects (28 failed, 2445 not attempted) and stopped at its failure limit (load.op.limit.fail.count 20)" + lifecycleSource},
		{"failures at the limit", RunEndCompleted, seed(seedLimits(28, false), seedCutRow), true,
			"Seed: wrote 27 of 2500 objects (28 failed, 2445 not attempted)" + later + lifecycleSource},
		{"a seed that wrote everything", RunEndCompleted, seed(seedLimits(20, false), seedFullRow), true, ""},
		// Two engines splitting fail count 21 as 11/10: the config shows 11, and only slice 1 stopped.
		{"odd split, slice 1 stops", RunEndCompleted, seed(seedLimits(11, false),
			"1,1,s,w0,1,true,250,200,5,45,0,205,0,0,0", "1,1,s,w1,1,true,250,30,11,209,0,41,0,0,0"), true,
			"Seed: wrote 230 of 500 objects (16 failed, 254 not attempted) and stopped at a node's failure limit (load.op.limit.fail.count of up to 11 per node)" + later + lifecycleSource},
		{"measured two-engine stop", RunEndCompleted, seed(seedLimits(10, false), seedDistRow0, seedDistRow1), true,
			"Seed: wrote 46 of 500 objects (138 failed, 316 not attempted) and stopped at a node's failure limit (load.op.limit.fail.count of up to 10 per node)" + later + lifecycleSource},
		// Two nodes stopped by the rate at 6 failures each, slice count 10: not the count.
		{"two nodes stopped by the rate", RunEndCompleted, seed(seedLimits(10, true),
			"1,1,s,w0,1,true,250,5,6,239,0,11,0,0,0", "1,1,s,w1,1,true,250,5,6,239,0,11,0,0,0"), true,
			"Seed: wrote 10 of 500 objects (12 failed, 478 not attempted) with the failure-rate limit on (load.op.limit.fail.rate), which stops a step when failures outpace successes" + later + lifecycleSource},
		{"rate on", RunEndCompleted, seed(seedLimits(100000, true), seedRateRow), true,
			"Seed: wrote 88 of 2500 objects (263 failed, 2149 not attempted) with the failure-rate limit on (load.op.limit.fail.rate), which stops a step when failures outpace successes" + later + lifecycleSource},
		{"rate on, late spike", RunEndCompleted, seed(seedLimits(100000, true), "1,1,s,w0,1,true,2500,2000,300,200,0,2300,0,0,0"), true,
			"Seed: wrote 2000 of 2500 objects (300 failed, 200 not attempted) with the failure-rate limit on (load.op.limit.fail.rate), which stops a step when failures outpace successes" + later + lifecycleSource},
		{"rate on, auto-terminated", RunEndAutoTerminated, seed(seedLimits(100000, true), seedRateRow), true,
			"Seed: wrote 88 of 2500 objects (263 failed, 2149 not attempted)" + later + lifecycleSource},
		{"rate on, interrupted", RunEndInterrupted, seed(seedLimits(100000, true), seedRateRow), true,
			"Seed: wrote 88 of 2500 objects (263 failed, 2149 not attempted)" + later + lifecycleSource},
		{"count cause on an interrupted run", RunEndInterrupted, seed(seedLimits(20, true), seedCutRow), true,
			"Seed: wrote 27 of 2500 objects (28 failed, 2445 not attempted) and stopped at its failure limit (load.op.limit.fail.count 20)" + later + lifecycleSource},
		{"rate on, nothing left unattempted", RunEndCompleted, seed(seedLimits(100000, true), seedTenthRow), true,
			"Seed: wrote 2250 of 2500 objects (250 failed, 0 not attempted)" + later + lifecycleSource},
		{"time-limited create", RunEndCompleted, &StepData{MetricsSuppressed: true,
			Limits: &StepLimits{OpType: "create", TimeSeconds: 2}, Lifecycle: seedLifecycle(t, timeLimitedCreateRow)}, true, ""},
		{"count and time limits", RunEndCompleted, &StepData{MetricsSuppressed: true,
			Limits: &StepLimits{OpType: "create", OpCount: 786432, TimeSeconds: 2}, Lifecycle: seedLifecycle(t, timeLimitedCreateRow)}, true, ""},
		{"size-limited create", RunEndCompleted, &StepData{MetricsSuppressed: true,
			Limits: &StepLimits{OpType: "create", OpCount: 2500, SizeLimited: true}, Lifecycle: seedLifecycle(t, seedCutRow)}, true, ""},
		{"suppressed read step", RunEndCompleted, &StepData{MetricsSuppressed: true,
			Limits: &StepLimits{OpType: "read", OpCount: 2500, FailCount: 20}, Lifecycle: seedLifecycle(t, seedCutRow)}, true, ""},
		{"metrics not suppressed", RunEndCompleted, &StepData{
			Limits: seedLimits(20, false), Lifecycle: seedLifecycle(t, seedCutRow)}, true, ""},
		{"no lifecycle file", RunEndCompleted, &StepData{MetricsSuppressed: true, Limits: seedLimits(20, false)}, true, ""},
		{"no readable limits", RunEndCompleted, &StepData{MetricsSuppressed: true, Lifecycle: seedLifecycle(t, seedCutRow)}, true, ""},
		{"counts unknown", RunEndCompleted, &StepData{MetricsSuppressed: true, Limits: seedLimits(20, false),
			Lifecycle: &OperationLifecycle{Total: LifecycleCounts{Unresolved: 3}}}, true, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := seedShortfallWarning("Seed", tc.end, tc.step, tc.later); got != tc.want {
				t.Fatalf("warning =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

// seedRun writes a two-step read run, a seed and the read phase, into dir and loads it.
func seedRun(t *testing.T, dir string, seedConfig []byte, lifecycle string) *RunData {
	t.Helper()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	seedFiles := []results.FileStatus{}
	if seedConfig != nil {
		name := seedStepID + "." + constants.ResultsArtifactSuffixConfig
		write(name, string(seedConfig))
		seedFiles = append(seedFiles, results.FileStatus{Name: name, Status: fileStatusOK})
	}
	if lifecycle != "" {
		name := seedStepID + "." + constants.ResultsArtifactSuffixOperationLifecycle
		write(name, lifecycle)
		seedFiles = append(seedFiles, results.FileStatus{Name: name, Status: fileStatusOK})
	}
	metricsName := lifecycleStep + "." + constants.ResultsArtifactSuffixMetricsTotal
	write(metricsName, sampleMetricsCSV([]string{lifecycleOKRow}))
	writeManifest(t, dir, &results.Manifest{OutputDir: dir, Steps: []results.StepManifest{
		{StepID: seedStepID, Files: seedFiles},
		{StepID: lifecycleStep, Files: []results.FileStatus{{Name: metricsName, Status: fileStatusOK}}},
	}})
	writeParams(t, dir, &RunParams{
		GeneratedAt:     time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC),
		ScenarioParams:  ScenarioParams{WorkloadType: "read", ObjectSize: "100MiB"},
		ExpectedStepIDs: []string{seedStepID, lifecycleStep},
	})
	data, err := NewLoader().Load(context.Background(), dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	data.End = RunEndCompleted
	return data
}

func seedWarnings(summary *RunSummary) []string {
	var lines []string
	for _, warning := range summary.Warnings {
		if strings.HasPrefix(warning, "Seed: wrote") {
			lines = append(lines, warning)
		}
	}
	return lines
}

func TestAggregateSeedShortfallWarning(t *testing.T) {
	t.Parallel()
	config, err := os.ReadFile(filepath.Join("testdata", "seed_step.config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	const want = "Seed: wrote 27 of 2500 objects (28 failed, 2445 not attempted) and stopped at its failure limit (load.op.limit.fail.count 20); later phases used only the objects written (operation.lifecycle.csv)"

	t.Run("cut seed", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		data := seedRun(t, dir, config, lifecycleHeader+"\n"+seedCutRow+"\n")
		if got := data.Steps[seedStepID].Limits; got == nil || *got != (StepLimits{OpType: "create", OpCount: 2500, FailCount: 20}) {
			t.Fatalf("seed limits = %+v", got)
		}
		summary := aggregateLifecycleRun(t, data)
		if lines := seedWarnings(summary); len(lines) != 1 || lines[0] != want {
			t.Fatalf("seed warnings = %q, want %q", lines, want)
		}
		for name, output := range lifecycleOutputs(summary) {
			// CompactSnippet wraps its warnings.
			if !strings.Contains(strings.Join(strings.Fields(output), " "), want) {
				t.Errorf("%s lacks the warning:\n%s", name, output)
			}
		}
		// The warning is the only difference from a seed that wrote everything.
		baseline := aggregateLifecycleRun(t, seedRun(t, dir, config, lifecycleHeader+"\n"+seedFullRow+"\n"))
		if !reflect.DeepEqual(summary.Steps, baseline.Steps) || !reflect.DeepEqual(summary.Totals, baseline.Totals) {
			t.Fatal("steps or totals changed")
		}
		summary.Warnings = slices.DeleteFunc(summary.Warnings, func(w string) bool { return w == want })
		assertSameLifecycleOutputs(t, summary, baseline)
	})
	for name, data := range map[string]func() *RunData{
		"full seed":         func() *RunData { return seedRun(t, t.TempDir(), config, lifecycleHeader+"\n"+seedFullRow+"\n") },
		"no lifecycle file": func() *RunData { return seedRun(t, t.TempDir(), config, "") },
		"bad lifecycle":     func() *RunData { return seedRun(t, t.TempDir(), config, lifecycleHeader+"\n1,42\n") },
		"no config":         func() *RunData { return seedRun(t, t.TempDir(), nil, lifecycleHeader+"\n"+seedCutRow+"\n") },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if lines := seedWarnings(aggregateLifecycleRun(t, data())); len(lines) != 0 {
				t.Fatalf("seed warnings = %q", lines)
			}
		})
	}
}
