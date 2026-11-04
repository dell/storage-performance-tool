package summary

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func phaseStep(label, operation string, success, failure int64) StepSummary {
	return StepSummary{
		PhaseLabel: label,
		Operation:  operation,
		Metrics:    &PhaseMetrics{SuccessCount: success, FailureCount: failure},
	}
}

func mixedStep(ops ...OperationBreakdown) StepSummary {
	step := phaseStep("Mixed", "MIXED", 1, 1)
	step.IsMixed = true
	step.OperationBreakdown = ops
	return step
}

func TestAllFailedPhaseSaysWhatFailed(t *testing.T) {
	tests := map[string]AllFailedPhase{
		"every request of phase Create failed (5 failed, 0 succeeded)":     {Label: "Create", FailureCount: 5},
		"every READ request of phase Mixed failed (5 failed, 0 succeeded)": {Label: "Mixed", Operation: "READ", FailureCount: 5},
		"phase List found no objects and 5 requests failed":                {Label: "List", FailureCount: 5, EmptyListing: true},
		"phase List found no objects and 1 request failed":                 {Label: "List", FailureCount: 1, EmptyListing: true},
	}
	for want, phase := range tests {
		if got := phase.String(); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestAllFailedPhasesNamesOnlyPhasesWhereNothingSucceeded(t *testing.T) {
	tests := []struct {
		name  string
		steps []StepSummary
		want  []AllFailedPhase
	}{
		{"every request failed", []StepSummary{phaseStep("Create", "CREATE", 0, 5301)}, []AllFailedPhase{{Label: "Create", FailureCount: 5301}}},
		{"one succeeded", []StepSummary{phaseStep("Create", "CREATE", 1, 5301)}, nil},
		{"an empty listing has neither", []StepSummary{phaseStep("List", "LIST", 0, 0)}, nil},
		{"a listing with no objects and failures can't show it was empty", []StepSummary{phaseStep("List", "LIST", 0, 47)},
			[]AllFailedPhase{{Label: "List", FailureCount: 47, EmptyListing: true}}},
		{"a listing that found objects is a partial failure", []StepSummary{phaseStep("List", "LIST", 150, 1)}, nil},
		{"a seed that worked doesn't hide a read that didn't", []StepSummary{
			phaseStep("Seed", "CREATE", 1000, 0), phaseStep("Read", "READ", 0, 42),
		}, []AllFailedPhase{{Label: "Read", FailureCount: 42}}},
		{"a seed that failed counts", []StepSummary{
			phaseStep("Seed", "CREATE", 0, 9), phaseStep("Read", "READ", 5, 0),
		}, []AllFailedPhase{{Label: "Seed", FailureCount: 9}}},
		{"both phases", []StepSummary{
			phaseStep("Seed", "CREATE", 0, 9), phaseStep("Read", "READ", 0, 3),
		}, []AllFailedPhase{{Label: "Seed", FailureCount: 9}, {Label: "Read", FailureCount: 3}}},
		{"a cleanup is best-effort", []StepSummary{
			phaseStep("Create", "CREATE", 500, 0), phaseStep("Delete", "DELETE", 0, 500),
		}, nil},
		{"one operation of a mix", []StepSummary{mixedStep(
			OperationBreakdown{Operation: "READ", Metrics: PhaseMetrics{FailureCount: 1364}},
			OperationBreakdown{Operation: "CREATE", Metrics: PhaseMetrics{SuccessCount: 1678}},
		)}, []AllFailedPhase{{Label: "Mixed", Operation: "READ", FailureCount: 1364}}},
		{"a mix's DELETE is measured, not a cleanup", []StepSummary{mixedStep(
			OperationBreakdown{Operation: "DELETE", Metrics: PhaseMetrics{FailureCount: 5}},
			OperationBreakdown{Operation: "READ", Metrics: PhaseMetrics{SuccessCount: 5}},
		)}, []AllFailedPhase{{Label: "Mixed", Operation: "DELETE", FailureCount: 5}}},
		{"a mix with partial failures and an idle operation", []StepSummary{mixedStep(
			OperationBreakdown{Operation: "READ", Metrics: PhaseMetrics{SuccessCount: 7, FailureCount: 90}},
			OperationBreakdown{Operation: "STAT"},
		)}, nil},
		{"no metrics say nothing", []StepSummary{{PhaseLabel: "Read", Operation: "READ"}}, nil},
		{"an unlabeled phase uses its step", []StepSummary{{
			StepID: "s-read", Operation: "READ", Metrics: &PhaseMetrics{FailureCount: 1},
		}}, []AllFailedPhase{{Label: "s-read", FailureCount: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := (&RunSummary{Steps: tt.steps}).AllFailedPhases()
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
	if got := (*RunSummary)(nil).AllFailedPhases(); got != nil {
		t.Fatalf("nil summary: got %v", got)
	}
}

func failureReport(t *testing.T, failures int64) *RunSummary {
	t.Helper()
	step := phaseStep("Read", "READ", 2698, failures)
	step.SuccessUnit = "ops"
	step.Metrics.DataBytes = 2762752
	step.Metrics.ObjectSizeHuman = "1.00 KiB"
	return &RunSummary{Steps: []StepSummary{step}, Totals: RunTotals{FailureCount: failures, DurationHuman: "1m 0s"}}
}

func TestSummaryShowsFailedRequests(t *testing.T) {
	summary := failureReport(t, 545)
	full := NewRenderer(RenderOptions{}).FullReport(summary)
	if !strings.Contains(full, "Failed requests") || !strings.Contains(full, "545") {
		t.Fatalf("full report hides the failures:\n%s", full)
	}
}

func TestSummaryOfAHealthyRunAddsNoFailureLine(t *testing.T) {
	summary := failureReport(t, 0)
	if full := NewRenderer(RenderOptions{}).FullReport(summary); strings.Contains(full, "Failed requests") {
		t.Fatalf("healthy full report:\n%s", full)
	}
}

func TestFailedRequestsAreAWarningThatWrapsInTheNarrowReport(t *testing.T) {
	const readStep = "spt-001-20261024.120000.000-read"
	row := steadyTestRow{op: "READ", tp: 340, bw: 340, steadyTP: 350, steadyBW: 350, status: steadyStatusValid, failed: 54321}
	steps := []steadyTestStep{{id: readStep, rows: []steadyTestRow{row}, steadyListed: fileStatusOK}}
	summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), steps))
	compact := NewRenderer(RenderOptions{MaxWidth: 72}).CompactSnippet(summary)
	if !strings.Contains(compact, "Read: 54321 requests failed") {
		t.Fatalf("compact report hides the failures:\n%s", compact)
	}
	row.failed = 0
	steps[0].rows = []steadyTestRow{row}
	healthy := loadSteadyTestSummary(t, writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), steps))
	for _, warning := range healthy.Warnings {
		if strings.Contains(warning, "requests failed") {
			t.Fatalf("healthy run warned: %q", warning)
		}
	}
}

// Comparison tooling reads these cells from the saved summary, so these forms are a contract.
func TestFailedColumnLeavesTheCellsToolsReadInPlace(t *testing.T) {
	for _, failures := range []int64{0, 545} {
		report := NewRenderer(RenderOptions{}).FullReport(failureReport(t, failures))
		beside := regexp.MustCompile(`│\s*(\d{3,})\s*│\s*[\d.]+\s*[KMGT]?[i]?[B]`).FindAllStringSubmatch(report, -1)
		if len(beside) != 1 || beside[0][1] != "2698" {
			t.Fatalf("failures %d: Success beside Data Moved = %v\n%s", failures, beside, report)
		}
		third := regexp.MustCompile(`│\s*Read\s*│[^│]*│\s*(\d+)\s*│`).FindStringSubmatch(report)
		if third == nil || third[1] != "2698" {
			t.Fatalf("failures %d: Success as third column = %v\n%s", failures, third, report)
		}
		if strings.Contains(report, "ops/s │ "+strconv.FormatInt(failures, 10)) {
			t.Fatalf("failures %d: a rate cell is followed by the count\n%s", failures, report)
		}
	}
}

func TestSteadyWarningCoversAValidWindowWhoseRateDidNotHold(t *testing.T) {
	const readStep = "spt-001-20261024.120000.000-read"
	valid := func(trend, cv string) steadyTestRow {
		return steadyTestRow{op: "READ", tp: 340, bw: 340, steadyTP: 350, steadyBW: 350, status: steadyStatusValid, trend: trend, cv: cv}
	}
	short := valid("-90", "2")
	short.status = steadyStatusShort
	tests := []struct {
		name string
		rows []steadyTestRow
		want string
	}{
		{"healthy", []steadyTestRow{valid("", "")}, ""},
		{"trend at the cutoff", []steadyTestRow{valid("-50", "0.1")}, "trend -50.0%, CV 0.10"},
		{"trend just inside", []steadyTestRow{valid("-49.9", "0.1")}, ""},
		{"cv at the cutoff", []steadyTestRow{valid("-1", "0.5")}, "trend -1.0%, CV 0.50"},
		{"cv just inside", []steadyTestRow{valid("-1", "0.49")}, ""},
		{"a rising ramp alone doesn't warn", []steadyTestRow{valid("80", "0.3")}, ""},
		{"an empty trend still warns on cv", []steadyTestRow{valid("-", "0.7")}, "(CV 0.70)"},
		{"empty cells say nothing", []steadyTestRow{valid("-", "-")}, ""},
		{"unreadable cells say nothing", []steadyTestRow{valid("n/a", "n/a")}, ""},
		{"only a valid window warns", []steadyTestRow{short}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			steps := []steadyTestStep{{id: readStep, rows: tt.rows, steadyListed: fileStatusOK}}
			summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), steps))
			var found []string
			for _, warning := range summary.Warnings {
				if strings.Contains(warning, "didn't hold") {
					found = append(found, warning)
				}
			}
			if tt.want == "" {
				if len(found) != 0 {
					t.Fatalf("unexpected warning: %v", found)
				}
				return
			}
			if len(found) != 1 || !strings.Contains(found[0], tt.want) || !strings.Contains(found[0], "uncalibrated") ||
				!strings.HasPrefix(found[0], "Read: ") {
				t.Fatalf("warnings = %v, want one with %q", summary.Warnings, tt.want)
			}
			if !summary.Steps[0].Metrics.Steady.Valid {
				t.Fatalf("the window must stay valid")
			}
			// Comparison tooling sums every rate it finds in the saved summary.
			if strings.Contains(found[0], "ops/s") || strings.Contains(found[0], "MB/s") {
				t.Fatalf("the warning must not carry a rate: %q", found[0])
			}
		})
	}
}

func TestAShortWindowDoesNotHideAValidWindowThatDidNotHold(t *testing.T) {
	const mixedStep = "spt-001-20261024.120000.000-mixed"
	steps := []steadyTestStep{{id: mixedStep, rows: []steadyTestRow{
		{op: "READ", tp: 340, bw: 340, steadyTP: 350, steadyBW: 350, status: steadyStatusValid, trend: "-90", cv: "1.2"},
		{op: "CREATE", tp: 40, bw: 40, steadyTP: 50, steadyBW: 50, status: steadyStatusShort},
	}, steadyListed: fileStatusOK}}
	summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "mixed", releaseEngineIdentity("5.17.0", false), steps))
	if summary.Steps[0].Metrics.Steady.Valid {
		t.Fatalf("a phase with a short window must show no steady rate")
	}
	found := false
	for _, warning := range summary.Warnings {
		found = found || strings.Contains(warning, "trend -90.0%, CV 1.20")
	}
	if !found {
		t.Fatalf("warnings = %v", summary.Warnings)
	}
}

func TestSteadyWarningOfAMixedPhaseUsesItsWorstReadings(t *testing.T) {
	unsteady := worseSteadyInstability(nil, SteadyTotalsRow{TrendPercent: parsed(-60), CV: parsed(0.2)})
	unsteady = worseSteadyInstability(unsteady, SteadyTotalsRow{TrendPercent: parsed(-5), CV: parsed(0.9)})
	if unsteady == nil || *unsteady.TrendPercent != -60 || *unsteady.CV != 0.9 {
		t.Fatalf("unsteady = %+v", unsteady)
	}
	if got := worseSteadyInstability(nil, SteadyTotalsRow{TrendPercent: parsed(-5), CV: parsed(0.1)}); got != nil {
		t.Fatalf("a healthy row warned: %+v", got)
	}
}

func parsed(v float64) *float64 { return &v }
