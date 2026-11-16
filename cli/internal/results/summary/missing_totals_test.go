package summary

import (
	"regexp"
	"strings"
	"testing"
)

const (
	missingTotalsSeedStep   = "spt-001-20261115.120000.000-seed"
	missingTotalsCreateStep = "spt-002-20261115.120000.000-create"
	missingTotalsReadStep   = "spt-003-20261115.120000.000-read"
)

// parsedTotalsLine is what tools match to read the run total from the console summary.
var parsedTotalsLine = regexp.MustCompile(`Totals:\s+duration\s+`)

type missingTotalsStep struct {
	id         string
	op         string
	bytes      int64
	seconds    float64
	failures   int64
	missing    bool
	suppressed bool
}

// missingTotalsRun builds a synthetic run: a suppressed seed step, then the given steps.
func missingTotalsRun(t *testing.T, steps ...missingTotalsStep) *RunSummary {
	t.Helper()
	runData := &RunData{
		RunID:        "spt-20261115.120000.000",
		Steps:        make(map[string]*StepData),
		Params:       &RunParams{},
		ManifestPath: "index.json",
		MetadataPath: "params.json",
	}
	runData.Params.ScenarioParams.ObjectSize = "1MiB"
	all := append([]missingTotalsStep{{id: missingTotalsSeedStep, suppressed: true}}, steps...)
	for _, step := range all {
		runData.StepOrder = append(runData.StepOrder, step.id)
		data := &StepData{StepID: step.id, Status: StepStatusComplete, MetricsSuppressed: step.suppressed}
		switch {
		case step.suppressed:
		case step.missing:
			data.Status = StepStatusPartial
			data.MissingRequired = []string{"metrics.total.csv"}
		default:
			data.Metrics = &MetricsTotals{StepID: step.id, Rows: []MetricsTotalsRow{{
				Operation:           step.op,
				SuccessCount:        100,
				FailureCount:        step.failures,
				SizeBytes:           step.bytes,
				StepDurationSeconds: step.seconds,
				ThroughputAvgOps:    10,
				BandwidthAvgMiBps:   100,
				LatencyAvgMicros:    1000,
				LatencyP50Micros:    1000,
				Concurrency:         1,
				NodeCount:           1,
			}}}
		}
		runData.Steps[step.id] = data
	}
	summary, err := Aggregate(runData)
	if err != nil {
		t.Fatalf("Aggregate returned error: %v", err)
	}
	return summary
}

func totalsLine(t *testing.T, snippet string) string {
	t.Helper()
	for _, line := range strings.Split(snippet, "\n") {
		if strings.HasPrefix(line, "Totals") {
			return line
		}
	}
	t.Fatalf("no totals line in:\n%s", snippet)
	return ""
}

func TestMissingTotalsNoneMissingKeepsTotalsLines(t *testing.T) {
	t.Parallel()

	summary := missingTotalsRun(t,
		missingTotalsStep{id: missingTotalsCreateStep, op: "CREATE", bytes: 600 * bytesInMiB, seconds: 6},
		missingTotalsStep{id: missingTotalsReadStep, op: "READ", bytes: 400 * bytesInMiB, seconds: 4},
	)
	if summary.Totals.StepsCounted != 2 || len(summary.Totals.MissingPhases) != 0 {
		t.Fatalf("totals counted %d, missing %v; want 2 and none", summary.Totals.StepsCounted, summary.Totals.MissingPhases)
	}
	renderer := NewRenderer(RenderOptions{MaxWidth: 120})
	compact := renderer.CompactSnippet(summary)
	if got := totalsLine(t, compact); got != "Totals: duration 10s, data moved 1000.00 MiB" {
		t.Fatalf("compact totals line = %q", got)
	}
	report := renderer.FullReport(summary)
	mustContain(t, report, "Run Totals\n  • Duration           10s\n  • Data moved         1000.00 MiB\n")
	mustNotContain(t, report, "Not counted")
	mustNotContain(t, report, "Failed requests")
	mustNotContain(t, report, "│ Seed")
}

func TestMissingTotalsAllMissingSaysUnavailable(t *testing.T) {
	t.Parallel()

	summary := missingTotalsRun(t,
		missingTotalsStep{id: missingTotalsCreateStep, missing: true},
		missingTotalsStep{id: missingTotalsReadStep, missing: true},
	)
	if !summary.Totals.Unavailable() || summary.Totals.Partial() {
		t.Fatalf("totals = %+v, want unavailable", summary.Totals)
	}
	if got := strings.Join(summary.Totals.MissingPhases, ","); got != "Create,Read" {
		t.Fatalf("missing phases = %q, want Create,Read (the suppressed seed never counts)", got)
	}
	renderer := NewRenderer(RenderOptions{MaxWidth: 120})

	compact := renderer.CompactSnippet(summary)
	line := totalsLine(t, compact)
	if line != "Totals: unavailable (no phase has metrics)" {
		t.Fatalf("compact totals line = %q", line)
	}
	if parsedTotalsLine.MatchString(compact) {
		t.Fatalf("unavailable totals match the parsed totals line:\n%s", compact)
	}
	mustNotContain(t, compact, "0 B")
	mustNotContain(t, compact, "duration 0s")

	report := renderer.FullReport(summary)
	mustContain(t, report, "Run Totals\n"+
		"  • Duration           unavailable\n"+
		"  • Data moved         unavailable\n"+
		"  • Failed requests    unavailable\n"+
		"  • Not counted        Create, Read (metrics unavailable)\n")
	mustNotContain(t, report, "Duration           0s")
	mustNotContain(t, report, "Data moved         0 B")

	table := renderer.performanceTable(summary)
	mustContain(t, table, "│ Create │")
	mustContain(t, table, "│ Read   │")
	mustNotContain(t, table, "Seed")
}

func TestMissingTotalsSomeMissingLabelsPartialSum(t *testing.T) {
	t.Parallel()

	summary := missingTotalsRun(t,
		missingTotalsStep{id: missingTotalsCreateStep, op: "CREATE", bytes: 1000 * bytesInMiB, seconds: 10, failures: 3},
		missingTotalsStep{id: missingTotalsReadStep, missing: true},
	)
	if !summary.Totals.Partial() || summary.Totals.StepsCounted != 1 {
		t.Fatalf("totals = %+v, want partial with one step counted", summary.Totals)
	}
	if !summary.Steps[2].MetricsMissing || summary.Steps[0].MetricsMissing || summary.Steps[1].MetricsMissing {
		t.Fatalf("only the Read step should be marked missing: %+v", summary.Steps)
	}
	renderer := NewRenderer(RenderOptions{MaxWidth: 120})

	compact := renderer.CompactSnippet(summary)
	if got := totalsLine(t, compact); got != "Totals (1 of 2 phases): duration 10s, data moved 1000.00 MiB" {
		t.Fatalf("compact totals line = %q", got)
	}
	if parsedTotalsLine.MatchString(compact) {
		t.Fatalf("partial totals match the parsed totals line:\n%s", compact)
	}

	report := renderer.FullReport(summary)
	mustContain(t, report, "Run Totals (1 of 2 phases)\n"+
		"  • Duration           10s\n"+
		"  • Data moved         1000.00 MiB\n"+
		"  • Failed requests    3 (counted phases only)\n"+
		"  • Not counted        Read (metrics unavailable)\n")
}

func TestMissingTotalsTablesShowMissingPhaseRow(t *testing.T) {
	t.Parallel()

	summary := missingTotalsRun(t,
		missingTotalsStep{id: missingTotalsCreateStep, op: "CREATE", bytes: 1000 * bytesInMiB, seconds: 10},
		missingTotalsStep{id: missingTotalsReadStep, missing: true},
	)
	renderer := NewRenderer(RenderOptions{MaxWidth: 120})
	rate := renderer.rateTable(summary)
	phase := renderer.phaseTable(summary)
	for name, table := range map[string]string{"rate": rate, "phase": phase} {
		mustContain(t, table, "│ Create │")
		mustNotContain(t, table, "Seed")
		var readRow string
		for _, line := range strings.Split(table, "\n") {
			if strings.HasPrefix(line, "│ Read ") {
				readRow = line
			}
		}
		if readRow == "" {
			t.Fatalf("%s table has no Read row:\n%s", name, table)
		}
		cells := strings.Split(strings.Trim(readRow, "│"), "│")
		for _, cell := range cells[1:] {
			if strings.TrimSpace(cell) != notApplicableCell {
				t.Fatalf("%s table Read row = %q, want a dash in every value cell", name, readRow)
			}
		}
	}
	// The report renders the same tables.
	mustContain(t, renderer.FullReport(summary), phase)
}

func TestMissingTotalsFailuresNotShownAsNone(t *testing.T) {
	t.Parallel()

	summary := missingTotalsRun(t,
		missingTotalsStep{id: missingTotalsCreateStep, op: "CREATE", bytes: bytesInMiB, seconds: 1},
		missingTotalsStep{id: missingTotalsReadStep, missing: true},
	)
	report := NewRenderer(RenderOptions{MaxWidth: 120}).FullReport(summary)
	// A complete run with no failures omits the line; a partial run must not, because the
	// missing phase's failures are unknown.
	mustContain(t, report, "  • Failed requests    0 (counted phases only)\n")
}
