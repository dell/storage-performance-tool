package summary

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

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

// The golden file was rendered by the code before this change (3227d078).
func TestMissingTotalsNoneMissingCompactSnippetUnchanged(t *testing.T) {
	t.Parallel()

	summary := missingTotalsRun(t,
		missingTotalsStep{id: missingTotalsCreateStep, op: "CREATE", bytes: 600 * bytesInMiB, seconds: 6, failures: 2},
		missingTotalsStep{id: missingTotalsReadStep, op: "READ", bytes: 400 * bytesInMiB, seconds: 4},
	)
	want, err := os.ReadFile(filepath.Join("testdata", "summary_compact_none_missing.golden"))
	if err != nil {
		t.Fatal(err)
	}
	if got := NewRenderer(RenderOptions{MaxWidth: 120}).CompactSnippet(summary); got != string(want) {
		t.Fatalf("compact snippet changed\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// An absent expected step decides only the case where nothing is counted.
func TestMissingTotalsAbsentStepsDecideOnlyTheUncountedCase(t *testing.T) {
	t.Parallel()

	const cleanupStep = "spt-004-20261115.120000.000-cleanup"
	renderer := NewRenderer(RenderOptions{MaxWidth: 120})

	mixed := missingTotalsRun(t,
		missingTotalsStep{id: missingTotalsCreateStep, missing: true},
		missingTotalsStep{id: missingTotalsReadStep, absent: true},
	)
	if got := totalsLine(t, renderer.CompactSnippet(mixed)); got != "Totals: unavailable (no phase has metrics)" {
		t.Fatalf("missing and absent: compact totals line = %q", got)
	}
	report := renderer.FullReport(mixed)
	mustContain(t, report, "  • Not counted        Create (metrics unavailable); Read (not in results)\n")
	tableRowLine(t, renderer.rateTable(mixed), "Read")
	tableRowLine(t, renderer.phaseTable(mixed), "Read")

	some := missingTotalsRun(t,
		missingTotalsStep{id: missingTotalsCreateStep, op: "CREATE", bytes: 1000 * bytesInMiB, seconds: 10},
		missingTotalsStep{id: missingTotalsReadStep, missing: true},
		missingTotalsStep{id: cleanupStep, absent: true},
	)
	if got := totalsLine(t, renderer.CompactSnippet(some)); got != "Totals (1 of 2 phases): duration 10s, data moved 1000.00 MiB" {
		t.Fatalf("some missing, one absent: compact totals line = %q", got)
	}
	report = renderer.FullReport(some)
	mustContain(t, report, "Run Totals (1 of 2 phases)\n")
	mustContain(t, report, "  • Not counted        Read (metrics unavailable)\n")
	mustContain(t, report, "  • Missing steps      "+cleanupStep+"\n")
	mustNotContain(t, renderer.performanceTable(some), "Cleanup")
}

func TestMissingTotalsNotCountedCollapsesRepeatsAndWraps(t *testing.T) {
	t.Parallel()

	steps := []missingTotalsStep{{id: missingTotalsCreateStep, op: "CREATE", bytes: bytesInMiB, seconds: 1}}
	for i := 0; i < 14; i++ {
		steps = append(steps, missingTotalsStep{id: fmt.Sprintf("spt-%03d-20261115.120000.000-custom%02d", i+3, i), missing: true})
	}
	steps = append(steps,
		missingTotalsStep{id: "spt-090-20261115.120000.000-read", missing: true},
		missingTotalsStep{id: "spt-091-20261115.120000.000-read", missing: true},
	)
	const width = 110
	report := NewRenderer(RenderOptions{MaxWidth: width}).FullReport(missingTotalsRun(t, steps...))

	start := strings.Index(report, "  • Not counted        ")
	if start < 0 {
		t.Fatalf("no Not counted line:\n%s", report)
	}
	block := report[start:]
	block = block[:strings.Index(block, "\n\n")]
	joined := strings.Join(strings.Fields(block), " ")
	mustContain(t, joined, "Custom13, Read ×2 (metrics unavailable)")
	mustNotContain(t, joined, "Read, Read")
	lines := strings.Split(block, "\n")
	if len(lines) < 2 {
		t.Fatalf("expected the Not counted line to wrap:\n%s", block)
	}
	for i, line := range lines {
		if n := len([]rune(line)); n > width {
			t.Errorf("Not counted line %d is %d wide, want <= %d: %q", i, n, width, line)
		}
		if i > 0 && !strings.HasPrefix(line, strings.Repeat(" ", 23)) {
			t.Errorf("continuation line %d not aligned under the value: %q", i, line)
		}
	}
}

// TestMissingTotalsThroughLoader loads a results directory whose Read totals were not
// fetched and whose Cleanup step is absent from the index: the absent step leaves the
// partial total as the indexed steps make it.
func TestMissingTotalsThroughLoader(t *testing.T) {
	t.Parallel()

	const (
		createStep  = "spt-001-20261024.120000.000-create"
		readStep    = "spt-002-20261024.120000.000-read"
		cleanupStep = "spt-003-20261024.120000.000-cleanup"
	)
	write := func(t *testing.T, complete bool) *RunData {
		t.Helper()
		runDir := t.TempDir()
		manifest := &results.Manifest{BaseURL: "http://entry.example:9999", OutputDir: runDir}
		params := &RunParams{
			GeneratedAt: time.Date(2026, 10, 24, 12, 2, 0, 0, time.UTC), WorkloadType: "read",
			ScenarioParams: ScenarioParams{RunID: steadyTestRunID, WorkloadType: "read", ObjectSize: "1MB"},
		}
		for _, step := range []struct{ id, op string }{{createStep, "CREATE"}, {readStep, "READ"}, {cleanupStep, "DELETE"}} {
			params.ExpectedStepIDs = append(params.ExpectedStepIDs, step.id)
			name := step.id + "." + constants.ResultsArtifactSuffixMetricsTotal
			switch {
			case !complete && step.id == cleanupStep:
				continue
			case !complete && step.id == readStep:
				manifest.Steps = append(manifest.Steps, results.StepManifest{StepID: step.id,
					Files: []results.FileStatus{{Name: name, Status: fileStatusMissing}}})
				continue
			}
			row := steadyTestRow{op: step.op, tp: 100, bw: 100}.totalsValues()
			writeSteadyTestFile(t, runDir, name, steadyTestCSV(steadyTestTotalsHeader, []map[string]string{row}))
			manifest.Steps = append(manifest.Steps, results.StepManifest{StepID: step.id,
				Files: []results.FileStatus{{Name: name, Status: fileStatusOK}}})
		}
		writeManifest(t, runDir, manifest)
		writeParams(t, runDir, params)
		data, err := NewLoader().Load(context.Background(), runDir)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		return data
	}
	renderer := NewRenderer(RenderOptions{MaxWidth: 120})

	complete := write(t, true)
	if len(complete.MissingExpectedSteps) != 0 {
		t.Fatalf("complete run lists missing expected steps %v", complete.MissingExpectedSteps)
	}
	if got := totalsLine(t, renderer.CompactSnippet(mustAggregate(t, complete))); !parsedTotalsLine.MatchString(got) {
		t.Fatalf("complete run totals line = %q, want the parsed form", got)
	}

	data := write(t, false)
	if strings.Join(data.MissingExpectedSteps, ",") != cleanupStep {
		t.Fatalf("missing expected steps = %v, want %s", data.MissingExpectedSteps, cleanupStep)
	}
	summary := mustAggregate(t, data)
	compact := renderer.CompactSnippet(summary)
	if got := totalsLine(t, compact); !strings.HasPrefix(got, "Totals (1 of 2 phases): duration ") {
		t.Fatalf("compact totals line = %q", got)
	}
	report := renderer.FullReport(summary)
	mustContain(t, report, "Run Totals (1 of 2 phases)\n")
	mustContain(t, report, "  • Not counted        Read (metrics unavailable)\n")
	tableRowLine(t, renderer.phaseTable(summary), "Read")
	mustNotContain(t, renderer.phaseTable(summary), "Cleanup")
}

// Too narrow for a value column beside the label, the bullet falls back to plain wrapping.
func TestMissingTotalsNotCountedFallsBackAtNarrowWidth(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	NewRenderer(RenderOptions{MaxWidth: 30}).writeIndentedBullet(&buf, "Not counted", "Create, Read ×2 (metrics unavailable)")
	out := strings.TrimSuffix(buf.String(), "\n")
	lines := strings.Split(out, "\n")
	if len(lines) < 2 {
		t.Fatalf("expected the bullet to wrap at width 30, got %q", out)
	}
	for _, line := range lines {
		if n := len([]rune(line)); n > 30 {
			t.Errorf("line is %d wide, want <= 30: %q", n, line)
		}
	}
	if got := strings.Join(strings.Fields(out), " "); got != "• Not counted Create, Read ×2 (metrics unavailable)" {
		t.Fatalf("bullet text = %q", got)
	}
}
