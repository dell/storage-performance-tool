package summary

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// completeRunsWithAbsentSteps are complete runs whose index lacks an expected step on a
// supported route. Each must render exactly as before the missing-totals change.
func completeRunsWithAbsentSteps(t *testing.T) []struct {
	name    string
	summary *RunSummary
} {
	t.Helper()
	return []struct {
		name    string
		summary *RunSummary
	}{
		{
			// spt run mock --cleanup: the engine runs the planned create and delete as one
			// pipeline step.
			name: "mock pipeline",
			summary: missingTotalsRunSteps(t, "mock",
				missingTotalsStep{id: "pipeline_20261115.120000.000", op: "CREATE", bytes: 1000 * bytesInMiB, seconds: 1.2},
				missingTotalsStep{id: "mt-001-20261115.120000.000-create", absent: true},
				missingTotalsStep{id: "mt-002-20261115.120000.000-delete", absent: true},
			),
		},
		{
			// spt run delete --cleanup: a no-work cleanup can end before it is seen.
			name: "seeded delete without its cleanup",
			summary: missingTotalsRunSteps(t, "delete",
				missingTotalsStep{id: "mt-001-20261115.120000.000-seed", op: "CREATE", bytes: 800 * bytesInKiB, seconds: 0.5},
				missingTotalsStep{id: "mt-002-20261115.120000.000-delete", op: "DELETE", seconds: 0.3},
				missingTotalsStep{id: "mt-003-20261115.120000.000-cleanup", absent: true},
			),
		},
		{
			// Runtime step IDs differ from the planned ones; the runtime seed is suppressed.
			name: "planned and runtime IDs with a suppressed seed",
			summary: missingTotalsRunSteps(t, "read",
				missingTotalsStep{id: "mt-001-20261115.120000.233-seed", suppressed: true},
				missingTotalsStep{id: "mt-002-20261115.120000.233-read", op: "READ", bytes: bytesInMiB, seconds: 1},
				missingTotalsStep{id: "mt-001-20261115.120000.000-seed", absent: true},
				missingTotalsStep{id: "mt-002-20261115.120000.000-read", absent: true},
			),
		},
	}
}

func renderCompleteRunsWithAbsentSteps(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	for _, tc := range completeRunsWithAbsentSteps(t) {
		b.WriteString("### " + tc.name + " (compact, 72)\n")
		b.WriteString(NewRenderer(RenderOptions{MaxWidth: 72}).CompactSnippet(tc.summary))
		b.WriteString("### " + tc.name + " (full, 110)\n")
		b.WriteString(NewRenderer(RenderOptions{MaxWidth: 110}).FullReport(tc.summary))
	}
	return b.String()
}

// The golden file was rendered by the code before the missing-totals change (3227d078).
func TestMissingTotalsCompleteRunsWithAbsentStepsUnchanged(t *testing.T) {
	t.Parallel()

	want, err := os.ReadFile(filepath.Join("testdata", "summary_complete_absent_steps.golden"))
	if err != nil {
		t.Fatal(err)
	}
	if got := renderCompleteRunsWithAbsentSteps(t); got != string(want) {
		t.Fatalf("complete runs with absent steps changed\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// A run whose every expected step is absent from the index counted nothing.
func TestMissingTotalsAllAbsentSaysUnavailable(t *testing.T) {
	t.Parallel()

	summary := missingTotalsRunSteps(t, "read",
		missingTotalsStep{id: "mt-001-20261115.120000.000-seed", absent: true},
		missingTotalsStep{id: "mt-002-20261115.120000.000-read", absent: true},
	)
	renderer := NewRenderer(RenderOptions{MaxWidth: 110})
	compact := renderer.CompactSnippet(summary)
	if got := totalsLine(t, compact); got != "Totals: unavailable (no phase has metrics)" {
		t.Fatalf("compact totals line = %q", got)
	}
	if parsedTotalsLine.MatchString(compact) {
		t.Fatalf("unavailable totals match the parsed totals line:\n%s", compact)
	}
	report := renderer.FullReport(summary)
	mustContain(t, report, "Run Totals\n"+
		"  • Duration           unavailable\n"+
		"  • Data moved         unavailable\n"+
		"  • Failed requests    unavailable\n"+
		"  • Not counted        Seed, Read (not in results)\n")
	for _, table := range []string{renderer.rateTable(summary), renderer.phaseTable(summary)} {
		tableRowLine(t, table, "Seed")
		tableRowLine(t, table, "Read")
	}
}
