package summary

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

const (
	lifecycleStep     = "spt-003-20261115.120000.000-read"
	lifecycleFile     = lifecycleStep + ".operation.lifecycle.csv"
	lifecycleNodeFile = lifecycleStep + ".operation.lifecycle.node-000.csv"
	lifecycleHeader   = "schema_version,engine_run_id,step_id,worker_id,contexts,terminal,selected,accepted,failed,unattempted,unresolved,terminal_results,generator_buffered,driver_queued,in_flight"
	lifecycleOKRow    = `"2026-11-15T12:00:00Z",READ,8,1,8,8,100,0,104857600,1,8,100,100,100,100,1000,500,750,900,950,2000,800,400,650,800,900,1800`
	lifecycleFailRow  = `"2026-11-15T12:00:00Z",READ,8,1,8,8,0,5,0,1,8,0,0,0,0,1000,500,750,900,950,2000,800,400,650,800,900,1800`
)

// lifecycleCSV is the file the engine writes, with one row per unresolved value.
func lifecycleCSV(unresolved ...string) string {
	var b strings.Builder
	b.WriteString(lifecycleHeader + "\n")
	for i, value := range unresolved {
		fmt.Fprintf(&b, "1,42,%s,worker-%d,1,true,10,5,0,0,%s,5,0,0,0\n", lifecycleStep, i, value)
	}
	return b.String()
}

// lifecycleArtifact is a file of the fake run. An empty status leaves it out of the manifest, and
// fileStatusMissing lists it with no file. absent lists it as ok with nothing on disk, directory
// puts a directory in its place, and outsideLink and insideLink a symlink to a file outside or
// inside the run directory.
type lifecycleArtifact struct {
	name        string
	content     string
	status      string
	absent      bool
	directory   bool
	outsideLink bool
	insideLink  bool
}

func okLifecycle(content string) lifecycleArtifact {
	return lifecycleArtifact{name: lifecycleFile, content: content, status: fileStatusOK}
}

// symlinkOrSkip skips the test on a Windows account that may not create links.
func symlinkOrSkip(t *testing.T, target, path string) {
	t.Helper()
	if err := os.Symlink(target, path); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("cannot create a symlink: %v", err)
		}
		t.Fatal(err)
	}
}

// loadLifecycleRun writes a one-step run into dir and loads it.
func loadLifecycleRun(t *testing.T, dir, metricsRow string, artifacts ...lifecycleArtifact) (*RunData, error) {
	t.Helper()
	metricsName := lifecycleStep + "." + constants.ResultsArtifactSuffixMetricsTotal
	if err := os.WriteFile(filepath.Join(dir, metricsName), []byte(sampleMetricsCSV([]string{metricsRow})), 0o600); err != nil {
		t.Fatal(err)
	}
	files := []results.FileStatus{{Name: metricsName, Status: fileStatusOK}}
	for _, artifact := range artifacts {
		path := filepath.Join(dir, artifact.name)
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		switch {
		case artifact.directory:
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		case artifact.outsideLink:
			outside := filepath.Join(t.TempDir(), "outside.csv")
			if err := os.WriteFile(outside, []byte(artifact.content), 0o600); err != nil {
				t.Fatal(err)
			}
			symlinkOrSkip(t, outside, path)
		case artifact.insideLink:
			const target = "lifecycle-target.csv"
			if err := os.WriteFile(filepath.Join(dir, target), []byte(artifact.content), 0o600); err != nil {
				t.Fatal(err)
			}
			symlinkOrSkip(t, target, path)
		case !artifact.absent && artifact.status != fileStatusMissing:
			if err := os.WriteFile(path, []byte(artifact.content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if artifact.status != "" {
			files = append(files, results.FileStatus{Name: artifact.name, Status: artifact.status})
		}
	}
	writeManifest(t, dir, &results.Manifest{OutputDir: dir, Steps: []results.StepManifest{{StepID: lifecycleStep, Files: files}}})
	writeParams(t, dir, &RunParams{
		GeneratedAt:    time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC),
		ScenarioParams: ScenarioParams{WorkloadType: "read", ObjectSize: "100MiB"},
	})
	return NewLoader().Load(context.Background(), dir)
}

func aggregateLifecycleRun(t *testing.T, data *RunData) *RunSummary {
	t.Helper()
	summary, err := Aggregate(data)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	return summary
}

// lifecycleOutputs renders the three summaries at the widths spt run uses.
func lifecycleOutputs(summary *RunSummary) map[string]string {
	return map[string]string{
		"FullReport":     NewRenderer(RenderOptions{MaxWidth: 110}).FullReport(summary),
		"ConsoleSnippet": NewRenderer(RenderOptions{MaxWidth: 100}).ConsoleSnippet(summary),
		"CompactSnippet": NewRenderer(RenderOptions{MaxWidth: 72}).CompactSnippet(summary),
	}
}

func assertSameLifecycleOutputs(t *testing.T, got, want *RunSummary) {
	t.Helper()
	wantOutputs := lifecycleOutputs(want)
	for name, output := range lifecycleOutputs(got) {
		if output != wantOutputs[name] {
			t.Errorf("%s changed:\n--- got\n%s\n--- want\n%s", name, output, wantOutputs[name])
		}
	}
}

func unresolvedWarnings(summary *RunSummary) []string {
	var lines []string
	for _, warning := range summary.Warnings {
		if strings.Contains(warning, "unresolved") {
			lines = append(lines, warning)
		}
	}
	return lines
}

func TestUnresolvedOperationsWarningFromLifecycleFile(t *testing.T) {
	t.Parallel()
	const tail = " unresolved at the stop, in neither Success nor Failed; Data Moved can include their bytes (operation.lifecycle.csv)"
	nodeCopy := lifecycleArtifact{name: lifecycleNodeFile, content: lifecycleCSV("500"), status: fileStatusOK}
	tests := []struct {
		name      string
		artifacts []lifecycleArtifact
		want      string
	}{
		{"plural", []lifecycleArtifact{okLifecycle(lifecycleCSV("1845"))}, "Read: 1845 operations" + tail},
		{"singular", []lifecycleArtifact{okLifecycle(lifecycleCSV("1"))}, "Read: 1 operation" + tail},
		{"merged rows are summed and node copies are not", []lifecycleArtifact{okLifecycle(lifecycleCSV("704", "108")), nodeCopy}, "Read: 812 operations" + tail},
		{"workers that sum to one stay singular", []lifecycleArtifact{okLifecycle(lifecycleCSV("0", "1"))}, "Read: 1 operation" + tail},
		{"a link inside the run directory", []lifecycleArtifact{{name: lifecycleFile, content: lifecycleCSV("9"), status: fileStatusOK, insideLink: true}}, "Read: 9 operations" + tail},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			data, err := loadLifecycleRun(t, t.TempDir(), lifecycleOKRow, test.artifacts...)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if status := data.Steps[lifecycleStep].Status; status != StepStatusComplete {
				t.Fatalf("status = %s", status)
			}
			got := unresolvedWarnings(aggregateLifecycleRun(t, data))
			if !slices.Equal(got, []string{test.want}) {
				t.Fatalf("warnings = %q, want %q", got, test.want)
			}
		})
	}
}

// An unusable file must leave the run exactly as it is without one.
func TestUnresolvedOperationsUnusableLifecycleFileChangesNothing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		artifact lifecycleArtifact
	}{
		{"zero", okLifecycle(lifecycleCSV("0"))},
		{"zero on every worker", okLifecycle(lifecycleCSV("0", "0"))},
		{"no rows", okLifecycle(lifecycleHeader + "\n")},
		{"empty file", okLifecycle("")},
		{"missing column", okLifecycle("schema_version,step_id\n1,step\n")},
		{"duplicate column", okLifecycle("unresolved,unresolved\n3,4\n")},
		{"non-numeric value", okLifecycle(lifecycleCSV("many"))},
		{"negative value", okLifecycle(lifecycleCSV("-3"))},
		{"empty value", okLifecycle(lifecycleCSV(""))},
		{"good row before a bad one", okLifecycle(lifecycleCSV("7", "bad"))},
		{"value beyond int64", okLifecycle(lifecycleCSV("9223372036854775808"))},
		{"sum beyond int64", okLifecycle(lifecycleCSV("9223372036854775807", "1"))},
		{"short row", okLifecycle(lifecycleHeader + "\n1,42\n")},
		{"short row after a good one", okLifecycle(lifecycleCSV("7") + "1,42\n")},
		{"listed as failed", lifecycleArtifact{name: lifecycleFile, content: lifecycleCSV("9"), status: "error"}},
		{"a link out of the run directory", lifecycleArtifact{name: lifecycleFile, content: lifecycleCSV("9"), status: fileStatusOK, outsideLink: true}},
		{"unterminated quote", okLifecycle("unresolved\n\"5\n")},
		{"listed as missing", lifecycleArtifact{name: lifecycleFile, status: fileStatusMissing}},
		{"listed but gone", lifecycleArtifact{name: lifecycleFile, status: fileStatusOK, absent: true}},
		{"a directory", lifecycleArtifact{name: lifecycleFile, status: fileStatusOK, directory: true}},
		{"on disk but not listed", lifecycleArtifact{name: lifecycleFile, content: lifecycleCSV("9")}},
		{"only a node copy", lifecycleArtifact{name: lifecycleNodeFile, content: lifecycleCSV("9"), status: fileStatusOK}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			// A file listed as missing is reported by Artifact Health today, so the baseline lists it too.
			var baselineArtifacts []lifecycleArtifact
			if test.artifact.status == fileStatusMissing {
				baselineArtifacts = append(baselineArtifacts, test.artifact)
			}
			baselineData, err := loadLifecycleRun(t, dir, lifecycleOKRow, baselineArtifacts...)
			if err != nil {
				t.Fatalf("baseline Load: %v", err)
			}
			baseline := aggregateLifecycleRun(t, baselineData)

			data, err := loadLifecycleRun(t, dir, lifecycleOKRow, test.artifact)
			if err != nil {
				t.Fatalf("Load must not report a lifecycle problem: %v", err)
			}
			step, baseStep := data.Steps[lifecycleStep], baselineData.Steps[lifecycleStep]
			if step.UnresolvedOperations != 0 {
				t.Fatalf("UnresolvedOperations = %d", step.UnresolvedOperations)
			}
			if step.Status != baseStep.Status || !slices.Equal(step.Notes, baseStep.Notes) {
				t.Fatalf("step changed: status %s notes %q, want %s %q", step.Status, step.Notes, baseStep.Status, baseStep.Notes)
			}
			assertSameLifecycleOutputs(t, aggregateLifecycleRun(t, data), baseline)
		})
	}
}

// DELETE and partial READ print their own unresolved counts in their report sections.
func TestUnresolvedOperationsSkipsStepsWithTheirOwnCount(t *testing.T) {
	t.Parallel()
	evidence, err := parseRangeRead(strings.NewReader(rangeFixture(nil)), "step-read")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		set  func(*StepData)
	}{
		{"delete", func(step *StepData) { step.Delete = &deletemetrics.Metrics{} }},
		{"partial read", func(step *StepData) { step.RangeRead = evidence }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			render := func(unresolved int64) *RunSummary {
				data, loadErr := loadLifecycleRun(t, dir, lifecycleOKRow)
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				data.Steps[lifecycleStep].UnresolvedOperations = unresolved
				test.set(data.Steps[lifecycleStep])
				return aggregateLifecycleRun(t, data)
			}
			with := render(5)
			if got := unresolvedWarnings(with); len(got) != 0 {
				t.Fatalf("warnings = %q", got)
			}
			assertSameLifecycleOutputs(t, with, render(0))
		})
	}
}

// A step without metrics can still have unresolved operations, and its step ID stands in for a
// missing phase label.
func TestUnresolvedOperationsWarningWithoutMetricsOrLabel(t *testing.T) {
	t.Parallel()
	got := unresolvedOperationsWarning(nonEmpty("", "step-id"), &StepData{StepID: "step-id", UnresolvedOperations: 2})
	if !strings.HasPrefix(got, "step-id: 2 operations unresolved at the stop") {
		t.Fatalf("warning = %q", got)
	}
	data, err := loadLifecycleRun(t, t.TempDir(), lifecycleOKRow, okLifecycle(lifecycleCSV("2")))
	if err != nil {
		t.Fatal(err)
	}
	data.Steps[lifecycleStep].Metrics = nil
	data.Steps[lifecycleStep].MetricsSuppressed = true
	if lines := unresolvedWarnings(aggregateLifecycleRun(t, data)); len(lines) != 1 {
		t.Fatalf("warnings = %q", lines)
	}
}

// Steps of one phase share a label, so each warning names its step: equal counts would otherwise
// merge into one line, and a plain label would not say which step it means.
func TestUnresolvedOperationsOfStepsWithOneLabelNameTheirSteps(t *testing.T) {
	t.Parallel()
	const tail = ": 5 operations unresolved at the stop, in neither Success nor Failed; Data Moved can include their bytes (operation.lifecycle.csv)"
	tests := []struct {
		name   string
		counts []int64
		want   []int
	}{
		{"equal counts", []int64{5, 5}, []int{0, 1}},
		{"a clean step first", []int64{0, 5}, []int{1}},
		{"a clean step between two", []int64{5, 0, 5}, []int{0, 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			data, err := loadLifecycleRun(t, t.TempDir(), lifecycleOKRow, okLifecycle(lifecycleCSV("5")))
			if err != nil {
				t.Fatal(err)
			}
			base := *data.Steps[lifecycleStep]
			data.Steps = make(map[string]*StepData, len(test.counts))
			data.StepOrder = nil
			ids := make([]string, len(test.counts))
			for i, count := range test.counts {
				step := base
				step.StepID = fmt.Sprintf("spt-%03d-20261115.120000.000-read", i+3)
				step.UnresolvedOperations = count
				ids[i] = step.StepID
				data.Steps[step.StepID] = &step
				data.StepOrder = append(data.StepOrder, step.StepID)
			}
			var want []string
			for _, i := range test.want {
				want = append(want, "Read ("+ids[i]+")"+tail)
			}
			if got := unresolvedWarnings(aggregateLifecycleRun(t, data)); !slices.Equal(got, want) {
				t.Fatalf("warnings = %q, want %q", got, want)
			}
		})
	}
}

// A step whose ID gives no phase label is named by its ID.
func TestUnresolvedOperationsNameAStepWithoutALabel(t *testing.T) {
	t.Parallel()
	data, err := loadLifecycleRun(t, t.TempDir(), lifecycleOKRow, okLifecycle(lifecycleCSV("2")))
	if err != nil {
		t.Fatal(err)
	}
	const unlabelled = "-"
	if label := phaseLabelFromStep(unlabelled); label != "" {
		t.Fatalf("phase label of %q = %q", unlabelled, label)
	}
	step := data.Steps[lifecycleStep]
	step.StepID = unlabelled
	data.Steps = map[string]*StepData{unlabelled: step}
	data.StepOrder = []string{unlabelled}
	lines := unresolvedWarnings(aggregateLifecycleRun(t, data))
	if len(lines) != 1 || !strings.HasPrefix(lines[0], unlabelled+": 2 operations unresolved") {
		t.Fatalf("warnings = %q", lines)
	}
}

func TestUnresolvedOperationsAppearInEverySummary(t *testing.T) {
	t.Parallel()
	data, err := loadLifecycleRun(t, t.TempDir(), lifecycleOKRow, okLifecycle(lifecycleCSV("1845")))
	if err != nil {
		t.Fatal(err)
	}
	const want = "Read: 1845 operations unresolved at the stop, in neither Success nor Failed; Data Moved can include their bytes (operation.lifecycle.csv)"
	for name, output := range lifecycleOutputs(aggregateLifecycleRun(t, data)) {
		// CompactSnippet wraps its warnings.
		if !strings.Contains(strings.Join(strings.Fields(output), " "), want) {
			t.Errorf("%s lacks the warning:\n%s", name, output)
		}
	}
}

// The warning is the only difference: tables, totals and the exit-code input stay as they were.
func TestUnresolvedOperationsChangeOnlyTheWarnings(t *testing.T) {
	t.Parallel()
	for name, metricsRow := range map[string]string{"successful run": lifecycleOKRow, "run that exits 1": lifecycleFailRow} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			baselineData, err := loadLifecycleRun(t, dir, metricsRow)
			if err != nil {
				t.Fatal(err)
			}
			baseline := aggregateLifecycleRun(t, baselineData)
			data, err := loadLifecycleRun(t, dir, metricsRow, okLifecycle(lifecycleCSV("3")))
			if err != nil {
				t.Fatal(err)
			}
			summary := aggregateLifecycleRun(t, data)
			if lines := unresolvedWarnings(summary); len(lines) != 1 {
				t.Fatalf("warnings = %q", lines)
			}
			if !reflect.DeepEqual(summary.Steps, baseline.Steps) || !reflect.DeepEqual(summary.Totals, baseline.Totals) {
				t.Fatal("steps or totals changed")
			}
			if !reflect.DeepEqual(summary.AllFailedPhases(), baseline.AllFailedPhases()) {
				t.Fatalf("all-failed phases = %v, want %v", summary.AllFailedPhases(), baseline.AllFailedPhases())
			}
			if metricsRow == lifecycleFailRow && len(summary.AllFailedPhases()) != 1 {
				t.Fatalf("fixture must fail its phase: %v", summary.AllFailedPhases())
			}
			summary.Warnings = slices.DeleteFunc(summary.Warnings, func(w string) bool { return strings.Contains(w, "unresolved") })
			assertSameLifecycleOutputs(t, summary, baseline)
		})
	}
}
