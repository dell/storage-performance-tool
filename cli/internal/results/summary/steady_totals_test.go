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
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

const steadyTestRunID = 1787750472685

var steadyTestTotalsHeader = []string{
	"DateTimeISO8601", "OpType", "Concurrency", "NodeCount", "ConcurrencyCurr", "ConcurrencyMean",
	"CountSucc", "CountFail", "CountCorrupt", "Size", "StepDuration[s]", "DurationSum[s]",
	"TPAvg[op/s]", "TPLast[op/s]", "BWAvg[MiB/s]", "BWLast[MiB/s]",
	"DurationAvg[us]", "DurationMin[us]", "DurationQ_0.5[us]", "DurationQ_0.99[us]", "DurationMax[us]",
	"LatencyAvg[us]", "LatencyMin[us]", "LatencyQ_0.5[us]", "LatencyQ_0.99[us]", "LatencyMax[us]",
	"TtfbAvg[us]", "TtfbMin[us]", "TtfbQ_0.5[us]", "TtfbQ_0.99[us]", "TtfbMax[us]",
}

var steadyTestHeader = append(append([]string(nil), steadyTestTotalsHeader...), steadyColumns...)

// steadyTestRow is one metrics context of a fixture step, with its whole-step and steady rates.
type steadyTestRow struct {
	op       string
	tp, bw   float64
	steadyTP float64
	steadyBW float64
	status   string
}

type steadyTestStep struct {
	id   string
	rows []steadyTestRow
	// steady overrides the generated steady file; steadyListed controls its manifest entry.
	steady       *string
	steadyListed string
}

func (row steadyTestRow) totalsValues() map[string]string {
	values := map[string]string{
		"DateTimeISO8601": `"2026-10-24T12:01:00Z"`, "OpType": row.op, "Concurrency": "8", "NodeCount": "2",
		"ConcurrencyCurr": "0", "ConcurrencyMean": "7.9", "CountSucc": fmt.Sprint(int64(row.tp * 60)),
		"CountFail": "0", "CountCorrupt": "0", "Size": fmt.Sprint(int64(row.bw * 60 * float64(constants.BytesPerMiB))),
		"StepDuration[s]": "60.2", "DurationSum[s]": "480.0", "TPAvg[op/s]": fmt.Sprint(row.tp),
		"TPLast[op/s]": fmt.Sprint(row.tp), "BWAvg[MiB/s]": fmt.Sprint(row.bw), "BWLast[MiB/s]": fmt.Sprint(row.bw),
		"DurationAvg[us]": "24000", "DurationMin[us]": "9000", "DurationQ_0.5[us]": "23000",
		"DurationQ_0.99[us]": "61000", "DurationMax[us]": "90000", "LatencyAvg[us]": "21000",
		"LatencyMin[us]": "8000", "LatencyQ_0.5[us]": "20500", "LatencyQ_0.99[us]": "58000",
		"LatencyMax[us]": "88000",
	}
	if row.op == "READ" {
		for column, value := range map[string]string{
			"TtfbAvg[us]": "4600", "TtfbMin[us]": "1200", "TtfbQ_0.5[us]": "4200", "TtfbQ_0.99[us]": "15000", "TtfbMax[us]": "31000",
		} {
			values[column] = value
		}
	}
	return values
}

func (row steadyTestRow) steadyValues() map[string]string {
	if row.status == steadyStatusNotReached || row.status == steadyStatusIncomplete {
		return map[string]string{
			"DateTimeISO8601": `"2026-10-24T12:01:00Z"`, "OpType": row.op, "Concurrency": "8", "NodeCount": "2",
			columnSteadyStatus: row.status,
		}
	}
	values := steadyTestRow{op: row.op, tp: row.steadyTP, bw: row.steadyBW}.totalsValues()
	values["StepDuration[s]"] = "57.4"
	values[columnSteadyStart] = "1.3"
	values[columnSteadyTurnovers] = "412.6"
	values[columnSteadyTrend] = "-0.8"
	values[columnSteadyCV] = "0.02"
	values[columnSteadyStatus] = row.status
	return values
}

func steadyTestCSV(header []string, rows []map[string]string) string {
	var b strings.Builder
	b.WriteString(strings.Join(header, ","))
	for _, values := range rows {
		cells := make([]string, len(header))
		for i, column := range header {
			cells[i] = values[column]
		}
		b.WriteString("\n" + strings.Join(cells, ","))
	}
	return b.String() + "\n"
}

func releaseEngineIdentity(version string, development bool) *engineinfo.Manifest {
	dirty := false
	return &engineinfo.Manifest{
		SchemaVersion: constants.EngineInfoManifestSchemaVersion,
		RunID:         steadyTestRunID,
		GeneratedAt:   "2026-10-24T12:00:00Z",
		Consistency: engineinfo.ManifestConsistency{
			Status: engineinfo.ConsistencyConsistent,
			Reason: "all 2 participants reported consistent engine build identity fields",
		},
		Builds: []engineinfo.ManifestBuild{{
			BuildID: "build-1", Product: constants.EngineBuildInfoProduct, Version: version,
			Revision: strings.Repeat("a", 40), BuildTime: "2026-10-20T12:00:00Z",
			Development: development, SourceDirty: &dirty,
		}},
		Participants: []engineinfo.ManifestParticipant{
			{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1"},
			{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1"},
		},
	}
}

// writeSteadyTestRun stores a fetched result bundle: a metrics total file per step, the steady
// file where listed as fetched, and the engine identity when one is given.
func writeSteadyTestRun(t *testing.T, workload string, identity *engineinfo.Manifest, steps []steadyTestStep) string {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "spt-20261024.120000.000")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := &results.Manifest{BaseURL: "http://entry.example:9999", OutputDir: runDir}
	params := &RunParams{
		GeneratedAt:  time.Date(2026, 10, 24, 12, 2, 0, 0, time.UTC),
		WorkloadType: workload,
		SptImage:     "ghcr.io/dell/storage-performance-tool:5.17.0",
		BaseURL:      "http://entry.example:9999",
		ResultsRoot:  "results/spt-20261024.120000.000",
		ScenarioParams: ScenarioParams{
			RunID: steadyTestRunID, WorkloadType: workload, ObjectSize: "1MB", Threads: 8, Duration: "60s",
			Endpoints: []string{"http://s3.example:9020"}, Bucket: "spt-bench",
		},
		Hosts: []RunHost{{Original: "entry.example"}, {Original: "worker.example"}},
		CLI:   RunCLI{Version: "5.17.0", Revision: strings.Repeat("c", 40)},
	}
	if identity != nil {
		if err := engineinfo.WriteManifestAtomic(runDir, *identity); err != nil {
			t.Fatal(err)
		}
		manifest.RunFiles = []results.FileStatus{{Name: constants.EngineInfoManifestName, Status: fileStatusOK}}
		params.EngineInfoFile = constants.EngineInfoManifestName
		params.EngineConsistency = identity.Consistency.Status
	}
	for _, step := range steps {
		totalsName := step.id + "." + constants.ResultsArtifactSuffixMetricsTotal
		steadyName := step.id + "." + constants.ResultsArtifactSuffixMetricsSteady
		totals := make([]map[string]string, 0, len(step.rows))
		steady := make([]map[string]string, 0, len(step.rows))
		for _, row := range step.rows {
			totals = append(totals, row.totalsValues())
			steady = append(steady, row.steadyValues())
		}
		writeSteadyTestFile(t, runDir, totalsName, steadyTestCSV(steadyTestTotalsHeader, totals))
		files := []results.FileStatus{{Name: totalsName, Status: fileStatusOK}}
		switch step.steadyListed {
		case fileStatusOK:
			content := steadyTestCSV(steadyTestHeader, steady)
			if step.steady != nil {
				content = *step.steady
			}
			writeSteadyTestFile(t, runDir, steadyName, content)
			files = append(files, results.FileStatus{Name: steadyName, Status: fileStatusOK})
		case fileStatusMissing:
			files = append(files, results.FileStatus{Name: steadyName, Status: fileStatusMissing})
		}
		manifest.Steps = append(manifest.Steps, results.StepManifest{StepID: step.id, Files: files})
		params.ExpectedStepIDs = append(params.ExpectedStepIDs, step.id)
	}
	writeManifest(t, runDir, manifest)
	writeParams(t, runDir, params)
	return runDir
}

func writeSteadyTestFile(t *testing.T, runDir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(runDir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func loadSteadyTestSummary(t *testing.T, runDir string) *RunSummary {
	t.Helper()
	data, err := NewLoader().Load(context.Background(), runDir)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	return mustAggregate(t, data)
}

// rateCells returns the rate-table cells of one phase: Avg rate, steady rate, Avg and steady bandwidth.
func rateCells(t *testing.T, summary *RunSummary, phase string) []string {
	t.Helper()
	line := tableRowLine(t, NewRenderer(RenderOptions{}).rateTable(summary), phase)
	cells := strings.Split(strings.Trim(line, "│"), "│")
	for i := range cells {
		cells[i] = strings.TrimSpace(cells[i])
	}
	if len(cells) != 5 {
		t.Fatalf("rate row has %d cells, want 5: %q", len(cells), line)
	}
	return cells[1:]
}

func TestSummarySteadyRatesShowValidWindowsAndExplainTheRest(t *testing.T) {
	const readStep = "spt-002-20261024.120000.000-read"
	malformedHeader := steadyTestCSV(steadyTestTotalsHeader, []map[string]string{
		steadyTestRow{op: "READ", tp: 340, bw: 340}.totalsValues(),
	})
	nonNumeric := strings.Replace(steadyTestCSV(steadyTestHeader, []map[string]string{
		steadyTestRow{op: "READ", steadyTP: 350, steadyBW: 350, status: steadyStatusValid}.steadyValues(),
	}), ",350,", ",fast,", 1)
	withoutRates := func(status string) string {
		values := steadyTestRow{op: "READ", steadyTP: 350, steadyBW: 350, status: status}.steadyValues()
		values[columnTPAvg] = ""
		values[columnBWAvg] = ""
		return steadyTestCSV(steadyTestHeader, []map[string]string{values})
	}
	validWithoutRates := withoutRates(steadyStatusValid)
	shortWithoutRates := withoutRates(steadyStatusShort)
	zeroRates := steadyTestCSV(steadyTestHeader, []map[string]string{
		steadyTestRow{op: "READ", status: steadyStatusValid}.steadyValues(),
	})
	empty := ""
	extraRow := steadyTestCSV(steadyTestHeader, []map[string]string{
		steadyTestRow{op: "READ", steadyTP: 350, steadyBW: 350, status: steadyStatusValid}.steadyValues(),
		steadyTestRow{op: "READ", steadyTP: 350, steadyBW: 350, status: steadyStatusValid}.steadyValues(),
	})
	release := releaseEngineIdentity("5.17.0", false)
	legacy := legacyEngineIdentityManifest(steadyTestRunID)

	tests := []struct {
		name     string
		identity *engineinfo.Manifest
		status   string
		listed   string
		steady   *string
		wantRate string
		wantBW   string
	}{
		{name: "valid window", identity: release, status: steadyStatusValid, listed: fileStatusOK, wantRate: "350 ops/s", wantBW: "350 MiB/s"},
		{name: "short window", identity: release, status: steadyStatusShort, listed: fileStatusOK, wantRate: "n/a (short)", wantBW: "n/a (short)"},
		{name: "window not reached", identity: release, status: steadyStatusNotReached, listed: fileStatusOK, wantRate: "n/a (not reached)", wantBW: "n/a (not reached)"},
		{name: "incomplete contributors", identity: release, status: steadyStatusIncomplete, listed: fileStatusOK, wantRate: "n/a (incomplete)", wantBW: "n/a (incomplete)"},
		{name: "unknown status", identity: release, status: "stable", listed: fileStatusOK, wantRate: "n/a (unreadable)", wantBW: "n/a (unreadable)"},
		{name: "malformed header", identity: release, listed: fileStatusOK, steady: &malformedHeader, wantRate: "n/a (unreadable)", wantBW: "n/a (unreadable)"},
		{name: "malformed value", identity: release, listed: fileStatusOK, steady: &nonNumeric, wantRate: "n/a (unreadable)", wantBW: "n/a (unreadable)"},
		{name: "valid window without rates", identity: release, listed: fileStatusOK, steady: &validWithoutRates, wantRate: "n/a (unreadable)", wantBW: "n/a (unreadable)"},
		{name: "short window without rates", identity: release, listed: fileStatusOK, steady: &shortWithoutRates, wantRate: "n/a (unreadable)", wantBW: "n/a (unreadable)"},
		{name: "valid window measuring zero", identity: release, listed: fileStatusOK, steady: &zeroRates, wantRate: "0 ops/s", wantBW: "0 MiB/s"},
		{name: "empty file", identity: release, listed: fileStatusOK, steady: &empty, wantRate: "n/a (unreadable)", wantBW: "n/a (unreadable)"},
		{name: "rows disagree with totals", identity: release, listed: fileStatusOK, steady: &extraRow, wantRate: "n/a (unreadable)", wantBW: "n/a (unreadable)"},
		{name: "new engine with file missing", identity: release, listed: fileStatusMissing, wantRate: "n/a (missing)", wantBW: "n/a (missing)"},
		{name: "new engine with file not indexed", identity: release, wantRate: "n/a (missing)", wantBW: "n/a (missing)"},
		{name: "old release with no steady file", identity: releaseEngineIdentity("5.16.2", false), wantRate: "n/a (engine < 5.17)", wantBW: "n/a (engine < 5.17)"},
		{name: "old release listed missing", identity: releaseEngineIdentity("5.16.2", false), listed: fileStatusMissing, wantRate: "n/a (engine < 5.17)", wantBW: "n/a (engine < 5.17)"},
		{name: "engine without version endpoint", identity: &legacy, wantRate: "n/a (engine < 5.17)", wantBW: "n/a (engine < 5.17)"},
		{name: "development build is not a release", identity: releaseEngineIdentity("5.16.2", true), wantRate: "n/a (missing)", wantBW: "n/a (missing)"},
		{name: "no engine identity", wantRate: "n/a (missing)", wantBW: "n/a (missing)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			step := steadyTestStep{
				id:           readStep,
				rows:         []steadyTestRow{{op: "READ", tp: 340, bw: 340, steadyTP: 350, steadyBW: 350, status: tc.status}},
				steady:       tc.steady,
				steadyListed: tc.listed,
			}
			summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "read", tc.identity, []steadyTestStep{step}))
			got := rateCells(t, summary, "Read")
			want := []string{"340 ops/s", tc.wantRate, "340 MiB/s", tc.wantBW}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("rate cells = %q, want %q", got, want)
			}
			if steps := summary.Steps; len(steps) != 1 || steps[0].Status != StepStatusComplete {
				t.Fatalf("steady artifact changed step status: %+v", steps)
			}
		})
	}
}

func TestSummarySteadyRatesSumMultiRowPhasesOnlyWhenEveryWindowIsValid(t *testing.T) {
	const mixedStep = "spt-001-20261024.120000.000-mixed"
	read := steadyTestRow{op: "READ", tp: 100, bw: 100, steadyTP: 110, steadyBW: 110, status: steadyStatusValid}
	create := steadyTestRow{op: "CREATE", tp: 50, bw: 50, steadyTP: 55, steadyBW: 55, status: steadyStatusValid}
	stat := steadyTestRow{op: "STAT", tp: 20, steadyTP: 22, status: steadyStatusValid}
	with := func(row steadyTestRow, status string) steadyTestRow {
		row.status = status
		return row
	}
	reversed := steadyTestCSV(steadyTestHeader, []map[string]string{
		stat.steadyValues(), create.steadyValues(), read.steadyValues(),
	})
	missingRow := steadyTestCSV(steadyTestHeader, []map[string]string{read.steadyValues(), create.steadyValues()})

	tests := []struct {
		name     string
		rows     []steadyTestRow
		steady   *string
		wantRate string
		wantBW   string
	}{
		{name: "every window valid", rows: []steadyTestRow{read, create, stat}, wantRate: "187 ops/s", wantBW: "165 MiB/s"},
		{name: "rows matched by operation, not position", rows: []steadyTestRow{read, create, stat}, steady: &reversed, wantRate: "187 ops/s", wantBW: "165 MiB/s"},
		{name: "one short window", rows: []steadyTestRow{read, with(create, steadyStatusShort), stat}, wantRate: "n/a (short)", wantBW: "n/a (short)"},
		{name: "worst status wins", rows: []steadyTestRow{with(read, steadyStatusShort), with(create, steadyStatusIncomplete), with(stat, steadyStatusNotReached)}, wantRate: "n/a (incomplete)", wantBW: "n/a (incomplete)"},
		{name: "not reached outranks short", rows: []steadyTestRow{with(read, steadyStatusShort), create, with(stat, steadyStatusNotReached)}, wantRate: "n/a (not reached)", wantBW: "n/a (not reached)"},
		{name: "steady row missing for one context", rows: []steadyTestRow{read, create, stat}, steady: &missingRow, wantRate: "n/a (unreadable)", wantBW: "n/a (unreadable)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			step := steadyTestStep{id: mixedStep, rows: tc.rows, steady: tc.steady, steadyListed: fileStatusOK}
			summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "mixed", releaseEngineIdentity("5.17.0", false), []steadyTestStep{step}))
			got := rateCells(t, summary, "Mixed")
			want := []string{"170 ops/s", tc.wantRate, "150 MiB/s", tc.wantBW}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("rate cells = %q, want %q", got, want)
			}
		})
	}
}

func TestSummarySteadyRatesFollowTheRowOfASingleOperationPhase(t *testing.T) {
	// The Avg columns of a non-mixed phase report one selected row; the steady columns
	// report that row's window and ignore the others.
	const readStep = "spt-002-20261024.120000.000-read"
	rows := []steadyTestRow{
		{op: "READ", tp: 340, bw: 340, steadyTP: 350, steadyBW: 350, status: steadyStatusValid},
		{op: "NOOP", tp: 5, steadyTP: 0, status: steadyStatusNotReached},
	}
	step := steadyTestStep{id: readStep, rows: rows, steadyListed: fileStatusOK}
	summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), []steadyTestStep{step}))
	got := rateCells(t, summary, "Read")
	want := []string{"340 ops/s", "350 ops/s", "340 MiB/s", "350 MiB/s"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("rate cells = %q, want %q", got, want)
	}
}

func TestSummarySteadyRatesFollowTheAvgRowWhenContextsShareAnOperation(t *testing.T) {
	// A phase whose contexts share one operation is not mixed: its Avg columns report one selected
	// row, so its steady columns report that row's window, whatever the other rows' windows are.
	const readStep = "spt-002-20261024.120000.000-read"
	first := steadyTestRow{op: "READ", tp: 100, bw: 100, steadyTP: 110, steadyBW: 110, status: steadyStatusValid}
	for _, status := range []string{steadyStatusValid, steadyStatusShort} {
		t.Run(status, func(t *testing.T) {
			second := steadyTestRow{op: "READ", tp: 50, bw: 50, steadyTP: 55, steadyBW: 55, status: status}
			step := steadyTestStep{id: readStep, rows: []steadyTestRow{first, second}, steadyListed: fileStatusOK}
			summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), []steadyTestStep{step}))
			got := rateCells(t, summary, "Read")
			want := []string{"100 ops/s", "110 ops/s", "100 MiB/s", "110 MiB/s"}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("rate cells = %q, want %q", got, want)
			}
		})
	}
}

func TestSummarySteadyCellsAreNotApplicableWhereAvgIs(t *testing.T) {
	summary := &RunSummary{Steps: []StepSummary{
		{PhaseLabel: "List", Operation: "LIST", Metrics: &PhaseMetrics{
			ThroughputAvgOps: 256, Steady: SteadyRates{Valid: true, ThroughputOps: 300, BandwidthMiBps: 1},
		}},
		{PhaseLabel: "Delete", Operation: "DELETE", Metrics: &PhaseMetrics{
			ThroughputAvgOps: 40, Steady: SteadyRates{Valid: true, ThroughputOps: 42, BandwidthMiBps: 1},
		}},
		{PhaseLabel: "Read", Operation: "READ", Metrics: &PhaseMetrics{ThroughputAvgOps: 40, BandwidthAvgMiBps: 40}},
	}}
	if got := rateCells(t, summary, "List"); strings.Join(got, "|") != "256 objects/s|—|—|—" {
		t.Fatalf("LIST rate cells = %q", got)
	}
	if got := rateCells(t, summary, "Delete"); strings.Join(got, "|") != "40.0 ops/s|42.0 ops/s|—|—" {
		t.Fatalf("DELETE rate cells = %q", got)
	}
	// Rates never derived from a steady file are not shown as a valid zero.
	if got := rateCells(t, summary, "Read"); strings.Join(got, "|") != "40.0 ops/s|n/a (missing)|40.0 MiB/s|n/a (missing)" {
		t.Fatalf("unset steady rate cells = %q", got)
	}
	table := NewRenderer(RenderOptions{}).rateTable(summary)
	mustContain(t, table, " Rate Avg │ ")
	mustContain(t, table, " Rate Steady │ ")
}

func TestSummaryShowsNoSteadyLatencyOrStabilityColumns(t *testing.T) {
	step := steadyTestStep{
		id:           "spt-002-20261024.120000.000-read",
		rows:         []steadyTestRow{{op: "READ", tp: 340, bw: 340, steadyTP: 350, steadyBW: 350, status: steadyStatusValid}},
		steadyListed: fileStatusOK,
	}
	summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), []steadyTestStep{step}))
	report := NewRenderer(RenderOptions{}).FullReport(summary)
	for _, unwanted := range []string{"Latency Steady", "TTFB Steady", "Trend", "CV", "Turnover", "SteadyStart"} {
		mustNotContain(t, report, unwanted)
	}
	// Whole-step latency stays in the phase table.
	mustContain(t, report, "20.5 ms")
}

func TestLoaderIndexesSteadyTotalsAsOptionalEntryNodeArtifact(t *testing.T) {
	step := steadyTestStep{
		id:           "spt-002-20261024.120000.000-read",
		rows:         []steadyTestRow{{op: "READ", tp: 340, bw: 340, steadyTP: 350, steadyBW: 350, status: steadyStatusValid}},
		steadyListed: fileStatusOK,
	}
	data, err := NewLoader().Load(context.Background(),
		writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), []steadyTestStep{step}))
	if err != nil {
		t.Fatal(err)
	}
	loaded := data.Steps[step.id]
	if loaded.Steady == nil || len(loaded.Steady.Rows) != 1 || loaded.Steady.Rows[0].Status != steadyStatusValid ||
		loaded.Steady.Rows[0].ThroughputAvgOps != 350 || loaded.Steady.Rows[0].StepDurationSeconds != 57.4 {
		t.Fatalf("steady totals not indexed: %+v", loaded.Steady)
	}

	step.steadyListed = fileStatusMissing
	data, err = NewLoader().Load(context.Background(),
		writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), []steadyTestStep{step}))
	if err != nil {
		t.Fatal(err)
	}
	loaded = data.Steps[step.id]
	if loaded.Status != StepStatusComplete || len(loaded.MissingRequired) != 0 ||
		strings.Join(loaded.MissingOptional, ",") != constants.ResultsArtifactSuffixMetricsSteady {
		t.Fatalf("absent steady totals must be optional: status=%s required=%v optional=%v",
			loaded.Status, loaded.MissingRequired, loaded.MissingOptional)
	}
}

func TestVersionBeforeComparesMajorMinor(t *testing.T) {
	tests := map[string]bool{
		"5.16.2": true, "5.9.0": true, "4.99.0": true, "5.17.0": false, "5.17.0-rc.1": false,
		"5.17.3+build.7": false, "6.0.0": false, "unknown": false, "5": false, "five.16.0": false,
	}
	for version, want := range tests {
		if got := versionBefore(version, 5, 17); got != want {
			t.Errorf("versionBefore(%q, 5, 17) = %t, want %t", version, got, want)
		}
	}
}

func TestFullSummaryWithSteadyRatesMatchesGoldenAtDefaultWidth(t *testing.T) {
	steps := []steadyTestStep{
		{
			id:           "spt-001-20261024.120000.000-seed",
			rows:         []steadyTestRow{{op: "CREATE", tp: 334.7, bw: 334.7, steadyTP: 351.2, steadyBW: 351.2, status: steadyStatusValid}},
			steadyListed: fileStatusOK,
		},
		{
			id:           "spt-002-20261024.120000.000-read",
			rows:         []steadyTestRow{{op: "READ", tp: 1180.4, bw: 1180.4, steadyTP: 1236.9, steadyBW: 1236.9, status: steadyStatusValid}},
			steadyListed: fileStatusOK,
		},
		{
			id:           "spt-003-20261024.120000.000-delete",
			rows:         []steadyTestRow{{op: "DELETE", tp: 1227, steadyTP: 1302, status: steadyStatusShort}},
			steadyListed: fileStatusOK,
		},
	}
	summary := loadSteadyTestSummary(t, writeSteadyTestRun(t, "read", releaseEngineIdentity("5.17.0", false), steps))
	report := NewRenderer(RenderOptions{}).FullReport(summary)
	for _, line := range strings.Split(report, "\n") {
		if width := len([]rune(line)); width > defaultMaxWidth {
			t.Errorf("line width = %d, want <= %d: %q", width, defaultMaxWidth, line)
		}
	}
	want, err := os.ReadFile(filepath.Join("testdata", "summary_steady_default_width.golden"))
	if err != nil {
		t.Fatal(err)
	}
	if report != string(want) {
		t.Fatalf("summary mismatch\n--- got ---\n%s\n--- want ---\n%s", report, want)
	}
	if snippet := NewRenderer(RenderOptions{}).ConsoleSnippet(summary); snippet != report {
		t.Fatalf("console snippet truncated the summary:\n%s", snippet)
	}
}
