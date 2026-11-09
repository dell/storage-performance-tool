package summary

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

func loopContext(index int, route, source string, loops int) results.EventLoopContext {
	threads := 0
	if source == results.EventLoopSourceDriverThreads {
		threads = loops
	}
	return results.EventLoopContext{ContextIndex: index, Route: route, DriverThreads: threads, Source: source, EventLoops: loops}
}

func loopEngine(node, hardwareThreads int, contexts ...results.EventLoopContext) results.EventLoopEngine {
	total := 0
	for _, context := range contexts {
		total += context.EventLoops
	}
	return results.EventLoopEngine{Node: node, HardwareThreads: hardwareThreads, EventLoops: total, Contexts: contexts}
}

func TestEventLoopsDescribesTheRunsEngines(t *testing.T) {
	const def, nicSet = results.EventLoopRouteDefault, results.EventLoopRouteNicSet
	const byDefault, byThreads = results.EventLoopSourceDefault, results.EventLoopSourceDriverThreads
	stock := loopEngine(0, 192, loopContext(0, def, byDefault, 24))
	stockOnNode1 := loopEngine(1, 192, loopContext(0, def, byDefault, 24))
	small := loopEngine(1, 6, loopContext(0, def, byDefault, 4))
	explicit := loopEngine(0, 192, loopContext(0, def, byThreads, 12))
	drivers := loopEngine(0, 192, loopContext(0, def, byDefault, 24), loopContext(1, def, byDefault, 24))
	differ := loopEngine(0, 192, loopContext(0, def, byDefault, 24), loopContext(1, def, byThreads, 12))
	routes := loopEngine(0, 192, loopContext(0, def, byDefault, 24), loopContext(1, nicSet, byDefault, 24))
	nic := loopEngine(0, 192, loopContext(0, nicSet, byDefault, 98))
	none := loopEngine(0, 192)

	tests := map[string]struct {
		engines    map[string][]results.EventLoopEngine
		incomplete bool
		want       string
	}{
		"no reports": {want: ""},
		"every report agrees": {engines: map[string][]results.EventLoopEngine{"s1": {stock, stockOnNode1}, "s2": {stock, stockOnNode1}},
			want: "24 per engine (default, 192 hardware threads)"},
		"an explicit value": {engines: map[string][]results.EventLoopEngine{"s1": {explicit}}, want: "12 per engine (storage.driver.threads)"},
		"several drivers": {engines: map[string][]results.EventLoopEngine{"s1": {drivers}},
			want: "48 per engine (2 drivers × 24, default, 192 hardware threads)"},
		"drivers that differ": {engines: map[string][]results.EventLoopEngine{"s1": {differ}},
			want: "36 per engine (2 drivers; see eventLoopEngines in spt_run_params.json)"},
		"drivers on different routes": {engines: map[string][]results.EventLoopEngine{"s1": {routes}},
			want: "48 per engine (2 drivers; see eventLoopEngines in spt_run_params.json)"},
		"a NIC set": {engines: map[string][]results.EventLoopEngine{"s1": {nic}}, want: "98 per engine (NIC set, default, 192 hardware threads)"},
		"engines differ": {engines: map[string][]results.EventLoopEngine{"s1": {small, stock}},
			want: "node-000 24 (default, 192 hardware threads); node-001 4 (default, 6 hardware threads)"},
		"a missing report never generalizes": {engines: map[string][]results.EventLoopEngine{"s1": {stock}}, incomplete: true,
			want: "node-000 24 (default, 192 hardware threads); " + nicEventLoopsOtherReports},
		"steps differ":                {engines: map[string][]results.EventLoopEngine{"s1": {stock}, "s2": {explicit}}, want: eventLoopsVaryByStep},
		"drivers without event loops": {engines: map[string][]results.EventLoopEngine{"s1": {none}}, want: ""},
		"a step without event loops adds nothing": {engines: map[string][]results.EventLoopEngine{"s1": {none}, "s2": {stock}},
			want: "24 per engine (default, 192 hardware threads)"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := eventLoops(tc.engines, []string{"s1", "s2"}, tc.incomplete); got != tc.want {
				t.Fatalf("event loops = %q, want %q", got, tc.want)
			}
		})
	}
}

// storeEventLoopMetadata replaces a run's metadata with endpoints, its configured hosts and its
// eventLoopEngines, plus extra fields.
func storeEventLoopMetadata(t *testing.T, runDir string, hosts []string, engines map[string][]results.EventLoopEngine,
	extra map[string]any,
) {
	t.Helper()
	records := make([]map[string]any, 0, len(hosts))
	for _, host := range hosts {
		records = append(records, map[string]any{"host": host})
	}
	metadata := map[string]any{
		"workloadType":    "write",
		"expectedStepIds": []string{nicPathsStepID},
		"scenarioParams":  map[string]any{"WorkloadType": "write", "Endpoints": []string{"http://192.0.2.10:9000"}},
		"hosts":           records,
	}
	if engines != nil {
		metadata["eventLoopEngines"] = engines
	}
	for key, value := range extra {
		metadata[key] = value
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsMetadataFileName), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
}

var eventLoopHosts = []string{"worker-a.example", "worker-b.example", "worker-c.example"}

func TestStoredDefaultRouteRunShowsItsEventLoopsAfterTheEndpoints(t *testing.T) {
	runDir := nicPathsRun(t, 1, nil, nicPathsFetched("event.loops.json"))
	storeEventLoopMetadata(t, runDir, eventLoopHosts[:1], map[string][]results.EventLoopEngine{
		nicPathsStepID: {loopEngine(0, 192, loopContext(0, results.EventLoopRouteDefault, results.EventLoopSourceDefault, 24))},
	}, nil)
	full := nicPathsReports(t, aggregateStoredRun(t, runDir))["full"]
	mustContain(t, full, "  • Endpoints          http://192.0.2.10:9000\n"+
		"  • Event loops        24 per engine (default, 192 hardware threads)\n")
	mustNotContain(t, full, "Interfaces")
	mustNotContain(t, full, "Artifact Health")
}

func TestStoredRunsFromEnginesWithoutTheReportReadAsBefore(t *testing.T) {
	tests := map[string]string{
		"one engine":  nicPathsRun(t, 1, nil, results.FileStatus{Name: nicPathsStepID + ".event.loops.json", Status: "missing"}),
		"distributed": nicPathsRun(t, 3, nil, results.FileStatus{Name: nicPathsStepID + ".event.loops.json", Status: "missing"}),
	}
	for name, runDir := range tests {
		t.Run(name, func(t *testing.T) {
			storeEventLoopMetadata(t, runDir, eventLoopHosts, nil, nil)
			summary := aggregateStoredRun(t, runDir)
			if missing := summary.Steps[0].EventLoopsMissing; len(missing) != 0 {
				t.Fatalf("missing event-loop reports = %+v, want none", missing)
			}
			full := nicPathsReports(t, summary)["full"]
			mustNotContain(t, full, "Event loops")
			mustNotContain(t, full, "Artifact Health")
			mustNotContain(t, full, "Event-loop reports")
		})
	}
}

func TestStoredDistributedRunNamesAMissingEventLoopReportAndShowsEachEngine(t *testing.T) {
	runDir := nicPathsRun(t, 3, nil, results.FileStatus{Name: nicPathsStepID + ".event.loops.json", Status: "missing"},
		nicPathsFetched("event.loops.node-000.json"), nicPathsFetched("event.loops.node-002.json"))
	stock := loopContext(0, results.EventLoopRouteDefault, results.EventLoopSourceDefault, 24)
	storeEventLoopMetadata(t, runDir, eventLoopHosts, map[string][]results.EventLoopEngine{
		nicPathsStepID: {loopEngine(0, 192, stock), loopEngine(2, 192, stock)},
	}, nil)
	summary := aggregateStoredRun(t, runDir)
	for _, missing := range summary.Steps[0].MissingOptional {
		if missing == constants.ResultsArtifactSuffixEventLoops {
			t.Fatal("a distributed step's canonical report is not missing: it keeps node copies")
		}
	}
	full := nicPathsReports(t, summary)["full"]
	mustContain(t, full, "  • Event loops        node-000 24 (default, 192 hardware threads); "+
		"node-002 24 (default, 192 hardware threads); "+nicEventLoopsOtherReports+"\n")
	mustContain(t, full, "Artifact Health\n  • "+nicPathsStepID+" missing event-loop reports: node-001, worker-b.example\n")
	mustNotContain(t, full, "per engine")
}

func TestStoredRunDoesNotGeneralizeFromAnUnreadableEventLoopReport(t *testing.T) {
	runDir := nicPathsRun(t, 1, nil, nicPathsFetched("event.loops.json"))
	storeEventLoopMetadata(t, runDir, eventLoopHosts[:1], map[string][]results.EventLoopEngine{
		nicPathsStepID: {loopEngine(0, 192, loopContext(0, results.EventLoopRouteDefault, results.EventLoopSourceDriverThreads, 12))},
	}, map[string]any{"eventLoopEnginesError": "first.event.loops.json: bad\nsecond.event.loops.json: worse"})
	full := nicPathsReports(t, aggregateStoredRun(t, runDir))["full"]
	mustContain(t, full, "  • Event loops        node-000 12 (storage.driver.threads); "+nicEventLoopsOtherReports+"\n")
	mustContain(t, full, "Event-loop reports not read: first.event.loops.json: bad; second.event.loops.json: worse\n")
	mustNotContain(t, full, "per engine")
}

func TestStoredNicSetRunKeepsItsNicSetBullet(t *testing.T) {
	runDir := nicSetRun(t, []string{"auto"}, map[string][]results.NicEngine{nicPathsStepID: {nicEngine(0, 16, 8, 0, "ens1f0", "ens1f1")}})
	path := filepath.Join(runDir, constants.ResultsMetadataFileName)
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	metadata["eventLoopEngines"] = map[string][]results.EventLoopEngine{
		nicPathsStepID: {loopEngine(0, 64, loopContext(0, results.EventLoopRouteNicSet, results.EventLoopSourceDefault, 18))},
	}
	if encoded, err = json.Marshal(metadata); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	full := nicPathsReports(t, aggregateStoredRun(t, runDir))["full"]
	mustContain(t, full, "  • Event loops        16 per engine (2 NICs × 8, default)\n")
	mustNotContain(t, full, "NIC set, default")
}

func TestEventLoopReportsAreIncompleteWhenOneIsMissingOrUnread(t *testing.T) {
	read := &StepData{Metrics: &MetricsTotals{}}
	if eventLoopReportsIncomplete(&RunData{Params: &RunParams{}, Steps: map[string]*StepData{"read": read, "nil": nil}}) {
		t.Fatal("every report read, nothing is incomplete")
	}
	missing := &StepData{EventLoopsMissing: []results.NicNodeFile{{Node: 1}}}
	if !eventLoopReportsIncomplete(&RunData{Params: &RunParams{}, Steps: map[string]*StepData{"read": missing}}) {
		t.Fatal("a missing node copy makes them incomplete")
	}
	if !eventLoopReportsIncomplete(&RunData{Params: &RunParams{EventLoopEnginesError: "bad"}, Steps: map[string]*StepData{}}) {
		t.Fatal("an unread report makes them incomplete")
	}
}
