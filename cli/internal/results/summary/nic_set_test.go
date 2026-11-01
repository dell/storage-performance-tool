package summary

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

func nicEngine(node, eventLoops, perNic, driverThreads int, nics ...string) results.NicEngine {
	return results.NicEngine{Node: node, Drivers: 1, DriverThreads: &driverThreads, HardwareThreads: 64,
		EventLoopsPerNic: &perNic, Nics: append([]string{}, nics...), EventLoops: eventLoops}
}

func TestNicEventLoopsDescribesTheRunsEngines(t *testing.T) {
	two := nicEngine(0, 16, 8, 0, "ens1f0", "ens1f1")
	twoOnNode1 := nicEngine(1, 16, 8, 0, "ens1f0", "ens1f1")
	explicit := nicEngine(0, 12, 6, 6, "ens1f0", "ens1f1")
	oneOnNode1 := nicEngine(1, 8, 8, 0, "ens2f0")
	drivers := two
	drivers.Drivers, drivers.EventLoops = 2, 32
	disagreeing := results.NicEngine{Node: 0, Drivers: 2, HardwareThreads: 64, Nics: []string{"ens1f0"}, EventLoops: 14}
	never := nicEngine(0, 0, 8, 0)

	otherDriver := two
	otherDriver.ContextsWithoutNicSet = 1
	otherDrivers := two
	otherDrivers.ContextsWithoutNicSet = 2

	tests := map[string]struct {
		engines    map[string][]results.NicEngine
		incomplete bool
		want       string
	}{
		"no reports":           {engines: nil, want: ""},
		"every report agrees":  {engines: map[string][]results.NicEngine{"s1": {two, twoOnNode1}, "s2": {two, twoOnNode1}}, want: "16 per engine (2 NICs × 8, default)"},
		"an explicit value":    {engines: map[string][]results.NicEngine{"s1": {explicit}}, want: "12 per engine (2 NICs × 6, storage.driver.threads)"},
		"several drivers":      {engines: map[string][]results.NicEngine{"s1": {drivers}}, want: "32 per engine (2 drivers × 2 NICs × 8, default)"},
		"drivers that differ":  {engines: map[string][]results.NicEngine{"s1": {disagreeing}}, want: "14 per engine (2 drivers; see nicEngines in spt_run_params.json)"},
		"a NIC set not opened": {engines: map[string][]results.NicEngine{"s1": {never}}, want: "0 per engine (0 NICs × 8, default)"},
		"engines differ": {engines: map[string][]results.NicEngine{"s1": {oneOnNode1, two}},
			want: "node-000 16 (2 NICs × 8, default); node-001 8 (1 NIC × 8, default)"},
		"a step's missing report": {engines: map[string][]results.NicEngine{"s1": {two, twoOnNode1}, "s2": {twoOnNode1}}, incomplete: true,
			want: "node-000 16 (2 NICs × 8, default); node-001 16 (2 NICs × 8, default); " + nicEventLoopsOtherReports},
		"an unread report never generalizes": {engines: map[string][]results.NicEngine{"s1": {two}}, incomplete: true,
			want: "node-000 16 (2 NICs × 8, default); " + nicEventLoopsOtherReports},
		"steps differ": {engines: map[string][]results.NicEngine{"s1": {two}, "s2": {explicit}}, want: nicEventLoopsVaryByStep},
		"a driver without a NIC set": {engines: map[string][]results.NicEngine{"s1": {otherDriver}},
			want: "16 per engine (2 NICs × 8, default), not counting 1 driver without a NIC set"},
		"drivers without a NIC set": {engines: map[string][]results.NicEngine{"s1": {otherDrivers}},
			want: "16 per engine (2 NICs × 8, default), not counting 2 drivers without a NIC set"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := nicEventLoops(tc.engines, []string{"s1", "s2"}, tc.incomplete); got != tc.want {
				t.Fatalf("event loops = %q, want %q", got, tc.want)
			}
		})
	}
}

// nicSetRun is nicPathsRun's one-engine run with endpoints, the run's --interfaces value and
// nicEngines in its metadata.
func nicSetRun(t *testing.T, interfaces []string, engines map[string][]results.NicEngine) string {
	t.Helper()
	files := []results.FileStatus{nicPathsFetched("nic.paths.csv")}
	if engines != nil {
		files = append(files, nicPathsFetched("nic.set.json"))
	}
	runDir := nicPathsRun(t, 1, nicPathsNodes[:1], files...)
	storeNicSetMetadata(t, runDir, interfaces, nicPathsNodes[:1], engines)
	return runDir
}

// storeNicSetMetadata replaces a run's metadata with endpoints, the run's --interfaces value,
// its nicNodes and its nicEngines.
func storeNicSetMetadata(t *testing.T, runDir string, interfaces []string, nodes []results.NicNode,
	engines map[string][]results.NicEngine,
) {
	t.Helper()
	params := map[string]any{"WorkloadType": "write", "Endpoints": []string{"http://192.0.2.10:9000"}}
	if interfaces != nil {
		params["Interfaces"] = interfaces
	}
	metadata := map[string]any{
		"workloadType":    "write",
		"expectedStepIds": []string{nicPathsStepID},
		"scenarioParams":  params,
		"nicNodes":        nodes,
	}
	if engines != nil {
		metadata["nicEngines"] = engines
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsMetadataFileName), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDistributedAutoRunWhoseHostsHaveDifferentNicSetsShowsEachEngine(t *testing.T) {
	nodes := nicPathsNodes[:2]
	runDir := nicPathsRun(t, 2, nodes,
		nicPathsFetched("nic.paths.node-000.csv"), nicPathsFetched("nic.paths.node-001.csv"),
		nicPathsFetched("nic.set.node-000.json"), nicPathsFetched("nic.set.node-001.json"))
	for node, nics := range map[string]string{
		"node-000": `["ens1f0","ens1f1"],"event_loops":16`,
		"node-001": `["ens2f0"],"event_loops":8`,
	} {
		report := `{"schema_version":1,"step_id":"` + nicPathsStepID + `","hardware_threads":64,"driver_threads":0,` +
			`"event_loops_per_nic":8,"nics":` + nics + `}`
		name := nicPathsStepID + ".nic.set." + node + ".json"
		if err := os.WriteFile(filepath.Join(runDir, name), []byte(report), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	engines, err := results.ReadNicEngines(runDir, []string{nicPathsStepID}, nodes)
	if err != nil || len(engines[nicPathsStepID]) != 2 {
		t.Fatalf("engines = %+v, err = %v", engines, err)
	}
	storeNicSetMetadata(t, runDir, []string{"auto"}, nodes, engines)

	full := nicPathsReports(t, aggregateStoredRun(t, runDir))["full"]
	mustContain(t, full, "  • Interfaces         auto\n"+
		"  • Event loops        node-000 16 (2 NICs × 8, default); node-001 8 (1 NIC × 8, default)\n")
}

func TestNicEventLoopsAddsTheBaseGroupsAndTheTotalWhenTheReportHasThem(t *testing.T) {
	base, total := 2, 18
	withBase := nicEngine(0, 16, 8, 0, "ens1f0", "ens1f1")
	withBase.BaseEventLoops, withBase.TotalEventLoops = &base, &total
	if got := nicEventLoops(map[string][]results.NicEngine{"step": {withBase}}, []string{"step"}, false); got !=
		"16 per engine (2 NICs × 8, default) + 2 base = 18" {
		t.Fatalf("got %q", got)
	}
	// One NIC per engine, as four one-NIC engines run: 24 per-NIC loops and the base group of 2.
	oneBase, oneTotal := 2, 26
	one := nicEngine(0, 24, 24, 0, "ens1f0")
	one.BaseEventLoops, one.TotalEventLoops = &oneBase, &oneTotal
	if got := nicEngineEventLoops(one); got != "24 (1 NIC × 24, default) + 2 base = 26" {
		t.Fatalf("got %q", got)
	}
	// A report that predates the fields describes the per-NIC loops alone.
	if got := nicEngineEventLoops(nicEngine(0, 16, 8, 0, "ens1f0", "ens1f1")); got != "16 (2 NICs × 8, default)" {
		t.Fatalf("got %q", got)
	}
}

func TestStoredRunShowsItsInterfacesAndEventLoopsAfterTheEndpoints(t *testing.T) {
	engines := map[string][]results.NicEngine{nicPathsStepID: {nicEngine(0, 16, 8, 0, "ens1f0", "ens1f1")}}
	full := nicPathsReports(t, aggregateStoredRun(t, nicSetRun(t, []string{"auto"}, engines)))["full"]
	mustContain(t, full, "  • Endpoints          http://192.0.2.10:9000\n"+
		"  • Interfaces         auto\n"+
		"  • Event loops        16 per engine (2 NICs × 8, default)\n")

	list := nicPathsReports(t, aggregateStoredRun(t, nicSetRun(t, []string{"ens1f0", "192.0.2.21"}, nil)))["full"]
	mustContain(t, list, "  • Interfaces         ens1f0,192.0.2.21\n")
	mustNotContain(t, list, "Event loops")
}

func TestStoredRunNamesTheNodeOfAMissingNicSetReportUnderArtifactHealth(t *testing.T) {
	// A single engine wrote its per-path CSV but no report; the fetch recorded the report as missing.
	single := nicPathsReports(t, aggregateStoredRun(t, nicPathsRun(t, 1, nicPathsNodes[:1], nicPathsFetched("nic.paths.csv"),
		results.FileStatus{Name: nicPathsStepID + ".nic.set.json", Status: "missing",
			Error: "expected from node-000 (worker-a.example); not listed in index.json"})))["full"]
	mustContain(t, single, "NIC paths: "+nicPathsStepID+".nic.paths.csv\n")
	mustContain(t, single, "Artifact Health\n  • "+nicPathsStepID+" missing NIC-set reports: node-000, worker-a.example\n")
	mustNotContain(t, single, "missing NIC paths")

	// A distributed step whose every CSV arrived and whose node-001 report did not.
	distributed := nicPathsReports(t, aggregateStoredRun(t, nicPathsRun(t, 3, nicPathsNodes, nicPathsCanonicalAbsent,
		nicPathsFetched("nic.paths.node-000.csv"), nicPathsFetched("nic.paths.node-001.csv"),
		nicPathsFetched("nic.paths.node-002.csv"), nicPathsFetched("nic.set.node-000.json"),
		nicPathsFetched("nic.set.node-002.json"))))["full"]
	mustContain(t, distributed, "NIC paths: "+nicPathsStepID+".nic.paths.node-NNN.csv (3 of 3 nodes)\n")
	mustContain(t, distributed, "Artifact Health\n  • "+nicPathsStepID+" missing NIC-set reports: node-001, worker-b.example\n")
	mustNotContain(t, distributed, "missing NIC paths")

	// Both files missing: each has its own line.
	both := nicPathsReports(t, aggregateStoredRun(t, nicPathsRun(t, 2, nicPathsNodes, nicPathsCanonicalAbsent,
		nicPathsFetched("nic.paths.node-000.csv"), nicPathsFetched("nic.set.node-000.json"))))["full"]
	mustContain(t, both, "Artifact Health\n"+
		"  • "+nicPathsStepID+" missing NIC paths files: node-001, worker-b.example\n"+
		"  • "+nicPathsStepID+" missing NIC-set reports: node-001, worker-b.example\n")
}

func TestStoredRunsWithEveryNicSetReportOrNoneExpectedHaveNoMissingReport(t *testing.T) {
	none := []results.NicNode{{Node: 0, Host: "worker-a.example", Interfaces: "none"}, {Node: 1, Host: "worker-b.example", Interfaces: "none"}}
	tests := map[string]string{
		"every report fetched": nicPathsRun(t, 2, nicPathsNodes, nicPathsCanonicalAbsent,
			nicPathsFetched("nic.paths.node-000.csv"), nicPathsFetched("nic.paths.node-001.csv"),
			nicPathsFetched("nic.set.node-000.json"), nicPathsFetched("nic.set.node-001.json")),
		"results of an older CLI": nicPathsRun(t, 2, nil, nicPathsCanonicalAbsent),
		"no node with a value":    nicPathsRun(t, 2, none, nicPathsCanonicalAbsent),
	}
	for name, runDir := range tests {
		t.Run(name, func(t *testing.T) {
			summary := aggregateStoredRun(t, runDir)
			if missing := summary.Steps[0].NicSetMissing; len(missing) != 0 {
				t.Fatalf("missing NIC-set reports = %+v, want none", missing)
			}
			mustNotContain(t, nicPathsReports(t, summary)["full"], "Artifact Health")
		})
	}
}

func TestStoredRunDoesNotGeneralizeEventLoopsFromAnUnreadableReport(t *testing.T) {
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
	metadata["nicEnginesError"] = "first.nic.set.json: bad\nsecond.nic.set.json: worse"
	if encoded, err = json.Marshal(metadata); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatal(err)
	}

	full := nicPathsReports(t, aggregateStoredRun(t, runDir))["full"]
	mustContain(t, full, "node-000 16 (2 NICs × 8, default); "+nicEventLoopsOtherReports+"\n")
	mustContain(t, full, "NIC-set reports not read: first.nic.set.json: bad; second.nic.set.json: worse\n")
	mustNotContain(t, full, "per engine")
}

func TestDistributedRunDoesNotGeneralizeEventLoopsPastAMissingReport(t *testing.T) {
	runDir := nicPathsRun(t, 3, nicPathsNodes, nicPathsCanonicalAbsent,
		nicPathsFetched("nic.paths.node-000.csv"), nicPathsFetched("nic.paths.node-001.csv"),
		nicPathsFetched("nic.paths.node-002.csv"), nicPathsFetched("nic.set.node-000.json"),
		nicPathsFetched("nic.set.node-002.json"))
	storeNicSetMetadata(t, runDir, []string{"auto"}, nicPathsNodes, map[string][]results.NicEngine{
		nicPathsStepID: {nicEngine(0, 16, 8, 0, "ens1f0", "ens1f1"), nicEngine(2, 16, 8, 0, "ens1f0", "ens1f1")},
	})

	full := nicPathsReports(t, aggregateStoredRun(t, runDir))["full"]
	mustContain(t, full, "node-000 16 (2 NICs × 8, default); node-002 16 (2 NICs × 8, default); "+nicEventLoopsOtherReports+"\n")
	mustContain(t, full, "missing NIC-set reports: node-001")
	mustNotContain(t, full, "per engine")
}

func TestStoredRunWithoutInterfacesShowsNoNicSetBullets(t *testing.T) {
	full := nicPathsReports(t, aggregateStoredRun(t, nicSetRun(t, nil, nil)))["full"]
	mustNotContain(t, full, "Interfaces")
	mustNotContain(t, full, "Event loops")
}
