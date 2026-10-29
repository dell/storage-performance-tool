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

	tests := map[string]struct {
		engines map[string][]results.NicEngine
		want    string
	}{
		"no reports":           {nil, ""},
		"every report agrees":  {map[string][]results.NicEngine{"s1": {two, twoOnNode1}, "s2": {two, twoOnNode1}}, "16 per engine (2 NICs × 8, default)"},
		"an explicit value":    {map[string][]results.NicEngine{"s1": {explicit}}, "12 per engine (2 NICs × 6, storage.driver.threads)"},
		"several drivers":      {map[string][]results.NicEngine{"s1": {drivers}}, "32 per engine (2 drivers × 2 NICs × 8, default)"},
		"drivers that differ":  {map[string][]results.NicEngine{"s1": {disagreeing}}, "14 per engine (2 drivers; see nicEngines in spt_run_params.json)"},
		"a NIC set not opened": {map[string][]results.NicEngine{"s1": {never}}, "0 per engine (0 NICs × 8, default)"},
		"engines differ": {map[string][]results.NicEngine{"s1": {oneOnNode1, two}},
			"node-000 16 (2 NICs × 8, default); node-001 8 (1 NIC × 8, default)"},
		"a missing report adds nothing": {map[string][]results.NicEngine{"s1": {two, twoOnNode1}, "s2": {twoOnNode1}},
			"16 per engine (2 NICs × 8, default)"},
		"steps differ": {map[string][]results.NicEngine{"s1": {two}, "s2": {explicit}}, nicEventLoopsVaryByStep},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := nicEventLoops(tc.engines, []string{"s1", "s2"}); got != tc.want {
				t.Fatalf("event loops = %q, want %q", got, tc.want)
			}
		})
	}
}

// nicSetRun is nicPathsRun's one-engine run with endpoints, the run's --interfaces value and
// nicEngines in its metadata.
func nicSetRun(t *testing.T, interfaces []string, engines map[string][]results.NicEngine) string {
	t.Helper()
	runDir := nicPathsRun(t, 1, nicPathsNodes[:1], nicPathsFetched("nic.paths.csv"))
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
		nicPathsFetched("nic.paths.node-000.csv"), nicPathsFetched("nic.paths.node-001.csv"))
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

func TestStoredRunWithoutInterfacesShowsNoNicSetBullets(t *testing.T) {
	full := nicPathsReports(t, aggregateStoredRun(t, nicSetRun(t, nil, nil)))["full"]
	mustNotContain(t, full, "Interfaces")
	mustNotContain(t, full, "Event loops")
}
