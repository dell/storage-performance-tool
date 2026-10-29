package results

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestNicSetReportsAreOptionalArtifactsWithDiscoverableNodeCopies(t *testing.T) {
	if !integrityNodeSourcePattern.MatchString("nic.set.node-002.json") {
		t.Fatal("a node's NIC-set report copy was not dynamically discoverable")
	}
	for _, name := range []string{"nic.set.node-002.csv", "nic.set.json", "nic.paths.node-002.json"} {
		if integrityNodeSourcePattern.MatchString(name) {
			t.Fatalf("%s is not a node copy", name)
		}
	}
	found := false
	for _, spec := range DefaultArtifacts {
		found = found || reflect.DeepEqual(spec, ArtifactSpec{Loggers: []string{"NicSet"}, Suffix: "nic.set.json"})
	}
	if !found {
		t.Fatal("the NIC-set report is not an optional default artifact")
	}
}

func TestExpectedNicSetFilesFollowThePerPathRules(t *testing.T) {
	got := ExpectedNicSetFiles("read1", threeNicNodes, 2)
	want := []NicNodeFile{
		{Name: "read1.nic.set.node-000.json", Node: 0, Host: "worker-a"},
		{Name: "read1.nic.set.node-001.json", Node: 1, Host: "worker-b"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected reports = %+v, want %+v", got, want)
	}
	single := ExpectedNicSetFiles("read1", threeNicNodes, 1)
	if !reflect.DeepEqual(single, []NicNodeFile{{Name: "read1.nic.set.json", Node: 0, Host: "worker-a"}}) {
		t.Fatalf("a single participant writes the canonical report, got %+v", single)
	}
}

func TestFetcherRecordsAnExpectedNicSetReportThatIsMissing(t *testing.T) {
	step := "nic-004-read"
	files := filesByName(fetchNicPathsStep(t, step, "2", threeNicNodes,
		"nic.paths.node-000.csv", "nic.set.node-000.json", "nic.paths.node-001.csv"))
	if status := files[step+".nic.set.node-000.json"]; status.Status != fileStatusOK {
		t.Fatalf("node 000's report = %+v, want ok", status)
	}
	missing := files[step+".nic.set.node-001.json"]
	if missing.Status != fileStatusMissing || !strings.Contains(missing.Error, "node-001 (worker-b)") {
		t.Fatalf("the participating node's absent report = %+v, want missing with its node", missing)
	}

	single := filesByName(fetchNicPathsStep(t, step, "1", threeNicNodes[:1], "NicPaths"))[step+".nic.set.json"]
	if single.Status != fileStatusMissing || !strings.Contains(single.Error, "node-000 (worker-a)") {
		t.Fatalf("a single engine's absent report = %+v, want missing with its node", single)
	}
	present := filesByName(fetchNicPathsStep(t, step, "1", threeNicNodes[:1], "NicPaths", "NicSet"))[step+".nic.set.json"]
	if present.Status != fileStatusOK || present.Error != "" {
		t.Fatalf("a single engine's report = %+v, want ok", present)
	}
}

func TestReadNicEnginesReadsEachParticipantsReportInNodeOrder(t *testing.T) {
	dir := t.TempDir()
	writeNicSetFixture(t, dir, "s1."+constants.ResultsArtifactSuffixMetricsTotal, "OpType,NodeCount\nREAD,2\n")
	writeNicSetFixture(t, dir, "s1.nic.set.node-001.json", `{"schema_version":1,"engine_run_id":7,"step_id":"s1",
		"worker_id":"w","hardware_threads":32,"driver_threads":0,"event_loops_per_nic":4,
		"nics":["ens1f0","ens1f1","ens2f0"],"event_loops":12,"added_later":true}`)
	writeNicSetFixture(t, dir, "s1.nic.set.node-000.json", `{"schema_version":1,"step_id":"s1","hardware_threads":64,
		"driver_threads":0,"nics":["ens1f0"],"event_loops":14,"contexts":[
		{"context_index":0,"driver_threads":0,"event_loops_per_nic":8,"nics":["ens1f0"],"event_loops":8},
		{"context_index":1,"driver_threads":6,"event_loops_per_nic":6,"nics":["ens1f0"],"event_loops":6}]}`)
	writeNicSetFixture(t, dir, "s2."+constants.ResultsArtifactSuffixMetricsTotal, "OpType,NodeCount\nREAD,1\n")
	writeNicSetFixture(t, dir, "s2.nic.set.json", `{"schema_version":1,"step_id":"s2","hardware_threads":8,
		"driver_threads":2,"event_loops_per_nic":2,"nics":[],"event_loops":0}`)
	writeNicSetFixture(t, dir, "s3.nic.set.json", `{"schema_version":1,"step_id":"s3"}`)

	got, err := ReadNicEngines(dir, []string{"s1", "s2", "s3", "s4"}, threeNicNodes)
	if err != nil {
		t.Fatal(err)
	}
	zero, four, two := 0, 4, 2
	want := map[string][]NicEngine{
		"s1": {
			{Node: 0, Host: "worker-a", Drivers: 2, DriverThreads: &zero, HardwareThreads: 64, Nics: []string{"ens1f0"}, EventLoops: 14,
				Contexts: []NicContext{
					{ContextIndex: 0, DriverThreads: 0, EventLoopsPerNic: 8, Nics: []string{"ens1f0"}, EventLoops: 8},
					{ContextIndex: 1, DriverThreads: 6, EventLoopsPerNic: 6, Nics: []string{"ens1f0"}, EventLoops: 6},
				}},
			{Node: 1, Host: "worker-b", Drivers: 1, DriverThreads: &zero, HardwareThreads: 32, EventLoopsPerNic: &four,
				Nics: []string{"ens1f0", "ens1f1", "ens2f0"}, EventLoops: 12},
		},
		"s2": {{Node: 0, Host: "worker-a", Drivers: 1, DriverThreads: &two, HardwareThreads: 8, EventLoopsPerNic: &two,
			Nics: []string{}, EventLoops: 0}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("engines = %+v, want %+v", got, want)
	}
	if engines, err := ReadNicEngines(dir, []string{"s1"}, nil); engines != nil || err != nil {
		t.Fatalf("a run without nicNodes reads nothing, got %+v, %v", engines, err)
	}
}

func TestReadNicEnginesNamesAReportItCannotRead(t *testing.T) {
	dir := t.TempDir()
	writeNicSetFixture(t, dir, "s1."+constants.ResultsArtifactSuffixMetricsTotal, "OpType,NodeCount\nREAD,3\n")
	writeNicSetFixture(t, dir, "s1.nic.set.node-000.json", `{"schema_version":2,"step_id":"s1"}`)
	writeNicSetFixture(t, dir, "s1.nic.set.node-001.json", `{"schema_version":1,"step_id":"other"}`)
	writeNicSetFixture(t, dir, "s1.nic.set.node-002.json", `{"schema_version":1,"step_id":"s1","event_loops":4`)

	got, err := ReadNicEngines(dir, []string{"s1"}, threeNicNodes)
	if got != nil {
		t.Fatalf("unreadable reports store nothing, got %+v", got)
	}
	for _, want := range []string{
		"s1.nic.set.node-000.json: schema_version 2, want 1",
		`s1.nic.set.node-001.json: step_id "other", want "s1"`,
		"s1.nic.set.node-002.json: unexpected end of JSON input",
	} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %v, want it to contain %q", err, want)
		}
	}
}

func writeNicSetFixture(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
