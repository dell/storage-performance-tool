package results

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

var threeHosts = RunNodes{Hosts: []string{"worker-a.example", "worker-b.example", "worker-c.example"}}

func TestEventLoopReportsAreOptionalArtifactsWithDiscoverableNodeCopies(t *testing.T) {
	if !integrityNodeSourcePattern.MatchString("event.loops.node-002.json") {
		t.Fatal("a node's event-loop report copy was not dynamically discoverable")
	}
	for _, name := range []string{"event.loops.json", "event.loops.node-002.csv", "event.loops.node-2.json", "loops.node-002.json"} {
		if integrityNodeSourcePattern.MatchString(name) {
			t.Fatalf("%s is not a node copy", name)
		}
	}
	found := false
	for _, spec := range DefaultArtifacts {
		found = found || reflect.DeepEqual(spec, ArtifactSpec{Loggers: []string{"EventLoops"}, Suffix: "event.loops.json"})
	}
	if !found {
		t.Fatal("the event-loop report is not an optional default artifact")
	}
}

func TestExpectedEventLoopsFilesNameEveryParticipant(t *testing.T) {
	tests := map[string]struct {
		nodes        RunNodes
		participants int
		want         []NicNodeFile
	}{
		"every configured host took part": {nodes: threeHosts, participants: 3, want: []NicNodeFile{
			{Name: "s1.event.loops.node-000.json", Node: 0, Host: "worker-a.example"},
			{Name: "s1.event.loops.node-001.json", Node: 1, Host: "worker-b.example"},
			{Name: "s1.event.loops.node-002.json", Node: 2, Host: "worker-c.example"},
		}},
		// A worker that did not start moves the later ones up: only the Entry Node is certain.
		"a configured host did not take part": {nodes: threeHosts, participants: 2, want: []NicNodeFile{
			{Name: "s1.event.loops.node-000.json", Node: 0, Host: "worker-a.example"},
			{Name: "s1.event.loops.node-001.json", Node: 1},
		}},
		// nicNodes follow the run's locked participants.
		"nicNodes name the slices": {nodes: RunNodes{NicNodes: threeNicNodes[:2], Hosts: threeHosts.Hosts}, participants: 2,
			want: []NicNodeFile{
				{Name: "s1.event.loops.node-000.json", Node: 0, Host: "worker-a.example"},
				{Name: "s1.event.loops.node-001.json", Node: 1, Host: "worker-b.example"},
			}},
		"one engine": {nodes: threeHosts, participants: 1, want: []NicNodeFile{
			{Name: "s1.event.loops.json", Node: 0, Host: "worker-a.example"},
		}},
		"no host recorded": {participants: 1, want: []NicNodeFile{{Name: "s1.event.loops.json", Node: 0}}},
		"no participant":   {nodes: threeHosts, participants: 0, want: []NicNodeFile{}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := ExpectedEventLoopsFiles("s1", tc.nodes, tc.participants); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("expected reports = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestRunHostNamesPreferTheHostThenTheOriginal(t *testing.T) {
	got := RunHostNames([]RunHostRecord{{Host: " a.example "}, {Original: "user@b.example"}, {}})
	if want := []string{"a.example", "user@b.example", ""}; !reflect.DeepEqual(got, want) {
		t.Fatalf("names = %q, want %q", got, want)
	}
	if RunHostNames(nil) != nil {
		t.Fatal("no hosts, no names")
	}
}

// fetchEventLoopsStep fetches one step whose metrics totals report nodeCount, with the configured
// hosts in the run metadata and listed as the index's extra loggers.
func fetchEventLoopsStep(t *testing.T, step, nodeCount string, hosts []string, listed ...string) map[string]FileStatus {
	t.Helper()
	totals := fmt.Sprintf("OpType,NodeCount,CountSucc\nREAD,%s,10\n", nodeCount)
	items := []map[string]any{{"logger": "metrics.FileTotal", "size": len(totals)}}
	handlers := map[string]http.HandlerFunc{
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(totals)) },
	}
	for _, logger := range listed {
		items = append(items, map[string]any{"logger": logger, "size": 2})
		handlers["/logs/"+step+"/"+logger] = func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{}")) }
	}
	handlers["/logs/"+step+"/index.json"] = func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	}
	srv := newTestServer(t, handlers)
	defer srv.Close()

	out := t.TempDir()
	records := make([]RunHostRecord, 0, len(hosts))
	for _, host := range hosts {
		records = append(records, RunHostRecord{Host: host})
	}
	metadata, err := json.Marshal(map[string]any{"hosts": records})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(out, constants.ResultsMetadataFileName), metadata, 0o600); err != nil {
		t.Fatal(err)
	}
	fetcher := NewFetcher(srv.URL, out)
	fetcher.Artifacts = []ArtifactSpec{
		{Loggers: []string{"metrics.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsTotal, Required: true},
		{Loggers: []string{"EventLoops"}, Suffix: constants.ResultsArtifactSuffixEventLoops},
	}
	manifest, err := fetcher.FetchArtifactsForSteps(context.Background(), []string{step})
	if err != nil {
		t.Fatalf("a missing event-loop report never fails the fetch: %v", err)
	}
	return filesByName(manifest)
}

func TestFetcherRecordsAMissingNodeCopyOnceOneArrived(t *testing.T) {
	const step = "loops-read"
	files := fetchEventLoopsStep(t, step, "3", threeHosts.Hosts, "event.loops.node-000.json", "event.loops.node-002.json")
	for _, name := range []string{step + ".event.loops.node-000.json", step + ".event.loops.node-002.json"} {
		if files[name].Status != fileStatusOK {
			t.Fatalf("%s = %+v, want ok", name, files[name])
		}
	}
	missing := files[step+".event.loops.node-001.json"]
	if missing.Status != fileStatusMissing || !strings.Contains(missing.Error, "node-001 (worker-b.example)") {
		t.Fatalf("the participating node's absent report = %+v, want missing with its node", missing)
	}
	if canonical := files[step+".event.loops.json"]; canonical.Status != fileStatusMissing || canonical.Error != "" {
		t.Fatalf("a distributed step's canonical report = %+v, want a plain optional entry", canonical)
	}
}

func TestFetcherRecordsNothingForEnginesThatPredateTheReport(t *testing.T) {
	const step = "loops-read"
	tests := map[string]map[string]FileStatus{
		"one engine": fetchEventLoopsStep(t, step, "1", threeHosts.Hosts[:1]),
		"no copy":    fetchEventLoopsStep(t, step, "3", threeHosts.Hosts),
	}
	for name, files := range tests {
		t.Run(name, func(t *testing.T) {
			for file, status := range files {
				if strings.Contains(file, "event.loops") && (file != step+".event.loops.json" || status.Error != "") {
					t.Fatalf("an older engine's step records no expected report, got %s %+v", file, status)
				}
			}
		})
	}
	single := fetchEventLoopsStep(t, step, "1", threeHosts.Hosts[:1], "EventLoops")[step+".event.loops.json"]
	if single.Status != fileStatusOK || single.Error != "" {
		t.Fatalf("a single engine's report = %+v, want ok", single)
	}
}

func TestMissingEventLoopsFilesNeedsAFetchedCopy(t *testing.T) {
	ok := func(name string) FileStatus { return FileStatus{Name: name, Status: fileStatusOK} }
	sm := &StepManifest{StepID: "s1", Files: []FileStatus{ok("s1.event.loops.node-001.json")}}
	got := MissingEventLoopsFiles(sm, threeHosts, 3)
	want := []NicNodeFile{
		{Name: "s1.event.loops.node-000.json", Node: 0, Host: "worker-a.example"},
		{Name: "s1.event.loops.node-002.json", Node: 2, Host: "worker-c.example"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("missing = %+v, want %+v", got, want)
	}
	if got := MissingEventLoopsFiles(&StepManifest{StepID: "s1"}, threeHosts, 3); got != nil {
		t.Fatalf("no copy at all = %+v, want none", got)
	}
	if got := MissingEventLoopsFiles(&StepManifest{StepID: "s1"}, threeHosts, 1); got != nil {
		t.Fatalf("one engine = %+v, want none", got)
	}
	failed := &StepManifest{StepID: "s1", Files: []FileStatus{{Name: "s1.event.loops.node-000.json", Status: "error"}}}
	if got := MissingEventLoopsFiles(failed, threeHosts, 2); got != nil {
		t.Fatalf("a copy that failed to download is not a fetched one, got %+v", got)
	}
}

func TestReadEventLoopEnginesReadsEachParticipantsReportInNodeOrder(t *testing.T) {
	dir := t.TempDir()
	writeNicSetFixture(t, dir, "s1."+constants.ResultsArtifactSuffixMetricsTotal, "OpType,NodeCount\nREAD,2\n")
	writeNicSetFixture(t, dir, "s1.event.loops.node-001.json", `{"schema_version":1,"engine_run_id":7,"step_id":"s1",
		"worker_id":"w","hardware_threads":32,"event_loops":4,"added_later":true,"contexts":[
		{"context_index":0,"route":"default","driver_threads":0,"source":"default","event_loops":4,"added_later":1}]}`)
	writeNicSetFixture(t, dir, "s1.event.loops.node-000.json", `{"schema_version":1,"step_id":"s1","hardware_threads":192,
		"event_loops":110,"contexts":[
		{"context_index":0,"route":"default","driver_threads":12,"source":"storage.driver.threads","event_loops":12},
		{"context_index":1,"route":"nic_set","driver_threads":0,"source":"default","event_loops":98}]}`)
	writeNicSetFixture(t, dir, "s2."+constants.ResultsArtifactSuffixMetricsTotal, "OpType,NodeCount\nREAD,1\n")
	writeNicSetFixture(t, dir, "s2.event.loops.json", `{"schema_version":1,"step_id":"s2","hardware_threads":8,
		"event_loops":0,"contexts":[]}`)
	// A step without metrics totals expects no report.
	writeNicSetFixture(t, dir, "s3.event.loops.json", `{"schema_version":1,"step_id":"s3"}`)
	// A step whose engines predate the report.
	writeNicSetFixture(t, dir, "s4."+constants.ResultsArtifactSuffixMetricsTotal, "OpType,NodeCount\nREAD,1\n")

	engines, err := ReadEventLoopEngines(dir, []string{"s1", "s2", "s3", "s4"}, threeHosts)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]EventLoopEngine{
		"s1": {
			{Node: 0, Host: "worker-a.example", HardwareThreads: 192, EventLoops: 110, Contexts: []EventLoopContext{
				{ContextIndex: 0, Route: EventLoopRouteDefault, DriverThreads: 12, Source: EventLoopSourceDriverThreads, EventLoops: 12},
				{ContextIndex: 1, Route: EventLoopRouteNicSet, Source: EventLoopSourceDefault, EventLoops: 98},
			}},
			{Node: 1, HardwareThreads: 32, EventLoops: 4, Contexts: []EventLoopContext{
				{Route: EventLoopRouteDefault, Source: EventLoopSourceDefault, EventLoops: 4},
			}},
		},
		"s2": {{Node: 0, Host: "worker-a.example", HardwareThreads: 8}},
	}
	if !reflect.DeepEqual(engines, want) {
		t.Fatalf("engines = %+v\nwant %+v", engines, want)
	}
	if got, _ := ReadEventLoopEngines(dir, []string{"s4"}, threeHosts); got != nil {
		t.Fatalf("results without the report = %+v, want none", got)
	}
}

func TestReadEventLoopEnginesNamesAReportItCannotRead(t *testing.T) {
	dir := t.TempDir()
	for _, step := range []string{"bad", "schema", "step", "good"} {
		writeNicSetFixture(t, dir, step+"."+constants.ResultsArtifactSuffixMetricsTotal, "OpType,NodeCount\nREAD,1\n")
	}
	writeNicSetFixture(t, dir, "bad.event.loops.json", "{")
	writeNicSetFixture(t, dir, "schema.event.loops.json", `{"schema_version":2,"step_id":"schema"}`)
	writeNicSetFixture(t, dir, "step.event.loops.json", `{"schema_version":1,"step_id":"other"}`)
	writeNicSetFixture(t, dir, "good.event.loops.json", `{"schema_version":1,"step_id":"good","hardware_threads":6,
		"event_loops":4,"contexts":[{"context_index":0,"route":"default","driver_threads":0,"source":"default","event_loops":4}]}`)
	engines, err := ReadEventLoopEngines(dir, []string{"bad", "schema", "step", "good"}, RunNodes{})
	if err == nil {
		t.Fatal("unreadable reports were not named")
	}
	for _, want := range []string{"bad.event.loops.json", "schema_version 2, want 1", `step_id "other", want "step"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q lacks %q", err, want)
		}
	}
	if len(engines) != 1 || len(engines["good"]) != 1 || engines["good"][0].EventLoops != 4 {
		t.Fatalf("the readable report was not kept: %+v", engines)
	}
}
