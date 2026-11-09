package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/results/summary"
)

func TestCaptureEventLoopEnginesStoresTheFetchedReportsForEveryRun(t *testing.T) {
	root := t.TempDir()
	for name, content := range map[string]string{
		"s1." + constants.ResultsArtifactSuffixMetricsTotal: "OpType,NodeCount\nREAD,2\n",
		"s1.event.loops.node-000.json": `{"schema_version":1,"step_id":"s1","hardware_threads":192,"event_loops":24,
			"contexts":[{"context_index":0,"route":"default","driver_threads":0,"source":"default","event_loops":24}]}`,
		"s1.event.loops.node-001.json": `{"schema_version":1,"step_id":"s1","hardware_threads":96,"event_loops":12,
			"contexts":[{"context_index":0,"route":"default","driver_threads":12,"source":"storage.driver.threads","event_loops":12}]}`,
		"s2." + constants.ResultsArtifactSuffixMetricsTotal: "OpType,NodeCount\nREAD,1\n",
		"s2.event.loops.json":                               `{"schema_version":9}`,
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// No NIC set: the reports are read all the same, with the configured hosts.
	metadata := buildRunMetadata(runMetadataInput{
		HostInfos: []*hostparse.HostInfo{{Host: "worker-a.example"}, {Original: "user@worker-b.example"}},
	})
	captureEventLoopEngines(root, []string{"s1", "s2"}, metadata)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	want := `"eventLoopEngines":{"s1":[` +
		`{"node":0,"host":"worker-a.example","hardwareThreads":192,"eventLoops":24,"contexts":[` +
		`{"contextIndex":0,"route":"default","driverThreads":0,"source":"default","eventLoops":24}]},` +
		`{"node":1,"host":"user@worker-b.example","hardwareThreads":96,"eventLoops":12,"contexts":[` +
		`{"contextIndex":0,"route":"default","driverThreads":12,"source":"storage.driver.threads","eventLoops":12}]}]},` +
		`"eventLoopEnginesError":"s2.event.loops.json: schema_version 9, want 1"`
	if !strings.Contains(string(encoded), want) {
		t.Fatalf("metadata lacks %s:\n%s", want, encoded)
	}

	// The summary reads back what the run stored.
	var params summary.RunParams
	if err := json.Unmarshal(encoded, &params); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(params.EventLoopEngines, metadata.EventLoopEngines) ||
		params.EventLoopEnginesError != metadata.EventLoopEnginesError {
		t.Fatalf("stored %+v %q, read back %+v %q", metadata.EventLoopEngines, metadata.EventLoopEnginesError,
			params.EventLoopEngines, params.EventLoopEnginesError)
	}

	older := buildRunMetadata(runMetadataInput{HostInfos: []*hostparse.HostInfo{{Host: "worker-a.example"}}})
	captureEventLoopEngines(t.TempDir(), []string{"s1"}, older)
	if older.EventLoopEngines != nil || older.EventLoopEnginesError != "" {
		t.Fatalf("results without reports store none, got %+v %q", older.EventLoopEngines, older.EventLoopEnginesError)
	}
	encoded, err = json.Marshal(older)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "eventLoopEngines") {
		t.Fatalf("results without reports add no field:\n%s", encoded)
	}
	captureEventLoopEngines(root, []string{"s1"}, nil)
}

func TestRunNodesFollowTheMetadataHostsAndNicNodes(t *testing.T) {
	metadata := &runMetadata{
		Hosts:    []runHostMetadata{{Host: "worker-a.example"}, {Original: "worker-b.example"}},
		NicNodes: []results.NicNode{{Node: 0, Host: "worker-a.example", Interfaces: "auto"}},
	}
	want := results.RunNodes{NicNodes: metadata.NicNodes, Hosts: []string{"worker-a.example", "worker-b.example"}}
	if got := metadata.runNodes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("run nodes = %+v, want %+v", got, want)
	}
}
