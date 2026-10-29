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

var threeNicNodes = []NicNode{
	{Node: 0, Host: "worker-a", Interfaces: "auto"},
	{Node: 1, Host: "worker-b", Interfaces: "ens1f0,ens1f1"},
	{Node: 2, Host: "worker-c", Interfaces: "auto"},
}

func TestNicPathsNodeSourcesAreDiscovered(t *testing.T) {
	if !integrityNodeSourcePattern.MatchString("nic.paths.node-002.csv") {
		t.Fatal("a node's NIC paths copy was not dynamically discoverable")
	}
	found := false
	for _, spec := range DefaultArtifacts {
		found = found || reflect.DeepEqual(spec, ArtifactSpec{Loggers: []string{"NicPaths"}, Suffix: "nic.paths.csv"})
	}
	if !found {
		t.Fatal("the NIC paths file is not an optional default artifact")
	}
}

func TestExpectedNicPathsFilesNameEachParticipatingNodeWithAValue(t *testing.T) {
	nodes := []NicNode{
		{Node: 0, Host: "worker-a", Interfaces: "auto"},
		{Node: 1, Host: "worker-b", Interfaces: "none"},
		{Node: 2, Host: "worker-c", Interfaces: "ens1f0"},
		{Node: 3, Host: "worker-d", Interfaces: ""},
		{Node: 4, Host: "worker-e", Interfaces: "auto"},
	}
	got := ExpectedNicPathsFiles("read1", nodes, 4)
	want := []NicNodeFile{
		{Name: "read1.nic.paths.node-000.csv", Node: 0, Host: "worker-a"},
		{Name: "read1.nic.paths.node-002.csv", Node: 2, Host: "worker-c"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected files = %+v, want %+v", got, want)
	}
	single := ExpectedNicPathsFiles("read1", nodes, 1)
	if !reflect.DeepEqual(single, []NicNodeFile{{Name: "read1.nic.paths.csv", Node: 0, Host: "worker-a"}}) {
		t.Fatalf("a single participant writes the canonical file, got %+v", single)
	}
	if got := ExpectedNicPathsFiles("read1", nil, 3); got != nil {
		t.Fatalf("without nicNodes nothing is expected, got %+v", got)
	}
}

func TestFetcherRecordsAnExpectedNodeFileThatIsMissing(t *testing.T) {
	step := "nic-001-read"
	nodes := append(append([]NicNode(nil), threeNicNodes...), NicNode{Node: 3, Host: "worker-d", Interfaces: "auto"})
	manifest := fetchNicPathsStep(t, step, "3", nodes, "nic.paths.node-000.csv", "nic.paths.node-001.csv")

	files := filesByName(manifest)
	for _, node := range []string{"000", "001"} {
		if status := files[step+".nic.paths.node-"+node+".csv"]; status.Status != fileStatusOK {
			t.Fatalf("node %s copy = %+v, want ok", node, status)
		}
	}
	missing := files[step+".nic.paths.node-002.csv"]
	if missing.Status != fileStatusMissing || !strings.Contains(missing.Error, "node-002 (worker-c)") {
		t.Fatalf("the participating node's absent copy = %+v, want missing with its node", missing)
	}
	if _, ok := files[step+".nic.paths.node-003.csv"]; ok {
		t.Fatal("a node that did not take part in the step has no entry")
	}
	if canonical := files[step+".nic.paths.csv"]; canonical.Status != fileStatusMissing || canonical.Error != "" {
		t.Fatalf("a distributed step's canonical name stays a plain optional entry, got %+v", canonical)
	}
}

func TestFetcherRecordsASingleEnginesMissingFileAndNothingForANodeWithoutAValue(t *testing.T) {
	step := "nic-002-read"
	single := fetchNicPathsStep(t, step, "1", threeNicNodes[:1])
	if canonical := filesByName(single)[step+".nic.paths.csv"]; canonical.Status != fileStatusMissing ||
		!strings.Contains(canonical.Error, "node-000 (worker-a)") {
		t.Fatalf("a single engine's absent file = %+v, want missing with its node", canonical)
	}

	present := filesByName(fetchNicPathsStep(t, step, "1", threeNicNodes[:1], "NicPaths"))[step+".nic.paths.csv"]
	if present.Status != fileStatusOK || present.Error != "" {
		t.Fatalf("a single engine's file = %+v, want ok", present)
	}

	none := []NicNode{{Node: 0, Host: "worker-a", Interfaces: "none"}, {Node: 1, Host: "worker-b", Interfaces: "none"}}
	for name, status := range filesByName(fetchNicPathsStep(t, step, "2", none)) {
		if strings.Contains(name, "nic.paths") && (status.Error != "" || name != step+".nic.paths.csv") {
			t.Fatalf("a node without a value gets no entry, got %s %+v", name, status)
		}
	}
}

func TestFetcherWithoutNicNodesIndexesAsBefore(t *testing.T) {
	step := "nic-003-read"
	files := filesByName(fetchNicPathsStep(t, step, "3", nil))
	for name, status := range files {
		if strings.Contains(name, "nic.paths") && (name != step+".nic.paths.csv" || status.Error != "") {
			t.Fatalf("a run without nicNodes records no expected NIC paths file, got %s %+v", name, status)
		}
	}
}

// fetchNicPathsStep fetches one step whose metrics totals report nodeCount, with
// nicNodes in the run metadata (none when nil) and listed as the index's extra loggers.
func fetchNicPathsStep(t *testing.T, step, nodeCount string, nicNodes []NicNode, listed ...string) *Manifest {
	t.Helper()
	totals := fmt.Sprintf("OpType,NodeCount,CountSucc\nREAD,%s,10\n", nodeCount)
	items := []map[string]any{{"logger": "metrics.FileTotal", "size": len(totals)}}
	handlers := map[string]http.HandlerFunc{
		"/logs/" + step + "/metrics.FileTotal": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(totals)) },
	}
	for _, logger := range listed {
		items = append(items, map[string]any{"logger": logger, "size": 4})
		handlers["/logs/"+step+"/"+logger] = func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("rows")) }
	}
	handlers["/logs/"+step+"/index.json"] = func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	}
	srv := newTestServer(t, handlers)
	defer srv.Close()

	out := t.TempDir()
	if nicNodes != nil {
		metadata, err := json.Marshal(map[string]any{"nicNodes": nicNodes})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, constants.ResultsMetadataFileName), metadata, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fetcher := NewFetcher(srv.URL, out)
	fetcher.Artifacts = []ArtifactSpec{
		{Loggers: []string{"metrics.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsTotal, Required: true},
		{Loggers: []string{"NicPaths"}, Suffix: constants.ResultsArtifactSuffixNicPaths},
		{Loggers: []string{"NicSet"}, Suffix: constants.ResultsArtifactSuffixNicSet},
	}
	manifest, err := fetcher.FetchArtifactsForSteps(context.Background(), []string{step})
	if err != nil {
		t.Fatalf("a missing NIC paths file never fails the fetch: %v", err)
	}
	return manifest
}

func filesByName(manifest *Manifest) map[string]FileStatus {
	files := make(map[string]FileStatus)
	for _, status := range manifest.Steps[0].Files {
		files[status.Name] = status
	}
	return files
}
