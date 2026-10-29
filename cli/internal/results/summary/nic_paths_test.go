package summary

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

const nicPathsStepID = "mt-001-create"

var nicPathsNodes = []results.NicNode{
	{Node: 0, Host: "worker-a", Interfaces: "auto"},
	{Node: 1, Host: "worker-b", Interfaces: "auto"},
	{Node: 2, Host: "worker-c", Interfaces: "ens1f0,ens1f1"},
}

// nicPathsRun writes a one-step run whose metrics report nodeCount participants, with
// nicNodes in its metadata (none when nil) and files added to the step's index entries.
func nicPathsRun(t *testing.T, nodeCount int, nicNodes []results.NicNode, files ...results.FileStatus) string {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "nic-paths-run")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := makeManifest(t, runDir, []stepFixture{{
		ID: nicPathsStepID,
		MetricsContent: sampleMetricsCSV([]string{fmt.Sprintf(
			`"2026-10-29T10:00:00Z",CREATE,8,%d,8,8,100,0,104857600,1,8,100,100,100,100,1000,500,750,900,950,2000,800,400,650,800,900,1800`,
			nodeCount)}),
	}})
	manifest.Steps[0].Files = append(manifest.Steps[0].Files, files...)
	writeManifest(t, runDir, manifest)
	metadata := map[string]any{
		"workloadType":    "write",
		"expectedStepIds": []string{nicPathsStepID},
		"scenarioParams":  map[string]any{"WorkloadType": "write"},
	}
	if nicNodes != nil {
		metadata["nicNodes"] = nicNodes
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, constants.ResultsMetadataFileName), encoded, 0o644); err != nil {
		t.Fatal(err)
	}
	return runDir
}

func nicPathsFetched(name string) results.FileStatus {
	return results.FileStatus{Name: nicPathsStepID + "." + name, Size: 4, Status: "ok"}
}

// nicPathsCanonicalAbsent is the plain optional entry of a step whose canonical file was not listed.
var nicPathsCanonicalAbsent = results.FileStatus{Name: nicPathsStepID + ".nic.paths.csv", Status: "missing"}

func nicPathsReports(t *testing.T, summary *RunSummary) map[string]string {
	t.Helper()
	if len(summary.Steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(summary.Steps))
	}
	step := summary.Steps[0]
	if len(step.MissingRequired) != 0 || step.Status == StepStatusPartial || step.Status == StepStatusError {
		t.Fatalf("NIC paths files never fail a step: status %s, missing required %q", step.Status, step.MissingRequired)
	}
	return rdmaReports(summary)
}

func TestStoredRunNamesASingleEnginesNicPathsFile(t *testing.T) {
	summary := aggregateStoredRun(t, nicPathsRun(t, 1, nicPathsNodes[:1], nicPathsFetched("nic.paths.csv")))
	for form, report := range nicPathsReports(t, summary) {
		if !strings.Contains(report, "NIC paths: "+nicPathsStepID+".nic.paths.csv\n") {
			t.Fatalf("%s report lacks the NIC paths line:\n%s", form, report)
		}
		mustNotContain(t, report, "missing NIC paths")
	}
}

func TestStoredRunCountsEachNodesNicPathsCopy(t *testing.T) {
	summary := aggregateStoredRun(t, nicPathsRun(t, 3, nicPathsNodes, nicPathsCanonicalAbsent,
		nicPathsFetched("nic.paths.node-000.csv"), nicPathsFetched("nic.paths.node-001.csv"),
		nicPathsFetched("nic.paths.node-002.csv")))
	if missing := summary.Steps[0].MissingOptional; strings.Contains(strings.Join(missing, ","), "nic.paths") {
		t.Fatalf("a distributed step's canonical name is not missing evidence, got %q", missing)
	}
	for form, report := range nicPathsReports(t, summary) {
		if !strings.Contains(report, "NIC paths: "+nicPathsStepID+".nic.paths.node-NNN.csv (3 of 3 nodes)\n") {
			t.Fatalf("%s report lacks the NIC paths line:\n%s", form, report)
		}
	}
}

func TestStoredRunNamesTheNodeOfAMissingNicPathsCopy(t *testing.T) {
	summary := aggregateStoredRun(t, nicPathsRun(t, 3, nicPathsNodes, nicPathsCanonicalAbsent,
		nicPathsFetched("nic.paths.node-000.csv"), nicPathsFetched("nic.paths.node-001.csv"),
		results.FileStatus{Name: nicPathsStepID + ".nic.paths.node-002.csv", Status: "missing", Error: "expected from node-002 (worker-c)"}))
	reports := nicPathsReports(t, summary)
	for form, report := range reports {
		want := "NIC paths: " + nicPathsStepID + ".nic.paths.node-NNN.csv (2 of 3 nodes; missing node-002, worker-c)\n"
		if !strings.Contains(report, want) {
			t.Fatalf("%s report lacks %q:\n%s", form, want, report)
		}
	}
	mustContain(t, reports["full"], "Artifact Health\n  • "+nicPathsStepID+" missing NIC paths files: node-002, worker-c\n")
}

func TestStoredRunWithoutAnyNicPathsFileListsTheMissingNodesOnly(t *testing.T) {
	single := nicPathsReports(t, aggregateStoredRun(t, nicPathsRun(t, 1, nicPathsNodes[:1], nicPathsCanonicalAbsent)))
	mustNotContain(t, single["full"], "NIC paths:")
	mustContain(t, single["full"], nicPathsStepID+" missing NIC paths files: node-000, worker-a\n")

	nodes := nicPathsReports(t, aggregateStoredRun(t, nicPathsRun(t, 2, nicPathsNodes, nicPathsCanonicalAbsent)))
	mustNotContain(t, nodes["full"], "NIC paths:")
	mustContain(t, nodes["full"], nicPathsStepID+" missing NIC paths files: node-000, worker-a; node-001, worker-b\n")
}

func TestStoredRunsWithoutNicPathsRenderAsBefore(t *testing.T) {
	none := []results.NicNode{{Node: 0, Host: "worker-a", Interfaces: "none"}, {Node: 1, Host: "worker-b", Interfaces: "none"}}
	tests := map[string][]results.NicNode{
		"results of an older CLI": nil,
		"no node with a value":    none,
		"an empty value":          {{Node: 0, Host: "worker-a"}},
	}
	for name, nicNodes := range tests {
		t.Run(name, func(t *testing.T) {
			summary := aggregateStoredRun(t, nicPathsRun(t, 2, nicNodes, nicPathsCanonicalAbsent))
			if summary.Steps[0].NicPaths != nil {
				t.Fatalf("NIC paths evidence = %+v, want none", summary.Steps[0].NicPaths)
			}
			for form, report := range nicPathsReports(t, summary) {
				if strings.Contains(report, "NIC paths") {
					t.Fatalf("%s report mentions NIC paths for a step without them:\n%s", form, report)
				}
			}
		})
	}
}
