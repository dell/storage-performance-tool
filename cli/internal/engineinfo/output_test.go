package engineinfo

import (
	"strings"
	"testing"
)

func TestFleetOutputLinesKeepNormalOutputConcise(t *testing.T) {
	dirty := false
	result := FleetResult{
		Consistency: ConsistencyAssessment{Status: ConsistencyConsistent},
		Builds: []GroupedBuild{{BuildID: "build-1", Information: BuildInformation{
			Version: "5.15.0", Revision: strings.Repeat("a", 40), SourceDirty: &dirty,
		}}},
		Participants: []ParticipantResult{
			{NodeID: "entry.example:9999", Role: RoleEntry, CollectionStatus: StatusCollected, BuildID: "build-1", Attempts: 1},
			{NodeID: "worker.example:9999", Role: RoleWorker, CollectionStatus: StatusCollected, BuildID: "build-1", Attempts: 2},
		},
	}

	normal := result.OutputLines(false)
	if len(normal) != 1 || normal[0] != "Engine identity: 5.15.0 (aaaaaaaaaaaa), 2 participants, consistency verified" {
		t.Fatalf("normal output = %q", normal)
	}
	verbose := result.OutputLines(true)
	if len(verbose) != 3 || !strings.Contains(verbose[2], "worker.example:9999") ||
		!strings.Contains(verbose[2], "attempts=2 retries=1") {
		t.Fatalf("verbose output = %q", verbose)
	}
}

func TestFleetBuildGroupLinesReportVersionRevisionAndNodeCounts(t *testing.T) {
	result := FleetResult{
		Builds: []GroupedBuild{
			{BuildID: "build-1", Information: BuildInformation{Version: "5.14.2", Revision: strings.Repeat("a", 40)}},
			{BuildID: "build-2", Information: BuildInformation{Version: "5.15.0", Revision: strings.Repeat("b", 40)}},
		},
		Participants: []ParticipantResult{
			{BuildID: "build-1"}, {BuildID: "build-1"}, {BuildID: "build-2"},
		},
	}
	got := result.BuildGroupLines()
	want := []string{
		"Engine build group: 5.14.2 (aaaaaaaaaaaa), 2 nodes",
		"Engine build group: 5.15.0 (bbbbbbbbbbbb), 1 node",
	}
	if len(got) != len(want) {
		t.Fatalf("groups = %q", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("groups[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
