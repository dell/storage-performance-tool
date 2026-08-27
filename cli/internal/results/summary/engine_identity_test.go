package summary

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
)

func TestStoredSummaryRendersCompactCLIAndVerifiedEngineIdentity(t *testing.T) {
	dirty := false
	data := identityRunData(&engineinfo.Manifest{
		Consistency: engineinfo.ManifestConsistency{Status: engineinfo.ConsistencyConsistent, Reason: "verified"},
		Builds: []engineinfo.ManifestBuild{{
			BuildID: "build-1", Version: "5.15.0", Revision: strings.Repeat("a", 40), SourceDirty: &dirty,
		}},
		Participants: []engineinfo.ManifestParticipant{
			{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected, BuildID: "build-1"},
			{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollected, BuildID: "build-1"},
		},
	})
	summary, err := Aggregate(data)
	if err != nil {
		t.Fatal(err)
	}
	for name, report := range map[string]string{
		"full": NewRenderer(RenderOptions{}).FullReport(summary),
		"tui":  NewRenderer(RenderOptions{}).CompactSnippet(summary),
	} {
		for _, want := range []string{
			"Environment", "CLI", "5.15.0 (cccccccccccc)",
			"Engine", "5.15.0 (aaaaaaaaaaaa), 2 participants, consistency verified",
		} {
			if !strings.Contains(report, want) {
				t.Errorf("%s report missing %q:\n%s", name, want, report)
			}
		}
	}
}

func TestStoredSummaryRendersIndeterminateNodesAndReasonsConcisely(t *testing.T) {
	manifest := &engineinfo.Manifest{
		Consistency: engineinfo.ManifestConsistency{Status: engineinfo.ConsistencyIndeterminate, Reason: "identity incomplete"},
		Participants: []engineinfo.ManifestParticipant{
			{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusLegacyEndpointUnavailable, Reason: "endpoint unavailable"},
			{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusUnsupportedSchema, Reason: "schema 2"},
		},
	}
	summary, err := Aggregate(identityRunData(manifest))
	if err != nil {
		t.Fatal(err)
	}
	report := NewRenderer(RenderOptions{}).FullReport(summary)
	for _, want := range []string{"consistency indeterminate", "entry.example:9999=legacy_endpoint_unavailable (endpoint unavailable)", "worker.example:9999=unsupported_schema (schema 2)"} {
		if !strings.Contains(report, want) {
			t.Errorf("report missing %q:\n%s", want, report)
		}
	}
	if strings.Contains(report, "response body") || strings.Contains(report, "attempts=") {
		t.Fatalf("stored summary leaked transport detail:\n%s", report)
	}
}

func TestStoredSummaryMakesForcedMismatchProminentAndGroupsNodes(t *testing.T) {
	dirty := false
	manifest := &engineinfo.Manifest{
		Consistency: engineinfo.ManifestConsistency{Status: engineinfo.ConsistencyMismatch, Forced: true, Reason: "overridden"},
		Builds: []engineinfo.ManifestBuild{
			{BuildID: "build-1", Version: "5.14.2", Revision: strings.Repeat("a", 40), SourceDirty: &dirty},
			{BuildID: "build-2", Version: "5.15.0", Revision: strings.Repeat("b", 40), SourceDirty: &dirty},
		},
		Participants: []engineinfo.ManifestParticipant{
			{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected, BuildID: "build-1"},
			{NodeID: "worker-a.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollected, BuildID: "build-1"},
			{NodeID: "worker-b.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollected, BuildID: "build-2"},
		},
	}
	report := NewRenderer(RenderOptions{}).FullReport(mustAggregate(t, identityRunData(manifest)))
	warning := "WARNING: PERFORMANCE RESULTS COMBINE DIFFERENT ENGINE BUILDS"
	firstGroup := "5.14.2 (aaaaaaaaaaaa), 2 nodes"
	secondGroup := "5.15.0 (bbbbbbbbbbbb), 1 node"
	if warningAt, groupAt := strings.Index(report, warning), strings.Index(report, firstGroup); warningAt < 0 || groupAt <= warningAt {
		t.Fatalf("forced warning must precede grouped builds:\n%s", report)
	}
	if !strings.Contains(report, secondGroup) {
		t.Fatalf("second build group missing:\n%s", report)
	}
}

func TestStoredSummaryRejectsPerformanceRenderingAfterIdentityMismatch(t *testing.T) {
	data := identityRunData(&engineinfo.Manifest{
		Consistency:  engineinfo.ManifestConsistency{Status: engineinfo.ConsistencyMismatch, Reason: "rejected"},
		Participants: []engineinfo.ManifestParticipant{{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected}},
	})
	data.Params.Lifecycle = &RunLifecycle{Workload: LifecyclePhase{State: "rejected", Error: "engine build identity mismatch rejected before scenario submission"}}
	summary := mustAggregate(t, data)
	for name, report := range map[string]string{
		"full": NewRenderer(RenderOptions{MaxWidth: 72}).FullReport(summary),
		"tui":  NewRenderer(RenderOptions{MaxWidth: 72}).CompactSnippet(summary),
	} {
		normalized := strings.Join(strings.Fields(report), " ")
		if !strings.Contains(report, "Run rejected") || !strings.Contains(normalized, data.Params.Lifecycle.Workload.Error) {
			t.Fatalf("%s rejection diagnostic missing:\n%s", name, report)
		}
		for _, unwanted := range []string{"Performance by Phase", "Run Totals"} {
			if strings.Contains(report, unwanted) {
				t.Fatalf("%s rejected report contains %q:\n%s", name, unwanted, report)
			}
		}
		if name == "tui" {
			for _, line := range strings.Split(report, "\n") {
				if len([]rune(line)) > 72 {
					t.Fatalf("compact rejection line width = %d, want <= 72: %q", len([]rune(line)), line)
				}
			}
		}
	}
}

func TestStoredSummaryToleratesLegacyResultsWithoutBuildInformation(t *testing.T) {
	data := identityRunData(nil)
	data.Params.CLI = RunCLI{}
	report := NewRenderer(RenderOptions{}).FullReport(mustAggregate(t, data))
	for _, want := range []string{"CLI", "unavailable (legacy result)", "Engine", "unavailable (legacy result; engine identity was not recorded)"} {
		if !strings.Contains(report, want) {
			t.Errorf("legacy report missing %q:\n%s", want, report)
		}
	}
	if strings.Contains(report, "Engine Build Information unavailable") {
		t.Fatalf("pre-identity result gained a new warning:\n%s", report)
	}
}

func TestStoredSummaryWarnsWhenEngineBuildInformationIsUnusable(t *testing.T) {
	data := identityRunData(nil)
	data.EngineInfoUnavailableReason = "manifest file is missing"
	summary := mustAggregate(t, data)
	for name, report := range map[string]string{
		"full": NewRenderer(RenderOptions{MaxWidth: 72}).FullReport(summary),
		"tui":  NewRenderer(RenderOptions{MaxWidth: 72}).CompactSnippet(summary),
	} {
		normalized := strings.Join(strings.Fields(report), " ")
		for _, want := range []string{
			"unavailable (engine build information could not be verified)",
			"WARNING: Engine Build Information unavailable: manifest file is missing",
		} {
			if !strings.Contains(normalized, want) {
				t.Errorf("%s report missing %q:\n%s", name, want, report)
			}
		}
		if strings.Contains(report, "legacy result; engine identity was not recorded") {
			t.Errorf("%s report mislabeled unusable identity as legacy:\n%s", name, report)
		}
	}
}

func TestCompactStoredSummaryKeepsEnvironmentIdentityOnlyAndWidthBounded(t *testing.T) {
	manifest := &engineinfo.Manifest{
		Consistency: engineinfo.ManifestConsistency{Status: engineinfo.ConsistencyIndeterminate, Reason: "legacy fleet"},
		Participants: []engineinfo.ManifestParticipant{
			{NodeID: "entry-" + strings.Repeat("x", 90) + ".example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusLegacyEndpointUnavailable, Reason: "engine version endpoint unavailable"},
			{NodeID: "worker-with-a-long-name.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusUnsupportedSchema, Reason: "engine schema is newer than this CLI supports"},
		},
	}
	data := identityRunData(manifest)
	data.Params.SptImage = "registry.example/spt:an-unnecessarily-long-tag"
	data.Params.BaseURL = "http://entry-with-a-long-name.example:9999"
	data.Params.ScenarioStoredPath = "an-unnecessarily-long-scenario-file-name.js"
	for index := range 64 {
		data.Params.Hosts = append(data.Params.Hosts, RunHost{Original: "worker-" + formatInt(int64(index)) + ".example"})
	}

	const width = 72
	report := NewRenderer(RenderOptions{MaxWidth: width}).CompactSnippet(mustAggregate(t, data))
	for _, unwanted := range []string{data.Params.SptImage, data.Params.BaseURL, data.Params.ScenarioStoredPath, "worker-63.example"} {
		if strings.Contains(report, unwanted) {
			t.Fatalf("compact identity Environment exposed full field %q:\n%s", unwanted, report)
		}
	}
	environment := strings.SplitN(report, "Performance by Phase", 2)[0]
	for _, line := range strings.Split(environment, "\n") {
		if len([]rune(line)) > width {
			t.Fatalf("compact line width = %d, want <= %d: %q", len([]rune(line)), width, line)
		}
	}
}

func identityRunData(manifest *engineinfo.Manifest) *RunData {
	return &RunData{
		RunID: "run-1", EngineInfo: manifest, Steps: map[string]*StepData{},
		Params: &RunParams{CLI: RunCLI{Version: "5.15.0", Revision: strings.Repeat("c", 40)}},
	}
}

func mustAggregate(t *testing.T, data *RunData) *RunSummary {
	t.Helper()
	summary, err := Aggregate(data)
	if err != nil {
		t.Fatal(err)
	}
	return summary
}
