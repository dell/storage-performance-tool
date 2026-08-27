package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/buildinfo"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	resultsummary "github.com/dell/storage-performance-tool/cli/internal/results/summary"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestWriteRunMetadataPersistsIdentityCLIReferenceIndexAndLegacyHint(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "step.config.yaml"), []byte("run:\n  version: 5.14.2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	initialIndex := results.Manifest{
		BaseURL: "http://127.0.0.1:9999", OutputDir: root,
		GeneratedAt: time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC),
		Steps:       []results.StepManifest{{StepID: "step"}},
		RunFiles:    []results.FileStatus{{Name: "trace.log", Status: "ok"}},
	}
	writeJSONFixture(t, filepath.Join(root, constants.ResultsManifestFileName), initialIndex)

	previousVersion, previousCommit, previousBuildDate := buildinfo.Version, buildinfo.Commit, buildinfo.BuildDate
	buildinfo.Version, buildinfo.Commit, buildinfo.BuildDate = "5.15.0-dev", strings.Repeat("c", 40), "2026-08-26T12:00:00Z"
	t.Cleanup(func() {
		buildinfo.Version, buildinfo.Commit, buildinfo.BuildDate = previousVersion, previousCommit, previousBuildDate
	})
	metadata := buildRunMetadata(runMetadataInput{Params: scenario.Params{RunID: 1787750472685}})
	metadata.engineIdentity = &engineinfo.GateOutcome{
		Decision: engineinfo.GateProceed, Proceed: true,
		Fleet: engineinfo.FleetResult{
			Consistency: engineinfo.ConsistencyAssessment{Status: engineinfo.ConsistencyIndeterminate, Reason: "legacy endpoint unavailable"},
			Participants: []engineinfo.ParticipantResult{{
				NodeID: "127.0.0.1:9999", Role: engineinfo.RoleStandalone,
				CollectionStatus: engineinfo.StatusLegacyEndpointUnavailable,
				Reason:           "engine version endpoint is unavailable",
			}},
		},
	}
	previousNow := runEngineIdentityNow
	runEngineIdentityNow = func() time.Time { return time.Date(2026, 8, 26, 13, 21, 42, 0, time.UTC) }
	t.Cleanup(func() { runEngineIdentityNow = previousNow })

	if err := writeRunMetadata(metadata, root); err != nil {
		t.Fatal(err)
	}
	if err := writeRunMetadata(metadata, root); err != nil {
		t.Fatal(err)
	}

	manifest, err := engineinfo.LoadManifest(filepath.Join(root, constants.EngineInfoManifestName))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.RunID != metadata.runID || len(manifest.Participants) != 1 ||
		manifest.Participants[0].ConfiguredVersionHint != "5.14.2" ||
		manifest.Consistency.Status != engineinfo.ConsistencyIndeterminate {
		t.Fatalf("engine manifest = %+v", manifest)
	}

	metadataData, err := os.ReadFile(filepath.Join(root, constants.ResultsMetadataFileName))
	if err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(metadataData, &persisted); err != nil {
		t.Fatal(err)
	}
	cli := persisted["cli"].(map[string]any)
	if cli["version"] != "5.15.0-dev" || cli["revision"] != strings.Repeat("c", 40) ||
		cli["buildTime"] != "2026-08-26T12:00:00Z" ||
		persisted["engineInfoFile"] != constants.EngineInfoManifestName ||
		persisted["engineConsistency"] != string(engineinfo.ConsistencyIndeterminate) {
		t.Fatalf("run metadata identity fields = %s", metadataData)
	}
	if _, duplicated := persisted["builds"]; duplicated {
		t.Fatalf("run metadata duplicated engine builds: %s", metadataData)
	}
	if _, duplicated := persisted["participants"]; duplicated {
		t.Fatalf("run metadata duplicated engine participants: %s", metadataData)
	}
	scenarioParams := persisted["scenarioParams"].(map[string]any)
	if scenarioParams["RunID"] != float64(manifest.RunID) {
		t.Fatalf("run metadata run ID does not match engine manifest: %s", metadataData)
	}

	var index results.Manifest
	indexData, err := os.ReadFile(filepath.Join(root, constants.ResultsManifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(indexData, &index); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, file := range index.RunFiles {
		if file.Name == constants.EngineInfoManifestName {
			count++
			if file.Status != "ok" || file.ContentType != "application/json" || file.Size == 0 {
				t.Fatalf("engine info index entry = %+v", file)
			}
		}
	}
	if count != 1 || len(index.Steps) != 1 {
		t.Fatalf("index run files/steps = %+v / %+v", index.RunFiles, index.Steps)
	}
}

func TestPublishedIdentitySurvivesRealArtifactFetchAndImmediateSummaryLoad(t *testing.T) {
	const stepID = "ticket-10-001-read"
	metrics := "DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc," +
		"CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MiB/s]," +
		"BWLast[MiB/s],DurationAvg[us],DurationMin[us],DurationQ_0.25[us],DurationQ_0.5[us]," +
		"DurationQ_0.75[us],DurationMax[us],LatencyAvg[us],LatencyMin[us],LatencyQ_0.25[us]," +
		"LatencyQ_0.5[us],LatencyQ_0.75[us],LatencyMax[us]\n" +
		`"2026-08-27T12:00:00Z",READ,8,1,8,8,100,0,104857600,1,8,100,100,100,100,` +
		"1000,500,750,900,950,2000,800,400,650,800,900,1800\n"
	mux := http.NewServeMux()
	mux.HandleFunc("/logs/"+stepID+"/index.json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
			{"logger": "metrics.FileTotal", "size": len(metrics)},
		}})
	})
	mux.HandleFunc("/logs/"+stepID+"/metrics.FileTotal", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(metrics))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	root := filepath.Join(t.TempDir(), "managed-run")
	dirty := false
	metadata := buildRunMetadata(runMetadataInput{
		WorkloadType:    scenario.WorkloadTypeRead,
		Params:          scenario.Params{RunID: 1787750472685, WorkloadType: scenario.WorkloadTypeRead},
		BaseURL:         server.URL,
		ExpectedStepIDs: []string{stepID},
	})
	metadata.engineIdentity = &engineinfo.GateOutcome{
		Decision: engineinfo.GateProceed,
		Proceed:  true,
		Fleet: engineinfo.FleetResult{
			Consistency: engineinfo.ConsistencyAssessment{
				Status: engineinfo.ConsistencyConsistent, Reason: "all participants reported the same engine build identity",
			},
			Builds: []engineinfo.GroupedBuild{{
				BuildID: "build-1",
				Information: engineinfo.BuildInformation{
					SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
					Revision: strings.Repeat("a", 40), BuildTime: "2026-08-27T12:00:00Z",
					SourceDirty: &dirty,
				},
			}},
			Participants: []engineinfo.ParticipantResult{{
				NodeID: "127.0.0.1:9999", Role: engineinfo.RoleStandalone,
				CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1",
			}},
		},
	}
	if err := writeRunMetadata(metadata, root); err != nil {
		t.Fatal(err)
	}

	fetcher := results.NewFetcher(server.URL, root)
	fetcher.Artifacts = []results.ArtifactSpec{{
		Loggers: []string{"metrics.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsTotal, Required: true,
	}}
	if _, err := fetcher.FetchArtifactsForSteps(context.Background(), []string{stepID}); err != nil {
		t.Fatal(err)
	}

	// A fresh loader represents a process that stopped immediately after artifact fetching;
	// no later writeRunMetadata pass repairs or republishes the on-disk bundle.
	loaded, err := resultsummary.NewLoader().Load(context.Background(), root)
	if err != nil {
		t.Fatalf("load immediately after artifact fetch: %v", err)
	}
	if loaded.EngineInfo == nil || loaded.EngineInfo.RunID != metadata.runID || loaded.EngineInfoUnavailableReason != "" {
		t.Fatalf("loaded engine identity = %+v, unavailable reason = %q", loaded.EngineInfo, loaded.EngineInfoUnavailableReason)
	}
	if loaded.Steps[stepID] == nil || loaded.Steps[stepID].Metrics == nil {
		t.Fatalf("loaded performance evidence = %+v", loaded.Steps)
	}
	foundEngineInfo := false
	for _, file := range loaded.Manifest.RunFiles {
		foundEngineInfo = foundEngineInfo || file.Name == constants.EngineInfoManifestName && file.Status == "ok"
	}
	if !foundEngineInfo {
		t.Fatalf("engine information was erased from index after fetch: %+v", loaded.Manifest.RunFiles)
	}
}

func TestPersistRejectedEngineIdentityRecordsLifecycleAndMinimalIndex(t *testing.T) {
	root := filepath.Join(t.TempDir(), "rejected")
	metadata := buildRunMetadata(runMetadataInput{})
	outcome := runMismatchOutcomeForPersistence()
	gateErr := os.ErrPermission
	if err := persistRejectedEngineIdentity(metadata, root, 73, outcome, gateErr); err != nil {
		t.Fatal(err)
	}
	if metadata.Lifecycle == nil || metadata.Lifecycle.Workload.State != "rejected" ||
		!metadata.Lifecycle.Workload.Completed || metadata.Lifecycle.Workload.Started ||
		metadata.Lifecycle.Workload.Error != gateErr.Error() {
		t.Fatalf("rejected lifecycle = %+v", metadata.Lifecycle)
	}
	updateRunLifecycleMetadata(metadata, &autoResultsOutcome{})
	if metadata.Lifecycle.Workload.State != "rejected" || metadata.Lifecycle.Workload.Error != gateErr.Error() {
		t.Fatalf("cleanup finalization erased rejection lifecycle = %+v", metadata.Lifecycle)
	}
	manifest, err := engineinfo.LoadManifest(filepath.Join(root, constants.EngineInfoManifestName))
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Consistency.Status != engineinfo.ConsistencyMismatch || manifest.Consistency.Forced ||
		!strings.Contains(manifest.Consistency.Reason, "rejected before scenario submission") {
		t.Fatalf("rejection manifest = %+v", manifest.Consistency)
	}
	var index results.Manifest
	data, err := os.ReadFile(filepath.Join(root, constants.ResultsManifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	if len(index.Steps) != 0 || len(index.RunFiles) != 1 || index.RunFiles[0].Name != constants.EngineInfoManifestName {
		t.Fatalf("rejected index = %+v", index)
	}
}

func TestEngineIdentityIndexIgnoresTemporaryManifestCandidates(t *testing.T) {
	root := t.TempDir()
	manifest := engineinfo.Manifest{
		SchemaVersion: constants.EngineInfoManifestSchemaVersion,
		RunID:         73,
		GeneratedAt:   "2026-08-26T13:21:42Z",
		Consistency: engineinfo.ManifestConsistency{
			Status: engineinfo.ConsistencyIndeterminate, Reason: "legacy engine",
		},
		Builds: []engineinfo.ManifestBuild{},
		Participants: []engineinfo.ManifestParticipant{{
			NodeID: "127.0.0.1:9999", Role: engineinfo.RoleStandalone,
			CollectionStatus: engineinfo.StatusLegacyEndpointUnavailable,
			Reason:           "engine version endpoint is unavailable",
		}},
	}
	if err := engineinfo.WriteManifestAtomic(root, manifest); err != nil {
		t.Fatal(err)
	}
	temporaryCandidate := "." + constants.EngineInfoManifestName + ".tmp-leftover"
	if err := os.WriteFile(filepath.Join(root, temporaryCandidate), []byte("incomplete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := indexEngineIdentityManifest(root); err != nil {
		t.Fatal(err)
	}

	var index results.Manifest
	data, err := os.ReadFile(filepath.Join(root, constants.ResultsManifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	if len(index.RunFiles) != 1 || index.RunFiles[0].Name != constants.EngineInfoManifestName {
		t.Fatalf("indexed run files = %+v, want only the completed public manifest", index.RunFiles)
	}
}

func TestDefaultArtifactFetcherNeverCopiesEngineLocalBuildRecords(t *testing.T) {
	for _, artifact := range results.DefaultArtifacts {
		if artifact.Suffix == "engine.build.json" {
			t.Fatalf("engine-local build record registered for CLI collection: %+v", artifact)
		}
		for _, logger := range artifact.Loggers {
			if logger == "EngineBuild" || logger == "engine.build.json" {
				t.Fatalf("engine-local build logger registered for CLI collection: %+v", artifact)
			}
		}
	}
}

func runMismatchOutcomeForPersistence() engineinfo.GateOutcome {
	dirty := false
	return engineinfo.GateOutcome{
		Decision: engineinfo.GateRejectedMismatch,
		Fleet: engineinfo.FleetResult{
			Consistency: engineinfo.ConsistencyAssessment{Status: engineinfo.ConsistencyMismatch, Reason: "different revisions"},
			Builds: []engineinfo.GroupedBuild{
				{BuildID: "build-1", Information: engineinfo.BuildInformation{
					SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
					Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:00:00Z", SourceDirty: &dirty,
				}},
				{BuildID: "build-2", Information: engineinfo.BuildInformation{
					SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
					Revision: strings.Repeat("b", 40), BuildTime: "2026-08-26T12:00:00Z", SourceDirty: &dirty,
				}},
			},
			Participants: []engineinfo.ParticipantResult{
				{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry,
					CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1"},
				{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker,
					CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-2"},
			},
		},
	}
}

func writeJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
