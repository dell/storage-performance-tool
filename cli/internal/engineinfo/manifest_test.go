package engineinfo_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
)

func TestManifestFromGateOutcomeMatchesSchemaOneGolden(t *testing.T) {
	dirty := false
	outcome := engineinfo.GateOutcome{
		Decision: engineinfo.GateProceed,
		Proceed:  true,
		Fleet: engineinfo.FleetResult{
			Consistency: engineinfo.ConsistencyAssessment{
				Status: engineinfo.ConsistencyConsistent,
				Reason: "all 2 participants reported consistent engine build identity fields",
			},
			Builds: []engineinfo.GroupedBuild{
				{BuildID: "old-b", Information: engineinfo.BuildInformation{
					SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
					Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:35:56Z",
					Development: false, SourceDirty: &dirty,
				}},
				{BuildID: "old-a", Information: engineinfo.BuildInformation{
					SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
					Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:34:56Z",
					Development: false, SourceDirty: &dirty,
				}},
			},
			Participants: []engineinfo.ParticipantResult{
				{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "old-b"},
				{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry, CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "old-a"},
			},
		},
	}

	manifest, err := engineinfo.NewManifest(
		1787750472685,
		time.Date(2026, 8, 26, 13, 21, 42, 0, time.UTC),
		outcome,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "engine.info.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(append(got, '\n')) != string(want) {
		t.Fatalf("manifest mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	root := t.TempDir()
	if err := engineinfo.WriteManifestAtomic(root, manifest); err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(filepath.Join(root, constants.EngineInfoManifestName))
	if err != nil {
		t.Fatal(err)
	}
	if string(stored) != string(want) {
		t.Fatalf("stored manifest bytes changed\n--- got ---\n%s\n--- want ---\n%s", stored, want)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestManifestConfiguredVersionHintIsNonAuthoritative(t *testing.T) {
	outcome := engineinfo.GateOutcome{Decision: engineinfo.GateProceed, Proceed: true, Fleet: engineinfo.FleetResult{
		Consistency: engineinfo.ConsistencyAssessment{Status: engineinfo.ConsistencyIndeterminate, Reason: "legacy engine"},
		Participants: []engineinfo.ParticipantResult{{
			NodeID: "127.0.0.1:9999", Role: engineinfo.RoleStandalone,
			CollectionStatus: engineinfo.StatusLegacyEndpointUnavailable,
			Reason:           "engine version endpoint is unavailable",
		}},
	}}
	manifest, err := engineinfo.NewManifest(17, time.Unix(0, 0).UTC(), outcome,
		map[string]string{"127.0.0.1:9999": "5.14.2"})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Consistency.Status != engineinfo.ConsistencyIndeterminate ||
		len(manifest.Builds) != 0 || len(manifest.Participants) != 1 ||
		manifest.Participants[0].ConfiguredVersionHint != "5.14.2" ||
		manifest.Participants[0].BuildID != "" {
		t.Fatalf("hint changed identity evidence: %+v", manifest)
	}
}

func TestManifestConfiguredVersionHintRejectsCollectedAndUnsafeValues(t *testing.T) {
	tests := []struct {
		name   string
		status engineinfo.CollectionStatus
		hint   string
	}{
		{name: "collected identity needs no hint", status: engineinfo.StatusCollected, hint: "5.14.2"},
		{name: "unsupported identity is not a legacy hint", status: engineinfo.StatusUnsupportedSchema, hint: "5.14.2"},
		{name: "control character", status: engineinfo.StatusLegacyEndpointUnavailable, hint: "5.14.2\nsecret"},
		{name: "oversized", status: engineinfo.StatusLegacyEndpointUnavailable, hint: strings.Repeat("v", 129)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dirty := false
			outcome := engineinfo.GateOutcome{Decision: engineinfo.GateProceed, Proceed: true, Fleet: engineinfo.FleetResult{
				Consistency: engineinfo.ConsistencyAssessment{Status: engineinfo.ConsistencyIndeterminate, Reason: "identity unavailable"},
				Participants: []engineinfo.ParticipantResult{{
					NodeID: "127.0.0.1:9999", Role: engineinfo.RoleStandalone, CollectionStatus: test.status,
					Reason: "identity unavailable",
				}},
			}}
			if test.status == engineinfo.StatusCollected {
				outcome.Fleet.Consistency = engineinfo.ConsistencyAssessment{Status: engineinfo.ConsistencyConsistent, Reason: "identity available"}
				outcome.Fleet.Builds = []engineinfo.GroupedBuild{{BuildID: "original", Information: engineinfo.BuildInformation{
					SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
					Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:00:00Z", SourceDirty: &dirty,
				}}}
				outcome.Fleet.Participants[0].ReportedSchemaVersion = 1
				outcome.Fleet.Participants[0].BuildID = "original"
				outcome.Fleet.Participants[0].Reason = ""
			} else if test.status == engineinfo.StatusUnsupportedSchema {
				outcome.Fleet.Participants[0].ReportedSchemaVersion = 2
			}
			manifest, err := engineinfo.NewManifest(23, time.Unix(0, 0).UTC(), outcome,
				map[string]string{"127.0.0.1:9999": test.hint})
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Participants[0].ConfiguredVersionHint != "" {
				t.Fatalf("unsafe or inapplicable hint retained: %q", manifest.Participants[0].ConfiguredVersionHint)
			}
		})
	}
}

func TestManifestPreservesForcedMismatchWithoutRelabeling(t *testing.T) {
	dirty := false
	outcome := engineinfo.GateOutcome{
		Decision: engineinfo.GateProceed,
		Proceed:  true,
		Fleet: engineinfo.FleetResult{
			Consistency: engineinfo.ConsistencyAssessment{
				Status: engineinfo.ConsistencyMismatch, Forced: true,
				Reason: "engine build identity mismatch overridden by --force",
			},
			Builds: []engineinfo.GroupedBuild{
				{BuildID: "original-a", Information: engineinfo.BuildInformation{
					SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
					Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:00:00Z",
					SourceDirty: &dirty,
				}},
				{BuildID: "original-b", Information: engineinfo.BuildInformation{
					SchemaVersion: 1, Product: "spt-engine", Version: "5.14.2",
					Revision: strings.Repeat("b", 40), BuildTime: "2026-08-26T12:00:00Z",
					SourceDirty: &dirty,
				}},
			},
			Participants: []engineinfo.ParticipantResult{
				{NodeID: "entry.example:9999", Role: engineinfo.RoleEntry,
					CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "original-a"},
				{NodeID: "worker.example:9999", Role: engineinfo.RoleWorker,
					CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "original-b"},
			},
		},
	}
	manifest, err := engineinfo.NewManifest(19, time.Unix(0, 0).UTC(), outcome, nil)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Consistency.Status != engineinfo.ConsistencyMismatch || !manifest.Consistency.Forced ||
		manifest.Consistency.Reason != "engine build identity mismatch overridden by --force" {
		t.Fatalf("forced mismatch was relabeled: %+v", manifest.Consistency)
	}
}

func TestWriteManifestAtomicReplacesOneRootFileAndLeavesNoTemporaryFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, constants.EngineInfoManifestName)
	if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := engineinfo.Manifest{
		SchemaVersion: constants.EngineInfoManifestSchemaVersion,
		RunID:         9,
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
	loaded, err := engineinfo.LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.RunID != 9 {
		t.Fatalf("run_id = %d, want 9", loaded.RunID)
	}
	matches, err := filepath.Glob(filepath.Join(root, ".engine.info.json.tmp-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary files = %v, error = %v", matches, err)
	}
}

func TestManifestValidateRejectsSemanticallyInvalidEvidence(t *testing.T) {
	valid := validCollectedManifest()
	tests := []struct {
		name   string
		mutate func(*engineinfo.Manifest)
		want   string
	}{
		{
			name: "forced consistent status",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Consistency.Forced = true
			},
			want: "only a mismatched",
		},
		{
			name: "nil build array",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Builds = nil
			},
			want: "must be arrays",
		},
		{
			name: "noncanonical node ID",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Participants[0].NodeID = "User@Example:9999"
			},
			want: "node_id",
		},
		{
			name: "collected participant without build",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Participants[0].BuildID = ""
				manifest.Builds = []engineinfo.ManifestBuild{}
			},
			want: "collected engine identity",
		},
		{
			name: "invalid build payload",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Builds[0].Product = "not-spt"
			},
			want: "invalid product",
		},
		{
			name: "noncanonical build order",
			mutate: func(manifest *engineinfo.Manifest) {
				second := manifest.Builds[0]
				second.BuildID = "build-2"
				second.BuildTime = "2026-08-26T11:00:00Z"
				manifest.Builds = append(manifest.Builds, second)
			},
			want: "canonical unique order",
		},
		{
			name: "duplicate participant",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Participants = append(manifest.Participants, manifest.Participants[0])
			},
			want: "duplicate participant",
		},
		{
			name: "noncanonical participant order",
			mutate: func(manifest *engineinfo.Manifest) {
				entry := manifest.Participants[0]
				entry.NodeID = "entry.example:9999"
				entry.Role = engineinfo.RoleEntry
				worker := entry
				worker.NodeID = "worker.example:9999"
				worker.Role = engineinfo.RoleWorker
				manifest.Participants = []engineinfo.ManifestParticipant{worker, entry}
			},
			want: "participants are not in canonical order",
		},
		{
			name: "unreferenced build",
			mutate: func(manifest *engineinfo.Manifest) {
				duplicate := manifest.Builds[0]
				duplicate.BuildID = "build-2"
				duplicate.BuildTime = "2026-08-26T12:01:00Z"
				manifest.Builds = append(manifest.Builds, duplicate)
			},
			want: "unreferenced",
		},
		{
			name: "status contradicts evidence",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Consistency.Status = engineinfo.ConsistencyMismatch
			},
			want: "does not match participant evidence",
		},
		{
			name: "incomplete status with complete evidence",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Participants[0].CollectionStatus = engineinfo.StatusIncompleteBuildInfo
				manifest.Participants[0].Reason = "comparison fields are incomplete"
				manifest.Consistency.Status = engineinfo.ConsistencyIndeterminate
			},
			want: "has complete comparison fields",
		},
		{
			name: "legacy status with build evidence",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Participants[0].CollectionStatus = engineinfo.StatusLegacyEndpointUnavailable
				manifest.Participants[0].ReportedSchemaVersion = 0
				manifest.Participants[0].Reason = "endpoint unavailable"
				manifest.Consistency.Status = engineinfo.ConsistencyIndeterminate
			},
			want: "contains build evidence",
		},
		{
			name: "unsupported status with supported schema",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Participants[0].CollectionStatus = engineinfo.StatusUnsupportedSchema
				manifest.Participants[0].BuildID = ""
				manifest.Participants[0].Reason = "unsupported schema"
				manifest.Builds = []engineinfo.ManifestBuild{}
				manifest.Consistency.Status = engineinfo.ConsistencyIndeterminate
			},
			want: "schema evidence is invalid",
		},
		{
			name: "worker-only topology",
			mutate: func(manifest *engineinfo.Manifest) {
				manifest.Participants[0].Role = engineinfo.RoleWorker
			},
			want: "exactly one entry",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := valid
			manifest.Builds = append([]engineinfo.ManifestBuild(nil), valid.Builds...)
			manifest.Participants = append([]engineinfo.ManifestParticipant(nil), valid.Participants...)
			test.mutate(&manifest)
			if err := manifest.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
}

func validCollectedManifest() engineinfo.Manifest {
	dirty := false
	return engineinfo.Manifest{
		SchemaVersion: constants.EngineInfoManifestSchemaVersion,
		RunID:         41,
		GeneratedAt:   "2026-08-26T13:21:42Z",
		Consistency: engineinfo.ManifestConsistency{
			Status: engineinfo.ConsistencyConsistent, Reason: "all participants match",
		},
		Builds: []engineinfo.ManifestBuild{{
			BuildID: "build-1", Product: "spt-engine", Version: "5.14.2",
			Revision: strings.Repeat("a", 40), BuildTime: "2026-08-26T12:00:00Z", SourceDirty: &dirty,
		}},
		Participants: []engineinfo.ManifestParticipant{{
			NodeID: "127.0.0.1:9999", Role: engineinfo.RoleStandalone,
			CollectionStatus: engineinfo.StatusCollected, ReportedSchemaVersion: 1, BuildID: "build-1",
		}},
	}
}

func TestWriteManifestAtomicValidationFailurePreservesPriorFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, constants.EngineInfoManifestName)
	if err := os.WriteFile(path, []byte("prior"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalid := engineinfo.Manifest{SchemaVersion: 99}
	if err := engineinfo.WriteManifestAtomic(root, invalid); err == nil {
		t.Fatal("WriteManifestAtomic() error = nil, want schema rejection")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "prior" {
		t.Fatalf("prior manifest changed after rejected write: %q", data)
	}
}

func TestWriteManifestAtomicPublishFailureCleansTemporaryFile(t *testing.T) {
	root := t.TempDir()
	publicPath := filepath.Join(root, constants.EngineInfoManifestName)
	if err := os.Mkdir(publicPath, 0o750); err != nil {
		t.Fatal(err)
	}

	if err := engineinfo.WriteManifestAtomic(root, validCollectedManifest()); err == nil {
		t.Fatal("WriteManifestAtomic() error = nil, want final rename failure")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != constants.EngineInfoManifestName || !entries[0].IsDir() {
		t.Fatalf("results root entries after failed publish = %+v, want only the preexisting destination directory", entries)
	}
	if _, err := engineinfo.LoadManifest(publicPath); err == nil {
		t.Fatal("LoadManifest() accepted an unpublished destination directory")
	}
}

func TestCanonicalIdentityIdentifiersHaveSingleProductionOwners(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() could not locate the engine information package")
	}
	packageDir := filepath.Dir(testFile)
	goSources, err := filepath.Glob(filepath.Join(packageDir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	buildReferenceOwners := 0
	manifestTemporaryPrefixLiterals := 0
	for _, sourcePath := range goSources {
		if strings.HasSuffix(sourcePath, "_test.go") {
			continue
		}
		source, readErr := os.ReadFile(sourcePath)
		if readErr != nil {
			t.Fatal(readErr)
		}
		buildReferenceOwners += strings.Count(string(source), `"build-`)
		manifestTemporaryPrefixLiterals += strings.Count(string(source), `.engine.info.json.tmp-*`)
	}
	if buildReferenceOwners != 1 {
		t.Fatalf("production build-reference wire-format owners = %d, want exactly 1", buildReferenceOwners)
	}
	if manifestTemporaryPrefixLiterals != 0 {
		t.Fatalf("production manifest temporary-prefix literals = %d, want 0 derived from the artifact name", manifestTemporaryPrefixLiterals)
	}

	publisherPath := filepath.Join(
		packageDir,
		"..", "..", "..", "engine", "core", "spt-base", "src", "main", "java", "com", "dell", "spt", "base",
		"buildinfo", "EngineBuildInfoPublisher.java",
	)
	publisherSource, err := os.ReadFile(publisherPath)
	if err != nil {
		t.Fatal(err)
	}
	if owners := strings.Count(string(publisherSource), `"engine.build.json"`); owners != 1 {
		t.Fatalf("production engine build-record filename owners = %d, want exactly 1", owners)
	}
	if prefixes := strings.Count(string(publisherSource), `.engine.build.`); prefixes != 0 {
		t.Fatalf("production engine build-record temporary-prefix literals = %d, want 0 derived from the artifact name", prefixes)
	}
}
