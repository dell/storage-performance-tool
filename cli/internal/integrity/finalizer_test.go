package integrity

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestFinalizeWriteVerifyPromotesValidatesAndDerivesRemaining(t *testing.T) {
	root := t.TempDir()
	createStep := "mt-001-test-create"
	readStep := "mt-002-test-verify"
	writeResultsIndex(t, root, createStep, readStep)

	written := [][]string{{"bucket", "key", "size", "version_id"}, {"b", "z", "3", "v2"}, {"b", "a", "1", ""}}
	verified := [][]string{{"bucket", "key", "size", "version_id"}, {"b", "a", "1", ""}}
	writeCommittedFixture(t, root, createStep, WrittenName, 101, createStep, written)
	writeCommittedFixture(t, root, readStep, VerifiedName, 101, readStep, verified)
	writeCSVFixture(t, filepath.Join(root, readStep+"."+IntegrityFailuresName), [][]string{failureHeader, {
		"now", "n0", readStep, "s3", "z", "v2", "v2", "req-1", "digest_mismatch", "sha256", "expected", "actual", "3", "3", "", "1",
	}})
	writeCSVFixture(t, filepath.Join(root, createStep+"."+IntegrityPerformanceName), append([][]string{performanceHeader}, []string{"n0", createStep, "s3", "write_prehash", "sha256", "2", "4194304", "0.1", "40", "0.2", "0"}))
	writeCSVFixture(t, filepath.Join(root, readStep+"."+IntegrityPerformanceName), append([][]string{performanceHeader}, []string{"n0", readStep, "s3", "read_verify", "sha256", "1", "1048576", "0.1", "10", "", "0"}))
	writeCSVFixture(t, filepath.Join(root, createStep+".metrics.total.csv"), [][]string{{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"CREATE", "2", "0", "0"}})
	writeCSVFixture(t, filepath.Join(root, readStep+".metrics.total.csv"), [][]string{{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"READ", "1", "1", "1"}})

	outcome, err := FinalizeResults(FinalizeOptions{ResultsRoot: root, Workload: workload.WriteVerify, RunID: 101, StepIDs: []string{createStep, readStep}, MaxConsoleFailures: 1})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.SelectionCount != 2 || outcome.VerificationAttemptedCount != 2 || outcome.VerifiedCount != 1 || outcome.RemainingCount != 1 || outcome.CorruptCount != 1 || len(outcome.FailureSamples) != 1 {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if outcome.DigestPerformance.Objects != 3 || outcome.DigestPerformance.Bytes != 5*1024*1024 ||
		outcome.DigestPerformance.HashWorkerSeconds != 0.2 || outcome.DigestPerformance.MeanWorkerHashMiBPerSecond != 25 ||
		outcome.DigestPerformance.InitialWriteDelaySecondsMaxNode == nil || *outcome.DigestPerformance.InitialWriteDelaySecondsMaxNode != 0.2 ||
		len(outcome.DigestPerformance.Phases) != 2 {
		t.Fatalf("unexpected digest performance summary: %+v", outcome.DigestPerformance)
	}
	remainingPath := filepath.Join(root, VerifyRemainingName)
	file, err := os.Open(remainingPath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[1][1] != "z" || records[1][3] != "v2" {
		t.Fatalf("unexpected remaining records: %#v", records)
	}
	resumeDir, resumeManifest, resumeCompletion, err := StageInputManifest(remainingPath, 102)
	if err != nil {
		t.Fatalf("verify-remaining.csv is not resumable: %v", err)
	}
	defer func() { _ = os.RemoveAll(resumeDir) }()
	resumeMarker, err := ValidateCompletion(
		resumeManifest, resumeCompletion, 102,
		constants.IntegrityProvenanceCLIStager, CLIStagerProducerID, VerifyInputName,
	)
	if err != nil {
		t.Fatalf("resumed manifest completion is invalid: %v", err)
	}
	if resumeMarker.SelectedRecordCount != 1 {
		t.Fatalf("resumed selection count = %d, want 1", resumeMarker.SelectedRecordCount)
	}
	indexData, err := os.ReadFile(filepath.Join(root, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var index results.Manifest
	if err = json.Unmarshal(indexData, &index); err != nil {
		t.Fatal(err)
	}
	if index.Integrity == nil || index.Integrity.SelectionCount != 2 ||
		index.Integrity.DigestPerformance.Bytes != 5*1024*1024 {
		t.Fatalf("index.json integrity summary = %+v", index.Integrity)
	}
	for _, required := range []string{WrittenName, WrittenCompletionName, VerifiedName, VerifiedCompletionName, VerifyRemainingName, IntegrityFailuresName, IntegrityPerformanceName} {
		found := false
		for _, file := range index.RunFiles {
			found = found || file.Name == required
		}
		if !found {
			t.Errorf("run file %s was not registered", required)
		}
	}
	if _, err = os.Stat(filepath.Join(root, createStep+"."+WrittenName)); err != nil {
		t.Fatalf("step-prefixed evidence was not preserved: %v", err)
	}
}

func TestFinalizeNotStartedReadRecordsIncompleteLifecycleWithoutMissingArtifactNoise(t *testing.T) {
	root := t.TempDir()
	readStep := "mt-001-verify"
	writeResultsIndex(t, root, readStep)
	outcome, err := FinalizeResults(FinalizeOptions{
		ResultsRoot: root, Workload: workload.ReadVerify, RunID: 201, StepIDs: []string{readStep},
		StepLifecycles: map[string]string{readStep: "not_started"},
	})
	if err != nil {
		t.Fatalf("not-started READ should preserve the tracker cause without artifact noise: %v", err)
	}
	if outcome.Complete {
		t.Fatal("not-started READ must not be represented as complete")
	}
	manifest, err := readResultsManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Integrity == nil || manifest.Integrity.Complete {
		t.Fatalf("index missing incomplete integrity lifecycle: %+v", manifest.Integrity)
	}
}

func TestReadOperationMetricsReportsSemanticErrorsWithoutNilWrapFormatting(t *testing.T) {
	root := t.TempDir()
	step := "verify"
	writeCSVFixture(t, filepath.Join(root, step+".metrics.total.csv"), [][]string{{"OpType", "CountSucc", "CountFail", "CountCorrupt"}})
	_, err := readOperationMetrics(root, step, "READ")
	if err == nil || strings.Contains(err.Error(), "%!w") || !strings.Contains(err.Error(), "no data rows") {
		t.Fatalf("header-only error = %v", err)
	}
	writeCSVFixture(t, filepath.Join(root, step+".metrics.total.csv"), [][]string{
		{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"READ", "-1", "0", "0"},
	})
	_, err = readOperationMetrics(root, step, "READ")
	if err == nil || strings.Contains(err.Error(), "%!w") || !strings.Contains(err.Error(), "negative value") {
		t.Fatalf("negative-count error = %v", err)
	}
}

func TestFinalizeReadVerifyStagesEmptySelectionOnlyWhenExplicitlyAllowed(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input.csv")
	writeCSVFixture(t, source, [][]string{canonicalHeader})
	staging, stagedManifest, stagedCompletion, err := StageInputManifest(source, 202)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(staging) }()

	root := t.TempDir()
	readStep := "mt-001-test-verify"
	writeResultsIndex(t, root, readStep)
	writeCommittedFixture(t, root, readStep, VerifiedName, 202, readStep, [][]string{canonicalHeader})
	writeCSVFixture(t, filepath.Join(root, readStep+"."+IntegrityFailuresName), [][]string{{
		"timestamp", "node", "step", "driver", "key", "requested_version_id", "returned_version_id", "request_id", "reason", "algorithm", "expected_digest", "actual_digest", "expected_size", "actual_size", "first_mismatch_offset", "attempt",
	}})
	writeCSVFixture(t, filepath.Join(root, readStep+"."+IntegrityPerformanceName), [][]string{performanceHeader})
	writeCSVFixture(t, filepath.Join(root, readStep+".metrics.total.csv"), [][]string{{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"READ", "0", "0", "0"}})

	outcome, err := FinalizeResults(FinalizeOptions{
		ResultsRoot: root, Workload: workload.ReadVerify, RunID: 202, StepIDs: []string{readStep},
		StagedManifest: stagedManifest, StagedCompletion: stagedCompletion, AllowEmptySelection: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.EmptySelection || !outcome.EmptyAllowed || outcome.SelectionCount != 0 || outcome.VerificationAttemptedCount != 0 {
		t.Fatalf("unexpected empty outcome: %+v", outcome)
	}
}

func TestFinalizeRejectsVerifiedIdentityOutsideSelection(t *testing.T) {
	root := t.TempDir()
	createStep, readStep := "mt-001-create", "mt-002-verify"
	writeResultsIndex(t, root, createStep, readStep)
	writeCommittedFixture(t, root, createStep, WrittenName, 303, createStep, [][]string{canonicalHeader, {"b", "a", "1", ""}})
	writeCommittedFixture(t, root, readStep, VerifiedName, 303, readStep, [][]string{canonicalHeader, {"b", "other", "1", ""}})
	writeCSVFixture(t, filepath.Join(root, readStep+"."+IntegrityFailuresName), [][]string{failureHeader})
	writeCSVFixture(t, filepath.Join(root, createStep+"."+IntegrityPerformanceName), [][]string{
		performanceHeader,
		{"n0", createStep, "s3", "write_prehash", "sha256", "1", "1", "0.1", "0.0000095367431640625", "0.1", "0"},
	})
	writeCSVFixture(t, filepath.Join(root, readStep+"."+IntegrityPerformanceName), [][]string{
		performanceHeader,
		{"n0", readStep, "s3", "read_verify", "sha256", "1", "1", "0.1", "0.0000095367431640625", "", "0"},
	})
	writeCSVFixture(t, filepath.Join(root, createStep+".metrics.total.csv"), [][]string{
		{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"CREATE", "1", "0", "0"},
	})
	writeCSVFixture(t, filepath.Join(root, readStep+".metrics.total.csv"), [][]string{
		{"OpType", "CountSucc", "CountFail", "CountCorrupt"}, {"READ", "1", "0", "0"},
	})

	_, err := FinalizeResults(FinalizeOptions{
		ResultsRoot: root, Workload: workload.WriteVerify, RunID: 303, StepIDs: []string{createStep, readStep},
	})
	if err == nil || !strings.Contains(err.Error(), "is not in the selected input") {
		t.Fatalf("error = %v, want outside-selection rejection", err)
	}
	manifest, readErr := readResultsManifest(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if manifest.Integrity == nil || manifest.Integrity.Complete || manifest.Integrity.FinalizationError == "" {
		t.Fatalf("partial integrity outcome was not registered: %+v", manifest.Integrity)
	}
	if len(manifest.RunFiles) == 0 {
		t.Fatal("promoted partial run files were not registered")
	}
}

func TestFinalizeRejectsIncompleteVerificationCoverage(t *testing.T) {
	root := t.TempDir()
	createStep, readStep := "mt-001-create", "mt-002-verify"
	writeResultsIndex(t, root, createStep, readStep)
	writeCommittedFixture(t, root, createStep, WrittenName, 404, createStep, [][]string{
		canonicalHeader,
		{"b", "a", "1", ""},
		{"b", "b", "1", ""},
	})
	writeCommittedFixture(t, root, readStep, VerifiedName, 404, readStep, [][]string{
		canonicalHeader,
		{"b", "a", "1", ""},
	})
	writeCSVFixture(t, filepath.Join(root, readStep+"."+IntegrityFailuresName), [][]string{failureHeader})
	writeCSVFixture(t, filepath.Join(root, createStep+"."+IntegrityPerformanceName), [][]string{
		performanceHeader,
		{"n0", createStep, "s3", "write_prehash", "sha256", "2", "2", "0.1", "0.000019073486328125", "0.1", "0"},
	})
	writeCSVFixture(t, filepath.Join(root, readStep+"."+IntegrityPerformanceName), [][]string{
		performanceHeader,
		{"n0", readStep, "s3", "read_verify", "sha256", "1", "1", "0.1", "0.0000095367431640625", "", "0"},
	})
	writeCSVFixture(t, filepath.Join(root, createStep+".metrics.total.csv"), [][]string{
		{"OpType", "CountSucc", "CountFail", "CountCorrupt"},
		{"CREATE", "2", "0", "0"},
	})
	writeCSVFixture(t, filepath.Join(root, readStep+".metrics.total.csv"), [][]string{
		{"OpType", "CountSucc", "CountFail", "CountCorrupt"},
		{"READ", "1", "0", "0"},
	})

	outcome, err := FinalizeResults(FinalizeOptions{
		ResultsRoot: root,
		Workload:    workload.WriteVerify,
		RunID:       404,
		StepIDs:     []string{createStep, readStep},
	})
	if err == nil {
		t.Fatalf("expected incomplete coverage to fail, outcome=%+v", outcome)
	}
	if outcome.SelectionCount != 2 || outcome.VerificationAttemptedCount != 1 ||
		outcome.VerifiedCount != 1 || outcome.RemainingCount != 1 {
		t.Fatalf("failure outcome lost reconciliation evidence: %+v", outcome)
	}
}

func TestFinalizeRejectsNonCanonicalVerifiedCompletionCounts(t *testing.T) {
	root := t.TempDir()
	createStep, readStep := "mt-001-create", "mt-002-verify"
	writeResultsIndex(t, root, createStep, readStep)
	writeCommittedFixture(t, root, createStep, WrittenName, 505, createStep, [][]string{
		canonicalHeader,
		{"b", "a", "1", ""},
	})
	writeCommittedFixture(t, root, readStep, VerifiedName, 505, readStep, [][]string{
		canonicalHeader,
		{"b", "a", "1", ""},
	})
	completionPath := filepath.Join(root, readStep+"."+VerifiedCompletionName)
	data, err := os.ReadFile(completionPath)
	if err != nil {
		t.Fatal(err)
	}
	var marker Completion
	if err = json.Unmarshal(data, &marker); err != nil {
		t.Fatal(err)
	}
	marker.SourceRecordCount = 2
	data, err = json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(completionPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	writeCSVFixture(t, filepath.Join(root, createStep+".metrics.total.csv"), [][]string{
		{"OpType", "CountSucc", "CountFail", "CountCorrupt"},
		{"CREATE", "1", "0", "0"},
	})
	writeCSVFixture(t, filepath.Join(root, readStep+".metrics.total.csv"), [][]string{
		{"OpType", "CountSucc", "CountFail", "CountCorrupt"},
		{"READ", "1", "0", "0"},
	})

	_, err = FinalizeResults(FinalizeOptions{
		ResultsRoot: root,
		Workload:    workload.WriteVerify,
		RunID:       505,
		StepIDs:     []string{createStep, readStep},
	})
	if err == nil || !strings.Contains(err.Error(), "verified.csv completion counts must match for READ output") {
		t.Fatalf("error = %v, want READ-output completion-count rejection", err)
	}
}

func TestValidateJSONCorruptCountRequiresFieldAndMatchesCSV(t *testing.T) {
	missing := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/fleet/json" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if missing {
			_, _ = w.Write([]byte(`[{"step_id":"read-step","op_type":"READ","operations":{}}]`))
			return
		}
		_, _ = w.Write([]byte(`[{"step_id":"read-step","op_type":"READ","operations":{"corrupt_count":2}}]`))
	}))
	defer server.Close()

	if err := validateJSONCorruptCount(server.URL, "read-step", 2); err != nil {
		t.Fatal(err)
	}
	missing = true
	if err := validateJSONCorruptCount(server.URL, "read-step", 2); err == nil {
		t.Fatal("expected missing operations.corrupt_count to fail")
	}
}

func TestFinalizeResultsHonorsCanceledContextBeforeArtifactWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	root := t.TempDir()
	_, err := FinalizeResults(FinalizeOptions{
		Context: ctx, ResultsRoot: root, Workload: workload.WriteVerify, RunID: 1,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("FinalizeResults() error = %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "index.json")); !os.IsNotExist(statErr) {
		t.Fatalf("canceled finalization unexpectedly created index.json: %v", statErr)
	}
}

func TestSubtractSortedManifestsReportsVerifiedParseError(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.csv")
	verifiedPath := filepath.Join(root, "verified.csv")
	destination := filepath.Join(root, VerifyRemainingName)
	writeCSVFixture(t, inputPath, [][]string{canonicalHeader, {"b", "a", "1", ""}})
	if err := os.WriteFile(
		verifiedPath,
		[]byte("bucket,key,size,version_id\r\nb,\"unterminated"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	_, err := subtractSortedManifests(context.Background(), inputPath, verifiedPath, destination)
	if err == nil || !strings.Contains(err.Error(), "parse verified manifest") {
		t.Fatalf("subtractSortedManifests() error = %v, want verified parse diagnostic", err)
	}
	if _, statErr := os.Stat(destination); !os.IsNotExist(statErr) {
		t.Fatalf("malformed verified input published remaining evidence: %v", statErr)
	}
}

func TestPromoteCompletionPairReusesMatchingPair(t *testing.T) {
	sourceManifest, sourceMarker := committedPairFixture(
		t, 601, "read-step", [][]string{canonicalHeader, {"b", "a", "1", ""}},
	)
	destinationRoot := t.TempDir()
	destinationManifest := filepath.Join(destinationRoot, VerifiedName)
	destinationMarker := filepath.Join(destinationRoot, VerifiedCompletionName)

	promote := func() error {
		return promoteCompletionPair(
			sourceManifest, sourceMarker,
			destinationManifest, destinationMarker,
			601, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
		)
	}
	if err := promote(); err != nil {
		t.Fatal(err)
	}
	beforeManifest, err := os.ReadFile(destinationManifest)
	if err != nil {
		t.Fatal(err)
	}
	beforeMarker, err := os.ReadFile(destinationMarker)
	if err != nil {
		t.Fatal(err)
	}
	if err = promote(); err != nil {
		t.Fatalf("matching canonical pair was not recoverable: %v", err)
	}
	afterManifest, _ := os.ReadFile(destinationManifest)
	afterMarker, _ := os.ReadFile(destinationMarker)
	if string(afterManifest) != string(beforeManifest) || string(afterMarker) != string(beforeMarker) {
		t.Fatal("matching canonical pair was replaced")
	}
}

func TestPromoteCompletionPairRejectsConflictingExistingPair(t *testing.T) {
	firstManifest, firstMarker := committedPairFixture(
		t, 602, "read-step", [][]string{canonicalHeader, {"b", "first", "1", ""}},
	)
	secondManifest, secondMarker := committedPairFixture(
		t, 602, "read-step", [][]string{canonicalHeader, {"b", "second", "2", ""}},
	)
	destinationRoot := t.TempDir()
	destinationManifest := filepath.Join(destinationRoot, VerifiedName)
	destinationMarker := filepath.Join(destinationRoot, VerifiedCompletionName)
	if err := promoteCompletionPair(
		firstManifest, firstMarker,
		destinationManifest, destinationMarker,
		602, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
	); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(destinationManifest)
	if err != nil {
		t.Fatal(err)
	}
	err = promoteCompletionPair(
		secondManifest, secondMarker,
		destinationManifest, destinationMarker,
		602, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
	)
	if err == nil || !strings.Contains(err.Error(), "does not match source") {
		t.Fatalf("conflicting pair error = %v", err)
	}
	after, readErr := os.ReadFile(destinationManifest)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(after) != string(original) {
		t.Fatal("conflicting publication replaced the canonical manifest")
	}
}

func TestPromoteCompletionPairRecoversMatchingManifestOnlyAndIgnoresStagingMarker(t *testing.T) {
	sourceManifest, sourceMarker := committedPairFixture(
		t, 603, "read-step", [][]string{canonicalHeader, {"b", "a", "1", ""}},
	)
	destinationRoot := t.TempDir()
	destinationManifest := filepath.Join(destinationRoot, VerifiedName)
	destinationMarker := filepath.Join(destinationRoot, VerifiedCompletionName)
	copyFixtureFile(t, sourceManifest, destinationManifest)
	stagingMarker := destinationMarker + ".staging"
	if err := os.WriteFile(stagingMarker, []byte("interrupted"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := promoteCompletionPair(
		sourceManifest, sourceMarker,
		destinationManifest, destinationMarker,
		603, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateCompletion(
		destinationManifest, destinationMarker,
		603, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
	); err != nil {
		t.Fatalf("recovered pair is invalid: %v", err)
	}
	if _, err := os.Stat(stagingMarker); err != nil {
		t.Fatalf("uncommitted marker staging evidence was not preserved: %v", err)
	}
}

func TestPromoteCompletionPairRejectsMismatchedManifestOnlyAndMarkerOnly(t *testing.T) {
	sourceManifest, sourceMarker := committedPairFixture(
		t, 604, "read-step", [][]string{canonicalHeader, {"b", "expected", "1", ""}},
	)
	otherManifest, _ := committedPairFixture(
		t, 604, "read-step", [][]string{canonicalHeader, {"b", "stale", "2", ""}},
	)
	t.Run("mismatched manifest only", func(t *testing.T) {
		destinationRoot := t.TempDir()
		destinationManifest := filepath.Join(destinationRoot, VerifiedName)
		destinationMarker := filepath.Join(destinationRoot, VerifiedCompletionName)
		copyFixtureFile(t, otherManifest, destinationManifest)
		err := promoteCompletionPair(
			sourceManifest, sourceMarker,
			destinationManifest, destinationMarker,
			604, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
		)
		if err == nil || !strings.Contains(err.Error(), "conflicts with source completion") {
			t.Fatalf("mismatched manifest-only error = %v", err)
		}
		if _, statErr := os.Stat(destinationMarker); !os.IsNotExist(statErr) {
			t.Fatalf("marker was published beside conflicting manifest: %v", statErr)
		}
	})
	t.Run("marker only", func(t *testing.T) {
		destinationRoot := t.TempDir()
		destinationManifest := filepath.Join(destinationRoot, VerifiedName)
		destinationMarker := filepath.Join(destinationRoot, VerifiedCompletionName)
		copyFixtureFile(t, sourceMarker, destinationMarker)
		err := promoteCompletionPair(
			sourceManifest, sourceMarker,
			destinationManifest, destinationMarker,
			604, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
		)
		if err == nil || !strings.Contains(err.Error(), "exists without manifest") {
			t.Fatalf("marker-only error = %v", err)
		}
		if _, statErr := os.Stat(destinationManifest); !os.IsNotExist(statErr) {
			t.Fatalf("manifest was published beside an orphan marker: %v", statErr)
		}
	})
}

func TestPromoteCompletionPairConcurrentPublishNeverOverwritesWinner(t *testing.T) {
	firstManifest, firstMarker := committedPairFixture(
		t, 605, "read-step", [][]string{canonicalHeader, {"b", "first", "1", ""}},
	)
	secondManifest, secondMarker := committedPairFixture(
		t, 605, "read-step", [][]string{canonicalHeader, {"b", "second", "2", ""}},
	)
	destinationRoot := t.TempDir()
	destinationManifest := filepath.Join(destinationRoot, VerifiedName)
	destinationMarker := filepath.Join(destinationRoot, VerifiedCompletionName)
	type sourcePair struct {
		manifest string
		marker   string
	}
	sources := []sourcePair{
		{manifest: firstManifest, marker: firstMarker},
		{manifest: secondManifest, marker: secondMarker},
	}
	start := make(chan struct{})
	errs := make(chan error, len(sources))
	var workers sync.WaitGroup
	for _, source := range sources {
		workers.Add(1)
		go func(source sourcePair) {
			defer workers.Done()
			<-start
			errs <- promoteCompletionPair(
				source.manifest, source.marker,
				destinationManifest, destinationMarker,
				605, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
			)
		}(source)
	}
	close(start)
	workers.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent publication successes = %d, want exactly one", successes)
	}
	if _, err := ValidateCompletion(
		destinationManifest, destinationMarker,
		605, constants.IntegrityProvenanceEngineStep, "read-step", VerifiedName,
	); err != nil {
		t.Fatalf("winning canonical pair is invalid: %v", err)
	}
}

func committedPairFixture(
	t *testing.T,
	runID int64,
	producer string,
	records [][]string,
) (string, string) {
	t.Helper()
	root := t.TempDir()
	const sourcePrefix = "source"
	writeCommittedFixture(t, root, sourcePrefix, VerifiedName, runID, producer, records)
	return filepath.Join(root, sourcePrefix+"."+VerifiedName),
		filepath.Join(root, sourcePrefix+"."+VerifiedCompletionName)
}

func copyFixtureFile(t *testing.T, source, destination string) {
	t.Helper()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(destination, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeResultsIndex(t *testing.T, root string, steps ...string) {
	t.Helper()
	manifest := results.Manifest{}
	for _, step := range steps {
		manifest.Steps = append(manifest.Steps, results.StepManifest{StepID: step})
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "index.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeCommittedFixture(t *testing.T, root, step, artifact string, runID int64, producer string, records [][]string) {
	t.Helper()
	manifestPath := filepath.Join(root, step+"."+artifact)
	writeCSVFixture(t, manifestPath, records)
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	marker := Completion{
		Version: 1, Status: "complete", RunID: runID, ProducerKind: "engine_step", ProducerID: producer,
		Artifact: artifact, SourceRecordCount: len(records) - 1, UniqueRecordCount: len(records) - 1,
		SelectedRecordCount: len(records) - 1, ManifestBytes: int64(len(data)), ManifestSHA256: hex.EncodeToString(digest[:]),
	}
	markerData, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	completionName := stringsTrimSuffix(artifact, ".csv") + ".complete.json"
	if err = os.WriteFile(filepath.Join(root, step+"."+completionName), markerData, 0o600); err != nil {
		t.Fatal(err)
	}
}

func stringsTrimSuffix(value, suffix string) string {
	return value[:len(value)-len(suffix)]
}

func writeCSVFixture(t *testing.T, path string, records [][]string) {
	t.Helper()
	file, err := os.Create(path) // #nosec G304 -- private test directory
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	writer.UseCRLF = true
	if err = writer.WriteAll(records); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}
