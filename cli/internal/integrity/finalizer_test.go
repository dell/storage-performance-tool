package integrity

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

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
	file, err := os.Open(filepath.Join(root, VerifyRemainingName))
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
	if _, err := FinalizeResults(FinalizeOptions{ResultsRoot: root, Workload: workload.WriteVerify, RunID: 303, StepIDs: []string{createStep, readStep}}); err == nil {
		t.Fatal("expected set validation failure")
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

	if _, err = FinalizeResults(FinalizeOptions{
		ResultsRoot: root,
		Workload:    workload.WriteVerify,
		RunID:       505,
		StepIDs:     []string{createStep, readStep},
	}); err == nil {
		t.Fatal("expected noncanonical verified completion counts to fail")
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
