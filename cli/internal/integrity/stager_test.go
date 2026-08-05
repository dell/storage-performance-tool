//go:build linux

package integrity

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestStageInputManifestCanonicalizesAndCommits(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input.csv")
	content := "bucket,key,size,version_id\r\n" +
		"b,z,3,\r\n" +
		"b,\"comma,key\",4,v1\r\n" +
		"b,z,3,\r\n" +
		"b,\"line\nkey\",5,\r\n" +
		"b,tilde~key,6,\r\n" +
		"b,雪,7,v雪\r\n"
	if err := os.WriteFile(source, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	dir, manifest, marker, err := StageInputManifest(source, 123)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	file, err := os.Open(manifest)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 6 {
		t.Fatalf("expected header plus 5 unique rows, got %d", len(records))
	}
	if records[1][1] != "comma,key" || records[2][1] != "line\nkey" ||
		records[3][1] != "tilde~key" || records[4][1] != "z" ||
		records[5][1] != "雪" || records[5][3] != "v雪" {
		t.Fatalf("records were not sorted/preserved: %#v", records)
	}

	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	var got Completion
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.RunID != 123 || got.ProducerID != CLIStagerProducerID || got.SourceRecordCount != 6 || got.SelectedRecordCount != 5 {
		t.Fatalf("unexpected completion: %+v", got)
	}
}

func TestStageInputManifestEmitsExactSharedLFBytes(t *testing.T) {
	fixture := filepath.Join(sharedCompletionFixtureDir(t, "nonempty"), VerifyInputName)
	dir, manifest, _, err := StageInputManifest(fixture, 124)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	expected, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(expected) {
		t.Fatalf("staged manifest bytes = %q, want shared fixture %q", actual, expected)
	}
}

func TestStageInputManifestPreservesEmbeddedCRLFIdentity(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input.csv")
	content := "bucket,key,size,version_id\r\n" +
		"b,plain,1,v1\r\n" +
		"b,\"line\r\nkey\",2,v2\r\n"
	if err := os.WriteFile(source, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	dir, manifest, _, err := StageInputManifest(source, 125)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if _, err = validateCanonicalManifestEvidence(manifest); err != nil {
		t.Fatalf("staged canonical manifest rejected: %v", err)
	}
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "b,\"line\r\nkey\",2,v2\n") {
		t.Fatalf("staged manifest did not preserve embedded CRLF: %q", data)
	}
	reader := newCanonicalCSVReader(strings.NewReader(string(data)))
	if _, err = reader.Read(); err != nil {
		t.Fatal(err)
	}
	first, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if first[1] != "line\r\nkey" {
		t.Fatalf("staged key = %q, want exact embedded CRLF", first[1])
	}
}

func TestStageInputManifestRejectsLegacyAndConflictingDuplicates(t *testing.T) {
	cases := map[string]string{
		"legacy":        "key,0,1,0/0\n",
		"conflict":      "bucket,key,size,version_id\nb,k,1,v\nb,k,2,v\n",
		"malformed csv": "bucket,key,size,version_id\nb,\"unterminated,1,v\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "input.csv")
			if err := os.WriteFile(source, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := StageInputManifest(source, 1); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestMergeSortedChunksRejectsConflictingSizesAcrossChunks(t *testing.T) {
	tempDir := t.TempDir()
	first := filepath.Join(tempDir, "first.csv")
	second := filepath.Join(tempDir, "second.csv")
	writeCSVFixture(t, first, [][]string{{"b", "same", "1", "v1"}})
	writeCSVFixture(t, second, [][]string{{"b", "same", "2", "v1"}})

	merged, _, err := mergeSortedChunksContext(context.Background(), tempDir, []string{first, second})
	if err == nil {
		_ = os.Remove(merged)
		t.Fatal("expected conflicting sizes across chunks to fail")
	}
	if merged != "" {
		t.Fatalf("failed merge published output %q", merged)
	}
	matches, globErr := filepath.Glob(filepath.Join(tempDir, ".integrity-sorted-*"))
	if globErr != nil {
		t.Fatal(globErr)
	}
	if len(matches) != 0 {
		t.Fatalf("failed merge leaked output files: %v", matches)
	}
}

func TestMergeSortedChunksUsesBoundedMultiPassFanIn(t *testing.T) {
	tempDir := t.TempDir()
	chunks := make([]string, 0, manifestMergeFanIn+1)
	for i := 0; i <= manifestMergeFanIn; i++ {
		path := filepath.Join(tempDir, fmt.Sprintf("chunk-%03d.csv", i))
		writeCSVFixture(t, path, [][]string{{"b", fmt.Sprintf("k-%03d", i), "1", ""}})
		chunks = append(chunks, path)
	}
	merged, count, err := mergeSortedChunksContext(context.Background(), tempDir, chunks)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(merged) }()
	if count != len(chunks) {
		t.Fatalf("merged count = %d, want %d", count, len(chunks))
	}
	file, err := os.Open(merged)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil || len(records) != len(chunks)+1 {
		t.Fatalf("merged records = %d, err = %v", len(records), err)
	}
	matches, err := filepath.Glob(filepath.Join(tempDir, ".integrity-sorted-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0] != merged {
		t.Fatalf("intermediate merge files were not cleaned: %v", matches)
	}
}

func TestValidateCompletionRejectsTrustBoundaryMismatches(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input.csv")
	if err := os.WriteFile(source, []byte("bucket,key,size,version_id\r\nb,k,1,v1\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir, manifest, markerPath, err := StageInputManifest(source, 77)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if _, err = ValidateCompletion(
		manifest, markerPath, 77, constants.IntegrityProvenanceCLIStager,
		CLIStagerProducerID, VerifyInputName); err != nil {
		t.Fatalf("valid completion rejected: %v", err)
	}
	data, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	var valid Completion
	if err = json.Unmarshal(data, &valid); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		mutate     func(*Completion)
		wantDetail string
	}{
		{name: "version", mutate: func(c *Completion) { c.Version++ }, wantDetail: "unsupported version/status"},
		{name: "status", mutate: func(c *Completion) { c.Status = "pending" }, wantDetail: "unsupported version/status"},
		{name: "run", mutate: func(c *Completion) { c.RunID++ }, wantDetail: "completion run_id"},
		{name: "producer kind", mutate: func(c *Completion) {
			c.ProducerKind = constants.IntegrityProvenanceEngineStep
		}, wantDetail: "completion producer"},
		{name: "producer id", mutate: func(c *Completion) { c.ProducerID = "stale" }, wantDetail: "completion producer"},
		{name: "artifact", mutate: func(c *Completion) { c.Artifact = WrittenName }, wantDetail: "completion artifact"},
		{name: "count order", mutate: func(c *Completion) {
			c.UniqueRecordCount = c.SourceRecordCount + 1
		}, wantDetail: "counts are inconsistent"},
		{name: "row count", mutate: func(c *Completion) {
			c.SourceRecordCount = 0
			c.UniqueRecordCount = 0
			c.SelectedRecordCount = 0
		}, wantDetail: "manifest has 1 records"},
		{name: "bytes", mutate: func(c *Completion) { c.ManifestBytes++ }, wantDetail: "length/digest"},
		{name: "digest", mutate: func(c *Completion) { c.ManifestSHA256 = "00" }, wantDetail: "length/digest"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			candidatePath := filepath.Join(t.TempDir(), "candidate.json")
			encoded, marshalErr := json.Marshal(candidate)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if writeErr := os.WriteFile(candidatePath, encoded, 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
			if _, validateErr := ValidateCompletion(
				manifest, candidatePath, 77, constants.IntegrityProvenanceCLIStager,
				CLIStagerProducerID, VerifyInputName,
			); validateErr == nil || !strings.Contains(validateErr.Error(), test.wantDetail) {
				t.Fatalf("validation error = %v, want detail %q", validateErr, test.wantDetail)
			}
		})
	}
	missing := filepath.Join(t.TempDir(), "missing.json")
	if _, err = ValidateCompletion(
		manifest, missing, 77, constants.IntegrityProvenanceCLIStager,
		CLIStagerProducerID, VerifyInputName); err == nil {
		t.Fatal("expected missing completion to fail")
	}
	if err = os.WriteFile(missing+".staging", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateCompletion(
		manifest, missing, 77, constants.IntegrityProvenanceCLIStager,
		CLIStagerProducerID, VerifyInputName); err == nil {
		t.Fatal("expected an unrenamed completion staging file to be ignored")
	}
	malformed := filepath.Join(t.TempDir(), "malformed.json")
	if err = os.WriteFile(malformed, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateCompletion(
		manifest, malformed, 77, constants.IntegrityProvenanceCLIStager,
		CLIStagerProducerID, VerifyInputName); err == nil {
		t.Fatal("expected malformed completion to fail")
	}
}

func TestValidateCompletionAcceptsSharedPublicV1Fixtures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
	}{
		{name: "nonempty", count: 1},
		{name: "empty", count: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := sharedCompletionFixtureDir(t, tc.name)
			markerPath := filepath.Join(dir, "verify-input.complete.json")
			markerBytes, err := os.ReadFile(markerPath)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(markerBytes), "emitted_record_count") {
				t.Fatal("public v1 fixture contains internal emission-count evidence")
			}
			marker, err := ValidateCompletion(
				filepath.Join(dir, "verify-input.csv"), markerPath, 1722369600000,
				constants.IntegrityProvenanceEngineStep,
				"mt-001-20260730.120000.000-list", VerifyInputName)
			if err != nil {
				t.Fatalf("shared %s fixture rejected: %v", tc.name, err)
			}
			if marker.SelectedRecordCount != tc.count {
				t.Fatalf("selected_record_count = %d, want %d", marker.SelectedRecordCount, tc.count)
			}
		})
	}
}

func TestValidateCompletionRequiresExactlyOneJSONDocument(t *testing.T) {
	fixtureRoot := filepath.Dir(sharedCompletionFixtureDir(t, "nonempty"))
	manifestPath := filepath.Join(fixtureRoot, "nonempty", VerifyInputName)
	for _, test := range []struct {
		name    string
		file    string
		wantErr bool
	}{
		{name: "trailing whitespace", file: "valid-whitespace.json"},
		{name: "trailing garbage", file: "trailing-garbage.json", wantErr: true},
		{name: "concatenated object", file: "concatenated-object.json", wantErr: true},
		{name: "truncated object", file: "truncated.json", wantErr: true},
		{name: "unknown field", file: "unknown-field.json", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ValidateCompletion(
				manifestPath,
				filepath.Join(fixtureRoot, "markers", test.file),
				1722369600000,
				constants.IntegrityProvenanceEngineStep,
				"mt-001-20260730.120000.000-list",
				VerifyInputName,
			)
			if (err != nil) != test.wantErr {
				t.Fatalf("ValidateCompletion() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func sharedCompletionFixtureDir(t *testing.T, variant string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		candidate := filepath.Join(dir, "testdata", "integrity", "completion-v1", variant)
		if info, statErr := os.Stat(candidate); statErr == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("shared completion fixture %q not found", variant)
		}
		dir = parent
	}
}
