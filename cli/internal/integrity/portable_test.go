package integrity

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

func TestResolveStepRolesPortablePreservesOrderedFirstMatchAcrossEvidenceSources(t *testing.T) {
	tests := []struct {
		name       string
		configured []string
		manifest   []string
		want       StepRoles
	}{
		{
			name: "runtime write roles precede generated and manifest roles",
			configured: []string{
				"mt-001-runtime-create", "mt-002-runtime-verify",
				"mt-001-expected-create", "mt-002-expected-verify",
			},
			manifest: []string{"mt-001-manifest-create", "mt-002-manifest-verify"},
			want:     StepRoles{Create: "mt-001-runtime-create", Read: "mt-002-runtime-verify"},
		},
		{
			name:       "expected-only write roles",
			configured: []string{"mt-001-expected-create", "mt-002-expected-verify"},
			want:       StepRoles{Create: "mt-001-expected-create", Read: "mt-002-expected-verify"},
		},
		{
			name:       "runtime discovery roles precede manifest roles",
			configured: []string{"mt-001-runtime-list", "mt-002-runtime-verify"},
			manifest:   []string{"mt-001-manifest-list", "mt-002-manifest-verify"},
			want:       StepRoles{List: "mt-001-runtime-list", Read: "mt-002-runtime-verify"},
		},
		{
			name:       "staged read has only verify role",
			configured: []string{"mt-001-runtime-verify"},
			want:       StepRoles{Read: "mt-001-runtime-verify"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := &results.Manifest{}
			for _, stepID := range test.manifest {
				manifest.Steps = append(manifest.Steps, results.StepManifest{StepID: stepID})
			}
			if got := ResolveStepRoles(test.configured, manifest); got != test.want {
				t.Fatalf("ResolveStepRoles() = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestResolveStepRolesPortableDoesNotGuessMalformedPlanPositions(t *testing.T) {
	if got := ResolveStepRoles([]string{"step-one", "step-two"}, nil); got != (StepRoles{}) {
		t.Fatalf("ResolveStepRoles(malformed plan) = %+v, want no guessed roles", got)
	}
}

func TestManifestRecordParsingPortablePreservesKeysAndRejectsInvalidSize(t *testing.T) {
	record, err := parseManifestRecord([]string{"bucket", "line\nkey,雪", "17", "v1"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if record.bucket != "bucket" || record.key != "line\nkey,雪" || record.size != 17 || record.version != "v1" {
		t.Fatalf("parsed manifest record = %+v", record)
	}
	for _, fields := range [][]string{
		{"bucket", "key", "-1", ""},
		{"bucket", "key", "not-a-size", ""},
		{"bucket", "key", "1"},
	} {
		if _, err := parseManifestRecord(fields, 3); err == nil {
			t.Fatalf("parseManifestRecord(%v) error = nil", fields)
		}
	}
}

func TestValidateCanonicalManifestPortableAcceptsQuotedNewline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.csv")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	for _, row := range [][]string{canonicalHeader, {"bucket", "line\nkey,雪", "3", ""}} {
		if err = writer.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if evidence, err := validateCanonicalManifestEvidence(path); err != nil || evidence.count != 1 {
		t.Fatalf("validateCanonicalManifestEvidence() = (%+v, %v), want one record", evidence, err)
	}
}

func TestValidateCanonicalManifestPortableRequiresStrictUniqueOrder(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "out of order",
			body: "bucket,key,size,version_id\nb,z,1,\nb,a,1,\n",
			want: "not strictly ordered",
		},
		{
			name: "duplicate identity",
			body: "bucket,key,size,version_id\nb,a,1,\nb,a,1,\n",
			want: "duplicate identity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.csv")
			if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := validateCanonicalManifestEvidence(path); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateCanonicalManifestPortableRejectsMalformedUTF8(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.csv")
	body := append([]byte("bucket,key,size,version_id\nb,"), 0xff)
	body = append(body, []byte(",1,\n")...)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateCanonicalManifestEvidence(path); err == nil || !strings.Contains(err.Error(), "valid UTF-8") {
		t.Fatalf("validation error = %v, want invalid UTF-8", err)
	}
}

func TestReadOperationMetricsPortableRejectsMissingAndNegativeCounts(t *testing.T) {
	root := t.TempDir()
	step := "mt-001-verify"
	path := filepath.Join(root, step+".metrics.total.csv")
	if err := os.WriteFile(path, []byte("OpType,CountSucc,CountFail,CountCorrupt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readOperationMetrics(root, step, "READ"); err == nil || !strings.Contains(err.Error(), "no data rows") {
		t.Fatalf("header-only metrics error = %v", err)
	}
	if err := os.WriteFile(path, []byte("OpType,CountSucc,CountFail,CountCorrupt\nREAD,-1,0,0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readOperationMetrics(root, step, "READ"); err == nil || !strings.Contains(err.Error(), "negative value") {
		t.Fatalf("negative metrics error = %v", err)
	}
}

func TestValidateCompletionPortableAcceptsOneIdentityBoundRecordAndRejectsTrailingJSON(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(root, WrittenName)
	manifestData := []byte("bucket,key,size,version_id\nb,key,3,v1\n")
	if err := os.WriteFile(manifestPath, manifestData, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(manifestData)
	marker := Completion{
		Version: 1, Status: "complete", RunID: 17,
		ProducerKind: constants.IntegrityProvenanceEngineStep,
		ProducerID:   "mt-001-create", Artifact: WrittenName,
		SourceRecordCount: 1, UniqueRecordCount: 1, SelectedRecordCount: 1,
		ManifestBytes: int64(len(manifestData)), ManifestSHA256: hex.EncodeToString(digest[:]),
	}
	markerData, err := json.Marshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	completionPath := filepath.Join(root, WrittenCompletionName)
	if err = os.WriteFile(completionPath, append(markerData, '\n', ' ', '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	validated, err := ValidateCompletion(
		manifestPath, completionPath, 17,
		constants.IntegrityProvenanceEngineStep, "mt-001-create", WrittenName,
	)
	if err != nil {
		t.Fatal(err)
	}
	if validated.SelectedRecordCount != 1 {
		t.Fatalf("selected record count = %d, want 1", validated.SelectedRecordCount)
	}

	if err = os.WriteFile(completionPath, append(markerData, []byte("\n{}\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateCompletion(
		manifestPath, completionPath, 17,
		constants.IntegrityProvenanceEngineStep, "mt-001-create", WrittenName,
	); err == nil || !strings.Contains(err.Error(), "multiple JSON values") {
		t.Fatalf("trailing JSON validation error = %v", err)
	}
}

func TestSortManifestBoundedPortableSortsAndDeduplicates(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.csv")
	input := "bucket,key,size,version_id\nb,z,3,v2\nb,a,1,\nb,z,3,v2\n"
	if err := os.WriteFile(inputPath, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	sortedPath, count, err := sortManifestBounded(inputPath, root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(sortedPath) }()
	file, err := os.Open(sortedPath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{canonicalHeader, {"b", "a", "1", ""}, {"b", "z", "3", "v2"}}
	if count != 2 || !reflect.DeepEqual(records, want) {
		t.Fatalf("sort result count=%d records=%#v, want count=2 records=%#v", count, records, want)
	}
}
