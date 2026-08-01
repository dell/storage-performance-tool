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
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

func TestResolveStepRolesPortablePreservesOrderedFirstMatchAcrossEvidenceSources(t *testing.T) {
	tests := []struct {
		name       string
		workload   string
		configured []string
		manifest   []string
		want       StepRoles
	}{
		{
			name: "runtime write roles precede generated and manifest roles", workload: workload.WriteVerify,
			configured: []string{
				"mt-001-runtime-create", "mt-002-runtime-verify",
				"mt-001-expected-create", "mt-002-expected-verify",
			},
			manifest: []string{"mt-001-manifest-create", "mt-002-manifest-verify"},
			want:     StepRoles{Create: "mt-001-runtime-create", Read: "mt-002-runtime-verify"},
		},
		{
			name: "expected-only write roles", workload: workload.WriteVerify,
			configured: []string{"mt-001-expected-create", "mt-002-expected-verify"},
			want:       StepRoles{Create: "mt-001-expected-create", Read: "mt-002-expected-verify"},
		},
		{
			name: "runtime discovery roles precede manifest roles", workload: workload.ReadVerify,
			configured: []string{"mt-001-runtime-list", "mt-002-runtime-verify"},
			manifest:   []string{"mt-001-manifest-list", "mt-002-manifest-verify"},
			want:       StepRoles{List: "mt-001-runtime-list", Read: "mt-002-runtime-verify"},
		},
		{
			name: "staged read has only verify role", workload: workload.ReadVerify,
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
			if got := ResolveStepRoles(test.workload, test.configured, manifest); got != test.want {
				t.Fatalf("ResolveStepRoles() = %+v, want %+v", got, test.want)
			}
		})
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
