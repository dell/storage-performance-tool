//go:build linux

package integrity

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// stageThroughEveryRoute runs content through the DELETE and read-verify stagers, with TMPDIR
// private to the subtest so that check can see what each leaves behind.
func stageThroughEveryRoute(
	t *testing.T, content string, check func(t *testing.T, tempDir, manifest string, err error),
) {
	t.Helper()
	routes := map[string]func(source string) (dir, manifest string, err error){
		"delete": func(source string) (string, string, error) {
			staged, err := StageDeleteInputManifest(source, 324, 0, "")
			return staged.StagingDir, staged.ManifestPath, err
		},
		"read-verify": func(source string) (string, string, error) {
			dir, manifest, _, err := StageInputManifest(source, 324)
			return dir, manifest, err
		},
	}
	for name, stage := range routes {
		t.Run(name, func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			source := filepath.Join(t.TempDir(), "input.csv")
			if err := os.WriteFile(source, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, manifest, err := stage(source)
			check(t, tempDir, manifest, err)
		})
	}
}

func TestStageRefusesSavedEngineItemRowsUnderCanonicalHeader(t *testing.T) {
	const header = "bucket,key,size,version_id\n"
	const path = "/dd2/s0000000/4ahd2fq98vuo"
	tests := []struct {
		name   string
		rows   string
		record string
	}{
		{"layer and mask zero", path + ",2a0f5ed8eab22f7e,1024,0/0\n", "2"},
		{"layer above zero", path + ",2a0f5ed8eab22f7e,1024,1/0\n", "2"},
		{"with a mask", path + ",2a0f5ed8eab22f7e,1024,0/0f\n", "2"},
		{"large layer and mask", path + ",2a0f5ed8eab22f7e,1024,ff/0102030405060708\n", "2"},
		{"negative offset", path + ",-7f0f5ed8eab22f7e,1024,0/0\n", "2"},
		{"offset zero", path + ",0,1024,0/0\n", "2"},
		{"name with a version", path + "~66984584-e910-4588-9d70-2fa1fbce3908,2a0f,1024,0/0\n", "2"},
		{"name without a leading slash", "s0000000/4ahd2fq98vuo,2a0f,1024,0/0\n", "2"},
		{"flat name", "4ahd2fq98vuo,2a0f,1024,0/0\n", "2"},
		{"upper-case digits", path + ",2A0F,1024,F/0A\n", "2"},
		{"empty mask", path + ",2a0f,1024,0/\n", "2"},
		{"several engine rows", path + "1,2a0f,1024,0/0\n" + path + "2,2a10,1024,0/0\n", "2"},
		{"canonical rows then an engine row", "b,s0/a,1024,\nb,s1/b,1024,\n" + path + ",2a0f,1024,0/0\n", "4"},
		{"an engine row then canonical rows", path + ",2a0f,1024,0/0\nb,s0/a,1024,\n", "2"},
		{"accepted false positive: plain bucket", "b,2a0f,1024,0/0\n", "2"},
		{"accepted false positive: hex key and hex/hex version", "b,abcd,16,ab/cd\n", "2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stageThroughEveryRoute(t, header+test.rows, func(t *testing.T, tempDir, _ string, err error) {
				if err == nil || err.Error() != savedItemRowMessage(test.record) {
					t.Fatalf("error = %v, want %q", err, savedItemRowMessage(test.record))
				}
				requireEmptyDir(t, tempDir)
			})
		})
	}
}

func TestStageDeleteChecksSavedItemRowsBeforeBucketAssertion(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input.csv")
	if err := os.WriteFile(source, []byte(
		"bucket,key,size,version_id\n/b/s0/name,2a0f,1024,0/0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := StageDeleteInputManifest(source, 325, 0, "asserted")
	if err == nil || err.Error() != savedItemRowMessage("2") {
		t.Fatalf("StageDeleteInputManifest() error = %v, want %q", err, savedItemRowMessage("2"))
	}
}

func TestStageHeaderRefusalExplainsSavedItemsFile(t *testing.T) {
	const headerMessage = `items file must use exact header ["bucket" "key" "size" "version_id"]`
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "saved items file",
			content: "/b/s0/name,2a0f,1024,0/0\n/b/s0/other,2a10,1024,0/0\n",
			want:    headerMessage + ": " + savedItemRowMessage("1"),
		},
		{name: "canonical rows", content: "b,k,1024,\n", want: headerMessage},
		{name: "canonical row with hex key", content: "b,2a0f,1024,\n", want: headerMessage},
		{name: "plain item list", content: "obj-1\n", want: headerMessage},
		{name: "invalid size", content: "/b/s0/name,2a0f,big,0/0\n", want: headerMessage},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stageThroughEveryRoute(t, test.content, func(t *testing.T, tempDir, _ string, err error) {
				if err == nil || err.Error() != test.want {
					t.Fatalf("error = %v, want %q", err, test.want)
				}
				requireEmptyDir(t, tempDir)
			})
		})
	}
}

func TestStageKeepsCanonicalRowsThatResembleSavedItems(t *testing.T) {
	tests := []struct {
		name string
		row  string
	}{
		{"plain bucket", "b,k,1024,"},
		{"bucket with a trailing slash", "b/,k,1024,"},
		{"bucket with a path", "b/sub,k,1024,"},
		{"bucket with a leading slash", "/b,k,1024,"},
		{"hex key, empty version", "b,2a0f5ed8eab22f7e,1024,"},
		{"hex key, MinIO version", "b,deadbeef,1024,66984584-e910-4588-9d70-2fa1fbce3908"},
		{"hex key, SeaweedFS version", "b,deadbeef,1024,6716d849a893dc5559d8890a16e47536"},
		{"hex key, null version", "b,deadbeef,1024,null"},
		{"null version", "b,k,1024,null"},
		{"MinIO version", "b,k,1024,66984584-e910-4588-9d70-2fa1fbce3908"},
		{"SeaweedFS version", "b,k,1024,6716d849a893dc5559d8890a16e47536"},
		{"LocalStack version", "b,k,1024,AaHu54M4EdA0yylw6terbA3iAG4qC2oY"},
		{"LocalStack version with dot and underscore", "b,k,1024,Aa.Hu_54M4EdA0yylw6terbA3iAG4qC2o"},
		{"VersityGW version", "b,k,1024,01M7QAQDDZ0RM492SH55DZPR5X"},
		{"CloudServer version", "b,k,1024,3938323034393934343036373234393939393939756e646566696e31"},
		{"AWS example version", "b,k,1024,3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"},
		{"AWS-style version with a slash", "b,deadbeef,1024,3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo"},
		{"hex/hex version with a non-hex key", "b,s0000000/abc,1024,0/0"},
		{"hex/hex version with an ordinary key", "b,k0,1024,ab/cd"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content := "bucket,key,size,version_id\n" + test.row + "\n"
			stageThroughEveryRoute(t, content, func(t *testing.T, _, manifest string, err error) {
				if err != nil {
					t.Fatalf("error = %v, want the row accepted", err)
				}
				staged, readErr := os.ReadFile(manifest)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if string(staged) != content {
					t.Fatalf("staged bytes = %q, want %q", staged, content)
				}
			})
		})
	}
}

func requireEmptyDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("refused staging left %d entries in %s, first %q", len(entries), dir, entries[0].Name())
	}
}

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

func TestStageDeleteInputManifestCapsCanonicalSelectionAndRetainsEvidence(t *testing.T) {
	source := filepath.Join(t.TempDir(), "delete-input.csv")
	content := "bucket,key,size,version_id\r\n" +
		"b,z,3,version-z\r\n" +
		"b,\"comma,key\",4,version-comma\r\n" +
		"b,z,3,version-z\r\n" +
		"b,alpha,5,\r\n"
	if err := os.WriteFile(source, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	staged, err := StageDeleteInputManifest(source, 321, 2, "b")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(staged.StagingDir) }()

	file, err := os.Open(staged.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	records, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"bucket", "key", "size", "version_id"},
		{"b", "alpha", "5", ""},
		{"b", "comma,key", "4", "version-comma"},
	}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("selected records = %#v, want %#v", records, want)
	}
	if staged.Completion.SourceRecordCount != 4 ||
		staged.Completion.UniqueRecordCount != 3 ||
		staged.Completion.SelectedRecordCount != 2 {
		t.Fatalf("unexpected completion counts: %+v", staged.Completion)
	}
	if staged.MultipleBuckets {
		t.Fatal("single-bucket selection reported multiple buckets")
	}
	if _, err = ValidateCompletion(
		staged.ManifestPath,
		staged.CompletionPath,
		321,
		constants.IntegrityProvenanceCLIStager,
		CLIStagerProducerID,
		VerifyInputName,
	); err != nil {
		t.Fatalf("delete completion evidence rejected: %v", err)
	}
}

func TestStageDeleteInputManifestRejectsUnsafeSelections(t *testing.T) {
	tests := []struct {
		name           string
		content        string
		expectedBucket string
		wantDetail     string
	}{
		{
			name:       "empty",
			content:    "bucket,key,size,version_id\n",
			wantDetail: "must contain at least one object identity",
		},
		{
			name: "bucket assertion checks every row before capping",
			content: "bucket,key,size,version_id\n" +
				"allowed,a,1,\n" +
				"other,z,1,\n",
			expectedBucket: "allowed",
			wantDetail:     "does not match --bucket",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "delete-input.csv")
			if err := os.WriteFile(source, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := StageDeleteInputManifest(source, 322, 1, test.expectedBucket)
			if err == nil || !strings.Contains(err.Error(), test.wantDetail) {
				t.Fatalf("StageDeleteInputManifest() error = %v, want %q", err, test.wantDetail)
			}
		})
	}
}

func TestStageDeleteInputManifestDetectsMultipleBuckets(t *testing.T) {
	source := filepath.Join(t.TempDir(), "delete-input.csv")
	if err := os.WriteFile(source, []byte(
		"bucket,key,size,version_id\n"+
			"b-one,key-a,1,\n"+
			"b-two,key-b,1,version-b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	staged, err := StageDeleteInputManifest(source, 323, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(staged.StagingDir) }()
	if !staged.MultipleBuckets {
		t.Fatal("multi-bucket manifest was not detected")
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
		{name: "version", mutate: func(c *Completion) { c.Version = 3 }, wantDetail: "unsupported version/status"},
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
