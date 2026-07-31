package integrity

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStageInputManifestCanonicalizesAndCommits(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input.csv")
	content := "bucket,key,size,version_id\r\n" +
		"b,z,3,\r\n" +
		"b,\"comma,key\",4,v1\r\n" +
		"b,z,3,\r\n" +
		"b,\"line\nkey\",5,\r\n"
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
	if len(records) != 4 {
		t.Fatalf("expected header plus 3 unique rows, got %d", len(records))
	}
	if records[1][1] != "comma,key" || records[2][1] != "line\nkey" || records[3][1] != "z" {
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
	if got.RunID != 123 || got.ProducerID != CLIStagerProducerID || got.SourceRecordCount != 4 || got.SelectedRecordCount != 3 {
		t.Fatalf("unexpected completion: %+v", got)
	}
}

func TestStageInputManifestRejectsLegacyAndConflictingDuplicates(t *testing.T) {
	cases := map[string]string{
		"legacy":   "key,0,1,0/0\n",
		"conflict": "bucket,key,size,version_id\nb,k,1,v\nb,k,2,v\n",
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
