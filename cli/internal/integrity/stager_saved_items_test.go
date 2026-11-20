//go:build linux

package integrity

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeStagerSource(t *testing.T, content string) string {
	t.Helper()
	source := filepath.Join(t.TempDir(), "input.csv")
	if err := os.WriteFile(source, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return source
}

func stageDeleteForTest(t *testing.T, content string, maxRecords int, bucket string) (StagedInputManifest, []byte, error) {
	t.Helper()
	staged, err := StageDeleteInputManifest(writeStagerSource(t, content), 440, maxRecords, bucket)
	if err != nil {
		return staged, nil, err
	}
	t.Cleanup(func() { _ = os.RemoveAll(staged.StagingDir) })
	manifest, readErr := os.ReadFile(staged.ManifestPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	return staged, manifest, nil
}

// A saved items file stages to the same bytes and SHA-256 as the canonical manifest of the same
// objects with empty versions.
func TestStageDeleteOfSavedItemsMatchesTheEquivalentCanonicalManifest(t *testing.T) {
	const header = "bucket,key,size,version_id\n"
	tests := []struct {
		name      string
		saved     string
		canonical string
		buckets   []string
	}{
		{
			name:      "plain write",
			saved:     "/dq29-a/s0000009/9dpngcferyc9,cb00c2fdd62852c,4096,0/0\n/dq29-a/s0000001/0aaa,cb00c2fdd62852d,4096,0/0\n",
			canonical: "dq29-a,s0000009/9dpngcferyc9,4096,\ndq29-a,s0000001/0aaa,4096,\n",
			buckets:   []string{"dq29-a"},
		},
		{
			name:      "prefix and flat prefix",
			saved:     "/b/pre/s0000000/k1,2a0f,10,0/0\n/b/flat/k2,2a10,20,0/0\n",
			canonical: "b,pre/s0000000/k1,10,\nb,flat/k2,20,\n",
			buckets:   []string{"b"},
		},
		{
			name:      "several buckets",
			saved:     "/b2/s0/k,2a0f,10,0/0\n/b1/s0/k,2a10,10,0/0\n/b3/s0/k,2a11,10,0/0\n/b1/s0/j,2a12,10,0/0\n",
			canonical: "b2,s0/k,10,\nb1,s0/k,10,\nb3,s0/k,10,\nb1,s0/j,10,\n",
			buckets:   []string{"b2", "b1", "b3"},
		},
		{
			name:      "bucket with a path",
			saved:     "/b/sub/s0000000/k,2a0f,10,0/0\n",
			canonical: "b,sub/s0000000/k,10,\n",
			buckets:   []string{"b"},
		},
		{
			name:      "inner empty segment",
			saved:     "/b/a//b/k,2a0f,10,0/0\n",
			canonical: "b,a//b/k,10,\n",
			buckets:   []string{"b"},
		},
		{
			name:      "CRLF rows",
			saved:     "/b/s0/k1,2a0f,10,0/0\r\n/b/s0/k2,2a10,10,0/0\r\n",
			canonical: "b,s0/k1,10,\nb,s0/k2,10,\n",
			buckets:   []string{"b"},
		},
		{
			name:      "no final line end",
			saved:     "/b/s0/k1,2a0f,10,0/0\n/b/s0/k2,2a10,10,0/0",
			canonical: "b,s0/k1,10,\nb,s0/k2,10,\n",
			buckets:   []string{"b"},
		},
		{
			name:      "negative offset and non-zero layer/mask",
			saved:     "/b/s0/k1,-ff,10,0/0\n/b/s0/k2,2a10,10,1/0f\n",
			canonical: "b,s0/k1,10,\nb,s0/k2,10,\n",
			buckets:   []string{"b"},
		},
		{
			name:      "key the canonical writer quotes",
			saved:     "/b/#k ,2a0f,10,0/0\n",
			canonical: "b,\"#k \",10,\n",
			buckets:   []string{"b"},
		},
		{
			// A write and its update keep the name and size and change the mask only.
			name:      "a write and its update collapse",
			saved:     "/b/s0/k,2a0f,10,0/0\n/b/s0/k,2a0f,10,1/1\n",
			canonical: "b,s0/k,10,\nb,s0/k,10,\n",
			buckets:   []string{"b"},
		},
		{
			// A valid engine row and a headerless canonical row can't be told apart: this is read
			// as bucket b, key k, size 10.
			name:      "headerless file of one valid engine row",
			saved:     "/b/k,ff,10,0/0\n",
			canonical: "b,k,10,\n",
			buckets:   []string{"b"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			saved, savedBytes, err := stageDeleteForTest(t, test.saved, 0, "")
			if err != nil {
				t.Fatalf("saved items file refused: %v", err)
			}
			canonical, canonicalBytes, err := stageDeleteForTest(t, header+test.canonical, 0, "")
			if err != nil {
				t.Fatalf("canonical manifest refused: %v", err)
			}
			if string(savedBytes) != string(canonicalBytes) {
				t.Fatalf("staged bytes = %q, want the canonical manifest's %q", savedBytes, canonicalBytes)
			}
			if saved.Completion != canonical.Completion {
				t.Fatalf("completion = %+v, want the canonical manifest's %+v", saved.Completion, canonical.Completion)
			}
			if saved.MultipleBuckets != canonical.MultipleBuckets {
				t.Fatalf("multiple buckets = %v, want %v", saved.MultipleBuckets, canonical.MultipleBuckets)
			}
			rows := strings.Count(strings.TrimSuffix(test.saved, "\n"), "\n") + 1
			if saved.Format != ItemsFileFormatSavedItems || saved.ConvertedRows != rows ||
				!reflect.DeepEqual(saved.Buckets, test.buckets) || saved.BucketCount != len(test.buckets) || saved.MoreBuckets {
				t.Fatalf("saved items summary = %+v, want %d rows and buckets %v", saved, rows, test.buckets)
			}
			if canonical.Format != ItemsFileFormatCanonical || canonical.ConvertedRows != 0 || canonical.Buckets != nil {
				t.Fatalf("canonical summary = %+v, want the canonical format only", canonical)
			}
		})
	}
}

func TestStageDeleteRefusesSavedItemsFilesWithAnUnsafeLine(t *testing.T) {
	const good = "/b/s0/good1,2a0f,10,0/0\n"
	tests := []struct {
		name   string
		rows   string
		line   int
		reason string
	}{
		{"canonical file without its header", "b,qq7,10,\n", 1, `has an object name that does not start with "/" (not an S3 object path)`},
		{"byte order mark", "\ufeff/b/qq7,2a0f,10,0/0\n", 1, `has an object name that does not start with "/" (not an S3 object path)`},
		{"relative name", good + "qq7/k,2a0f,10,0/0\n", 2, `has an object name that does not start with "/" (not an S3 object path)`},
		{"empty bucket", good + "//qq7,2a0f,10,0/0\n", 2, "has an empty bucket"},
		{"bucket only", good + "/qq7,2a0f,10,0/0\n", 2, "has no object key after the bucket"},
		{"bucket and slash only", good + "/qq7/,2a0f,10,0/0\n", 2, "has no object key after the bucket"},
		{"key starting with a slash", good + "/b//qq7,2a0f,10,0/0\n", 2, `has an object key that starts with "/"`},
		{"version suffix", good + "/b/s0/qq7~v1,2a0f,10,0/0\n", 2,
			`has a "~" in the last segment of its key, as a versioned write names a version (key~versionId); ` +
				"a saved items file deletes current keys only, and exact versions need a canonical manifest"},
		{"comma in the key", good + "/b/a,qq7/k,2a0f,10,0/0\n", 2, "has more than 4 fields: a comma in the object key cannot be told from a field separator"},
		{"quote in the name", good + `/b/qq7"x,2a0f,10,0/0` + "\n", 2, "has a quote in the object name"},
		{"invalid UTF-8", good + "/b/qq7\xff,2a0f,10,0/0\n", 2, "is not valid UTF-8"},
		{"blank middle line", good + "\n/b/qq7,2a0f,10,0/0\n", 2, "is blank"},
		{"blank last line", good + "\n", 2, "is blank"},
		{"three fields", good + "/b/qq7,2a0f,10\n", 2, "has 3 fields, want 4"},
		{"bad hex offset", good + "/b/qq7,zz,10,0/0\n", 2, "has an offset that is not a hexadecimal 64-bit integer"},
		{"negative size", good + "/b/qq7,2a0f,-1,0/0\n", 2, "has a size that is not a nonnegative decimal integer"},
		{"bad layer/mask", good + "/b/qq7,2a0f,10,0-0\n", 2, "has a layer/mask that is not <hex layer>/<hex mask>"},
		{"bare CR inside a row", good + "/b/qq7\rx,2a0f,10,0/0\n", 2, "holds a CR that does not end the line"},
		{"bare CR line end", good + "/b/qq7,2a0f,10,0/0\r", 2, "holds a CR that does not end the line"},
		{"canonical rows after engine rows", good + "b,qq7,10,\n", 2, `has an object name that does not start with "/" (not an S3 object path)`},
		{"canonical header after engine rows", good + "bucket,key,size,version_id\nb,qq7,10,\n", 2, `has an object name that does not start with "/" (not an S3 object path)`},
		{"refused line after many good ones", strings.Repeat(good, 5000) + "/b/qq7,2a0f,10,0/0,x\n", 5001,
			"has more than 4 fields: a comma in the object key cannot be told from a field separator"},
		{"line over 1 MiB", good + "/b/" + strings.Repeat("q", maxCanonicalCSVRecordBytes) + ",2a0f,10,0/0\n", 2,
			fmt.Sprintf("exceeds %d bytes", maxCanonicalCSVRecordBytes)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			_, err := StageDeleteInputManifest(writeStagerSource(t, test.rows), 441, 0, "")
			want := savedItemsLineMessage(test.line, test.reason)
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
			if strings.Contains(err.Error(), "qq7") {
				t.Fatalf("error %q holds the refused row", err)
			}
			requireEmptyDir(t, tempDir)
		})
	}
}

// The canonical reader refuses these first records, so DELETE keeps today's refusal for them.
func TestStageDeleteRefusesASavedItemsFileWhoseFirstRecordCannotBeRead(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"empty file", ""},
		{"quote inside the first name", `/b/k"1,2a0f,10,0/0` + "\n/b/k2,2a10,10,0/0\n"},
		{"unterminated quote", `"/b/k,2a0f,10,0/0`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			_, err := StageDeleteInputManifest(writeStagerSource(t, test.content), 442, 0, "")
			if err == nil {
				t.Fatal("error = nil, want the file refused")
			}
			_, _, _, readVerifyErr := StageInputManifest(writeStagerSource(t, test.content), 442)
			if readVerifyErr == nil || readVerifyErr.Error() != err.Error() {
				t.Fatalf("DELETE error = %v, want read-verify's %v", err, readVerifyErr)
			}
			requireEmptyDir(t, tempDir)
		})
	}
}

func TestStageInputManifestStillRefusesASavedItemsFile(t *testing.T) {
	_, _, _, err := StageInputManifest(writeStagerSource(t, "/b/s0/k,2a0f,10,0/0\n"), 443)
	want := `items file must use exact header ["bucket" "key" "size" "version_id"]: ` + savedItemRowMessage("1")
	if err == nil || err.Error() != want {
		t.Fatalf("StageInputManifest() error = %v, want %q", err, want)
	}
}

func TestStageDeleteOfSavedItemsAppliesTheBucketAssertionToEveryRow(t *testing.T) {
	content := "/asserted/s0/k1,2a0f,10,0/0\n/asserted/s0/k2,2a10,10,0/0\n/other/s0/k3,2a11,10,0/0\n"
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	_, err := StageDeleteInputManifest(writeStagerSource(t, content), 444, 1, "asserted")
	want := `items file line 3 bucket "other" does not match --bucket "asserted"`
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
	requireEmptyDir(t, tempDir)

	staged, manifest, err := stageDeleteForTest(t, content[:strings.LastIndex(content, "/other")], 0, "asserted")
	if err != nil {
		t.Fatalf("matching bucket refused: %v", err)
	}
	if staged.MultipleBuckets || string(manifest) != "bucket,key,size,version_id\nasserted,s0/k1,10,\nasserted,s0/k2,10,\n" {
		t.Fatalf("staged = %+v, manifest %q", staged, manifest)
	}
}

func TestStageDeleteOfSavedItemsCapsAfterTheCanonicalSort(t *testing.T) {
	content := "/b/s0/z,2a0f,10,0/0\n/b/s0/m,2a10,20,0/0\n/b/s0/a,2a11,30,0/0\n/b/s0/z,2a0f,10,1/1\n"
	staged, manifest, err := stageDeleteForTest(t, content, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if string(manifest) != "bucket,key,size,version_id\nb,s0/a,30,\nb,s0/m,20,\n" {
		t.Fatalf("selected manifest = %q, want the first two in canonical order", manifest)
	}
	if staged.Completion.SourceRecordCount != 4 || staged.Completion.UniqueRecordCount != 3 ||
		staged.Completion.SelectedRecordCount != 2 || staged.ConvertedRows != 4 {
		t.Fatalf("staged = %+v, want source 4, unique 3, selected 2", staged)
	}
}

func TestStageDeleteOfSavedItemsMarksMultipleBuckets(t *testing.T) {
	staged, _, err := stageDeleteForTest(t, "/b1/s0/k,2a0f,10,0/0\n/b2/s0/k,2a10,10,0/0\n", 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if !staged.MultipleBuckets {
		t.Fatalf("staged = %+v, want multiple buckets", staged)
	}
}

func TestStageDeleteOfSavedItemsReportsABoundedBucketSummary(t *testing.T) {
	var content strings.Builder
	for bucket := range savedBucketsTracked + 5 {
		fmt.Fprintf(&content, "/b%05d/k,2a0f,10,0/0\n/b%05d/j,2a10,10,0/0\n", bucket, bucket)
	}
	staged, _, err := stageDeleteForTest(t, content.String(), 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if staged.ConvertedRows != 2*(savedBucketsTracked+5) ||
		!reflect.DeepEqual(staged.Buckets, []string{"b00000", "b00001", "b00002"}) ||
		staged.BucketCount != savedBucketsTracked || !staged.MoreBuckets {
		t.Fatalf("summary = rows %d, buckets %v, count %d, more %v",
			staged.ConvertedRows, staged.Buckets, staged.BucketCount, staged.MoreBuckets)
	}
}

func TestStageKeepsAQuotedCanonicalHeaderOnEveryRoute(t *testing.T) {
	content := `"bucket","key","size","version_id"` + "\r\nb,k2,10,\r\nb,k1,20,v\r\n"
	stageThroughEveryRoute(t, content, func(t *testing.T, _, manifest string, err error) {
		if err != nil {
			t.Fatalf("error = %v, want the file accepted", err)
		}
		staged, readErr := os.ReadFile(manifest)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if want := "bucket,key,size,version_id\nb,k1,20,v\nb,k2,10,\n"; string(staged) != want {
			t.Fatalf("staged bytes = %q, want %q", staged, want)
		}
	})
}
