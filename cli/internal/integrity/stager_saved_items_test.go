//go:build linux

package integrity

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
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
		{"version suffix", good + "/b/s0/qq7~v1,2a0f,10,0/0\n", 2, tildeReason},
		// The examples of review 1: a version ID holding "/" puts the "~" before the last segment.
		{"version ID with a slash on line 1", "/b/s0/qq7~3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY,ff,10,0/0\n" + good, 1, tildeReason},
		{"version ID with a slash on a later line", good + good + "/b/s0/qq7~3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY,ff,10,0/0\n", 3, tildeReason},
		// A Netty write can store r%41 as rA, so the row may name another object.
		{"percent escape on line 1", "/b/r%41/qq7,2a0f,10,0/0\n" + good, 1, percentReason},
		{"percent escape on a later line", good + good + "/b/r%41/qq7,2a0f,10,0/0\n", 3, percentReason},
		{"plus on line 1", "/b/a+qq7/k,2a0f,10,0/0\n" + good, 1, plusReason},
		{"plus on a later line", good + "/b/a+qq7/k,2a0f,10,0/0\n", 2, plusReason},
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
			fmt.Sprintf("exceeds %d bytes without its line end", maxCanonicalCSVRecordBytes)},
		// The canonical reader refuses these first lines: DELETE reads them as saved rows, so
		// line 1 gets its own reason, not the header's.
		{"quote inside the first name", `/b/qq7"1,2a0f,10,0/0` + "\n" + good, 1, "has a quote in the object name"},
		{"unterminated quote on line 1", `"/b/qq7,2a0f,10,0/0`, 1, "has a quote in the object name"},
		{"first line over 1 MiB", "/b/" + strings.Repeat("q", maxCanonicalCSVRecordBytes) + ",2a0f,10,0/0\n" + good, 1,
			fmt.Sprintf("exceeds %d bytes without its line end", maxCanonicalCSVRecordBytes)},
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

// An empty file has no line to read as a saved row, so DELETE keeps read-verify's refusal.
func TestStageDeleteRefusesAnEmptyItemsFileAsReadVerifyDoes(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	_, err := StageDeleteInputManifest(writeStagerSource(t, ""), 442, 0, "")
	if err == nil {
		t.Fatal("error = nil, want the file refused")
	}
	_, _, _, readVerifyErr := StageInputManifest(writeStagerSource(t, ""), 442)
	if readVerifyErr == nil || readVerifyErr.Error() != err.Error() {
		t.Fatalf("DELETE error = %v, want read-verify's %v", err, readVerifyErr)
	}
	requireEmptyDir(t, tempDir)
}

// read-verify keeps today's refusal of a first line the canonical reader refuses.
func TestStageInputManifestKeepsItsRefusalOfAnUnreadableFirstLine(t *testing.T) {
	const headerMessage = `items file must use exact header ["bucket" "key" "size" "version_id"]`
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"quote inside the first name", `/b/k"1,2a0f,10,0/0` + "\n/b/k2,2a10,10,0/0\n",
			headerMessage + ": " + savedItemRowMessage("1")},
		{"unterminated quote", `"/b/k,2a0f,10,0/0`, "read canonical header: unterminated quoted field"},
		{"first line over 1 MiB", "/b/" + strings.Repeat("q", maxCanonicalCSVRecordBytes) + ",2a0f,10,0/0\n",
			headerMessage + ": " + savedItemRowMessage("1")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := StageInputManifest(writeStagerSource(t, test.content), 442)
			if err == nil || err.Error() != test.want {
				t.Fatalf("StageInputManifest() error = %v, want %q", err, test.want)
			}
		})
	}
}

// A saved line has one payload limit, 1 MiB without its line end, on every line and with every
// line end.
func TestStageDeleteOfSavedItemsHasOneLineLimit(t *testing.T) {
	const good = "/b/s0/good,2a0f,10,0/0"
	// row returns a saved row of exactly length bytes.
	row := func(length int) string {
		const fixed = "/b/" + ",2a0f,10,0/0"
		return "/b/" + strings.Repeat("q", length-len(fixed)) + ",2a0f,10,0/0"
	}
	tooLong := fmt.Sprintf("exceeds %d bytes without its line end", maxCanonicalCSVRecordBytes)
	for _, ending := range []struct{ name, value string }{{"LF", "\n"}, {"CRLF", "\r\n"}, {"no line end", ""}} {
		for _, length := range []int{maxCanonicalCSVRecordBytes, maxCanonicalCSVRecordBytes + 1} {
			for _, line := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/%d bytes/line %d", ending.name, length, line), func(t *testing.T) {
					content := row(length) + ending.value
					if line == 2 {
						separator := ending.value
						if separator == "" {
							separator = "\n"
						}
						content = good + separator + content
					}
					tempDir := t.TempDir()
					t.Setenv("TMPDIR", tempDir)
					staged, err := StageDeleteInputManifest(writeStagerSource(t, content), 446, 0, "")
					if length == maxCanonicalCSVRecordBytes {
						if err != nil {
							t.Fatalf("error = %v, want the line accepted", err)
						}
						_ = os.RemoveAll(staged.StagingDir)
						if staged.ConvertedRows != line || staged.Completion.UniqueRecordCount != line {
							t.Fatalf("staged = %+v, want %d rows", staged, line)
						}
						return
					}
					if want := savedItemsLineMessage(line, tooLong); err == nil || err.Error() != want {
						t.Fatalf("error = %v, want %q", err, want)
					}
					requireEmptyDir(t, tempDir)
				})
			}
		}
	}
}

// "%" and "~" are refused only in saved rows: a canonical manifest names the stored key and
// version itself, and stages as before on every route.
func TestStageKeepsCanonicalManifestsWithPercentAndTilde(t *testing.T) {
	const header = "bucket,key,size,version_id\n"
	rows := "b,a+b/k,10,\nb,r%41/k,10,\nb,s0/k~3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY,10,\nb,a~x/k,10,v~1\nb,50%,10,\n"
	want := header + "b,50%,10,\nb,a+b/k,10,\nb,a~x/k,10,v~1\nb,r%41/k,10,\nb,s0/k~3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY,10,\n"
	stageThroughEveryRoute(t, header+rows, func(t *testing.T, _, manifest string, err error) {
		if err != nil {
			t.Fatalf("error = %v, want the manifest accepted", err)
		}
		staged, readErr := os.ReadFile(manifest)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(staged) != want {
			t.Fatalf("staged bytes = %q, want %q", staged, want)
		}
	})
}

// A saved file that gives one object two sizes is refused with both line numbers, not the object.
func TestStageDeleteOfSavedItemsNamesTheLinesOfConflictingSizes(t *testing.T) {
	content := "/b/s0/qq7,2a0f,10,0/0\n/b/s0/other,2a10,10,0/0\n/b/s0/qq7,2a0f,10,1/1\n/b/s0/qq7,2a0f,20,0/0\n"
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	_, err := StageDeleteInputManifest(writeStagerSource(t, content), 447, 0, "")
	want := "items file lines 1 and 4 give the same object two sizes"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
	requireEmptyDir(t, tempDir)

	// A NUL in a bucket and in another row's key gives two objects the same display string: the
	// lines are found by the sort's own comparison, so the lines of the real conflict are named.
	content = "/b\x00x/k,2a0f,10,0/0\n/b/x\x00k,2a0f,20,0/0\n/b\x00x/k,2a0f,30,0/0\n"
	_, err = StageDeleteInputManifest(writeStagerSource(t, content), 447, 0, "")
	want = "items file lines 1 and 3 give the same object two sizes"
	if err == nil || err.Error() != want {
		t.Fatalf("NUL error = %v, want %q", err, want)
	}
	requireEmptyDir(t, tempDir)

	// A canonical manifest keeps its message.
	_, err = StageDeleteInputManifest(writeStagerSource(t,
		"bucket,key,size,version_id\nb,s0/qq7,10,\nb,s0/qq7,20,\n"), 447, 0, "")
	if err == nil || !strings.Contains(err.Error(), `canonicalize staged input: manifest identity "b\x00s0/qq7\x00" has conflicting sizes`) {
		t.Fatalf("canonical error = %v, want the identity message", err)
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
	want := `items file line 3 has a bucket (the first segment of its object name) that does not match --bucket "asserted"`
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

// A first line the canonical reader refuses, but that the engine reads as the canonical header, is
// not read as a saved row: a saved row after it gets the refusal a header file gets, on a regular
// file and through a named pipe.
func TestStageDeleteRefusesSavedRowsUnderAHeaderOnlyTheEngineReads(t *testing.T) {
	const savedRow = "/b/s0/name,2a0f,1024,0/0\n"
	for _, test := range []struct{ name, content string }{
		{"space after a quoted header field", `"bucket" ,key,size,version_id` + "\n" + savedRow},
		{"quoted header field and a saved row on bare-CR line ends", `"bucket" ,key,size,version_id` + "\r" + savedRow},
	} {
		t.Run(test.name+"/regular file", func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			_, err := StageDeleteInputManifest(writeStagerSource(t, test.content), 448, 0, "")
			if want := deleteSavedItemRowMessage("2"); err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
			requireEmptyDir(t, tempDir)
		})
		t.Run(test.name+"/named pipe", func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			source := filepath.Join(t.TempDir(), "input.csv")
			if err := syscall.Mkfifo(source, 0o600); err != nil {
				t.Skipf("mkfifo: %v", err)
			}
			writer, err := os.OpenFile(source, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = writer.Close() })
			if _, err := writer.WriteString(test.content); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, stageErr := StageDeleteInputManifest(source, 448, 0, "")
				done <- stageErr
			}()
			select {
			case err := <-done:
				if want := deleteSavedItemRowMessage("2"); err == nil || err.Error() != want {
					t.Fatalf("error = %v, want %q", err, want)
				}
			case <-time.After(10 * time.Second):
				_ = writer.Close()
				t.Fatal("a refusal waited for a writer that stays open")
			}
			requireEmptyDir(t, tempDir)
		})
	}
}

// A first line over 1 MiB on a stream whose writer stays open is refused once its first 1 MiB and
// one more byte are read, without waiting for its line end.
func TestStageDeleteRefusesALongFirstLineWithoutWaitingForItsEnd(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	source := filepath.Join(t.TempDir(), "input.csv")
	if err := syscall.Mkfifo(source, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	writer, err := os.OpenFile(source, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	// The pipe holds less than the line: write it while the stager reads.
	go func() { _, _ = writer.WriteString("/b/" + strings.Repeat("q", maxCanonicalCSVRecordBytes+10)) }()
	done := make(chan error, 1)
	go func() {
		_, stageErr := StageDeleteInputManifest(source, 449, 0, "")
		done <- stageErr
	}()
	select {
	case err := <-done:
		want := savedItemsLineMessage(1, fmt.Sprintf("exceeds %d bytes without its line end", maxCanonicalCSVRecordBytes))
		if err == nil || err.Error() != want {
			t.Fatalf("error = %v, want %q", err, want)
		}
	case <-time.After(10 * time.Second):
		_ = writer.Close()
		t.Fatal("a refusal waited for a writer that stays open")
	}
	requireEmptyDir(t, tempDir)
}
