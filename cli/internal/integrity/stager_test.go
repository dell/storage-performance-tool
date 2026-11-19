//go:build linux

package integrity

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

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
		{"quote inside the object path", `/sample/pre"fix/s0000000/item,2a0f,1024,0/0` + "\n", "2"},
		{"canonical row then a quote inside an object path", "b,k,1024,\n" + `/b/pre"fix/item,2a0f,1024,0/0` + "\n", "3"},
		{"bare-CR line ends", path + ",2a0f,1024,0/0\r" + path + "2,2a10,1024,0/0\r", "2"},
		{"bare-CR line ends after canonical rows", "b,k,1024,\rb,k2,1024,\r" + path + ",2a0f,1024,0/0\r", "4"},
		{"accepted false positive: hex key and hex/hex version", "b,abcd,16,ab/cd\n", "2"},
		{"refused row, then a saved row past the read buffer", `b,dir/k"1,1024,` + "\n" +
			strings.Repeat("b,k,1024,\n", 1000) + path + ",2a0f,1024,0/0\n", "1003"},
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
		{
			name:    "bare-CR saved items file",
			content: "/b/s0/name,2a0f,1024,0/0\r/b/s0/other,2a10,1024,0/0\r",
			want:    headerMessage + ": " + savedItemRowMessage("1"),
		},
		{
			name:    "header and rows on bare-CR line ends",
			content: "bucket,key,size,version_id\r/b/s0/name,2a0f,1024,0/0\r",
			want:    savedItemRowMessage("2"),
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

func TestStageKeepsTheCanonicalReadersErrorWhenNoSavedRowIsFound(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "quote inside a canonical key",
			content: "bucket,key,size,version_id\n" + `b,dir/k"1,1024,` + "\n",
			want:    "stage canonical input: parse canonical record 2: unexpected quote in unquoted field",
		},
		{
			name:    "bare-CR canonical rows",
			content: "bucket,key,size,version_id\rb,k,1024,\r",
			want:    `items file must use exact header ["bucket" "key" "size" "version_id"]`,
		},
		{
			name:    "five fields",
			content: "bucket,key,size,version_id\n/b/s0/name,2a0f,1024,0/0,x\n",
			want:    "stage canonical input: canonical record 2 has 5 fields, want 4",
		},
		{
			// The engine refuses the file at record 2, so the saved row after it is not judged.
			name:    "five fields before a saved row",
			content: "bucket,key,size,version_id\n/b/s0/name,2a0f,1024,0/0,x\n/b/s0/other,2a10,1024,0/0\n",
			want:    "stage canonical input: canonical record 2 has 5 fields, want 4",
		},
		{
			name: "saved row past the rescan bound",
			content: "bucket,key,size,version_id\n" + `b,dir/k"1,1024,` + "\n" +
				strings.Repeat("b,k,1024,\n", maxCanonicalCSVRecordBytes/10+1) + "/b/s0/name,2a0f,1024,0/0\n",
			want: "stage canonical input: parse canonical record 2: unexpected quote in unquoted field",
		},
		{
			// The bound cuts "/b/s0/name,2a0f,1024,0/0z" after "0/0": the cut row is not judged.
			name:    "row cut by the rescan bound",
			content: rowCutByTheRescanBound("z\n"),
			want:    "stage canonical input: parse canonical record 2: unexpected quote in unquoted field",
		},
		{
			// A final saved row whose last byte is the last byte within the bound is not judged
			// either: the end of the file is not known without another read.
			name:    "final saved row ending at the rescan bound",
			content: rowCutByTheRescanBound(""),
			want:    "stage canonical input: parse canonical record 2: unexpected quote in unquoted field",
		},
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

// The engine ends a record at a bare CR, which the canonical reader keeps in a field: a saved row
// so ended is refused whatever the other line ends, also when the canonical reader accepts the
// record.
func TestStageRefusesSavedRowsEndedByABareCR(t *testing.T) {
	const header = "bucket,key,size,version_id\n"
	const saved = "/sample/s0000000/item,2a0f,1024,0/0"
	tests := []struct {
		name   string
		rows   string
		record string
	}{
		{"single row with a final bare CR", saved + "\r", "2"},
		{"single row with a bare CR before CRLF", saved + "\r\r\n", "2"},
		{"canonical row then a final bare CR", "b,k,1024,\n" + saved + "\r", "3"},
		{"bare CR then an LF row", saved + "\rb,k,1024,\n", "2"},
		{"LF row ended by a bare CR before the saved row", "b,k,1024,\r" + saved + "\n", "3"},
		{"mixed LF and CR", "b,k,1024,\nb,k2,1024,\r" + saved + "\rb,k3,1024,\n", "4"},
		{"CRLF rows then a final bare CR", "b,k,1024,\r\n" + saved + "\r", "3"},
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

func TestStageKeepsCanonicalFilesWithCRLFAndQuotedCR(t *testing.T) {
	tests := []struct {
		name    string
		content string
		staged  string
	}{
		{
			name:    "CRLF line ends",
			content: "bucket,key,size,version_id\r\nb,k,1024,\r\nb,2a0f,1024,v\r\n",
			staged:  "bucket,key,size,version_id\nb,2a0f,1024,v\nb,k,1024,\n",
		},
		{
			name:    "CRLF without a final line end",
			content: "bucket,key,size,version_id\r\nb,k,1024,",
			staged:  "bucket,key,size,version_id\nb,k,1024,\n",
		},
		{
			name:    "quoted CR in a key",
			content: "bucket,key,size,version_id\nb,\"k\r1\",1024,\nb,\"2a0f\r\",1024,0/0\n",
			staged:  "bucket,key,size,version_id\nb,\"2a0f\r\",1024,0/0\nb,\"k\r1\",1024,\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stageThroughEveryRoute(t, test.content, func(t *testing.T, _, manifest string, err error) {
				if err != nil {
					t.Fatalf("error = %v, want the file accepted", err)
				}
				staged, readErr := os.ReadFile(manifest)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if string(staged) != test.staged {
					t.Fatalf("staged bytes = %q, want %q", staged, test.staged)
				}
			})
		})
	}
}

// A named pipe is read once: a refusal reads the bytes already read and what is left of the same
// open input, and never opens the path again, which would wait for a writer that has gone.
func TestStageReadsANamedPipeOnce(t *testing.T) {
	const headerMessage = `items file must use exact header ["bucket" "key" "size" "version_id"]`
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"wrong header", "wrong,header\nb,k,1024,\n", headerMessage},
		{"quote inside a canonical key", "bucket,key,size,version_id\n" + `b,dir/k"1,1024,` + "\n",
			"stage canonical input: parse canonical record 2: unexpected quote in unquoted field"},
		{"blank line after the header", "bucket,key,size,version_id\n\nb,k,1024,\n",
			"stage canonical input: canonical record 2 has 1 fields, want 4"},
		{"unterminated quote", `"unterminated`, "read canonical header: unterminated quoted field"},
		{"quote inside a saved object path", "bucket,key,size,version_id\n" + `/b/pre"fix/item,2a0f,1024,0/0` + "\n",
			savedItemRowMessage("2")},
		{
			// The canonical reader met the end of the stream, so the last row is complete.
			name:    "final saved row without a line end after a bare CR",
			content: "bucket,key,size,version_id\nb,k,1024,\r/b/s0/one,2a0f,1024,0/0",
			want:    savedItemRowMessage("3"),
		},
	}
	routes := map[string]func(source string) error{
		"delete": func(source string) error {
			_, err := StageDeleteInputManifest(source, 326, 0, "")
			return err
		},
		"read-verify": func(source string) error {
			_, _, _, err := StageInputManifest(source, 326)
			return err
		},
	}
	for _, test := range tests {
		for name, stage := range routes {
			t.Run(test.name+"/"+name, func(t *testing.T) {
				tempDir := t.TempDir()
				t.Setenv("TMPDIR", tempDir)
				source := filepath.Join(t.TempDir(), "input.csv")
				if err := syscall.Mkfifo(source, 0o600); err != nil {
					t.Skipf("mkfifo: %v", err)
				}
				go func() {
					writer, err := os.OpenFile(source, os.O_WRONLY, 0)
					if err != nil {
						return
					}
					_, _ = writer.WriteString(test.content)
					_ = writer.Close()
				}()
				done := make(chan error, 1)
				go func() { done <- stage(source) }()
				select {
				case err := <-done:
					if err == nil || err.Error() != test.want {
						t.Fatalf("error = %v, want %q", err, test.want)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("staging a named pipe blocked")
				}
				requireEmptyDir(t, tempDir)
			})
		}
	}
}

// The engine reads the stager's copy, not the source: a saved row ended by a bare CR is refused
// whatever the engine made of an earlier row, and refusals give the engine's record number.
func TestStageJudgesEveryBareCRRecordAndCountsAsTheEngine(t *testing.T) {
	const header = "bucket,key,size,version_id\n"
	const saved = "/sample/s0/item,2a0f,1024,0/0"
	var hundredRows strings.Builder
	for row := range 100 {
		fmt.Fprintf(&hundredRows, "b,k%d,1024,\r\r\n", row)
	}
	tests := []struct {
		name   string
		rows   string
		record string
	}{
		{"bare CR inside an earlier key", "sample,key\rwithcr,1024,\n" + saved + "\r", "4"},
		{"earlier row ending CR CRLF", "b,k,1024,v1\r\r\n" + saved + "\r\r\n", "4"},
		{"earlier row ending CR CRLF, then a final bare CR", "b,k,1024,v1\r\r\n" + saved + "\r", "4"},
		{"100 rows ending CR CRLF, then an LF row", hundredRows.String() + saved + "\n", "202"},
		{"100 rows ending CR CRLF, then a final bare CR", hundredRows.String() + saved + "\r", "202"},
		{"earlier row ending CR CRLF, then a refused row", "b,k,1024,v1\r\r\n" + `/b/pre"fix/item,2a0f,1024,0/0` + "\n", "4"},
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

// The controls for the test above: each earlier row is accepted on its own.
func TestStageKeepsEarlierBareCRRowsOnTheirOwn(t *testing.T) {
	tests := []struct {
		name   string
		rows   string
		staged string
	}{
		{"bare CR inside a key", "sample,key\rwithcr,1024,\n", "sample,\"key\rwithcr\",1024,\n"},
		{"row ending CR CRLF", "b,k,1024,v1\r\r\n", "b,k,1024,\"v1\r\"\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const header = "bucket,key,size,version_id\n"
			stageThroughEveryRoute(t, header+test.rows, func(t *testing.T, _, manifest string, err error) {
				if err != nil {
					t.Fatalf("error = %v, want the row accepted", err)
				}
				staged, readErr := os.ReadFile(manifest)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if string(staged) != header+test.staged {
					t.Fatalf("staged bytes = %q, want %q", staged, header+test.staged)
				}
			})
		})
	}
}

// A refusal on a named pipe or a pipe whose writer stays open does not wait for more input: it
// judges only what is already buffered, and otherwise keeps the canonical reader's error.
func TestStageRefusalDoesNotWaitOnAnOpenWriter(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"quote inside a canonical key", "bucket,key,size,version_id\n" + `sample,key"x,1024,` + "\n",
			"stage canonical input: parse canonical record 2: unexpected quote in unquoted field"},
		{"buffered saved row with a quote inside its path", "bucket,key,size,version_id\n" + `/b/pre"fix/item,2a0f,1024,0/0` + "\n",
			savedItemRowMessage("2")},
	}
	sources := map[string]func(t *testing.T) (path string, writer *os.File){
		"named pipe": func(t *testing.T) (string, *os.File) {
			path := filepath.Join(t.TempDir(), "input.csv")
			if err := syscall.Mkfifo(path, 0o600); err != nil {
				t.Skipf("mkfifo: %v", err)
			}
			// Read-write, so that the open does not wait for a reader and the pipe stays open.
			writer, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			return path, writer
		},
		"pipe": func(t *testing.T) (string, *os.File) {
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			return fmt.Sprintf("/proc/self/fd/%d", reader.Fd()), writer
		},
	}
	routes := map[string]func(source string) error{
		"delete": func(source string) error {
			_, err := StageDeleteInputManifest(source, 327, 0, "")
			return err
		},
		"read-verify": func(source string) error {
			_, _, _, err := StageInputManifest(source, 327)
			return err
		},
	}
	for _, test := range tests {
		for sourceName, makeSource := range sources {
			for routeName, stage := range routes {
				t.Run(test.name+"/"+sourceName+"/"+routeName, func(t *testing.T) {
					tempDir := t.TempDir()
					t.Setenv("TMPDIR", tempDir)
					source, writer := makeSource(t)
					t.Cleanup(func() { _ = writer.Close() })
					if _, err := writer.WriteString(test.content); err != nil {
						t.Fatal(err)
					}
					done := make(chan error, 1)
					go func() { done <- stage(source) }()
					select {
					case err := <-done:
						if err == nil || err.Error() != test.want {
							t.Fatalf("error = %v, want %q", err, test.want)
						}
					case <-time.After(10 * time.Second):
						_ = writer.Close() // lets the stager finish
						t.Fatal("a refusal waited for a writer that stays open")
					}
					requireEmptyDir(t, tempDir)
				})
			}
		}
	}
}

// On a regular file, a refusal reads at most stagingRescanTailBytes past what the canonical reader
// read; on any other input, nothing more.
func TestStagingRefusalReadsABoundedPartOfTheInput(t *testing.T) {
	for _, test := range []struct {
		regularFile bool
		maxExtra    int
	}{{true, maxCanonicalCSVRecordBytes}, {false, 0}} {
		t.Run(fmt.Sprintf("regular file %t", test.regularFile), func(t *testing.T) {
			input := &endlessRows{prefix: "bucket,key,size,version_id\n" + `b,dir/k"1,1024,` + "\n"}
			reader := newStagingCSVReader(input)
			check := stagingEngineCheck{regularFile: test.regularFile}
			if _, err := reader.Read(); err != nil {
				t.Fatal(err)
			}
			if _, err := reader.Read(); err == nil {
				t.Fatal("Read() error = nil, want the quote refused")
			}
			before := input.read
			if err := check.refusal(reader, 2); err != nil {
				t.Fatalf("refusal() = %v, want nil", err)
			}
			if extra := input.read - before; extra > test.maxExtra {
				t.Fatalf("refusal read %d more bytes, want at most %d", extra, test.maxExtra)
			}
		})
	}
}

// rowCutByTheRescanBound returns a file whose refusal rescan ends where a saved row's text ends,
// followed by suffix.
func rowCutByTheRescanBound(suffix string) string {
	const refused = "bucket,key,size,version_id\n" + `b,dir/k"`
	const looksSaved = "/b/s0/name,2a0f,1024,0/0"
	tail := "1,1024,\n"
	fill := stagingRescanTailBytes - len(tail) - len(looksSaved)
	rows := strings.Repeat("b,k,1024,\n", fill/10-1)
	last := fill - len(rows) - len("b,k,1,\n")
	tail += rows + "b,k" + strings.Repeat("0", last) + ",1,\n"
	return refused + tail + looksSaved + suffix
}

// endlessRows yields prefix and then canonical rows, up to 64 MiB so that an unbounded read ends,
// counting the bytes read.
type endlessRows struct {
	prefix string
	read   int
}

func (r *endlessRows) Read(buffer []byte) (int, error) {
	const end = 64 << 20
	if r.read >= end {
		return 0, io.EOF
	}
	buffer = buffer[:min(len(buffer), end-r.read)]
	for n := range buffer {
		position := r.read + n
		if position < len(r.prefix) {
			buffer[n] = r.prefix[position]
		} else {
			buffer[n] = "b,k,1024,\n"[(position-len(r.prefix))%10]
		}
	}
	r.read += len(buffer)
	return len(buffer), nil
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
