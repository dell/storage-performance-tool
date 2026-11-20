package scenario

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareExternalItemFilesRewritesReadItemsFileAndRecordsMount(t *testing.T) {
	dir := t.TempDir()
	itemsPath := filepath.Join(dir, "items.csv")
	if err := os.WriteFile(itemsPath, []byte("obj-1\n"), 0o600); err != nil {
		t.Fatalf("write items file: %v", err)
	}

	params, err := PrepareExternalItemFiles(Params{WorkloadType: WorkloadTypeRead, ItemsFile: itemsPath})
	if err != nil {
		t.Fatalf("PrepareExternalItemFiles() error = %v", err)
	}

	if params.ItemsFile != "/spt-input/items/read-items.csv" {
		t.Fatalf("ItemsFile = %q, want container path", params.ItemsFile)
	}
	if len(params.ItemFileMounts) != 1 {
		t.Fatalf("ItemFileMounts len = %d, want 1", len(params.ItemFileMounts))
	}
	mount := params.ItemFileMounts[0]
	if mount.HostPath != itemsPath {
		t.Fatalf("mount host path = %q, want %q", mount.HostPath, itemsPath)
	}
	if mount.ContainerPath != params.ItemsFile {
		t.Fatalf("mount container path = %q, want %q", mount.ContainerPath, params.ItemsFile)
	}
}

func TestPrepareExternalItemFilesRewritesMixedItemFiles(t *testing.T) {
	dir := t.TempDir()
	readPath := filepath.Join(dir, "read.csv")
	deletePath := filepath.Join(dir, "delete.csv")
	for _, p := range []string{readPath, deletePath} {
		if err := os.WriteFile(p, []byte("obj\n"), 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	params, err := PrepareExternalItemFiles(Params{WorkloadType: WorkloadTypeMixed, ReadItemsFile: readPath, DeleteItemsFile: deletePath})
	if err != nil {
		t.Fatalf("PrepareExternalItemFiles() error = %v", err)
	}

	if params.ReadItemsFile != "/spt-input/items/mixed-read-items.csv" {
		t.Fatalf("ReadItemsFile = %q", params.ReadItemsFile)
	}
	if params.DeleteItemsFile != "/spt-input/items/mixed-delete-items.csv" {
		t.Fatalf("DeleteItemsFile = %q", params.DeleteItemsFile)
	}
	if len(params.ItemFileMounts) != 2 {
		t.Fatalf("ItemFileMounts len = %d, want 2", len(params.ItemFileMounts))
	}
}

func TestGenerateScenarioAfterPreparingExternalItemFileUsesContainerPath(t *testing.T) {
	dir := t.TempDir()
	itemsPath := filepath.Join(dir, "items.csv")
	if err := os.WriteFile(itemsPath, []byte("obj-1\n"), 0o600); err != nil {
		t.Fatalf("write items file: %v", err)
	}
	params, err := PrepareExternalItemFiles(Params{WorkloadType: WorkloadTypeRead, Bucket: "bucket", Threads: 1, ObjectSize: "1MB", Duration: "1m", ItemsFile: itemsPath})
	if err != nil {
		t.Fatalf("PrepareExternalItemFiles() error = %v", err)
	}
	scenarioJS, err := GenerateScenario(params)
	if err != nil {
		t.Fatalf("GenerateScenario() error = %v", err)
	}
	if !strings.Contains(scenarioJS, `/spt-input/items/read-items.csv`) {
		t.Fatalf("scenario missing container items path:\n%s", scenarioJS)
	}
	if strings.Contains(scenarioJS, itemsPath) {
		t.Fatalf("scenario leaked host items path %q:\n%s", itemsPath, scenarioJS)
	}
}

func TestPrepareExternalItemFilesRejectsMissingFile(t *testing.T) {
	_, err := PrepareExternalItemFiles(Params{WorkloadType: WorkloadTypeRead, ItemsFile: filepath.Join(t.TempDir(), "missing.csv")})
	if err == nil {
		t.Fatal("expected error for missing items file")
	}
	if !strings.Contains(err.Error(), "--items-file") || !strings.Contains(err.Error(), "stat") {
		t.Fatalf("unexpected error: %v", err)
	}
}

const (
	canonicalItemsHeader = "bucket,key,size,version_id\n"
	savedItemRow         = "/b/s0000000/name,2a0f5ed8eab22f7e,1024,0/0\n"
	savedItemRowRefusal  = "record 2 is a saved engine items row (as --save-items writes it: object path, " +
		"hex offset, size, layer/mask), not a canonical manifest row; the " +
		`"bucket,key,size,version_id" header does not convert it. ` +
		"Give each object as <bucket>,<key>,<size>,<version_id or empty>"
)

// mountedItemFileRoutes lists the flags whose file reaches the engine without being staged.
var mountedItemFileRoutes = []struct {
	flag   string
	params func(path string) Params
}{
	{"--items-file", func(path string) Params {
		return Params{WorkloadType: WorkloadTypeRead, ItemsFile: path}
	}},
	{"--read-items-file", func(path string) Params {
		return Params{WorkloadType: WorkloadTypeMixed, ReadItemsFile: path}
	}},
	{"--delete-items-file", func(path string) Params {
		return Params{WorkloadType: WorkloadTypeMixed, DeleteItemsFile: path}
	}},
}

func writeItemFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "items.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write items file: %v", err)
	}
	return path
}

func TestPrepareExternalItemFilesRefusesSavedItemsUnderHeaderOnMountedRoutes(t *testing.T) {
	// The check reads the file as the engine does, so rows the canonical reader refuses are
	// judged too: a quote inside an object path is literal, and a bare CR ends a record.
	tests := []struct {
		name    string
		content string
	}{
		{"LF line ends", canonicalItemsHeader + savedItemRow},
		{"quote inside the object path", canonicalItemsHeader + `/sample/pre"fix/s0000000/item,2a0f,1024,0/0` + "\n"},
		{"bare-CR line ends", "bucket,key,size,version_id\r/b/s0000000/name,2a0f5ed8eab22f7e,1024,0/0\r"},
	}
	for _, route := range mountedItemFileRoutes {
		for _, test := range tests {
			t.Run(route.flag+"/"+test.name, func(t *testing.T) {
				path := writeItemFile(t, test.content)
				_, err := PrepareExternalItemFiles(route.params(path))
				want := route.flag + ": " + savedItemRowRefusal
				if err == nil || err.Error() != want {
					t.Fatalf("PrepareExternalItemFiles() error = %v, want %q", err, want)
				}
			})
		}
	}
}

func TestPrepareExternalItemFilesKeepsOtherFilesOnMountedRoutes(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"saved items file without the header", savedItemRow},
		{"canonical manifest", canonicalItemsHeader + "b,k,1024,\n"},
		{"canonical manifest with a hex key", canonicalItemsHeader + "b,2a0f,1024,\n"},
		{"engine row after the first data row", canonicalItemsHeader + "b,k,1024,\n" + savedItemRow},
		{"header only", canonicalItemsHeader},
		{"first data row does not parse", canonicalItemsHeader + "\"unterminated\n"},
		{"canonical row with a quoted field", canonicalItemsHeader + `b,"dir/k,""1""",1024,` + "\n"},
		{"hex key with an AWS-style version", canonicalItemsHeader +
			"b,deadbeef,1024,3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo\n"},
	}
	for _, route := range mountedItemFileRoutes {
		for _, test := range tests {
			t.Run(route.flag+"/"+test.name, func(t *testing.T) {
				path := writeItemFile(t, test.content)
				params, err := PrepareExternalItemFiles(route.params(path))
				if err != nil {
					t.Fatalf("PrepareExternalItemFiles() error = %v", err)
				}
				if len(params.ItemFileMounts) != 1 || params.ItemFileMounts[0].HostPath != path {
					t.Fatalf("ItemFileMounts = %+v, want one mount of %q", params.ItemFileMounts, path)
				}
			})
		}
	}
}

func TestPrepareExternalItemFilesLeavesContainerPathsUnchecked(t *testing.T) {
	const containerPath = containerItemFilesDir + "/already-there.csv"
	params, err := PrepareExternalItemFiles(Params{WorkloadType: WorkloadTypeRead, ItemsFile: containerPath})
	if err != nil {
		t.Fatalf("PrepareExternalItemFiles() error = %v", err)
	}
	if params.ItemsFile != containerPath || len(params.ItemFileMounts) != 0 {
		t.Fatalf("ItemsFile = %q, mounts = %+v", params.ItemsFile, params.ItemFileMounts)
	}
}

func TestPrepareExternalItemFilesRefusesSavedItemsOnStagedRoutes(t *testing.T) {
	// Two object paths: each counts as a bucket, so the refusal must come before the
	// multi-bucket one, which would tell the user to pass --delete-batch-size=1.
	rows := savedItemRow + "/b/s0000000/other,2a10,1024,0/0\n"
	tests := []struct {
		name   string
		params Params
	}{
		{"delete", Params{WorkloadType: WorkloadTypeDelete, RunID: 41, DeleteBatchSize: 100}},
		{"read-verify", Params{WorkloadType: WorkloadTypeReadVerify, RunID: 41}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			params := test.params
			params.ItemsFile = writeItemFile(t, canonicalItemsHeader+rows)
			_, err := PrepareExternalItemFiles(params)
			want := "--items-file: " + savedItemRowRefusal
			if err == nil || err.Error() != want {
				t.Fatalf("PrepareExternalItemFiles() error = %v, want %q", err, want)
			}
			entries, readErr := os.ReadDir(tempDir)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("refused run left staging behind: %v", entries)
			}
		})
	}
}

// A row the engine splits at a bare CR before a saved row ended the same way: the saved row is
// still refused on the staged routes, before DELETE's multi-bucket hint.
func TestPrepareExternalItemFilesRefusesSavedItemsAfterAnEarlierBareCRRow(t *testing.T) {
	content := canonicalItemsHeader + "b,k,1024,v1\r\r\n/sample/s0000000/item,2a0f,1024,0/0\r\r\n"
	tests := []struct {
		name   string
		params Params
	}{
		{"delete at the default batch size", Params{WorkloadType: WorkloadTypeDelete, RunID: 42, DeleteBatchSize: DefaultDeleteBatchSize}},
		{"delete at batch size 1", Params{WorkloadType: WorkloadTypeDelete, RunID: 42, DeleteBatchSize: 1}},
		{"read-verify", Params{WorkloadType: WorkloadTypeReadVerify, RunID: 42}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv("TMPDIR", tempDir)
			params := test.params
			params.ItemsFile = writeItemFile(t, content)
			_, err := PrepareExternalItemFiles(params)
			want := "--items-file: " + strings.Replace(savedItemRowRefusal, "record 2 ", "record 4 ", 1)
			if err == nil || err.Error() != want {
				t.Fatalf("PrepareExternalItemFiles() error = %v, want %q", err, want)
			}
			entries, readErr := os.ReadDir(tempDir)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("refused run left staging behind: %v", entries)
			}
		})
	}
}

// DELETE reads a file without the canonical header as the items file an S3 write saved and
// records what it read; everything after parsing is the canonical selection.
func TestPrepareExternalItemFilesStagesASavedItemsFileForDelete(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	params, err := PrepareExternalItemFiles(Params{
		WorkloadType: WorkloadTypeDelete, RunID: 43, DeleteBatchSize: DefaultDeleteBatchSize,
		Bucket: "b", ObjectCount: 2,
		ItemsFile: writeItemFile(t, "/b/s0/z,2a0f,10,0/0\n/b/s0/a,2a10,20,0/0\n/b/s0/m,2a11,30,1/0f\n"),
	})
	if err != nil {
		t.Fatalf("PrepareExternalItemFiles() error = %v", err)
	}
	t.Cleanup(func() {
		for _, dir := range params.ItemStagingDirs {
			_ = os.RemoveAll(dir)
		}
	})
	if params.SelectionSourceCount != 3 || params.SelectionUniqueCount != 3 ||
		params.SelectionSelectedCount != 2 || params.SelectionOrder != SelectionOrderCanonical ||
		params.SelectionSHA256 == "" {
		t.Fatalf("selection = %+v", params)
	}
	want := &SavedItemsSource{Rows: 3, Buckets: []string{"b"}, BucketCount: 1}
	if params.DeleteSavedItems == nil || params.DeleteSavedItems.Rows != want.Rows ||
		strings.Join(params.DeleteSavedItems.Buckets, ",") != "b" || params.DeleteSavedItems.BucketCount != 1 ||
		params.DeleteSavedItems.MoreBuckets {
		t.Fatalf("DeleteSavedItems = %+v, want %+v", params.DeleteSavedItems, want)
	}
	if len(params.ItemFileMounts) != 2 {
		t.Fatalf("item mounts = %+v", params.ItemFileMounts)
	}
	staged, err := os.ReadFile(params.ItemFileMounts[0].HostPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(staged) != canonicalItemsHeader+"b,s0/a,20,\nb,s0/m,30,\n" {
		t.Fatalf("staged manifest = %q", staged)
	}
}

func TestPrepareExternalItemFilesLeavesACanonicalDeleteUnmarked(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	params, err := PrepareExternalItemFiles(Params{
		WorkloadType: WorkloadTypeDelete, RunID: 44, DeleteBatchSize: 1,
		ItemsFile: writeItemFile(t, canonicalItemsHeader+"b,k,10,\n"),
	})
	if err != nil {
		t.Fatalf("PrepareExternalItemFiles() error = %v", err)
	}
	t.Cleanup(func() {
		for _, dir := range params.ItemStagingDirs {
			_ = os.RemoveAll(dir)
		}
	})
	if params.DeleteSavedItems != nil {
		t.Fatalf("DeleteSavedItems = %+v, want nil for a canonical manifest", params.DeleteSavedItems)
	}
}

func TestPrepareExternalItemFilesRefusesAMultiBucketSavedItemsFileAtTheDefaultBatchSize(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	_, err := PrepareExternalItemFiles(Params{
		WorkloadType: WorkloadTypeDelete, RunID: 45, DeleteBatchSize: DefaultDeleteBatchSize,
		ItemsFile: writeItemFile(t, "/b1/s0/k,2a0f,10,0/0\n/b2/s0/k,2a10,10,0/0\n"),
	})
	want := "--items-file is a multi-bucket manifest; use --delete-batch-size=1"
	if err == nil || err.Error() != want {
		t.Fatalf("PrepareExternalItemFiles() error = %v, want %q", err, want)
	}
	if entries, _ := os.ReadDir(tempDir); len(entries) != 0 {
		t.Fatalf("refused run left staging behind: %v", entries)
	}
}

func TestPrepareExternalItemFilesRefusesAnUnsafeSavedItemsFileForDelete(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("TMPDIR", tempDir)
	_, err := PrepareExternalItemFiles(Params{
		WorkloadType: WorkloadTypeDelete, RunID: 46, DeleteBatchSize: 1,
		ItemsFile: writeItemFile(t, "/b/s0/k,2a0f,10,0/0\n/b/s0/k~v1,2a10,10,0/0\n"),
	})
	if err == nil || !strings.HasPrefix(err.Error(), "--items-file: items file is neither a canonical manifest") ||
		!strings.Contains(err.Error(), "line 2 has a \"~\"") {
		t.Fatalf("PrepareExternalItemFiles() error = %v, want the line 2 refusal", err)
	}
	if entries, _ := os.ReadDir(tempDir); len(entries) != 0 {
		t.Fatalf("refused run left staging behind: %v", entries)
	}
}
