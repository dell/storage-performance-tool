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
