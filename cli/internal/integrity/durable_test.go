//go:build linux

package integrity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestWriteFileDurableAtomicRealFilesystemCanary(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "manifest.complete.json")
	if err := writeFileDurableAtomicWithOperations(
		destination,
		[]byte("durable-content"),
		".completion.staging-*",
		osDurablePublicationOperations{},
	); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "durable-content" {
		t.Fatalf("published content = %q", content)
	}
	info, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("published mode = %o, want 600", info.Mode().Perm())
	}
}

func TestDurableRenameNoReplacePreservesExistingDestination(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, ".manifest.staging")
	destination := filepath.Join(directory, "manifest.csv")
	if err := os.WriteFile(source, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := durableRenameNoReplace(source, destination)
	if err == nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("durableRenameNoReplace() error = %v, want existing-destination error", err)
	}
	content, readErr := os.ReadFile(destination)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(content) != "existing" {
		t.Fatalf("existing destination was replaced with %q", content)
	}
	sourceContent, readErr := os.ReadFile(source)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(sourceContent) != "new" {
		t.Fatalf("failed publication changed staging content to %q", sourceContent)
	}
}

func TestUnsupportedNoReplaceProviderErrorNamesResultsDirectoryAndContract(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "run-42", "verified.csv")
	for _, cause := range []error{unix.EINVAL, unix.ENOSYS, unix.EOPNOTSUPP} {
		err := unsupportedNoReplaceProviderError(destination, cause)
		for _, detail := range []string{
			filepath.Dir(destination), "filesystem/provider", "renameat2(RENAME_NOREPLACE)",
			"crash-durable integrity publication contract",
		} {
			if !strings.Contains(err.Error(), detail) {
				t.Errorf("diagnostic %q does not contain %q", err, detail)
			}
		}
		if !errors.Is(err, cause) {
			t.Errorf("diagnostic %q does not preserve %v", err, cause)
		}
	}
}

func TestDurableRenameNoReplaceOrMatchRecoversMatchingDestination(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, ".manifest.staging")
	destination := filepath.Join(directory, "manifest.csv")
	for _, path := range []string{source, destination} {
		if err := os.WriteFile(path, []byte("same"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := durableRenameNoReplaceOrMatch(source, destination); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatalf("matching staging source was not removed: %v", err)
	}
	content, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "same" {
		t.Fatalf("matching destination changed to %q", content)
	}
}

func TestDurableRenameNoReplaceOrMatchRejectsConflictingDestination(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, ".manifest.staging")
	destination := filepath.Join(directory, "manifest.csv")
	if err := os.WriteFile(source, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := durableRenameNoReplaceOrMatch(source, destination)
	if err == nil || !errors.Is(err, os.ErrExist) ||
		!strings.Contains(err.Error(), "differs from current derivation") {
		t.Fatalf("durableRenameNoReplaceOrMatch() error = %v, want conflict", err)
	}
	for path, expected := range map[string]string{source: "new", destination: "existing"} {
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(content) != expected {
			t.Errorf("%s content = %q, want %q", path, content, expected)
		}
	}
}

func TestDurableRenameOrdersFileSyncRenameAndDirectorySync(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, ".manifest.staging")
	destination := filepath.Join(directory, "manifest.csv")
	operations := &recordingDurableOperations{}

	if err := durableRenameWithOperations(source, destination, operations); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"sync-file:" + source,
		"rename:" + source + "->" + destination,
		"sync-directory:" + directory,
	}
	if !reflect.DeepEqual(operations.events, want) {
		t.Fatalf("durability operations = %v, want %v", operations.events, want)
	}
}

func TestDurableRenameFailuresStopAtTheUnsafeBoundary(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, ".manifest.staging")
	destination := filepath.Join(directory, "manifest.csv")
	tests := []struct {
		name       string
		failure    string
		wantEvents int
		message    string
	}{
		{name: "file sync", failure: "sync-file", wantEvents: 1, message: "synchronize publication file"},
		{name: "rename", failure: "rename", wantEvents: 2, message: "atomically publish"},
		{name: "directory sync", failure: "sync-directory", wantEvents: 3, message: "durable state is indeterminate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operations := &recordingDurableOperations{failOperation: test.failure}
			err := durableRenameWithOperations(source, destination, operations)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("error = %v, want message containing %q", err, test.message)
			}
			if len(operations.events) != test.wantEvents {
				t.Fatalf("events = %v, want %d operations", operations.events, test.wantEvents)
			}
		})
	}
}

func TestDurableRenameRejectsCrossDirectoryBeforeOperations(t *testing.T) {
	operations := &recordingDurableOperations{}
	err := durableRenameWithOperations(
		filepath.Join(t.TempDir(), ".manifest.staging"),
		filepath.Join(t.TempDir(), "manifest.csv"),
		operations,
	)
	if err == nil || !strings.Contains(err.Error(), "one directory") {
		t.Fatalf("error = %v, want same-directory rejection", err)
	}
	if len(operations.events) != 0 {
		t.Fatalf("durability operations ran before precondition validation: %v", operations.events)
	}
}

func TestStageInputManifestOrdersAndFailsClosedAtEveryCommitBarrier(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input.csv")
	if err := os.WriteFile(
		source,
		[]byte("bucket,key,size,version_id\r\nb,k,1,\r\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	for failAt := 1; failAt <= 6; failAt++ {
		t.Run(fmt.Sprintf("barrier-%d", failAt), func(t *testing.T) {
			operations := &faultingOSDurableOperations{failAt: failAt}
			_, _, _, err := stageInputManifestWithOperations(source, 41, operations)
			if err == nil {
				t.Fatalf("expected barrier %d to fail", failAt)
			}
			if len(operations.events) != failAt {
				t.Fatalf("events = %v, want %d", operations.events, failAt)
			}
			if failAt <= 3 && containsCompletionPublication(operations.events) {
				t.Fatalf("marker publication started after manifest barrier failure: %v", operations.events)
			}
			if operations.publicationDirectory != "" {
				if _, statErr := os.Stat(operations.publicationDirectory); !os.IsNotExist(statErr) {
					t.Fatalf("failed private staging directory remains: %s (%v)", operations.publicationDirectory, statErr)
				}
			}
		})
	}

	operations := &faultingOSDurableOperations{}
	dir, manifest, marker, err := stageInputManifestWithOperations(source, 41, operations)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if len(operations.events) != 6 {
		t.Fatalf("commit events = %v, want six barriers", operations.events)
	}
	if !strings.Contains(operations.events[0], "sync-file:") ||
		!strings.Contains(operations.events[1], "rename:") ||
		!strings.Contains(operations.events[2], "sync-directory:") ||
		!strings.Contains(operations.events[3], "sync-file:") ||
		!strings.Contains(operations.events[4], "rename:") ||
		!strings.Contains(operations.events[5], "sync-directory:") {
		t.Fatalf("unexpected commit ordering: %v", operations.events)
	}
	if _, err = ValidateCompletion(
		manifest,
		marker,
		41,
		"cli_stager",
		CLIStagerProducerID,
		VerifyInputName,
	); err != nil {
		t.Fatalf("durably published pair did not validate: %v", err)
	}
}

func containsCompletionPublication(events []string) bool {
	for _, event := range events {
		if strings.Contains(event, VerifyInputCompletionName) {
			return true
		}
	}
	return false
}

type recordingDurableOperations struct {
	failOperation string
	events        []string
}

type faultingOSDurableOperations struct {
	failAt               int
	events               []string
	publicationDirectory string
	base                 osDurablePublicationOperations
}

func (operations *faultingOSDurableOperations) syncFile(path string) error {
	operations.captureDirectory(path)
	if err := operations.record("sync-file:" + path); err != nil {
		return err
	}
	return operations.base.syncFile(path)
}

func (operations *faultingOSDurableOperations) rename(source, destination string) error {
	operations.captureDirectory(source)
	if err := operations.record("rename:" + source + "->" + destination); err != nil {
		return err
	}
	return operations.base.rename(source, destination)
}

func (operations *faultingOSDurableOperations) syncDirectory(path string) error {
	operations.captureDirectory(path)
	if err := operations.record("sync-directory:" + path); err != nil {
		return err
	}
	return operations.base.syncDirectory(path)
}

func (operations *faultingOSDurableOperations) record(event string) error {
	operations.events = append(operations.events, event)
	if operations.failAt == len(operations.events) {
		return fmt.Errorf("injected durability failure at barrier %d", operations.failAt)
	}
	return nil
}

func (operations *faultingOSDurableOperations) captureDirectory(path string) {
	if operations.publicationDirectory == "" {
		operations.publicationDirectory = filepath.Dir(path)
	}
}

func (operations *recordingDurableOperations) syncFile(path string) error {
	return operations.record("sync-file", "sync-file:"+path)
}

func (operations *recordingDurableOperations) rename(source, destination string) error {
	return operations.record("rename", "rename:"+source+"->"+destination)
}

func (operations *recordingDurableOperations) syncDirectory(path string) error {
	return operations.record("sync-directory", "sync-directory:"+path)
}

func (operations *recordingDurableOperations) record(operation, event string) error {
	operations.events = append(operations.events, event)
	if operations.failOperation == operation {
		return errors.New("injected " + operation + " failure")
	}
	return nil
}
