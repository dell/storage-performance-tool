//go:build unix

package summary

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// Nothing writes to the pipe, so opening it would never return.
func TestUnresolvedOperationsSkipANamedPipe(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(dir, lifecycleFile), 0o600); err != nil {
		t.Skipf("no named pipes here: %v", err)
	}
	step := &results.StepManifest{StepID: lifecycleStep, Files: []results.FileStatus{{Name: lifecycleFile, Status: fileStatusOK}}}
	got := make(chan *OperationLifecycle, 1)
	go func() { got <- loadOperationLifecycle(dir, step) }()
	select {
	case lifecycle := <-got:
		if lifecycle != nil {
			t.Fatalf("lifecycle = %+v", lifecycle)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the summary waited on a named pipe")
	}
}
