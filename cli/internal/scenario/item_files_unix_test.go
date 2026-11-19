//go:build unix

package scenario

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestPrepareExternalItemFilesDoesNotOpenANamedPipe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "items.csv")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := PrepareExternalItemFiles(Params{WorkloadType: WorkloadTypeRead, ItemsFile: path})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("PrepareExternalItemFiles() error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("PrepareExternalItemFiles() blocked opening a named pipe")
	}
}
