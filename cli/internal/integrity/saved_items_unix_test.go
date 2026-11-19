//go:build unix

package integrity

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// A regular file the caller examined, replaced by a named pipe before the check opens it, is not
// waited on.
func TestRefuseSavedItemsUnderHeaderDoesNotWaitOnAPipeThatReplacedTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "items.csv")
	if err := os.WriteFile(path, []byte("bucket,key,size,version_id\nb,k,1024,\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	info := statFile(t, path)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- RefuseSavedItemsUnderHeader(path, info) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RefuseSavedItemsUnderHeader() error = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RefuseSavedItemsUnderHeader() blocked opening a named pipe")
	}
}
