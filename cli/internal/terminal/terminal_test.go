package terminal

import (
	"os"
	"path/filepath"
	"testing"
)

// A terminal can't be opened in CI, so only the non-terminal inputs a run
// from cron, systemd or a script gets are checked.
func TestIsTerminalFalseForNonTerminalInput(t *testing.T) {
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	t.Cleanup(func() { _ = devNull.Close() })

	pipeRead, pipeWrite, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() {
		_ = pipeRead.Close()
		_ = pipeWrite.Close()
	})

	regular, err := os.Create(filepath.Join(t.TempDir(), "input"))
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	t.Cleanup(func() { _ = regular.Close() })

	// Fd is read before Close: a closed *os.File reports an invalid fd, and
	// the descriptor number itself must be tested once nothing holds it.
	closed, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	closedFd := closed.Fd()
	if err := closed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for _, tc := range []struct {
		name string
		fd   uintptr
	}{
		{"dev null", devNull.Fd()},
		{"pipe", pipeRead.Fd()},
		{"regular file", regular.Fd()},
		{"closed descriptor", closedFd},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if IsTerminal(tc.fd) {
				t.Errorf("IsTerminal(%s) = true, want false", tc.name)
			}
		})
	}
}
