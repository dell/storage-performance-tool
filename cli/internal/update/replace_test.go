package update

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestWriteFileAtomicCreatesOutput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spt")
	if err := WriteFileAtomic(path, []byte("new"), 0o755); err != nil {
		t.Fatalf("WriteFileAtomic returned error: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("file contents = %q, want new", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o755 {
		t.Fatalf("mode = %v, want 0755", info.Mode().Perm())
	}
}

func TestReplaceExecutablePreservesExistingMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode preservation is Unix-specific")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "spt")
	if err := os.WriteFile(path, []byte("old"), 0o750); err != nil {
		t.Fatalf("WriteFile old: %v", err)
	}
	if err := ReplaceExecutable(path, []byte("new")); err != nil {
		t.Fatalf("ReplaceExecutable returned error: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("file contents = %q, want new", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0o750 {
		t.Fatalf("mode = %v, want 0750", info.Mode().Perm())
	}
}

func TestReplaceExecutableRejectsDirectory(t *testing.T) {
	if err := ReplaceExecutable(t.TempDir(), []byte("new")); err == nil {
		t.Fatal("ReplaceExecutable accepted directory target")
	}
}

func TestResolveExecutableTargetFollowsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink behavior differs on Windows")
	}
	dir := t.TempDir()
	realPath := filepath.Join(dir, "real-spt")
	linkPath := filepath.Join(dir, "spt")
	if err := os.WriteFile(realPath, []byte("old"), 0o755); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.Symlink(realPath, linkPath); err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	got, err := ResolveExecutableTarget(linkPath)
	if err != nil {
		t.Fatalf("ResolveExecutableTarget returned error: %v", err)
	}
	if got != realPath {
		t.Fatalf("target = %q, want %q", got, realPath)
	}
}
