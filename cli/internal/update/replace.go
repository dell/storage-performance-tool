package update

import (
	"fmt"
	"os"
	"path/filepath"
)

// ResolveExecutableTarget resolves symlinks for the current executable path.
func ResolveExecutableTarget(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

// VerifyReplaceAccess checks that target is a file and its parent directory is writable.
func VerifyReplaceAccess(target string) error {
	info, err := os.Stat(target)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("target %q is a directory", target)
	}
	dir := filepath.Dir(target)
	tmp, err := os.CreateTemp(dir, ".spt-access-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	closeErr := tmp.Close()
	removeErr := os.Remove(name)
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

// ReplaceExecutable replaces target with data while preserving target permissions.
func ReplaceExecutable(target string, data []byte) error {
	info, err := os.Stat(target)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("target %q is a directory", target)
	}
	mode := info.Mode().Perm() & 0o777
	if mode == 0 {
		mode = 0o755
	}
	return WriteFileAtomic(target, data, mode)
}

// WriteFileAtomic writes data to path through a same-directory temporary file.
func WriteFileAtomic(path string, data []byte, mode os.FileMode) error {
	if mode == 0 {
		mode = 0o755
	}
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	tmp, err := os.CreateTemp(dir, "."+base+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	keepTemp := true
	defer func() {
		if keepTemp {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	keepTemp = false
	_ = syncParentDir(dir)
	return nil
}

func syncParentDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
