package update

import (
	"fmt"
	"os"
	"path/filepath"
)

func ResolveExecutableTarget(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return resolved, nil
}

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
	defer d.Close()
	return d.Sync()
}
