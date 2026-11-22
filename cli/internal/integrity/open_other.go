//go:build !unix

package integrity

import "os"

// openWithoutWaiting opens path for reading. Without unix named pipes there is no writer to wait for.
func openWithoutWaiting(path string) (*os.File, error) {
	return os.Open(path) // #nosec G304 -- explicit user-selected local input file
}

// StatItemsFile returns path's file information from an open handle where it can, so that it holds
// the file's identity. On Windows, a path-based os.Stat result does not, and os.SameFile would
// open the path again to fetch it, judging whatever file is there by then. When the path cannot be
// opened, it falls back to os.Stat, wrapped so that it never matches a file in os.SameFile: the
// check then leaves the file to the engine rather than judge one it never identified.
func StatItemsFile(path string) (os.FileInfo, error) {
	in, err := os.Open(path) // #nosec G304 -- explicit user-selected local input file
	if err != nil {
		info, statErr := os.Stat(path)
		if statErr != nil {
			return nil, statErr
		}
		return unidentifiedFileInfo{info}, nil
	}
	defer func() { _ = in.Close() }()
	return in.Stat()
}

// unidentifiedFileInfo is file information without the file's identity. os.SameFile reports false
// for it, because it is not the os package's own type.
type unidentifiedFileInfo struct {
	os.FileInfo
}
