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
// opened, it falls back to os.Stat.
func StatItemsFile(path string) (os.FileInfo, error) {
	in, err := os.Open(path) // #nosec G304 -- explicit user-selected local input file
	if err != nil {
		return os.Stat(path)
	}
	defer func() { _ = in.Close() }()
	return in.Stat()
}
